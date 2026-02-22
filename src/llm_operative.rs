use async_trait::async_trait;
use gbe_jobs_domain::{TaskDefinition, TaskOutcome, TaskType};
use std::sync::Arc;
use tracing::{debug, info};

use crate::llm_client::{ChatMessage, LlmClient, LlmRequest};
use crate::operative::{Operative, OperativeError};

/// Validates that an env var name contains only alphanumeric chars and underscores (S1).
fn validate_env_var_name(name: &str) -> Result<(), OperativeError> {
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(OperativeError::Execution(format!(
            "invalid env var name: {name}"
        )));
    }
    Ok(())
}

/// LLM operative: executes LLM chat completions as DAG nodes.
///
/// Params:
/// - `model` (required): model identifier, e.g. `gpt-4o-mini`
/// - `prompt` (one of prompt/messages): single user message shorthand
/// - `messages` (one of prompt/messages): JSON-encoded `[{"role":"user","content":"..."}]`
/// - `system` (optional): prepended as system message
/// - `temperature` (optional): parsed as f64
/// - `max_tokens` (optional): parsed as u32
/// - `api_url` (optional): override base URL per-task
/// - `api_key_env` (optional): env var name for API key (default: `OPENAI_API_KEY`)
pub struct LlmOperative {
    task_types: Vec<TaskType>,
    client_factory: Arc<dyn LlmClientFactory>,
}

/// Factory for constructing LLM clients, enabling per-task URL overrides
/// and test injection.
pub trait LlmClientFactory: Send + Sync {
    fn build(&self, base_url: &str, api_key: Option<String>) -> Arc<dyn LlmClient>;
}

/// Default factory using `OpenAiClient`.
pub struct OpenAiClientFactory;

impl LlmClientFactory for OpenAiClientFactory {
    fn build(&self, base_url: &str, api_key: Option<String>) -> Arc<dyn LlmClient> {
        Arc::new(crate::llm_client::OpenAiClient::new(
            base_url.to_string(),
            api_key,
        ))
    }
}

impl LlmOperative {
    #[must_use]
    pub fn new(task_types: Vec<TaskType>, client_factory: Arc<dyn LlmClientFactory>) -> Self {
        Self {
            task_types,
            client_factory,
        }
    }

    /// # Errors
    ///
    /// Returns `JobsDomainError` if any type string is invalid.
    pub fn for_types(
        types: &[&str],
        client_factory: Arc<dyn LlmClientFactory>,
    ) -> Result<Self, gbe_jobs_domain::JobsDomainError> {
        let task_types: Result<Vec<TaskType>, _> = types.iter().map(|t| TaskType::new(t)).collect();
        Ok(Self::new(task_types?, client_factory))
    }

    /// Build the default operative wired to a real OpenAI-compatible endpoint.
    #[must_use]
    pub fn with_defaults(task_types: Vec<TaskType>, default_base_url: String) -> Self {
        Self::new(
            task_types,
            Arc::new(DefaultClientFactory { default_base_url }),
        )
    }
}

/// Factory that uses a default base URL but allows per-task overrides.
struct DefaultClientFactory {
    default_base_url: String,
}

impl LlmClientFactory for DefaultClientFactory {
    fn build(&self, base_url: &str, api_key: Option<String>) -> Arc<dyn LlmClient> {
        let url = if base_url.is_empty() {
            &self.default_base_url
        } else {
            base_url
        };
        Arc::new(crate::llm_client::OpenAiClient::new(
            url.to_string(),
            api_key,
        ))
    }
}

fn parse_messages(json_str: &str) -> Result<Vec<ChatMessage>, OperativeError> {
    #[derive(serde::Deserialize)]
    struct MsgEntry {
        role: String,
        content: String,
    }
    let entries: Vec<MsgEntry> = serde_json::from_str(json_str)
        .map_err(|e| OperativeError::Execution(format!("invalid messages JSON: {e}")))?;
    Ok(entries
        .into_iter()
        .map(|m| ChatMessage {
            role: m.role,
            content: m.content,
        })
        .collect())
}

#[async_trait]
impl Operative for LlmOperative {
    fn handles(&self) -> &[TaskType] {
        &self.task_types
    }

    async fn execute(&self, task: &TaskDefinition) -> Result<TaskOutcome, OperativeError> {
        let model = task
            .params
            .entries
            .get("model")
            .ok_or_else(|| OperativeError::MissingParam("model".to_string()))?
            .clone();

        // Build messages from either `prompt` or `messages` param
        let mut messages = match (
            task.params.entries.get("prompt"),
            task.params.entries.get("messages"),
        ) {
            (Some(prompt), _) => vec![ChatMessage {
                role: "user".to_string(),
                content: prompt.clone(),
            }],
            (None, Some(msgs_json)) => parse_messages(msgs_json)?,
            (None, None) => {
                return Err(OperativeError::MissingParam(
                    "prompt or messages".to_string(),
                ));
            }
        };

        // Prepend system message if provided
        if let Some(system) = task.params.entries.get("system") {
            messages.insert(
                0,
                ChatMessage {
                    role: "system".to_string(),
                    content: system.clone(),
                },
            );
        }

        let temperature = task
            .params
            .entries
            .get("temperature")
            .map(|t| {
                t.parse::<f64>()
                    .map_err(|e| OperativeError::Execution(format!("invalid temperature: {e}")))
            })
            .transpose()?;

        let max_tokens = task
            .params
            .entries
            .get("max_tokens")
            .map(|t| {
                t.parse::<u32>()
                    .map_err(|e| OperativeError::Execution(format!("invalid max_tokens: {e}")))
            })
            .transpose()?;

        // Resolve API key from env var (S3: never store key in params/errors)
        let api_key_env = task
            .params
            .entries
            .get("api_key_env")
            .cloned()
            .unwrap_or_else(|| "OPENAI_API_KEY".to_string());
        validate_env_var_name(&api_key_env)?;

        let api_key = std::env::var(&api_key_env).ok();

        let base_url = task
            .params
            .entries
            .get("api_url")
            .cloned()
            .unwrap_or_default();

        let client = self.client_factory.build(&base_url, api_key);

        let llm_req = LlmRequest {
            model,
            messages,
            temperature,
            max_tokens,
        };

        debug!(task = %task.name, model = %llm_req.model, "executing LLM request");

        let resp = client
            .chat(&llm_req)
            .await
            .map_err(|e| OperativeError::LlmClient(e.to_string()))?;

        info!(task = %task.name, model = %resp.model, "LLM request completed");

        let mut data = serde_json::json!({
            "content": resp.content,
            "model": resp.model,
        });

        if let Some(fr) = &resp.finish_reason {
            data["finish_reason"] = serde_json::json!(fr);
        }
        if let Some(usage) = &resp.usage {
            data["usage"] = serde_json::json!({
                "prompt_tokens": usage.prompt_tokens,
                "completion_tokens": usage.completion_tokens,
                "total_tokens": usage.total_tokens,
            });
        }

        Ok(TaskOutcome::Completed {
            output: vec![resp.content],
            result_ref: None,
            data: Some(data),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm_client::{LlmClientError, LlmResponse, LlmUsage};
    use gbe_jobs_domain::TaskParams;
    use std::collections::HashMap;
    use tokio::sync::Mutex;

    /// Mock LLM client for unit testing without network calls.
    struct MockLlmClient {
        response: Mutex<Result<LlmResponse, String>>,
        last_request: Mutex<Option<LlmRequest>>,
    }

    impl MockLlmClient {
        fn succeeding(content: &str) -> Self {
            Self {
                response: Mutex::new(Ok(LlmResponse {
                    content: content.to_string(),
                    finish_reason: Some("stop".to_string()),
                    usage: Some(LlmUsage {
                        prompt_tokens: 5,
                        completion_tokens: 10,
                        total_tokens: 15,
                    }),
                    model: "test-model".to_string(),
                })),
                last_request: Mutex::new(None),
            }
        }

        fn failing(msg: &str) -> Self {
            Self {
                response: Mutex::new(Err(msg.to_string())),
                last_request: Mutex::new(None),
            }
        }
    }

    #[async_trait]
    impl LlmClient for MockLlmClient {
        async fn chat(&self, req: &LlmRequest) -> Result<LlmResponse, LlmClientError> {
            *self.last_request.lock().await = Some(req.clone());
            let guard = self.response.lock().await;
            match &*guard {
                Ok(resp) => Ok(resp.clone()),
                Err(msg) => Err(LlmClientError::Request(msg.clone())),
            }
        }
    }

    struct MockFactory {
        client: Arc<dyn LlmClient>,
    }

    impl LlmClientFactory for MockFactory {
        fn build(&self, _base_url: &str, _api_key: Option<String>) -> Arc<dyn LlmClient> {
            self.client.clone()
        }
    }

    fn llm_task(name: &str, params: Vec<(&str, &str)>) -> TaskDefinition {
        let mut tp = TaskParams::default();
        for (k, v) in params {
            tp.entries.insert(k.to_string(), v.to_string());
        }
        TaskDefinition {
            name: name.to_string(),
            task_type: TaskType::new("llm").unwrap(),
            depends_on: vec![],
            params: tp,
            input_from: HashMap::new(),
            timeout_secs: None,
            max_retries: None,
        }
    }

    fn make_operative(client: Arc<dyn LlmClient>) -> LlmOperative {
        let factory = Arc::new(MockFactory { client });
        LlmOperative::for_types(&["llm"], factory).unwrap()
    }

    #[tokio::test]
    async fn missing_model_errors() {
        let client = Arc::new(MockLlmClient::succeeding("ok"));
        let op = make_operative(client);
        let task = llm_task("no-model", vec![("prompt", "hello")]);
        let err = op.execute(&task).await.unwrap_err();
        assert!(matches!(err, OperativeError::MissingParam(ref p) if p == "model"));
    }

    #[tokio::test]
    async fn missing_prompt_and_messages_errors() {
        let client = Arc::new(MockLlmClient::succeeding("ok"));
        let op = make_operative(client);
        let task = llm_task("no-prompt", vec![("model", "gpt-4o-mini")]);
        let err = op.execute(&task).await.unwrap_err();
        assert!(matches!(err, OperativeError::MissingParam(ref p) if p == "prompt or messages"));
    }

    #[tokio::test]
    async fn invalid_messages_json_errors() {
        let client = Arc::new(MockLlmClient::succeeding("ok"));
        let op = make_operative(client);
        let task = llm_task(
            "bad-json",
            vec![("model", "gpt-4o-mini"), ("messages", "not json")],
        );
        let err = op.execute(&task).await.unwrap_err();
        assert!(err.to_string().contains("invalid messages JSON"));
    }

    #[tokio::test]
    async fn prompt_maps_to_user_message() {
        let mock = Arc::new(MockLlmClient::succeeding("response"));
        let op = make_operative(mock.clone());
        let task = llm_task(
            "prompt-test",
            vec![("model", "gpt-4o-mini"), ("prompt", "What is 2+2?")],
        );

        op.execute(&task).await.unwrap();

        let last = mock.last_request.lock().await;
        let req = last.as_ref().unwrap();
        assert_eq!(req.messages.len(), 1);
        assert_eq!(req.messages[0].role, "user");
        assert_eq!(req.messages[0].content, "What is 2+2?");
    }

    #[tokio::test]
    async fn messages_param_parsed_correctly() {
        let mock = Arc::new(MockLlmClient::succeeding("response"));
        let op = make_operative(mock.clone());
        let msgs = r#"[{"role":"user","content":"hi"},{"role":"assistant","content":"hello"},{"role":"user","content":"bye"}]"#;
        let task = llm_task(
            "msgs-test",
            vec![("model", "gpt-4o-mini"), ("messages", msgs)],
        );

        op.execute(&task).await.unwrap();

        let last = mock.last_request.lock().await;
        let req = last.as_ref().unwrap();
        assert_eq!(req.messages.len(), 3);
        assert_eq!(req.messages[2].content, "bye");
    }

    #[tokio::test]
    async fn system_prepended_to_messages() {
        let mock = Arc::new(MockLlmClient::succeeding("response"));
        let op = make_operative(mock.clone());
        let task = llm_task(
            "system-test",
            vec![
                ("model", "gpt-4o-mini"),
                ("prompt", "Hello"),
                ("system", "You are a helpful assistant."),
            ],
        );

        op.execute(&task).await.unwrap();

        let last = mock.last_request.lock().await;
        let req = last.as_ref().unwrap();
        assert_eq!(req.messages.len(), 2);
        assert_eq!(req.messages[0].role, "system");
        assert_eq!(req.messages[0].content, "You are a helpful assistant.");
        assert_eq!(req.messages[1].role, "user");
    }

    #[tokio::test]
    async fn response_mapped_to_outcome() {
        let client = Arc::new(MockLlmClient::succeeding("The answer is 4."));
        let op = make_operative(client);
        let task = llm_task(
            "response-test",
            vec![("model", "gpt-4o-mini"), ("prompt", "2+2?")],
        );

        let outcome = op.execute(&task).await.unwrap();
        match outcome {
            TaskOutcome::Completed { output, data, .. } => {
                assert_eq!(output, vec!["The answer is 4."]);
                let data = data.unwrap();
                assert_eq!(data["content"], "The answer is 4.");
                assert_eq!(data["model"], "test-model");
                assert_eq!(data["finish_reason"], "stop");
                assert_eq!(data["usage"]["prompt_tokens"], 5);
                assert_eq!(data["usage"]["completion_tokens"], 10);
                assert_eq!(data["usage"]["total_tokens"], 15);
            }
            TaskOutcome::Failed { .. } => panic!("expected Completed"),
        }
    }

    #[tokio::test]
    async fn temperature_and_max_tokens_forwarded() {
        let mock = Arc::new(MockLlmClient::succeeding("ok"));
        let op = make_operative(mock.clone());
        let task = llm_task(
            "params-test",
            vec![
                ("model", "gpt-4o-mini"),
                ("prompt", "hi"),
                ("temperature", "0.7"),
                ("max_tokens", "100"),
            ],
        );

        op.execute(&task).await.unwrap();

        let last = mock.last_request.lock().await;
        let req = last.as_ref().unwrap();
        assert!((req.temperature.unwrap() - 0.7).abs() < f64::EPSILON);
        assert_eq!(req.max_tokens.unwrap(), 100);
    }

    #[tokio::test]
    async fn invalid_temperature_errors() {
        let client = Arc::new(MockLlmClient::succeeding("ok"));
        let op = make_operative(client);
        let task = llm_task(
            "bad-temp",
            vec![
                ("model", "gpt-4o-mini"),
                ("prompt", "hi"),
                ("temperature", "not-a-number"),
            ],
        );
        let err = op.execute(&task).await.unwrap_err();
        assert!(err.to_string().contains("invalid temperature"));
    }

    #[tokio::test]
    async fn invalid_max_tokens_errors() {
        let client = Arc::new(MockLlmClient::succeeding("ok"));
        let op = make_operative(client);
        let task = llm_task(
            "bad-tokens",
            vec![
                ("model", "gpt-4o-mini"),
                ("prompt", "hi"),
                ("max_tokens", "-5"),
            ],
        );
        let err = op.execute(&task).await.unwrap_err();
        assert!(err.to_string().contains("invalid max_tokens"));
    }

    #[tokio::test]
    async fn invalid_api_key_env_name_rejected() {
        let client = Arc::new(MockLlmClient::succeeding("ok"));
        let op = make_operative(client);
        let task = llm_task(
            "bad-env",
            vec![
                ("model", "gpt-4o-mini"),
                ("prompt", "hi"),
                ("api_key_env", "MY-KEY!"),
            ],
        );
        let err = op.execute(&task).await.unwrap_err();
        assert!(err.to_string().contains("invalid env var name"));
    }

    #[tokio::test]
    async fn llm_client_error_maps_to_operative_error() {
        let client = Arc::new(MockLlmClient::failing("connection refused"));
        let op = make_operative(client);
        let task = llm_task(
            "fail-test",
            vec![("model", "gpt-4o-mini"), ("prompt", "hi")],
        );
        let err = op.execute(&task).await.unwrap_err();
        assert!(matches!(err, OperativeError::LlmClient(_)));
        assert!(err.to_string().contains("connection refused"));
    }
}
