use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;

/// A single message in an LLM chat conversation.
#[derive(Debug, Clone)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

/// Request to an LLM chat endpoint.
#[derive(Debug, Clone)]
pub struct LlmRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u32>,
}

/// Successful response from an LLM.
#[derive(Debug, Clone)]
pub struct LlmResponse {
    pub content: String,
    pub finish_reason: Option<String>,
    pub usage: Option<LlmUsage>,
    pub model: String,
}

/// Token usage from the LLM response.
#[derive(Debug, Clone)]
pub struct LlmUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

/// Errors from the LLM client.
#[derive(Debug, thiserror::Error)]
pub enum LlmClientError {
    #[error("request failed: {0}")]
    Request(String),

    #[error("response too large (>{max_bytes} bytes)")]
    ResponseTooLarge { max_bytes: usize },

    #[error("invalid response: {0}")]
    InvalidResponse(String),

    #[error("API error {status}: {body}")]
    ApiError { status: u16, body: String },
}

/// Abstraction over LLM backends for testability.
#[async_trait]
pub trait LlmClient: Send + Sync {
    async fn chat(&self, req: &LlmRequest) -> Result<LlmResponse, LlmClientError>;
}

// --- OpenAI-compatible concrete implementation ---

const MAX_RESPONSE_BYTES: usize = 1_048_576; // 1 MiB
const DEFAULT_TIMEOUT_SECS: u64 = 120;

/// Concrete deserialization structs for the OpenAI chat completions API.
/// Using typed structs instead of `serde_json::Value` per GUIDE_SECURITY S2.
#[derive(Deserialize)]
struct ApiResponse {
    choices: Vec<ApiChoice>,
    usage: Option<ApiUsage>,
    model: String,
}

#[derive(Deserialize)]
struct ApiChoice {
    message: ApiMessage,
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct ApiMessage {
    content: Option<String>,
}

#[derive(Deserialize)]
struct ApiUsage {
    prompt_tokens: u32,
    completion_tokens: u32,
    total_tokens: u32,
}

/// OpenAI-compatible LLM client using reqwest.
pub struct OpenAiClient {
    client: Client,
    base_url: String,
    api_key: Option<String>,
}

impl OpenAiClient {
    #[must_use]
    pub fn new(base_url: String, api_key: Option<String>) -> Self {
        Self {
            client: Client::new(),
            base_url,
            api_key,
        }
    }
}

#[async_trait]
impl LlmClient for OpenAiClient {
    async fn chat(&self, req: &LlmRequest) -> Result<LlmResponse, LlmClientError> {
        let url = format!(
            "{}/v1/chat/completions",
            self.base_url.trim_end_matches('/')
        );

        let messages: Vec<serde_json::Value> = req
            .messages
            .iter()
            .map(|m| {
                serde_json::json!({
                    "role": m.role,
                    "content": m.content,
                })
            })
            .collect();

        let mut body = serde_json::json!({
            "model": req.model,
            "messages": messages,
        });

        if let Some(t) = req.temperature {
            body["temperature"] = serde_json::json!(t);
        }
        if let Some(mt) = req.max_tokens {
            body["max_tokens"] = serde_json::json!(mt);
        }

        let mut http_req = self
            .client
            .post(&url)
            .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .header("content-type", "application/json");

        if let Some(key) = &self.api_key {
            http_req = http_req.header("authorization", format!("Bearer {key}"));
        }

        let response = http_req
            .json(&body)
            .send()
            .await
            .map_err(|e| LlmClientError::Request(e.to_string()))?;

        let status = response.status().as_u16();

        // Enforce 1 MiB size limit before deserialization (S2)
        if let Some(len) = response.content_length() {
            if len as usize > MAX_RESPONSE_BYTES {
                return Err(LlmClientError::ResponseTooLarge {
                    max_bytes: MAX_RESPONSE_BYTES,
                });
            }
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| LlmClientError::Request(e.to_string()))?;

        if bytes.len() > MAX_RESPONSE_BYTES {
            return Err(LlmClientError::ResponseTooLarge {
                max_bytes: MAX_RESPONSE_BYTES,
            });
        }

        if !(200..300).contains(&status) {
            let body_text = String::from_utf8_lossy(&bytes).to_string();
            return Err(LlmClientError::ApiError {
                status,
                body: body_text,
            });
        }

        let api_resp: ApiResponse = serde_json::from_slice(&bytes)
            .map_err(|e| LlmClientError::InvalidResponse(e.to_string()))?;

        let choice = api_resp
            .choices
            .first()
            .ok_or_else(|| LlmClientError::InvalidResponse("no choices in response".to_string()))?;

        let content = choice.message.content.clone().unwrap_or_default();

        let usage = api_resp.usage.map(|u| LlmUsage {
            prompt_tokens: u.prompt_tokens,
            completion_tokens: u.completion_tokens,
            total_tokens: u.total_tokens,
        });

        Ok(LlmResponse {
            content,
            finish_reason: choice.finish_reason.clone(),
            usage,
            model: api_resp.model,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn openai_response(content: &str) -> serde_json::Value {
        serde_json::json!({
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "model": "gpt-4o-mini",
            "choices": [{
                "index": 0,
                "message": { "role": "assistant", "content": content },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 20,
                "total_tokens": 30
            }
        })
    }

    fn sample_request() -> LlmRequest {
        LlmRequest {
            model: "gpt-4o-mini".to_string(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: "Hello".to_string(),
            }],
            temperature: None,
            max_tokens: None,
        }
    }

    #[tokio::test]
    async fn successful_chat_returns_content() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .and(header("content-type", "application/json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("Hello back!")))
            .mount(&server)
            .await;

        let client = OpenAiClient::new(server.uri(), None);
        let resp = client.chat(&sample_request()).await.unwrap();

        assert_eq!(resp.content, "Hello back!");
        assert_eq!(resp.finish_reason.as_deref(), Some("stop"));
        assert_eq!(resp.model, "gpt-4o-mini");
        let usage = resp.usage.unwrap();
        assert_eq!(usage.prompt_tokens, 10);
        assert_eq!(usage.completion_tokens, 20);
        assert_eq!(usage.total_tokens, 30);
    }

    #[tokio::test]
    async fn api_key_sent_as_bearer() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .and(header("authorization", "Bearer sk-test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("ok")))
            .mount(&server)
            .await;

        let client = OpenAiClient::new(server.uri(), Some("sk-test-key".to_string()));
        let resp = client.chat(&sample_request()).await.unwrap();
        assert_eq!(resp.content, "ok");
    }

    #[tokio::test]
    async fn no_api_key_for_ollama_path() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(openai_response("local llm")))
            .mount(&server)
            .await;

        let client = OpenAiClient::new(server.uri(), None);
        let resp = client.chat(&sample_request()).await.unwrap();
        assert_eq!(resp.content, "local llm");
    }

    #[tokio::test]
    async fn api_error_429_returns_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
            .mount(&server)
            .await;

        let client = OpenAiClient::new(server.uri(), None);
        let err = client.chat(&sample_request()).await.unwrap_err();
        match err {
            LlmClientError::ApiError { status, body } => {
                assert_eq!(status, 429);
                assert_eq!(body, "rate limited");
            }
            other => panic!("expected ApiError, got: {other}"),
        }
    }

    #[tokio::test]
    async fn api_error_500_returns_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(500).set_body_string("internal server error"))
            .mount(&server)
            .await;

        let client = OpenAiClient::new(server.uri(), None);
        let err = client.chat(&sample_request()).await.unwrap_err();
        match err {
            LlmClientError::ApiError { status, .. } => assert_eq!(status, 500),
            other => panic!("expected ApiError, got: {other}"),
        }
    }

    #[tokio::test]
    async fn response_size_limit_enforced() {
        let server = MockServer::start().await;
        // 2 MiB body exceeds 1 MiB limit
        let huge_body = "x".repeat(2 * 1024 * 1024);
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_string(huge_body))
            .mount(&server)
            .await;

        let client = OpenAiClient::new(server.uri(), None);
        let err = client.chat(&sample_request()).await.unwrap_err();
        assert!(
            matches!(err, LlmClientError::ResponseTooLarge { .. }),
            "got: {err}"
        );
    }
}
