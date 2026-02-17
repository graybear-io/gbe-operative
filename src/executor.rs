use crate::config::RunnerConfig;
use crate::error::RunnerError;
use crate::router_client::RouterClient;
use async_trait::async_trait;
use gbe_jobs_domain::{OrgId, TaskDefinition};
use gbe_protocol::{ControlMessage, DataFrame, ToolId};
use std::process::Stdio;
use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;
use tokio::process::Command;
use tracing::{debug, info, warn};

/// Result of executing a single task.
#[derive(Debug, Clone)]
pub struct TaskResult {
    pub exit_code: i32,
    pub output: Vec<String>,
}

/// Context passed to the executor for each task invocation.
#[derive(Debug, Clone)]
pub struct TaskContext {
    pub org_id: OrgId,
    pub date: String,
    pub router_socket: String,
}

/// Trait for executing a single task. Implementations handle the mechanics
/// of running the underlying command and capturing output.
#[async_trait]
pub trait TaskExecutor: Send + Sync {
    async fn execute(
        &self,
        task: &TaskDefinition,
        ctx: &TaskContext,
    ) -> Result<TaskResult, RunnerError>;
}

/// Production executor: spawns a gbe-adapter wrapping a Python command,
/// discovers the tool via the router, subscribes to its data stream,
/// and captures output until the adapter exits.
pub struct EnvoyExecutor {
    config: RunnerConfig,
}

impl EnvoyExecutor {
    pub fn new(config: RunnerConfig) -> Self {
        Self { config }
    }

    /// Build the command string from task definition.
    /// Convention: `python -m gbe_tools.{task_type} --key value --org {org} --date {date}`
    fn build_command_args(task: &TaskDefinition, ctx: &TaskContext) -> Vec<String> {
        let module = format!("gbe_tools.{}", task.task_type.as_str().replace('-', "_"));
        let mut args = vec![
            "python".to_string(),
            "-m".to_string(),
            module,
        ];
        for (key, value) in &task.params.entries {
            args.push(format!("--{key}"));
            args.push(value.clone());
        }
        args.push("--org".to_string());
        args.push(ctx.org_id.as_str().to_string());
        args.push("--date".to_string());
        args.push(ctx.date.clone());
        args
    }
}

#[async_trait]
impl TaskExecutor for EnvoyExecutor {
    async fn execute(
        &self,
        task: &TaskDefinition,
        ctx: &TaskContext,
    ) -> Result<TaskResult, RunnerError> {
        // 1. Connect and register with router
        let mut client = RouterClient::connect(&ctx.router_socket).await?;
        let runner_id = client.handshake().await?;
        debug!(runner_id = %runner_id, "registered with router");

        // 2. Snapshot existing tools
        let before: Vec<ToolId> = client
            .query_tools()
            .await?
            .into_iter()
            .map(|t| t.tool_id)
            .collect();

        // 3. Build command and spawn adapter
        let cmd_args = Self::build_command_args(task, ctx);
        debug!(task = %task.name, cmd = ?cmd_args, "spawning adapter");

        let mut child = Command::new(&self.config.adapter_bin)
            .arg("--router")
            .arg(&ctx.router_socket)
            .arg("--")
            .args(&cmd_args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| RunnerError::Other(format!("spawn adapter: {e}")))?;

        // 4. Poll for new ToolId
        let new_tool_id = discover_new_tool(
            &mut client,
            &before,
            self.config.discovery_timeout,
            self.config.discovery_interval,
        )
        .await?;

        info!(task = %task.name, tool_id = %new_tool_id, "discovered adapter tool");

        // 5. Subscribe to the tool's data stream
        client
            .send(&ControlMessage::Subscribe {
                target: new_tool_id.clone(),
            })
            .await?;

        let data_address = match client.recv().await? {
            ControlMessage::SubscribeAck {
                data_connect_address,
                ..
            } => data_connect_address,
            ControlMessage::Error { code, message } => {
                return Err(RunnerError::Router(format!(
                    "subscribe failed: {code}: {message}"
                )));
            }
            other => {
                return Err(RunnerError::Router(format!(
                    "unexpected subscribe response: {other:?}"
                )));
            }
        };

        // 6. Read data frames until EOF
        let output = read_data_stream(&data_address).await?;

        // 7. Wait for adapter exit code
        let status = child.wait().await?;
        let exit_code = status.code().unwrap_or(1);

        info!(task = %task.name, exit_code, "adapter exited");

        Ok(TaskResult { exit_code, output })
    }
}

/// Poll `query_tools` until a new ToolId appears that wasn't in `before`.
async fn discover_new_tool(
    client: &mut RouterClient,
    before: &[ToolId],
    timeout: std::time::Duration,
    interval: std::time::Duration,
) -> Result<ToolId, RunnerError> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(RunnerError::Timeout(
                "tool discovery timed out".to_string(),
            ));
        }
        let current = client.query_tools().await?;
        for tool in &current {
            if !before.contains(&tool.tool_id) {
                return Ok(tool.tool_id.clone());
            }
        }
        tokio::time::sleep(interval).await;
    }
}

/// Connect to the data socket and read all DataFrames, returning lines.
async fn read_data_stream(address: &str) -> Result<Vec<String>, RunnerError> {
    let path = address
        .strip_prefix("unix://")
        .unwrap_or(address);
    let mut stream = UnixStream::connect(path).await?;
    let mut output = Vec::new();
    let mut buf = Vec::new();

    // Read all available data
    stream.read_to_end(&mut buf).await?;

    // Parse data frames from the buffer
    let mut cursor = std::io::Cursor::new(&buf);
    while let Ok(frame) = DataFrame::read_from(&mut cursor) {
        if let Ok(line) = String::from_utf8(frame.payload) {
            output.push(line);
        } else {
            warn!("non-utf8 data frame, skipping");
        }
    }

    Ok(output)
}

/// Mock executor for testing. Returns configurable results per task name.
#[cfg(test)]
pub mod mock {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    pub struct MockExecutor {
        results: Mutex<HashMap<String, TaskResult>>,
        default_result: TaskResult,
    }

    impl MockExecutor {
        pub fn new() -> Self {
            Self {
                results: Mutex::new(HashMap::new()),
                default_result: TaskResult {
                    exit_code: 0,
                    output: vec![],
                },
            }
        }

        pub fn set_result(&self, task_name: &str, result: TaskResult) {
            self.results
                .lock()
                .unwrap()
                .insert(task_name.to_string(), result);
        }
    }

    #[async_trait]
    impl TaskExecutor for MockExecutor {
        async fn execute(
            &self,
            task: &TaskDefinition,
            _ctx: &TaskContext,
        ) -> Result<TaskResult, RunnerError> {
            let results = self.results.lock().unwrap();
            Ok(results
                .get(&task.name)
                .cloned()
                .unwrap_or_else(|| self.default_result.clone()))
        }
    }
}
