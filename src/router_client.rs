use crate::error::RunnerError;
use gbe_protocol::{ControlMessage, ToolInfo};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

/// Async client for the gbe-router control channel.
///
/// Newline-delimited JSON over a Unix socket, mirroring the sync
/// `RouterConnection` in gbe-envoy/adapter but using tokio.
pub struct RouterClient {
    reader: BufReader<tokio::net::unix::OwnedReadHalf>,
    writer: tokio::net::unix::OwnedWriteHalf,
}

impl RouterClient {
    pub async fn connect(socket_path: &str) -> Result<Self, RunnerError> {
        let stream = UnixStream::connect(socket_path).await?;
        let (read_half, write_half) = stream.into_split();
        Ok(Self {
            reader: BufReader::new(read_half),
            writer: write_half,
        })
    }

    pub async fn send(&mut self, msg: &ControlMessage) -> Result<(), RunnerError> {
        let mut json = serde_json::to_string(msg)
            .map_err(gbe_protocol::ProtocolError::Json)?;
        json.push('\n');
        self.writer.write_all(json.as_bytes()).await?;
        self.writer.flush().await?;
        Ok(())
    }

    pub async fn recv(&mut self) -> Result<ControlMessage, RunnerError> {
        let mut line = String::new();
        self.reader.read_line(&mut line).await?;
        let msg = serde_json::from_str(line.trim())
            .map_err(gbe_protocol::ProtocolError::Json)?;
        Ok(msg)
    }

    /// Query all connected tools via the router.
    pub async fn query_tools(&mut self) -> Result<Vec<ToolInfo>, RunnerError> {
        self.send(&ControlMessage::QueryTools).await?;
        match self.recv().await? {
            ControlMessage::ToolsResponse { tools } => Ok(tools),
            ControlMessage::Error { code, message } => {
                Err(RunnerError::Router(format!("{code}: {message}")))
            }
            other => Err(RunnerError::Router(format!(
                "unexpected response to QueryTools: {other:?}"
            ))),
        }
    }
}
