use anyhow::Result;

use wasmq_config::{Config, transport::TransportConfig};
use wasmq_ipc::protocol::ProcessType;
use wasmq_ipc::transport::{Transport, unix_socket::UnixSocketTransport};

// TODO: Instead of accessing the Transport directly, we should only access the `IpcService`
pub async fn make_transport(
    config: Config,
    process_type: ProcessType,
) -> Result<Box<dyn Transport>> {
    match config.transport {
        TransportConfig::UnixSocket { base_path } => Ok(Box::new(
            UnixSocketTransport::new(base_path, process_type).await?,
        )),
    }
}
