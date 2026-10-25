use std::collections::HashMap;
use std::fs::remove_file;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use tokio::fs::create_dir_all;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::oneshot::{Sender, channel};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tracing::debug;
use ulid::Ulid;

use crate::protocol::{Message, ProcessType};
use crate::transport::Transport;

pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
pub const UNIX_SOCKET_CONNECTION_RETRIES: usize = 3;

/// A Maximum Message Size of 10MB
pub const MAX_MESSAGE_SIZE: usize = 10_000_000;

pub struct UnixSocketTransport {
    process_type: ProcessType,
    socket_path: PathBuf,
    base_path: PathBuf,
    message_rx: Mutex<UnboundedReceiver<Message>>,
    message_tx: UnboundedSender<Message>,
    /// Registry for pending responses
    pending_responses: Arc<Mutex<HashMap<Ulid, Sender<Message>>>>,
    listener_handle: Option<JoinHandle<()>>,
}

impl UnixSocketTransport {
    pub async fn new(base_path: PathBuf, process_type: ProcessType) -> Result<Self> {
        create_dir_all(&base_path).await?;

        let socket_path = Self::socket_path_for_process(&base_path, &process_type);

        if socket_path.exists() {
            tokio::fs::remove_file(&socket_path).await?;
        }

        let (message_tx, message_rx) = unbounded_channel();
        let message_rx = Mutex::new(message_rx);

        let mut transport = Self {
            process_type,
            socket_path,
            base_path,
            message_rx,
            message_tx,
            pending_responses: Arc::new(Mutex::new(HashMap::new())),
            listener_handle: None,
        };

        transport.listen().await?;

        Ok(transport)
    }

    fn socket_path_for_process(base_path: &Path, process_type: &ProcessType) -> PathBuf {
        let filename = match process_type {
            ProcessType::Hub => "hub.sock".to_string(),
            ProcessType::Storage => "storage.sock".to_string(),
            ProcessType::Scheduler => "scheduler.sock".to_string(),
            ProcessType::Executor(id) => format!("executor_{}.sock", id),
        };

        base_path.join(filename)
    }

    async fn listen(&mut self) -> Result<()> {
        let listener = UnixListener::bind(&self.socket_path)?;
        let message_tx = self.message_tx.clone();
        let pending_responses = self.pending_responses.clone();
        let process_type = self.process_type;

        let handle = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _addr)) => {
                        let tx = message_tx.clone();
                        let pending = Arc::clone(&pending_responses);

                        tokio::spawn(async move {
                            if let Err(e) = Self::handle_connection(stream, tx, pending).await {
                                eprintln!("Connection handling error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        eprintln!("Accept error for {:?}: {}", process_type, e);
                    }
                }
            }
        });

        self.listener_handle = Some(handle);

        Ok(())
    }

    /// Handles an incoming `UnixStream` connection.
    ///
    /// # Messages
    ///
    /// Messages are read in parts, the first 4 bytes (little-endian u32)
    /// represent message's length if message exceeds [`MAX_MESSAGE_SIZE`]
    /// then error is returned.
    ///
    /// # Handling
    ///
    /// Once message is deserialized, the payload is checked to be a
    /// response to a pending request, if so the message is replied
    /// to the sender. Otherwise is treated as "new message" and is
    /// forwarded to "message channel"
    async fn handle_connection(
        mut stream: UnixStream,
        tx: UnboundedSender<Message>,
        pending: Arc<Mutex<HashMap<Ulid, Sender<Message>>>>,
    ) -> Result<()> {
        // Message length (4 bytes, little-endian u32)
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let len = u32::from_le_bytes(len_buf) as usize;

        if len > MAX_MESSAGE_SIZE {
            bail!("Message too large: {} bytes", len);
        }

        // Message payload
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await?;

        let message: Message = serde_json::from_slice(&buf)?;

        if let Some(reply_to) = message.reply_to {
            let mut pending_map = pending.lock().await;
            if let Some(sender) = pending_map.remove(&reply_to) {
                // This is a response - send it to the waiting request
                let _ = sender.send(message);
                return Ok(());
            }
        }

        tx.send(message)?;

        Ok(())
    }

    /// Perform actual message send by performing a serialization of the
    /// message.
    ///
    /// Message is sent in parts:
    ///
    /// 1. Message length
    /// 2. Message payload
    ///
    /// Finally frees the connection
    async fn send_message_internal(&self, msg: &Message) -> Result<()> {
        let target_socket = Self::socket_path_for_process(&self.base_path, &msg.to);
        let mut stream =
            Self::connect_with_retry(&target_socket, UNIX_SOCKET_CONNECTION_RETRIES).await?;
        let serialized = serde_json::to_vec(msg)?;
        let len = (serialized.len() as u32).to_le_bytes();

        stream.write_all(&len).await?;
        stream.write_all(&serialized).await?;
        stream.flush().await?;
        stream.shutdown().await?;

        Ok(())
    }

    /// Performs connection to the `UnixStream` in `socket_path` with `retries` times.
    /// It uses an exponential backoff when retrying so retries are not abusive.
    async fn connect_with_retry(socket_path: &Path, retries: usize) -> Result<UnixStream> {
        let mut last_error = None;

        for attempt in 0..retries {
            match UnixStream::connect(socket_path).await {
                Ok(stream) => return Ok(stream),
                Err(e) => {
                    last_error = Some(e);
                    if attempt < retries - 1 {
                        debug!(
                            "Failed to connect to {:?} (attempt {}/{}): {}. Retrying...",
                            socket_path,
                            attempt + 1,
                            retries,
                            last_error.as_ref().unwrap()
                        );
                        let delay = Duration::from_millis(10 * (1 << attempt));
                        sleep(delay).await;
                    }
                }
            }
        }

        Err(anyhow!(
            "Failed to connect to {:?} after {} retries: {}",
            socket_path,
            retries,
            last_error.unwrap()
        ))
    }
}

#[async_trait]
impl Transport for UnixSocketTransport {
    async fn send(&self, msg: Message) -> Result<()> {
        self.send_message_internal(&msg).await
    }

    async fn recv(&self) -> Result<Message> {
        let mut rx = self.message_rx.lock().await;
        rx.recv()
            .await
            .ok_or_else(|| anyhow!("Message channel closed"))
    }

    /// Creates a Request to another `UnixSocket`.
    ///
    /// Every request is stored in the `pending_responses` registry
    /// allowing to keep track similar to _ticket_ system.
    async fn request(&self, msg: Message) -> Result<Message> {
        let (tx, rx) = channel();

        {
            let mut pending = self.pending_responses.lock().await;
            pending.insert(msg.id, tx);
        }

        self.send_message_internal(&msg).await?;
        let response = timeout(REQUEST_TIMEOUT, rx).await;

        match response {
            Ok(Ok(msg)) => Ok(msg),
            Ok(Err(_)) => {
                // Clean up pending request
                let mut pending = self.pending_responses.lock().await;
                pending.remove(&msg.id);
                Err(anyhow!("Response channel closed"))
            }
            Err(_) => {
                // Timeout - clean up pending request
                let mut pending = self.pending_responses.lock().await;
                pending.remove(&msg.id);
                Err(anyhow!("Request timeout after {:?}", REQUEST_TIMEOUT))
            }
        }
    }

    /// Performs `UnixSocket` clean up and socket close.
    ///
    /// 1. Aborts the listener task
    /// 2. Removes the Socket file (`.sock`)
    /// 3. Clears pending responses
    async fn close(&self) -> Result<()> {
        if let Some(ref handle) = self.listener_handle {
            handle.abort();
        }

        if self.socket_path.exists() {
            tokio::fs::remove_file(&self.socket_path).await?;
        }

        {
            let mut pending = self.pending_responses.lock().await;
            pending.clear();
        }

        Ok(())
    }
}

impl Drop for UnixSocketTransport {
    fn drop(&mut self) {
        if self.socket_path.exists() {
            let _ = remove_file(&self.socket_path);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tempfile::TempDir;
    use tokio::time::sleep;

    use crate::protocol::MessagePayload;

    use super::*;

    #[tokio::test]
    async fn unix_socket_send_recv() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let base_path = temp_dir.path().to_path_buf();

        // Create two transports
        let transport1 = UnixSocketTransport::new(base_path.clone(), ProcessType::Hub).await?;

        let transport2 = UnixSocketTransport::new(base_path.clone(), ProcessType::Storage).await?;

        // Give listeners time to start
        sleep(Duration::from_millis(100)).await;

        // Send message from Hub to Storage
        let msg = Message {
            id: Ulid::generate(),
            from: ProcessType::Hub,
            to: ProcessType::Storage,
            payload: MessagePayload::Ping,
            reply_to: None,
        };

        transport1.send(msg.clone()).await?;

        // Receive message on Storage
        let received = transport2.recv().await?;

        assert_eq!(received.id, msg.id);
        assert_eq!(received.from, ProcessType::Hub);
        assert_eq!(received.to, ProcessType::Storage);

        Ok(())
    }

    #[tokio::test]
    async fn unix_socket_request_response() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let base_path = temp_dir.path().to_path_buf();

        let transport1 = UnixSocketTransport::new(base_path.clone(), ProcessType::Hub).await?;
        let transport2 = UnixSocketTransport::new(base_path.clone(), ProcessType::Storage).await?;

        sleep(Duration::from_millis(100)).await;

        // Spawn a task to handle the request on transport2
        tokio::spawn(async move {
            if let Ok(request) = transport2.recv().await {
                if request.from == ProcessType::Hub
                    && matches!(request.payload, MessagePayload::Ping)
                {
                    let response = Message {
                        id: Ulid::generate(),
                        from: ProcessType::Storage,
                        to: request.from,
                        payload: MessagePayload::Pong,
                        reply_to: Some(request.id),
                    };
                    let _ = transport2.send(response).await;
                }
            }
        });

        // Send request from transport1
        let request = Message {
            id: Ulid::generate(),
            from: ProcessType::Hub,
            to: ProcessType::Storage,
            payload: MessagePayload::Ping,
            reply_to: None,
        };

        let response = transport1.request(request).await?;

        assert!(matches!(response.payload, MessagePayload::Pong));

        Ok(())
    }

    #[tokio::test]
    async fn unix_socket_cleanup() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let base_path = temp_dir.path().to_path_buf();
        let socket_path = base_path.join("hub.sock");

        {
            let _ust = UnixSocketTransport::new(base_path.clone(), ProcessType::Hub).await?;
            assert!(socket_path.exists());
        }

        sleep(std::time::Duration::from_millis(100)).await;
        assert!(!socket_path.exists());

        Ok(())
    }
}
