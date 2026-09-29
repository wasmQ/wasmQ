use std::path::PathBuf;

use anyhow::Result;

use wasmq_ipc::transport::Transport;
use wasmq_storage::Storage;

pub struct StorageProcess {
    storage: Storage,
}

impl StorageProcess {
    pub async fn new(transport: Box<dyn Transport>, home: PathBuf) -> Result<Self> {
        let storage = Storage::new(transport, home).await?;
        Ok(Self { storage })
    }

    pub async fn run(&mut self) -> Result<()> {
        self.storage.run().await
    }
}
