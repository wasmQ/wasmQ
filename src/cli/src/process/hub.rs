use std::env::current_exe;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use tempfile::TempDir;
use tokio::process::{Child, Command};
use tracing::debug;

use wasmq_config::Config;
use wasmq_ipc::channel::IpcServer;
use wasmq_ipc::protocol::{Message, MessagePayload, ProcessType};

use crate::transport::make_transport;

pub const IPC_SENDER_HUB: ProcessType = ProcessType::Hub;

pub struct Hub {
    config: Config,
    ipc: Arc<IpcServer>,
    config_path: PathBuf,
    // Keep the TempDir alive for the lifetime of the Hub
    #[allow(dead_code)]
    session_dir: TempDir,
}

impl Hub {
    pub async fn new(config: Config) -> Result<Self> {
        let session_dir = TempDir::new()?;
        let session_dir_path = session_dir.path();
        let config_path = session_dir_path.join("config.toml");
        let mut config_file = File::create(&config_path)?;
        let config_toml = config.to_toml()?;
        config_file.write_all(config_toml.as_bytes())?;

        let transport = make_transport(config.clone(), IPC_SENDER_HUB).await?;
        let ipc = IpcServer::new(IPC_SENDER_HUB, transport);
        let ipc = Arc::new(ipc);

        Ok(Self {
            config,
            ipc,
            config_path,
            session_dir,
        })
    }

    #[inline]
    pub fn config(&self) -> &Config {
        &self.config
    }

    pub async fn spawn_processes(&mut self) -> Result<Vec<Child>> {
        let wasmq_exe = current_exe()?;
        let mut child_processes = Vec::new();
        let storage = Command::new(&wasmq_exe)
            .arg("component")
            .arg("storage")
            .arg("--config")
            .arg(self.config_path.to_str().unwrap())
            .spawn()?;

        child_processes.push(storage);

        let scheduler = Command::new(&wasmq_exe)
            .arg("component")
            .arg("scheduler")
            .arg("--config")
            .arg(self.config_path.to_str().unwrap())
            .spawn()?;

        child_processes.push(scheduler);

        for i in 0..self.config.executors.count {
            let executor = Command::new(&wasmq_exe)
                .arg("component")
                .arg("executor")
                .arg("--config")
                .arg(self.config_path.to_str().unwrap())
                .arg("--id")
                .arg(i.to_string())
                .spawn()?;

            child_processes.push(executor);
        }

        self.wait_for_components().await?;

        Ok(child_processes)
    }

    pub async fn wait_for_components(&self) -> Result<()> {
        self.ipc
            .request(Message::new(
                IPC_SENDER_HUB,
                ProcessType::Storage,
                MessagePayload::Ping,
            ))
            .await?;

        debug!("✓ Storage");

        self.ipc
            .request(Message::new(
                IPC_SENDER_HUB,
                ProcessType::Scheduler,
                MessagePayload::Ping,
            ))
            .await?;

        debug!("✓ Scheduler");

        for i in 0..1 {
            self.ipc
                .request(Message::new(
                    IPC_SENDER_HUB,
                    ProcessType::Executor(i),
                    MessagePayload::Ping,
                ))
                .await?;

            debug!("✓ Executor({i})");
        }

        Ok(())
    }

    pub fn ipc(&self) -> Arc<IpcServer> {
        Arc::clone(&self.ipc)
    }
}
