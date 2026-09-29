use std::process::exit;
use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use clap::Parser;
use wasmq_repository::TaskRepository;
use tracing::error;

use crate::process::hub::Hub;
use crate::server::run_server;
use crate::utils::shutdown_signal;

#[derive(Debug, Parser)]
pub struct HubStartOpt {
    /// Path to a wasmq Config file
    #[clap(long, short)]
    config: PathBuf,
}

impl HubStartOpt {
    pub async fn exec(&self) -> Result<()> {
        // FIXME: Transport should not know about `ProcessType` it should be only handled by IPC
        let mut hub = Hub::new(self.config.clone()).await?;
        let child_processes = hub.spawn_processes().await?;
        let hub = Arc::new(hub);
        let repo = Arc::new(TaskRepository::local().await?);

        hub.wait_for_components().await?;

        tokio::select! {
            Err(err) = run_server(hub.config(), Arc::clone(&hub), Arc::clone(&repo)) => {
                error!("Server returned an error. {:#?}", err);
            },
            _ = shutdown_signal() => {
                for mut cp in child_processes {
                    if let Err(err) = cp.kill().await {
                        error!("Failed to kill process. {:#?}", err);
                    }
                }

                exit(0);
            },
        }

        Ok(())
    }
}
