use std::{process::exit, sync::Arc};

use anyhow::Result;
use clap::Parser;

use tracing::error;
use wasmq_config::Config;
use wasmq_repository::TaskRepository;

use crate::process::hub::Hub;
use crate::server::run_server;
use crate::utils::shutdown_signal;

#[derive(Debug, Parser)]
pub enum RunCmd {}

impl RunCmd {
    pub async fn run() -> Result<()> {
        // FIXME: Transport should not know about `ProcessType` it should be only handled by IPC
        let mut hub = Hub::new(Config::default()).await?;
        let child_processes = hub.spawn_processes().await?;
        let hub = Arc::new(hub);
        let repo = Arc::new(TaskRepository::local().await?);
        let config = Arc::new(hub.config().to_owned());

        tokio::select! {
            Err(err) = run_server(Arc::clone(&config), Arc::clone(&hub), Arc::clone(&repo)) => {
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
