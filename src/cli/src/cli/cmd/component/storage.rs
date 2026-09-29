use std::env::home_dir;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use tracing::debug;

use wasmq_config::Config;
use wasmq_ipc::protocol::ProcessType;

use crate::{process::storage::StorageProcess, transport::make_transport};

#[derive(Debug, Parser)]
pub struct StorageSpawnOpt {
    #[clap(long, short)]
    config: PathBuf,
}

impl StorageSpawnOpt {
    pub async fn exec(&self) -> Result<()> {
        let config = Config::from_file(&self.config)?;
        let mut home = home_dir().context("Failed to get home directory")?;
        home.push(".wasmq");
        let transport = make_transport(config.clone(), ProcessType::Storage).await?;
        let mut storage = StorageProcess::new(transport, home).await?;

        debug!("Starting storage process…");
        storage.run().await?;

        Ok(())
    }
}
