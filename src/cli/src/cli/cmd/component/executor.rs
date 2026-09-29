use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

use wasmq_config::Config;
use wasmq_ipc::protocol::ProcessType;
use tracing::debug;

use crate::process::executor::ExecutorProcess;
use crate::transport::make_transport;

#[derive(Debug, Parser)]
pub struct ExecutorSpawnOpt {
    #[clap(long, short)]
    config: PathBuf,
    #[clap(long, short)]
    id: usize,
}

impl ExecutorSpawnOpt {
    pub async fn exec(&self) -> Result<()> {
        let config = Config::from_file(&self.config)?;
        let transport = make_transport(config.clone(), ProcessType::Executor(self.id)).await?;
        let mut executor = ExecutorProcess::new(transport, self.id).await?;

        debug!(id=%self.id, "Starting executor process…");
        executor.run().await?;

        Ok(())
    }
}
