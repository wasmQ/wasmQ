use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

use tracing::debug;
use wasmq_config::Config;
use wasmq_ipc::protocol::ProcessType;

use crate::process::scheduler::SchedulerProcess;
use crate::transport::make_transport;

#[derive(Debug, Parser)]
pub struct SchedulerSpawnOpt {
    #[clap(long, short)]
    config: PathBuf,
}

impl SchedulerSpawnOpt {
    pub async fn exec(&self) -> Result<()> {
        let config = Config::from_file(&self.config)?;
        let transport = make_transport(config.clone(), ProcessType::Scheduler).await?;
        let mut scheduler = SchedulerProcess::new(transport, 1).await?;

        debug!("Starting scheduler process…");
        scheduler.run().await?;

        Ok(())
    }
}
