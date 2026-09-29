mod start;

use anyhow::Result;
use clap::Parser;

use self::start::HubStartOpt;

#[derive(Debug, Parser)]
pub enum HubCmd {
    /// Starts the `Hub` for wasmq
    Start(HubStartOpt),
}

impl HubCmd {
    pub async fn exec(&self) -> Result<()> {
        match &self {
            HubCmd::Start(cmd) => cmd.exec().await,
        }
    }
}
