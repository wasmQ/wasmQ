pub mod component;
pub mod job;
pub mod run;
pub mod task;

use anyhow::Result;
use clap::Parser;

use crate::cli::cmd::component::ComponentCmd;
use crate::cli::cmd::job::JobCmd;
use crate::cli::cmd::run::RunCmd;
use crate::cli::cmd::task::TaskCmd;

#[derive(Debug, Parser)]
pub enum Cmd {
    #[clap(subcommand, hide = true)]
    Component(ComponentCmd),
    /// Job management
    #[clap(subcommand)]
    Job(JobCmd),
    /// Runs an instance of wasmq's Hub
    Run,
    /// Task management and development
    #[clap(subcommand)]
    Task(TaskCmd),
}

impl Cmd {
    pub async fn exec(&self) -> Result<()> {
        match &self {
            Cmd::Component(cmd) => cmd.exec().await,
            Cmd::Job(cmd) => cmd.exec().await,
            Cmd::Run => RunCmd::run().await,
            Cmd::Task(cmd) => cmd.exec().await,
        }
    }
}
