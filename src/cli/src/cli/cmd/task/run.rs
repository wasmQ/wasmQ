use std::fs::read;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use serde_json::Value;

use wasmq_executor::Executor;

#[derive(Debug, Parser)]
pub struct TaskRunOpt {
    /// Task source file
    #[clap(long)]
    pub source: PathBuf,
    /// Arguments to pass to the task
    #[clap(long)]
    pub args: String,
}

impl TaskRunOpt {
    pub async fn exec(&self) -> Result<()> {
        serde_json::from_str::<Value>(&self.args)
            .context("Failed to deserialize string into JSON")?;

        let args = self.args.clone();
        let args = args.as_bytes().to_vec();
        let source = self.source.clone();
        let wasm = read(source)?;
        let executor = Executor::new();
        let output: Value = executor.run(wasm.into(), args.into()).await?;

        println!("{}", serde_json::to_string_pretty(&output)?);

        Ok(())
    }
}
