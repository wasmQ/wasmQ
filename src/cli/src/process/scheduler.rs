use anyhow::Result;

use wasmq_ipc::transport::Transport;
use wasmq_scheduler::Scheduler;

pub struct SchedulerProcess {
    scheduler: Scheduler,
}

impl SchedulerProcess {
    pub async fn new(transport: Box<dyn Transport>, executor_count: usize) -> Result<Self> {
        let scheduler = Scheduler::new(transport, executor_count).await?;

        Ok(Self { scheduler })
    }

    pub async fn run(&mut self) -> Result<()> {
        self.scheduler.run().await
    }
}
