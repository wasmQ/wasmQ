use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use tokio::sync::RwLock;
use tracing::{debug, error};

use wasmq::proto::job::{Job, JobResult};
use wasmq::proto::task::TaskIdentifier;
use wasmq_executor::Executor;
use wasmq_ipc::channel::IpcServer;
use wasmq_ipc::protocol::{Message, MessagePayload, ProcessType};
use wasmq_ipc::transport::Transport;
use wasmq_repository::TaskRepository;

pub struct ExecutorProcess {
    id: usize,
    ipc: Arc<IpcServer>,
    executor: Arc<Executor>,
    repository: TaskRepository,
    cache: Arc<RwLock<HashMap<TaskIdentifier, Bytes>>>,
}

impl ExecutorProcess {
    pub async fn new(transport: Box<dyn Transport>, id: usize) -> Result<Self> {
        let cache = Arc::new(RwLock::new(HashMap::new()));
        let executor = Arc::new(Executor::new());
        let repository = TaskRepository::local().await?;
        let ipc = Arc::new(IpcServer::new(ProcessType::Executor(id), transport));

        Ok(Self {
            id,
            ipc,
            executor,
            repository,
            cache,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        tokio::select! {
            Err(err) = self.message_consumer() => {
                bail!("Executor message consumer failed. {:#?}", err);
            }
        }
    }

    /// Load module on-demand with caching
    async fn get_or_load_module(&self, tid: &TaskIdentifier) -> Result<Bytes> {
        {
            let cache = self.cache.read().await;

            if let Some(module) = cache.get(tid) {
                return Ok(module.clone());
            }
        }

        let task_bytes = self
            .repository
            .find(tid)
            .await
            .context(format!("Failed to find Task in repository: {}", tid))?;

        if let Some(wasm) = task_bytes {
            let cached_wasm = {
                let mut cache = self.cache.write().await;
                cache
                    .entry(tid.clone())
                    .or_insert_with(|| wasm.clone())
                    .clone()
            };

            return Ok(cached_wasm);
        }

        bail!("Task not found in repository: {}", tid);
    }

    pub async fn execute(&self, job: Job) -> Result<()> {
        let ipc = Arc::clone(&self.ipc);
        let executor = Arc::clone(&self.executor);
        let tid = job.task.clone();
        let exid = self.id;
        let job_id = job.id;
        let args = job.args.clone();
        let process_type = self.process_type();
        let task = match self.get_or_load_module(&tid).await {
            Ok(m) => m,
            Err(err) => {
                error!(?err, ?tid, "Failed to load WASM Task");

                if let Err(err) = ipc
                    .request(Message::new(
                        process_type,
                        ProcessType::Storage,
                        MessagePayload::JobCompleted(
                            job_id,
                            JobResult::Failure(format!("Task load failed: {}", err)),
                        ),
                    ))
                    .await
                {
                    error!(?err, "Failed to send message to Storage");
                }

                bail!("Failed to load module: {}", err);
            }
        };

        let args_bytes: Bytes = serde_json::to_vec(&args)?.into();

        tokio::spawn(async move {
            debug!(%job_id, %tid, %exid, "Executing Job");

            let result = executor.run(task, args_bytes).await;
            let job_result = match result {
                Ok(output) => JobResult::Success(output),
                Err(err) => {
                    error!(?err, %job_id, "Job execution failed");
                    JobResult::Failure(err.to_string())
                }
            };

            if let Err(err) = ipc
                .request(Message::new(
                    process_type,
                    ProcessType::Storage,
                    MessagePayload::JobCompleted(job_id, job_result),
                ))
                .await
            {
                error!(?err, "Failed to send JobCompleted message to Storage");
            }
        });

        Ok(())
    }

    async fn message_consumer(&self) -> Result<()> {
        let ipc_clone = Arc::clone(&self.ipc);

        tokio::spawn(async move {
            let _ = ipc_clone.listen().await;
        });

        let rx = self.ipc.receiver().await;
        let mut rx = rx.lock().await;

        while let Some(msg) = rx.recv().await {
            if let Some(response) = self.handle_message(msg.clone()).await {
                let response_msg = Message::new(ProcessType::Executor(self.id), msg.from, response)
                    .reply_to(msg.id)
                    .to_owned();

                if let Err(err) = self.ipc.send(response_msg).await {
                    eprintln!(
                        "Error while sending message from Executor to IPC. {:#?}",
                        err
                    );
                }
            }
        }

        Ok(())
    }

    async fn handle_message(&self, msg: Message) -> Option<MessagePayload> {
        match msg.payload {
            MessagePayload::ExecuteJob(job) => match self.execute(job.clone()).await {
                Ok(()) => Some(MessagePayload::JobAccepted(job.id)),
                Err(err) => Some(MessagePayload::JobFailed(job.id, err.to_string())),
            },
            MessagePayload::Ping => Some(MessagePayload::Pong),
            MessagePayload::Shutdown => Some(MessagePayload::ShutdownAck),
            _ => None,
        }
    }

    fn process_type(&self) -> ProcessType {
        ProcessType::Executor(self.id)
    }
}
