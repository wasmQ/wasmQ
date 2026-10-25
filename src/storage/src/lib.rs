mod backend;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Result, bail};

use tracing::error;
use wasmq_ipc::channel::IpcServer;
use wasmq_ipc::protocol::{Message, MessagePayload, ProcessType};
use wasmq_ipc::transport::Transport;

use crate::backend::Backend;
use crate::backend::sqlite::SqliteBackend;

const IPC_SENDER_STORAGE: ProcessType = ProcessType::Storage;

pub struct Storage {
    ipc: Arc<IpcServer>,
    backend: Arc<dyn Backend>,
}

impl Storage {
    pub async fn new(transport: Box<dyn Transport>, home: PathBuf) -> Result<Self> {
        let ipc = Arc::new(IpcServer::new(IPC_SENDER_STORAGE, transport));
        let home = home.join("storage.sqlite");
        let backend = Arc::new(SqliteBackend::new(home.to_str().unwrap()).await?);

        Ok(Self { ipc, backend })
    }

    pub async fn run(&mut self) -> Result<()> {
        tokio::select! {
            Err(err) = self.message_consumer() => {
                bail!("Storage message consumer failed. {:#?}", err);
            }
        }
    }

    async fn message_consumer(&mut self) -> Result<()> {
        let ipc_clone = Arc::clone(&self.ipc);

        tokio::spawn(async move {
            let _ = ipc_clone.listen().await;
        });

        let rx = self.ipc.receiver().await;
        let mut rx = rx.lock().await;

        while let Some(msg) = rx.recv().await {
            if let Some(response) = self.handle_message(msg.clone()).await {
                let response_msg = Message::new(IPC_SENDER_STORAGE, msg.from, response)
                    .reply_to(msg.id)
                    .to_owned();

                if let Err(err) = self.ipc.send(response_msg).await {
                    eprintln!(
                        "Error while sending message from Storage to IPC. {:#?}",
                        err
                    );
                }
            }
        }

        Ok(())
    }

    async fn handle_message(&mut self, msg: Message) -> Option<MessagePayload> {
        match msg.payload {
            MessagePayload::JobCompleted(id, result) => {
                match self.backend.update_job_completed(id, result).await {
                    Ok(_job) => Some(MessagePayload::JobUpdated(Ok(()))),
                    Err(err) => Some(MessagePayload::JobUpdated(Err(format!(
                        "Failed to update completion status for job {id}: {err}"
                    )))),
                }
            }
            MessagePayload::StoreJob(job) => match self.backend.create_job(job.clone()).await {
                Ok(job) => Some(MessagePayload::JobStored(Ok(job))),
                Err(err) => Some(MessagePayload::JobStored(Err(format!(
                    "Failed to store job {}: {err}",
                    job.id
                )))),
            },
            MessagePayload::QueryJobs(query) => match self.backend.retrieve_jobs(query).await {
                Ok(jobs) => Some(MessagePayload::JobsResult(jobs)),
                Err(_err) => Some(MessagePayload::JobsResult(vec![])),
            },
            MessagePayload::ClaimJob {
                executor_id,
                job_id,
                claimed_by,
            } => match self.backend.claim_job(job_id, claimed_by.clone()).await {
                Ok(job) => Some(MessagePayload::JobsResult(vec![job])),
                Err(err) => {
                    error!(job_id=?job_id, executor_id=?executor_id, claimed_by=?claimed_by, err=?err, "Failed to claim job for executor");
                    Some(MessagePayload::JobsResult(vec![]))
                }
            },
            MessagePayload::Ping => Some(MessagePayload::Pong),
            MessagePayload::Shutdown => Some(MessagePayload::ShutdownAck),
            _ => None,
        }
    }
}
