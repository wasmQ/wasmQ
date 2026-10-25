use serde::{Deserialize, Serialize};
use ulid::Ulid;

use wasmq::proto::job::{Job, JobQuery, JobResult, JobStatus};

pub type ExecutorId = usize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessagePayload {
    // Hub -> Storage
    StoreJob(Job),
    QueryJobs(JobQuery),
    UpdateJobStatus(Ulid, JobStatus),

    // Storage -> Hub/Scheduler/Executor
    JobStored(Result<Job, String>),
    JobsResult(Vec<Job>),
    JobUpdated(Result<(), String>),

    // Scheduler -> Storage
    ClaimJob {
        executor_id: ExecutorId,
        job_id: Ulid,
        claimed_by: String,
    },

    // Scheduler -> Executor
    ExecuteJob(Job),

    // Executor -> Storage
    JobStarted(Ulid),
    JobCompleted(Ulid, JobResult),
    JobFailed(Ulid, String),

    // Executor -> Scheduler (acknowledgment)
    JobAccepted(Ulid),

    // Health checks
    Ping,
    Pong,

    // Shutdown
    Shutdown,
    ShutdownAck,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ProcessType {
    Hub,
    Executor(ExecutorId),
    Storage,
    Scheduler,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: Ulid,
    pub from: ProcessType,
    pub to: ProcessType,
    pub payload: MessagePayload,
    pub reply_to: Option<Ulid>,
}

impl Message {
    pub fn new(from: ProcessType, to: ProcessType, payload: MessagePayload) -> Self {
        Self {
            id: Ulid::generate(),
            from,
            to,
            payload,
            reply_to: None,
        }
    }

    pub fn reply_to(&mut self, id: Ulid) -> &mut Self {
        self.reply_to = Some(id);
        self
    }
}
