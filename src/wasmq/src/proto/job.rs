use std::fmt::Display;
use std::time::SystemTime;
use std::{cmp::Ordering, str::FromStr};

use anyhow::{Error, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use ulid::Ulid;

use crate::proto::task::TaskIdentifier;

pub type ExecutorId = usize;

pub const JOB_CLAIMED_STATUS: &str = "claimed";
pub const JOB_PENDING_STATUS: &str = "pending";
pub const JOB_SCHEDULED_STATUS: &str = "scheduled";
pub const JOB_RUNNING_STATUS: &str = "running";
pub const JOB_COMPLETED_STATUS: &str = "completed";
pub const JOB_FAILED_STATUS: &str = "failed";
pub const JOB_CANCELLED_STATUS: &str = "cancelled";
pub const JOB_SUCCESS_RESULT: &str = "success";
pub const JOB_FAILURE_RESULT: &str = "failure";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub id: Ulid,
    pub name: String,
    pub args: Value,
    pub status: JobStatus,
    pub scheduled_at: SystemTime,
    pub task: TaskIdentifier,
    pub started_at: Option<SystemTime>,
    pub completed_at: Option<SystemTime>,
    pub errors: Vec<String>,
    pub result: Option<JobResult>,
    pub attempts: u32,
    pub max_attempts: u32,
    pub metadata: Map<String, Value>,
    pub claimed_at: Option<SystemTime>,
    pub claimed_by: Option<String>,
}

impl Job {
    pub fn new(
        name: String,
        args: Value,
        scheduled_at: SystemTime,
        task: TaskIdentifier,
    ) -> Result<Self> {
        if name.is_empty() || name.contains(' ') {
            bail!("Job name cannot contain spaces and cannot be empty");
        }

        Ok(Self {
            id: Ulid::generate(),
            name,
            args,
            status: JobStatus::Scheduled,
            scheduled_at,
            task,
            started_at: None,
            completed_at: None,
            errors: Vec::new(),
            result: None,
            attempts: 0,
            max_attempts: 3,
            metadata: Map::new(),
            claimed_at: None,
            claimed_by: None,
        })
    }

    pub fn set_max_attempts(&mut self, max_attempts: u32) -> Result<()> {
        if max_attempts == 0 {
            bail!("max_attempts must be greater than 0");
        }

        self.max_attempts = max_attempts;

        Ok(())
    }
}

impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Job {
    fn cmp(&self, other: &Self) -> Ordering {
        other.scheduled_at.cmp(&self.scheduled_at) // Earlier first
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobStatus {
    Claimed,
    Pending,
    Scheduled,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status_str = match self {
            JobStatus::Claimed => JOB_CLAIMED_STATUS,
            JobStatus::Pending => JOB_PENDING_STATUS,
            JobStatus::Scheduled => JOB_SCHEDULED_STATUS,
            JobStatus::Running => JOB_RUNNING_STATUS,
            JobStatus::Completed => JOB_COMPLETED_STATUS,
            JobStatus::Failed => JOB_FAILED_STATUS,
            JobStatus::Cancelled => JOB_CANCELLED_STATUS,
        };
        write!(f, "{}", status_str)
    }
}

impl FromStr for JobStatus {
    type Err = Error;

    fn from_str(input: &str) -> Result<JobStatus, Self::Err> {
        match input.to_ascii_lowercase().as_str() {
            JOB_CLAIMED_STATUS => Ok(JobStatus::Claimed),
            JOB_PENDING_STATUS => Ok(JobStatus::Pending),
            JOB_SCHEDULED_STATUS => Ok(JobStatus::Scheduled),
            JOB_RUNNING_STATUS => Ok(JobStatus::Running),
            JOB_COMPLETED_STATUS => Ok(JobStatus::Completed),
            JOB_FAILED_STATUS => Ok(JobStatus::Failed),
            JOB_CANCELLED_STATUS => Ok(JobStatus::Cancelled),
            _ => bail!(
                "The value {} doesn't correspond to a valid JobStatus",
                input
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobResult {
    Success(Value),
    Failure(String),
}

impl Display for JobResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobResult::Success(_) => write!(f, "{}", JOB_SUCCESS_RESULT),
            JobResult::Failure(_) => write!(f, "{}", JOB_FAILURE_RESULT),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobQuery {
    pub status: Option<JobStatus>,
    pub min_time: Option<SystemTime>,
    pub max_time: Option<SystemTime>,
    pub limit: Option<usize>,
}
