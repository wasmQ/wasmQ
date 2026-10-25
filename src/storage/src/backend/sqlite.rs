use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::{Map, Value};
use sqlx::FromRow;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
use ulid::Ulid;

use wasmq::proto::job::{Job, JobQuery, JobResult};

#[derive(Debug, FromRow)]
pub(crate) struct JobRecord {
    pub id: String,
    pub name: String,
    pub args: String,
    pub status: String,
    pub scheduled_at: i64,
    pub task: String,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub errors: String,
    pub result: Option<String>,
    pub attempts: i64,
    pub max_attempts: i64,
    pub metadata: String,
    pub claimed_at: Option<i64>,
    pub claimed_by: Option<String>,
}

impl TryFrom<JobRecord> for Job {
    type Error = anyhow::Error;

    fn try_from(record: JobRecord) -> Result<Self, Self::Error> {
        let id = Ulid::from_string(&record.id)?;
        let args = serde_json::from_str(&record.args)?;
        let task = record.task.parse()?;
        let status = record.status.parse()?;
        let scheduled_at = into_system_time(record.scheduled_at)?;
        let completed_at = record.completed_at.map(into_system_time).transpose()?;
        let started_at = record.started_at.map(into_system_time).transpose()?;
        let claimed_at = record.claimed_at.map(into_system_time).transpose()?;
        let errors: Vec<String> = serde_json::from_str(&record.errors)?;
        let result: Option<JobResult> = record
            .result
            .map(|r| serde_json::from_str(&r))
            .transpose()?;
        let attempts = record.attempts as u32;
        let max_attempts = record.max_attempts as u32;
        let metadata: Map<String, Value> = serde_json::from_str(&record.metadata)?;

        Ok(Job {
            id,
            name: record.name,
            args,
            status,
            scheduled_at,
            task,
            started_at,
            completed_at,
            errors,
            result,
            attempts,
            max_attempts,
            metadata,
            claimed_at,
            claimed_by: record.claimed_by,
        })
    }
}

pub struct SqliteBackend {
    pool: SqlitePool,
}

impl SqliteBackend {
    pub async fn new(url: &str) -> Result<Self> {
        let conn_opts = SqliteConnectOptions::new()
            .create_if_missing(true)
            .filename(url);
        let pool = SqlitePool::connect_with(conn_opts).await?;

        Self::migrate(&pool).await?;
        Ok(Self { pool })
    }

    async fn migrate(pool: &SqlitePool) -> Result<()> {
        sqlx::migrate!("src/backend/sqlite/migrations")
            .run(pool)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl super::Backend for SqliteBackend {
    async fn create_job(&self, job: Job) -> Result<Job> {
        let id = job.id.to_string();
        let args = serde_json::to_string(&job.args)?;
        let task = job.task.to_string();
        let status = job.status.to_string();
        let scheduled_at = into_unix_timestamp(job.scheduled_at)?;
        let max_attempts = job.max_attempts as i64;
        let metadata = serde_json::to_string(&job.metadata)?;
        let record = sqlx::query_as!(
            JobRecord,
            r#"
            INSERT INTO jobs (
                id,
                name,
                args,
                status,
                scheduled_at,
                task,
                started_at,
                completed_at,
                max_attempts,
                metadata
            ) VALUES (
                $1,
                $2,
                $3,
                $4,
                $5,
                $6,
                $7,
                $8,
                $9,
                $10
            ) RETURNING *"#,
            id,
            job.name,
            args,
            status,
            scheduled_at,
            task,
            Option::<i64>::None,
            Option::<i64>::None,
            max_attempts,
            metadata,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(record.try_into()?)
    }

    async fn retrieve_jobs(&self, query: JobQuery) -> Result<Vec<Job>> {
        let mut sql = String::from("SELECT * FROM jobs WHERE 1=1");

        if query.status.is_some() {
            sql.push_str(" AND status = ?");
        }

        if query.min_time.is_some() {
            sql.push_str(" AND scheduled_at >= ?");
        }

        if query.max_time.is_some() {
            sql.push_str(" AND scheduled_at <= ?");
        }

        if query.limit.is_some() {
            sql.push_str(" ORDER BY scheduled_at LIMIT ?");
        }

        let mut q = sqlx::query_as::<_, JobRecord>(sqlx::AssertSqlSafe(sql));

        if let Some(status) = query.status {
            q = q.bind(status.to_string());
        }

        if let Some(min_time) = query.min_time {
            q = q.bind(into_unix_timestamp(min_time)?);
        }

        if let Some(max_time) = query.max_time {
            q = q.bind(into_unix_timestamp(max_time)?);
        }

        if let Some(limit) = query.limit {
            q = q.bind(limit as i64);
        }

        let records = q.fetch_all(&self.pool).await?;
        records.into_iter().map(|r| r.try_into()).collect()
    }

    async fn update_job_completed(&self, id: Ulid, result: JobResult) -> Result<()> {
        let id = id.to_string();
        let result_json = serde_json::to_string(&result)?;
        let completed_at = into_unix_timestamp(SystemTime::now())?;

        match &result {
            JobResult::Success(_) => {
                sqlx::query(
                    r#"UPDATE jobs
                        SET
                            status = 'completed',
                            result = ?,
                            completed_at = ?,
                            attempts = attempts + 1
                        WHERE id = ?"#,
                )
                .bind(result_json)
                .bind(completed_at)
                .bind(id)
                .execute(&self.pool)
                .await?;
            }
            JobResult::Failure(error) => {
                sqlx::query(
                    r#"UPDATE jobs
                        SET
                            status = 'failed',
                            result = ?,
                            completed_at = ?,
                            attempts = attempts + 1,
                            errors = json_insert(errors, '$[#]', ?)
                        WHERE id = ?"#,
                )
                .bind(result_json)
                .bind(completed_at)
                .bind(error)
                .bind(id)
                .execute(&self.pool)
                .await?;
            }
        }

        Ok(())
    }

    async fn claim_job(&self, job_id: Ulid, claimed_by: String) -> Result<Job> {
        sqlx::query_as::<_, JobRecord>(
            r#"
            UPDATE jobs
            SET
                status = 'claimed',
                claimed_at = CAST(strftime('%s','now') AS INTEGER),
                claimed_by = ?
            WHERE
                id = ?
                AND status IN ('scheduled', 'failed')
                AND attempts < max_attempts
            RETURNING *;
            "#,
        )
        .bind(claimed_by)
        .bind(job_id.to_string())
        .fetch_one(&self.pool)
        .await?
        .try_into()
    }
}

fn into_unix_timestamp(time: SystemTime) -> Result<i64> {
    Ok(time
        .duration_since(SystemTime::UNIX_EPOCH)
        .context("Time went backwards")?
        .as_secs() as i64)
}

fn into_system_time(timestamp: i64) -> Result<SystemTime> {
    SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_secs(timestamp as u64))
        .context("Invalid timestamp")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::SystemTime;

    use serde_json::json;
    use ulid::Ulid;

    use wasmq::proto::job::{Job, JobStatus};
    use wasmq::proto::task::TaskIdentifier;

    use super::SqliteBackend;
    use crate::backend::Backend;

    async fn make_backend() -> (SqliteBackend, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.sqlite");
        let backend = SqliteBackend::new(path.to_str().unwrap())
            .await
            .expect("backend");
        (backend, dir)
    }

    fn make_job() -> Job {
        let task: TaskIdentifier = "test/my-task@0.1.0".parse().expect("task identifier");
        Job::new("test-job".to_string(), json!({}), SystemTime::now(), task).expect("job")
    }

    #[tokio::test]
    async fn concurrent_claim_does_not_duplicate() {
        let (backend, _dir) = make_backend().await;
        let backend = Arc::new(backend);
        let job = make_job();
        let stored = backend.create_job(job).await.expect("create_job");
        assert_eq!(stored.status, JobStatus::Scheduled);

        let job_id = stored.id;

        // Spawn two tasks that both try to claim the same job simultaneously.
        let b1 = Arc::clone(&backend);
        let b2 = Arc::clone(&backend);

        let h1 = tokio::spawn(async move { b1.claim_job(job_id, "worker-1".to_string()).await });
        let h2 = tokio::spawn(async move { b2.claim_job(job_id, "worker-2".to_string()).await });

        let r1 = h1.await.expect("join h1");
        let r2 = h2.await.expect("join h2");

        // Exactly one claim must succeed; the other must fail (no rows matched).
        let successes = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
        assert_eq!(
            successes, 1,
            "exactly one worker should claim the job, got r1={r1:?} r2={r2:?}"
        );

        let winner = if r1.is_ok() { r1.unwrap() } else { r2.unwrap() };
        assert_eq!(
            winner.status,
            JobStatus::Claimed,
            "the winning job must be in 'running' status"
        );
        assert_eq!(
            winner.attempts, 0,
            "attempts must not be incremented on claim"
        );
        assert!(winner.claimed_at.is_some());
        assert!(winner.claimed_by.is_some());
    }

    #[tokio::test]
    async fn attempt_is_not_incremented_on_claim() {
        let (backend, _dir) = make_backend().await;
        let job = make_job();
        let stored = backend.create_job(job).await.expect("create_job");
        let job_id = stored.id;
        let claimed = backend
            .claim_job(job_id, "worker-A".to_string())
            .await
            .expect("first claim");

        assert_eq!(claimed.status, JobStatus::Claimed);
        assert_eq!(claimed.attempts, 0);
        assert_eq!(claimed.claimed_by.as_deref(), Some("worker-A"));
        assert!(claimed.claimed_at.is_some());
    }

    /// claimed_by identity is stored and returned correctly.
    #[tokio::test]
    async fn claimed_by_is_stored() {
        let (backend, _dir) = make_backend().await;

        let job = make_job();
        let stored = backend.create_job(job).await.expect("create_job");

        let worker_id = format!("scheduler-executor0-{}", Ulid::generate());
        let claimed = backend
            .claim_job(stored.id, worker_id.clone())
            .await
            .expect("claim_job");

        assert_eq!(claimed.claimed_by.as_deref(), Some(worker_id.as_str()));
        assert!(claimed.claimed_at.is_some());
    }
}
