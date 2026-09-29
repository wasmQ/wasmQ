use axum::extract::Query;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::Deserialize;
use ulid::Ulid;

use wasmq_ipc::protocol::{Message, MessagePayload, ProcessType};
use wasmq::proto::job::{Job, JobQuery, JobStatus};

use crate::server::api::v0::ApiError;
use crate::server::state::SharedServices;

#[derive(Deserialize)]
pub struct RetrieveJobsQuery {
    id: Option<Ulid>,
    status: Option<JobStatus>,
}

pub async fn handler(
    Extension(services): Extension<SharedServices>,
    Query(query): Query<RetrieveJobsQuery>,
) -> Result<Json<Vec<Job>>, ApiError> {
    let msg = Message {
        id: Ulid::new(),
        from: ProcessType::Hub,
        to: ProcessType::Storage,
        payload: MessagePayload::QueryJobs(JobQuery {
            status: query.status,
            max_time: None,
            min_time: None,
            limit: None,
        }),
        reply_to: None,
    };

    let response = services
        .hub
        .ipc()
        .request(msg)
        .await
        .map_err(|e| ApiError {
            message: e.to_string(),
            status: StatusCode::INTERNAL_SERVER_ERROR,
        })?;

    match response.payload {
        MessagePayload::JobsResult(jobs) => {
            if let Some(id) = query.id {
                let jobs: Vec<Job> = jobs.into_iter().filter(|job| job.id == id).collect();
                return Ok(Json(jobs));
            }

            Ok(Json(jobs))
        }
        _ => Err(ApiError {
            message: "Unexpected response".into(),
            status: StatusCode::INTERNAL_SERVER_ERROR,
        }),
    }
}
