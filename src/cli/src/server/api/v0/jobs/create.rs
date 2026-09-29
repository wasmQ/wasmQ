use axum::http::StatusCode;
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use wasmq::proto::task::TaskIdentifier;

use wasmq_ipc::protocol::{Message, MessagePayload, ProcessType};
use wasmq::proto::job::Job;

use crate::server::api::v0::ApiError;
use crate::server::state::SharedServices;

#[derive(Debug, Deserialize)]
pub struct CreateJobRequest {
    name: String,
    task: TaskIdentifier,
    args: Value,
    max_attempts: Option<u32>,
    scheduled_at: DateTime<Utc>,
}

pub async fn handler(
    Extension(services): Extension<SharedServices>,
    Json(payload): Json<CreateJobRequest>,
) -> Result<Json<Job>, ApiError> {
    let mut job = Job::new(
        payload.name,
        payload.args,
        payload.scheduled_at.into(),
        payload.task,
    )
    .map_err(|err| ApiError {
        message: err.to_string(),
        status: StatusCode::BAD_REQUEST,
    })?;

    if let Some(max_attempts) = payload.max_attempts {
        job.set_max_attempts(max_attempts).map_err(|err| ApiError {
            message: err.to_string(),
            status: StatusCode::BAD_REQUEST,
        })?;
    }

    let msg = Message::new(
        ProcessType::Hub,
        ProcessType::Storage,
        MessagePayload::StoreJob(job.clone()),
    );

    let message = services
        .hub
        .ipc()
        .request(msg)
        .await
        .map_err(|e| e.to_string())
        .map_err(|err| ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: err,
        })?;

    match message.payload {
        MessagePayload::JobStored(Ok(job)) => Ok(Json(job)),
        MessagePayload::JobStored(Err(message)) => Err(ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message,
        }),
        _ => Err(ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: String::from("Unexpected response from storage service"),
        }),
    }
}
