use std::{net::SocketAddr, str::FromStr, sync::Arc};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{ProgressRationaleTemplate, RefreshSummary, Schedule, ScheduleMetadata, Task};

#[derive(Clone)]
pub struct AppState {
    schedule: Arc<RwLock<Schedule>>,
}

impl AppState {
    pub fn new(schedule: Schedule) -> Self {
        Self {
            schedule: Arc::new(RwLock::new(schedule)),
        }
    }

    pub fn with_shared(schedule: Arc<RwLock<Schedule>>) -> Self {
        Self { schedule }
    }

    fn schedule(&self) -> Arc<RwLock<Schedule>> {
        self.schedule.clone()
    }
}

#[derive(Debug, Serialize)]
struct ErrorBody<'a> {
    error: &'a str,
    message: String,
}

#[derive(Debug)]
enum ApiError {
    NotFound(String),
    Conflict(String),
    Invalid(String),
    Internal(String),
}

#[derive(Debug, Deserialize)]
struct ApplyTemplatePayload {
    template: String,
}

impl ApiError {
    fn not_found(message: impl Into<String>) -> Self {
        ApiError::NotFound(message.into())
    }

    fn invalid(message: impl Into<String>) -> Self {
        ApiError::Invalid(message.into())
    }
}

impl From<polars::prelude::PolarsError> for ApiError {
    fn from(value: polars::prelude::PolarsError) -> Self {
        ApiError::Invalid(value.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::NotFound(message) => {
                let body = Json(ErrorBody {
                    error: "not_found",
                    message,
                });
                (StatusCode::NOT_FOUND, body).into_response()
            }
            ApiError::Conflict(message) => {
                let body = Json(ErrorBody {
                    error: "conflict",
                    message,
                });
                (StatusCode::CONFLICT, body).into_response()
            }
            ApiError::Invalid(message) => {
                let body = Json(ErrorBody {
                    error: "invalid_request",
                    message,
                });
                (StatusCode::BAD_REQUEST, body).into_response()
            }
            ApiError::Internal(message) => {
                let body = Json(ErrorBody {
                    error: "internal_error",
                    message,
                });
                (StatusCode::INTERNAL_SERVER_ERROR, body).into_response()
            }
        }
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metadata", get(get_metadata).put(update_metadata))
        .route("/tasks", get(list_tasks).post(create_task))
        .route(
            "/tasks/:id",
            get(get_task).put(update_task).delete(delete_task),
        )
        .route(
            "/tasks/:id/rationale_template",
            post(apply_rationale_template),
        )
        .route("/refresh", post(refresh_schedule))
        .with_state(state)
}

pub async fn serve(addr: SocketAddr, schedule: Schedule) -> std::io::Result<()> {
    let state = AppState::new(schedule);
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await
}

async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

async fn get_metadata(State(state): State<AppState>) -> Json<ScheduleMetadata> {
    let schedule = state.schedule();
    let metadata = {
        let guard = schedule.read();
        guard.metadata().clone()
    };
    Json(metadata)
}

async fn update_metadata(
    State(state): State<AppState>,
    Json(metadata): Json<ScheduleMetadata>,
) -> Result<Json<ScheduleMetadata>, ApiError> {
    let schedule = state.schedule();
    {
        let mut guard = schedule.write();
        guard
            .set_metadata(metadata.clone())
            .map_err(|err| ApiError::invalid(err.to_string()))?;
        guard.refresh().map_err(ApiError::from)?;
    }
    let current = {
        let guard = schedule.read();
        guard.metadata().clone()
    };
    Ok(Json(current))
}

async fn list_tasks(State(state): State<AppState>) -> Result<Json<Vec<Task>>, ApiError> {
    let schedule = state.schedule();
    let tasks = {
        let guard = schedule.read();
        guard.tasks()?
    };
    Ok(Json(tasks))
}

async fn get_task(
    State(state): State<AppState>,
    Path(task_id): Path<i32>,
) -> Result<Json<Task>, ApiError> {
    let schedule = state.schedule();
    let result = {
        let guard = schedule.read();
        guard.find_task(task_id)?
    };
    match result {
        Some(task) => Ok(Json(task)),
        None => Err(ApiError::not_found(format!("task {task_id} not found"))),
    }
}

async fn create_task(
    State(state): State<AppState>,
    Json(task): Json<Task>,
) -> Result<(StatusCode, Json<Task>), ApiError> {
    let schedule = state.schedule();
    {
        let mut guard = schedule.write();
        if guard.find_task(task.id)?.is_some() {
            return Err(ApiError::Conflict(format!(
                "task {} already exists",
                task.id
            )));
        }
        guard
            .upsert_task_record(task.clone())
            .map_err(ApiError::from)?;
        guard.refresh().map_err(ApiError::from)?;
    }
    let created = {
        let guard = schedule.read();
        guard
            .find_task(task.id)?
            .ok_or_else(|| ApiError::internal("task not found after creation"))?
    };
    Ok((StatusCode::CREATED, Json(created)))
}

async fn update_task(
    State(state): State<AppState>,
    Path(task_id): Path<i32>,
    Json(task): Json<Task>,
) -> Result<Json<Task>, ApiError> {
    if task.id != task_id {
        return Err(ApiError::invalid(
            "task id in payload does not match path parameter",
        ));
    }
    let schedule = state.schedule();
    {
        let mut guard = schedule.write();
        if guard.find_task(task_id)?.is_none() {
            return Err(ApiError::not_found(format!("task {task_id} not found")));
        }
        guard
            .upsert_task_record(task.clone())
            .map_err(ApiError::from)?;
        guard.refresh().map_err(ApiError::from)?;
    }
    let updated = {
        let guard = schedule.read();
        guard
            .find_task(task_id)?
            .ok_or_else(|| ApiError::internal("task not found after update"))?
    };
    Ok(Json(updated))
}

async fn delete_task(
    State(state): State<AppState>,
    Path(task_id): Path<i32>,
) -> Result<StatusCode, ApiError> {
    let schedule = state.schedule();
    let removed = {
        let mut guard = schedule.write();
        guard.delete_task(task_id)?
    };
    if !removed {
        return Err(ApiError::not_found(format!("task {task_id} not found")));
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn refresh_schedule(State(state): State<AppState>) -> Result<Json<RefreshSummary>, ApiError> {
    let schedule = state.schedule();
    let summary = {
        let mut guard = schedule.write();
        guard.refresh().map_err(ApiError::from)?
    };
    Ok(Json(summary))
}

async fn apply_rationale_template(
    State(state): State<AppState>,
    Path(task_id): Path<i32>,
    Json(payload): Json<ApplyTemplatePayload>,
) -> Result<Json<Task>, ApiError> {
    let template = ProgressRationaleTemplate::from_str(payload.template.trim()).map_err(|_| {
        ApiError::invalid(format!("unknown rationale template '{}'", payload.template))
    })?;
    let schedule = state.schedule();
    {
        let mut guard = schedule.write();
        guard
            .apply_rationale_template(task_id, template)
            .map_err(ApiError::from)?;
    }
    let updated = {
        let guard = schedule.read();
        guard
            .find_task(task_id)
            .map_err(ApiError::from)?
            .ok_or_else(|| {
                ApiError::internal("task not found after rationale template application")
            })?
    };
    Ok(Json(updated))
}

impl ApiError {
    fn internal(message: impl Into<String>) -> Self {
        ApiError::Internal(message.into())
    }
}
