#![cfg(feature = "http_api")]

use axum::{
    body::{self, Body},
    http::{Request, StatusCode},
};
use schedule_tool::{ProgressMeasurement, Schedule, Task, http_api};
use serde_json::json;
use tower::util::ServiceExt;

fn new_router() -> axum::Router {
    let schedule = Schedule::new();
    let state = http_api::AppState::new(schedule);
    http_api::router(state)
}

#[tokio::test]
async fn task_lifecycle_via_http_api() {
    let app = new_router();
    let task = Task::new(1, "HTTP Demo", 5);

    // Create task
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tasks")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&task).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // Fetch created task
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/tasks/1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let fetched: Task = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(fetched.id, 1);
    assert_eq!(fetched.name, "HTTP Demo");

    // Delete the task
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/tasks/1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Ensure the task is gone
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/tasks/1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let bytes = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["error"], json!("not_found"));
}

#[tokio::test]
async fn apply_rationale_template_via_http_api() {
    let app = new_router();
    let task = Task::new(1, "HTTP Demo", 5);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tasks")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&task).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let payload = serde_json::json!({ "template": "fifty_fifty" });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tasks/1/rationale_template")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let updated: Task = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        updated.progress_measurement,
        ProgressMeasurement::PreDefinedRationale
    );
    assert_eq!(updated.pre_defined_rationale.len(), 2);
}

#[tokio::test]
async fn invalid_progress_payload_returns_bad_request() {
    let app = new_router();
    let mut task = Task::new(1, "Bad Progress", 5);
    task.progress_measurement = schedule_tool::ProgressMeasurement::ZeroOneHundred;
    task.percent_complete = Some(0.3);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tasks")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&task).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let bytes = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["error"], json!("invalid_request"));
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("progress_measurement=0_100")
    );
}
