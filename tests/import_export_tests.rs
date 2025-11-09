use chrono::NaiveDate;
use schedule_tool::{
    load_schedule_from_csv, load_schedule_from_json, save_schedule_to_csv, save_schedule_to_json,
    task::{ProgressMeasurement, RationaleItem},
    PersistenceError, Schedule, ScheduleMetadata, Task,
};
use tempfile::NamedTempFile;

fn d(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

fn build_sample_schedule() -> Schedule {
    let mut metadata = ScheduleMetadata::default();
    metadata.project_name = "Export Project".into();
    metadata.project_description = "Testing persistence helpers".into();
    metadata.project_start_date = d(2025, 1, 6);
    metadata.project_end_date = d(2025, 2, 28);

    let mut schedule = Schedule::new_with_metadata(metadata);

    let mut task1 = Task::new(1, "Design", 5);
    task1.early_start = Some(d(2025, 1, 6));
    task1.early_finish = Some(d(2025, 1, 10));
    task1.percent_complete = Some(0.5);
    task1.task_notes = Some("Initial design sprint".into());
    task1.task_attachments = vec!["design-spec.pdf".into()];
    schedule.upsert_task_record(task1).unwrap();

    let mut task2 = Task::new(2, "Build", 8);
    task2.predecessors = vec![1];
    task2.early_start = Some(d(2025, 1, 13));
    task2.early_finish = Some(d(2025, 1, 22));
    task2.baseline_start = Some(d(2025, 1, 13));
    task2.baseline_finish = Some(d(2025, 1, 22));
    task2.actual_start = Some(d(2025, 1, 14));
    task2.schedule_variance_days = Some(1);
    task2.is_critical = Some(true);
    task2.parent_id = Some(10);
    task2.progress_measurement = ProgressMeasurement::PreDefinedRationale;
    task2.pre_defined_rationale = vec![
        RationaleItem::new(1, "Assembly", 0.3, false),
        RationaleItem::new(2, "Configuration", 0.4, false),
        RationaleItem::new(3, "Verification", 0.3, false),
    ];
    schedule.upsert_task_record(task2).unwrap();

    schedule
}

fn collect_tasks(schedule: &Schedule) -> Vec<Task> {
    let df = schedule.dataframe();
    let mut tasks = Vec::with_capacity(df.height());
    for idx in 0..df.height() {
        tasks.push(Task::from_dataframe_row(df, idx).unwrap());
    }
    tasks
}

#[test]
fn json_round_trip_preserves_schedule() {
    let schedule = build_sample_schedule();
    let file = NamedTempFile::new().unwrap();

    save_schedule_to_json(&schedule, file.path()).unwrap();
    let loaded = load_schedule_from_json(file.path()).unwrap();

    assert_eq!(
        loaded.metadata().project_name,
        schedule.metadata().project_name
    );
    assert_eq!(
        loaded.metadata().project_description,
        schedule.metadata().project_description
    );

    let mut original_tasks = collect_tasks(&schedule);
    original_tasks.sort_by_key(|t| t.id);
    let mut loaded_tasks = collect_tasks(&loaded);
    loaded_tasks.sort_by_key(|t| t.id);
    assert_eq!(original_tasks, loaded_tasks);
}

#[test]
fn csv_round_trip_preserves_tasks() {
    let schedule = build_sample_schedule();
    let file = NamedTempFile::new().unwrap();

    save_schedule_to_csv(&schedule, file.path()).unwrap();
    let loaded = load_schedule_from_csv(file.path()).unwrap();

    let mut original_tasks = collect_tasks(&schedule);
    original_tasks.sort_by_key(|t| t.id);
    let mut loaded_tasks = collect_tasks(&loaded);
    loaded_tasks.sort_by_key(|t| t.id);
    assert_eq!(original_tasks, loaded_tasks);
}

#[test]
fn json_load_rejects_duplicate_ids() {
    let snapshot = serde_json::json!({
        "metadata": ScheduleMetadata::default(),
        "tasks": [
            Task::new(1, "A", 1),
            Task::new(1, "B", 2)
        ]
    });

    let file = NamedTempFile::new().unwrap();
    serde_json::to_writer_pretty(file.as_file(), &snapshot).unwrap();

    let result = load_schedule_from_json(file.path());
    match result {
        Ok(_) => panic!("expected duplicate ids to be rejected"),
        Err(PersistenceError::InvalidData(msg)) => assert!(
            msg.contains("duplicate task id"),
            "unexpected message: {msg}"
        ),
        Err(other) => panic!("expected InvalidData error, got {other:?}"),
    }
}

#[test]
fn json_load_rejects_negative_duration() {
    let task = Task::new(1, "A", -5);
    let snapshot = serde_json::json!({
        "metadata": ScheduleMetadata::default(),
        "tasks": [task]
    });

    let file = NamedTempFile::new().unwrap();
    serde_json::to_writer_pretty(file.as_file(), &snapshot).unwrap();

    let result = load_schedule_from_json(file.path());
    match result {
        Ok(_) => panic!("expected negative duration to be rejected"),
        Err(PersistenceError::InvalidData(msg)) => assert!(
            msg.contains("negative duration"),
            "unexpected message: {msg}"
        ),
        Err(other) => panic!("expected InvalidData error, got {other:?}"),
    }
}

#[test]
fn csv_save_rejects_negative_duration() {
    let mut schedule = Schedule::new();
    let task = Task::new(1, "Bad Task", -1);
    schedule.upsert_task_record(task).unwrap();

    let file = NamedTempFile::new().unwrap();
    let result = save_schedule_to_csv(&schedule, file.path());
    match result {
        Ok(_) => panic!("expected negative duration to be rejected"),
        Err(PersistenceError::InvalidData(msg)) => assert!(
            msg.contains("negative duration"),
            "unexpected message: {msg}"
        ),
        Err(other) => panic!("expected InvalidData error, got {other:?}"),
    }
}

#[test]
fn json_save_rejects_invalid_zero_one_hundred_percent() {
    let mut schedule = Schedule::new();
    let mut task = Task::new(1, "Milestone", 1);
    task.progress_measurement = ProgressMeasurement::ZeroOneHundred;
    task.percent_complete = Some(0.5);
    schedule.upsert_task_record(task).unwrap();

    let file = NamedTempFile::new().unwrap();
    let result = save_schedule_to_json(&schedule, file.path());
    match result {
        Err(PersistenceError::InvalidData(msg)) => assert!(
            msg.contains("progress_measurement=0_100"),
            "unexpected message: {msg}"
        ),
        Err(other) => panic!("expected InvalidData error, got {other:?}"),
        Ok(_) => panic!("expected zero/one-hundred validation failure"),
    }
}

#[test]
fn json_save_rejects_empty_predefined_rationale() {
    let mut schedule = Schedule::new();
    let mut task = Task::new(1, "Composite", 3);
    task.progress_measurement = ProgressMeasurement::PreDefinedRationale;
    task.pre_defined_rationale = Vec::new();
    schedule.upsert_task_record(task).unwrap();

    let file = NamedTempFile::new().unwrap();
    let result = save_schedule_to_json(&schedule, file.path());
    match result {
        Err(PersistenceError::InvalidData(msg)) => assert!(
            msg.contains("requires at least one rationale item"),
            "unexpected message: {msg}"
        ),
        Err(other) => panic!("expected InvalidData error, got {other:?}"),
        Ok(_) => panic!("expected pre_defined_rationale validation failure"),
    }
}

#[test]
fn json_save_rejects_predefined_rationale_weight_sum() {
    let mut schedule = Schedule::new();
    let mut task = Task::new(1, "Composite", 3);
    task.progress_measurement = ProgressMeasurement::PreDefinedRationale;
    task.pre_defined_rationale = vec![
        RationaleItem::new(1, "Phase A", 0.4, false),
        RationaleItem::new(2, "Phase B", 0.4, false),
    ];
    schedule.upsert_task_record(task).unwrap();

    let file = NamedTempFile::new().unwrap();
    let result = save_schedule_to_json(&schedule, file.path());
    match result {
        Err(PersistenceError::InvalidData(msg)) => assert!(
            msg.contains("weights must sum to 1.0"),
            "unexpected message: {msg}"
        ),
        Err(other) => panic!("expected InvalidData error, got {other:?}"),
        Ok(_) => panic!("expected pre_defined_rationale sum validation failure"),
    }
}

