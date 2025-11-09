#![cfg(feature = "sqlite")]

use chrono::NaiveDate;
use schedule_tool::{
    task::{ProgressMeasurement, RationaleItem},
    Schedule, ScheduleMetadata, SqliteScheduleStore, Task, ScheduleStore,
};
use tempfile::NamedTempFile;

fn d(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

#[test]
fn sqlite_store_round_trip_schedule() {
    let file = NamedTempFile::new().unwrap();
    let store = SqliteScheduleStore::new(file.path()).unwrap();

    let mut metadata = ScheduleMetadata::default();
    metadata.project_name = "SQLite Project".into();
    metadata.project_start_date = d(2025, 1, 6);
    metadata.project_end_date = d(2025, 2, 28);

    let mut schedule = Schedule::new_with_metadata(metadata.clone());
    schedule
        .upsert_task(1, "Design", 5, None)
        .expect("insert task 1");
    schedule
        .upsert_task(2, "Build", 10, Some(vec![1]))
        .expect("insert task 2");
    schedule.forward_pass().unwrap();
    schedule.backward_pass().unwrap();

    let mut build_task = Task::from_dataframe_row(schedule.dataframe(), 1).unwrap();
    build_task.progress_measurement = ProgressMeasurement::PreDefinedRationale;
    build_task.pre_defined_rationale = vec![
        RationaleItem::new(1, "Assembly", 0.6, true),
        RationaleItem::new(2, "Validation", 0.4, false),
    ];
    build_task.percent_complete = Some(0.6);
    schedule.upsert_task_record(build_task).unwrap();

    store.save_schedule(&schedule).expect("save schedule");

    let loaded = store
        .load_schedule()
        .expect("load schedule")
        .expect("schedule exists");

    assert_eq!(loaded.metadata().project_name, "SQLite Project");
    assert_eq!(
        loaded.metadata().project_start_date,
        d(2025, 1, 6)
    );
    assert_eq!(loaded.dataframe().height(), 2);

    let task = Task::from_dataframe_row(loaded.dataframe(), 1).unwrap();
    assert_eq!(task.id, 2);
    assert_eq!(task.predecessors, vec![1]);
    assert_eq!(task.progress_measurement, ProgressMeasurement::PreDefinedRationale);
    assert_eq!(
        task.pre_defined_rationale,
        vec![
            RationaleItem::new(1, "Assembly", 0.6, true),
            RationaleItem::new(2, "Validation", 0.4, false)
        ]
    );
    assert_eq!(task.percent_complete, Some(0.6));
}

