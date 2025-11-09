use chrono::NaiveDate;
use schedule_tool::{Schedule, ScheduleMetadata, Task};

fn d(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

#[test]
fn new_with_metadata_uses_calendar_year_range() {
    let mut metadata = ScheduleMetadata::default();
    metadata.project_start_date = d(2024, 12, 1);
    metadata.project_end_date = d(2026, 1, 31);

    let schedule = Schedule::new_with_metadata(metadata);

    assert!(!schedule.calendar().is_available(d(2024, 12, 25)));
    assert!(!schedule.calendar().is_available(d(2026, 1, 1)));
}

#[test]
fn new_with_year_range_sets_metadata_bounds() {
    let schedule = Schedule::new_with_year_range(2023, 2024);
    assert_eq!(schedule.metadata().project_start_date, d(2023, 1, 1));
    assert_eq!(schedule.metadata().project_end_date, d(2024, 12, 31));
    assert!(!schedule.calendar().is_available(d(2024, 7, 4)));
}

#[test]
fn set_metadata_refreshes_calendar() {
    let mut schedule = Schedule::new();
    let mut metadata = ScheduleMetadata::default();
    metadata.project_start_date = d(2027, 1, 1);
    metadata.project_end_date = d(2027, 12, 31);
    schedule.set_metadata(metadata);

    // New Year's Day 2027 should be unavailable in the refreshed calendar.
    assert!(!schedule.calendar().is_available(d(2027, 1, 1)));
}

#[test]
fn updating_duration_recomputes_downstream_dates() {
    let mut schedule = Schedule::new();
    let mut metadata = ScheduleMetadata::default();
    metadata.project_start_date = d(2025, 1, 6);
    metadata.project_end_date = d(2025, 2, 28);
    schedule.set_metadata(metadata);

    schedule.upsert_task(1, "T1", 2, None).unwrap();
    schedule.upsert_task(2, "T2", 2, Some(vec![1])).unwrap();

    schedule.forward_pass().unwrap();
    schedule.backward_pass().unwrap();

    let before_t2 = Task::from_dataframe_row(schedule.dataframe(), 1).unwrap();
    assert_eq!(before_t2.early_start, Some(d(2025, 1, 9)));
    assert_eq!(before_t2.early_finish, Some(d(2025, 1, 13)));

    // Increase duration of task 1; this should push task 2's early dates forward.
    schedule.upsert_task(1, "T1", 5, None).unwrap();

    let after_t2 = Task::from_dataframe_row(schedule.dataframe(), 1).unwrap();
    assert_eq!(after_t2.early_start, Some(d(2025, 1, 14)));
    assert_eq!(after_t2.early_finish, Some(d(2025, 1, 16)));
}

#[test]
fn refresh_runs_full_pipeline() {
    let mut schedule = Schedule::new();
    let mut metadata = ScheduleMetadata::default();
    metadata.project_start_date = d(2025, 1, 6);
    metadata.project_end_date = d(2025, 1, 17);
    schedule.set_metadata(metadata);

    schedule.upsert_task(1, "T1", 2, None).unwrap();
    schedule.upsert_task(2, "T2", 3, Some(vec![1])).unwrap();
    schedule.upsert_task(3, "T3", 1, Some(vec![1])).unwrap();
    schedule.upsert_task(4, "T4", 2, Some(vec![2, 3])).unwrap();

    let summary = schedule.refresh().unwrap();

    assert_eq!(summary.task_count, 4);
    assert_eq!(summary.critical_count, 3);
    assert_eq!(summary.latest_finish, Some(d(2025, 1, 17)));
    assert_eq!(summary.positive_variance_count, 0);
    assert_eq!(summary.negative_variance_count, 0);
    assert_eq!(summary.on_track_variance_count, 0);
    assert_eq!(summary.critical_path, vec![1, 2, 4]);

    let df = schedule.dataframe();
    let mut map = std::collections::HashMap::new();
    for idx in 0..df.height() {
        let task = Task::from_dataframe_row(df, idx).unwrap();
        map.insert(task.id, task);
    }

    let t1 = map.get(&1).unwrap();
    let t2 = map.get(&2).unwrap();
    let t3 = map.get(&3).unwrap();
    let t4 = map.get(&4).unwrap();

    assert_eq!(t1.early_start, Some(d(2025, 1, 6)));
    assert_eq!(t4.late_finish, Some(d(2025, 1, 17)));
    assert_eq!(t2.is_critical, Some(true));
    assert!(t3.total_float.unwrap() > 0);
    assert_eq!(t1.successors, vec![2, 3]);
    assert_eq!(t2.successors, vec![4]);
    assert!(t4.successors.is_empty());
}

#[test]
fn refresh_computes_schedule_variance_from_baseline_and_actual() {
    let mut schedule = Schedule::new();
    let mut metadata = ScheduleMetadata::default();
    metadata.project_start_date = d(2025, 1, 6);
    metadata.project_end_date = d(2025, 2, 28);
    schedule.set_metadata(metadata);

    schedule.upsert_task(1, "T1", 2, None).unwrap();

    let mut task = Task::from_dataframe_row(schedule.dataframe(), 0).unwrap();
    task.baseline_finish = Some(d(2025, 1, 8));
    task.actual_finish = Some(d(2025, 1, 10));
    schedule.upsert_task_record(task).unwrap();

    let summary = schedule.refresh().unwrap();
    assert_eq!(summary.positive_variance_count, 1);
    assert_eq!(summary.negative_variance_count, 0);
    assert_eq!(summary.on_track_variance_count, 0);
    assert!(summary.critical_path.is_empty() || summary.critical_path == vec![1]);

    let refreshed = Task::from_dataframe_row(schedule.dataframe(), 0).unwrap();
    assert_eq!(refreshed.schedule_variance_days, Some(2));
}

#[test]
fn refresh_errors_when_project_end_before_finish() {
    let mut schedule = Schedule::new();
    let mut metadata = ScheduleMetadata::default();
    metadata.project_start_date = d(2025, 1, 6);
    metadata.project_end_date = d(2025, 1, 10);
    schedule.set_metadata(metadata);

    schedule.upsert_task(1, "T1", 2, None).unwrap();
    schedule.upsert_task(2, "T2", 3, Some(vec![1])).unwrap();

    let err = schedule.refresh().expect_err("should fail horizon validation");
    assert!(err.to_string().contains("precedes schedule finish"));
}

