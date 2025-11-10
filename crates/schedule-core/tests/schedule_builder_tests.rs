use chrono::{Datelike, Duration, NaiveDate, Weekday};
use schedule_tool::{
    ProgressMeasurement, ProgressRationaleTemplate, Schedule, ScheduleMetadata,
    ScheduleMetadataError, Task, WorkCalendar, WorkCalendarConfig,
};

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
    schedule.set_metadata(metadata).unwrap();

    // New Year's Day 2027 should be unavailable in the refreshed calendar.
    assert!(!schedule.calendar().is_available(d(2027, 1, 1)));
}

#[test]
fn updating_duration_recomputes_downstream_dates() {
    let mut schedule = Schedule::new();
    let mut metadata = ScheduleMetadata::default();
    metadata.project_start_date = d(2025, 1, 6);
    metadata.project_end_date = d(2025, 2, 28);
    schedule.set_metadata(metadata).unwrap();

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
    schedule.set_metadata(metadata).unwrap();

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
    schedule.set_metadata(metadata).unwrap();

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
    schedule.set_metadata(metadata).unwrap();

    schedule.upsert_task(1, "T1", 2, None).unwrap();
    schedule.upsert_task(2, "T2", 3, Some(vec![1])).unwrap();

    let err = schedule
        .refresh()
        .expect_err("should fail horizon validation");
    assert!(err.to_string().contains("precedes schedule finish"));
}

#[test]
fn set_metadata_preserves_custom_calendar_flag() {
    let mut schedule = Schedule::new();
    let custom_calendar = WorkCalendar::custom(
        vec![
            Weekday::Mon,
            Weekday::Tue,
            Weekday::Wed,
            Weekday::Thu,
            Weekday::Sat,
        ],
        vec![d(2025, 6, 19), d(2025, 7, 3)],
    );
    schedule.set_calendar(custom_calendar.clone()).unwrap();
    assert!(schedule.calendar_is_custom());

    let mut metadata = schedule.metadata().clone();
    metadata.project_end_date = metadata.project_end_date + Duration::days(7);
    schedule.set_metadata(metadata).unwrap();

    assert!(schedule.calendar_is_custom());
    assert_eq!(schedule.calendar().to_config(), custom_calendar.to_config());
}

#[test]
fn set_metadata_regenerates_default_calendar_when_not_custom() {
    let mut schedule = Schedule::new();
    assert!(!schedule.calendar_is_custom());

    let mut metadata = schedule.metadata().clone();
    metadata.project_end_date = d(2026, 12, 31);
    schedule.set_metadata(metadata.clone()).unwrap();

    assert!(!schedule.calendar_is_custom());
    let expected = WorkCalendar::with_year_range(
        metadata.project_start_date.year(),
        metadata.project_end_date.year(),
    )
    .to_config();
    assert_eq!(schedule.calendar().to_config(), expected);
}

#[test]
fn reset_calendar_to_default_clears_custom_flag() {
    let mut schedule = Schedule::new();
    let custom_calendar = WorkCalendar::custom(
        vec![
            Weekday::Mon,
            Weekday::Tue,
            Weekday::Wed,
            Weekday::Thu,
            Weekday::Sat,
        ],
        vec![d(2025, 6, 19)],
    );
    schedule.set_calendar(custom_calendar).unwrap();
    assert!(schedule.calendar_is_custom());

    schedule.reset_calendar_to_default().unwrap();

    assert!(!schedule.calendar_is_custom());
    let expected = WorkCalendar::with_year_range(
        schedule.metadata().project_start_date.year(),
        schedule.metadata().project_end_date.year(),
    )
    .to_config();
    assert_eq!(schedule.calendar().to_config(), expected);
}

#[test]
fn upsert_task_record_rejects_invalid_progress_combination() {
    let mut schedule = Schedule::new();
    let mut task = Task::new(1, "Invalid", 5);
    task.progress_measurement = ProgressMeasurement::ZeroOneHundred;
    task.percent_complete = Some(0.3);

    let err = schedule
        .upsert_task_record(task)
        .expect_err("should reject invalid progress data");
    assert!(
        err.to_string()
            .contains("progress_measurement=0_100 requires percent_complete")
    );
}

#[test]
fn apply_rationale_template_updates_task() {
    let mut schedule = Schedule::new();
    schedule.upsert_task(1, "Task", 5, None).unwrap();
    schedule
        .apply_rationale_template(1, ProgressRationaleTemplate::FiftyFifty)
        .unwrap();
    let task = schedule
        .find_task(1)
        .unwrap()
        .expect("task should exist after applying template");
    assert_eq!(
        task.progress_measurement,
        ProgressMeasurement::PreDefinedRationale
    );
    assert_eq!(task.pre_defined_rationale.len(), 2);
}

#[test]
fn set_calendar_from_config_applies_configuration() {
    let mut schedule = Schedule::new();
    let calendar = WorkCalendar::custom(
        vec![Weekday::Mon, Weekday::Tue, Weekday::Wed],
        vec![d(2025, 3, 1)],
    );
    let config: WorkCalendarConfig = calendar.to_config();

    schedule.set_calendar_from_config(&config).unwrap();

    assert!(schedule.calendar_is_custom());
    assert_eq!(schedule.calendar().to_config(), config);
}

#[test]
fn set_project_dates_validates_order() {
    let mut schedule = Schedule::new();
    let err = schedule
        .set_project_dates(d(2025, 2, 1), d(2025, 1, 1))
        .expect_err("start after end should error");
    assert!(matches!(err, ScheduleMetadataError::StartAfterEnd { .. }));
}

#[test]
fn set_project_end_date_rejects_finish_before_schedule() {
    let mut schedule = Schedule::new();
    schedule
        .set_project_dates(d(2025, 1, 1), d(2025, 2, 28))
        .unwrap();

    schedule.upsert_task(1, "T1", 40, None).unwrap();
    schedule.refresh().unwrap();

    let err = schedule
        .set_project_end_date(d(2025, 1, 15))
        .expect_err("project end before finish should error");
    assert!(matches!(
        err,
        ScheduleMetadataError::EndPrecedesScheduleFinish { .. }
    ));
}

#[test]
fn set_project_dates_updates_calendar_when_not_custom() {
    let mut schedule = Schedule::new();
    schedule
        .set_project_dates(d(2026, 1, 1), d(2026, 12, 31))
        .unwrap();

    assert_eq!(schedule.project_start_date(), d(2026, 1, 1));
    assert_eq!(schedule.project_end_date(), d(2026, 12, 31));
    assert!(!schedule.calendar().is_available(d(2026, 12, 25)));
}
