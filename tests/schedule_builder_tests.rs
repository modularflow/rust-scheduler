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

