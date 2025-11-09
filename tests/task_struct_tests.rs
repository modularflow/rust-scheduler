use chrono::NaiveDate;
use schedule_tool::{
    task::{ProgressMeasurement, RationaleItem},
    Schedule, Task,
};

fn d(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

#[test]
fn task_roundtrips_through_schedule_dataframe() {
    let mut schedule = Schedule::new();

    let mut task = Task::new(1, "Design", 5);
    task.predecessors = vec![42];
    task.successors = vec![2, 3];
    task.percent_complete = Some(0.25);
    task.baseline_start = Some(d(2025, 1, 6));
    task.baseline_finish = Some(d(2025, 1, 10));
    task.task_notes = Some("Initial design phase".to_string());
    task.task_attachments = vec!["spec.pdf".to_string()];
    task.parent_id = Some(7);
    task.progress_measurement = ProgressMeasurement::FiftyFifty;
    task.pre_defined_rationale = vec![
        RationaleItem::new(1, "Draft", 0.5, false),
        RationaleItem::new(2, "Review", 0.5, true),
    ];

    schedule.upsert_task_record(task.clone()).unwrap();

    assert_eq!(schedule.dataframe().height(), 1);

    let row = Task::from_dataframe_row(schedule.dataframe(), 0).unwrap();

    assert_eq!(row.id, task.id);
    assert_eq!(row.name, task.name);
    assert_eq!(row.predecessors, task.predecessors);
    assert_eq!(row.successors, task.successors);
    assert_eq!(row.percent_complete, task.percent_complete);
    assert_eq!(row.baseline_start, task.baseline_start);
    assert_eq!(row.baseline_finish, task.baseline_finish);
    assert_eq!(row.task_notes, task.task_notes);
    assert_eq!(row.task_attachments, task.task_attachments);
    assert_eq!(row.parent_id, task.parent_id);
    assert_eq!(row.progress_measurement, task.progress_measurement);
    assert_eq!(row.pre_defined_rationale, task.pre_defined_rationale);
}

