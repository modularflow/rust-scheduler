use super::{PersistenceError, PersistenceResult};
use crate::{
    task::{ProgressMeasurement, RationaleItem},
    Schedule, ScheduleMetadata, Task,
};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::Path;

#[derive(Serialize, Deserialize)]
struct ScheduleSnapshot {
    metadata: ScheduleMetadata,
    tasks: Vec<Task>,
}

impl ScheduleSnapshot {
    fn from_schedule(schedule: &Schedule) -> PersistenceResult<Self> {
        let df = schedule.dataframe();
        let mut tasks = Vec::with_capacity(df.height());
        for row_idx in 0..df.height() {
            tasks.push(Task::from_dataframe_row(df, row_idx)?);
        }
        super::validate_tasks(&tasks)?;
        Ok(Self {
            metadata: schedule.metadata().clone(),
            tasks,
        })
    }

    fn into_schedule(self) -> PersistenceResult<Schedule> {
        super::validate_tasks(&self.tasks)?;
        let mut schedule = Schedule::new_with_metadata(self.metadata);
        for task in self.tasks {
            schedule.upsert_task_record(task)?;
        }
        Ok(schedule)
    }
}

pub fn save_schedule_to_json<P: AsRef<Path>>(
    schedule: &Schedule,
    path: P,
) -> PersistenceResult<()> {
    let snapshot = ScheduleSnapshot::from_schedule(schedule)?;
    let file = File::create(path)?;
    serde_json::to_writer_pretty(file, &snapshot)?;
    Ok(())
}

pub fn load_schedule_from_json<P: AsRef<Path>>(path: P) -> PersistenceResult<Schedule> {
    let file = File::open(path)?;
    let snapshot: ScheduleSnapshot = serde_json::from_reader(file)?;
    snapshot.into_schedule()
}

#[derive(Serialize, Deserialize)]
struct TaskCsvRecord {
    id: i32,
    name: String,
    duration_days: i64,
    predecessors: String,
    early_start: String,
    early_finish: String,
    late_start: String,
    late_finish: String,
    baseline_start: String,
    baseline_finish: String,
    actual_start: String,
    actual_finish: String,
    percent_complete: String,
    progress_measurement: String,
    pre_defined_rationale: String,
    schedule_variance_days: String,
    total_float: String,
    is_critical: String,
    successors: String,
    parent_id: String,
    wbs_code: String,
    task_notes: String,
    task_attachments: String,
}

impl From<&Task> for TaskCsvRecord {
    fn from(task: &Task) -> Self {
        Self {
            id: task.id,
            name: task.name.clone(),
            duration_days: task.duration_days,
            predecessors: join_i32(&task.predecessors),
            early_start: format_date(task.early_start),
            early_finish: format_date(task.early_finish),
            late_start: format_date(task.late_start),
            late_finish: format_date(task.late_finish),
            baseline_start: format_date(task.baseline_start),
            baseline_finish: format_date(task.baseline_finish),
            actual_start: format_date(task.actual_start),
            actual_finish: format_date(task.actual_finish),
            percent_complete: format_option_f64(task.percent_complete),
            progress_measurement: task.progress_measurement.as_str().to_string(),
            pre_defined_rationale: serde_json::to_string(&task.pre_defined_rationale)
                .unwrap_or_else(|_| "[]".to_string()),
            schedule_variance_days: format_option_i64(task.schedule_variance_days),
            total_float: format_option_i64(task.total_float),
            is_critical: format_option_bool(task.is_critical),
            successors: join_i32(&task.successors),
            parent_id: format_option_i32(task.parent_id),
            wbs_code: task.wbs_code.clone().unwrap_or_default(),
            task_notes: task.task_notes.clone().unwrap_or_default(),
            task_attachments: join_strings(&task.task_attachments),
        }
    }
}

impl TaskCsvRecord {
    fn into_task(self) -> PersistenceResult<Task> {
        let mut task = Task::new(self.id, self.name, self.duration_days);
        task.predecessors = split_i32(&self.predecessors)?;
        task.successors = split_i32(&self.successors)?;
        task.early_start = parse_date(&self.early_start)?;
        task.early_finish = parse_date(&self.early_finish)?;
        task.late_start = parse_date(&self.late_start)?;
        task.late_finish = parse_date(&self.late_finish)?;
        task.baseline_start = parse_date(&self.baseline_start)?;
        task.baseline_finish = parse_date(&self.baseline_finish)?;
        task.actual_start = parse_date(&self.actual_start)?;
        task.actual_finish = parse_date(&self.actual_finish)?;
        task.percent_complete = parse_f64(&self.percent_complete)?;
        task.schedule_variance_days = parse_i64(&self.schedule_variance_days)?;
        task.total_float = parse_i64(&self.total_float)?;
        task.is_critical = parse_bool(&self.is_critical)?;
        task.parent_id = parse_i32(&self.parent_id)?;
        task.wbs_code = parse_string_option(self.wbs_code);
        task.task_notes = parse_string_option(self.task_notes);
        task.task_attachments = split_strings(&self.task_attachments);
        task.progress_measurement = ProgressMeasurement::from_str(self.progress_measurement.trim())
            .ok_or_else(|| {
                PersistenceError::InvalidData(format!(
                    "invalid progress_measurement '{}'",
                    self.progress_measurement
                ))
            })?;
        task.pre_defined_rationale = if self.pre_defined_rationale.trim().is_empty() {
            Vec::new()
        } else {
            serde_json::from_str::<Vec<RationaleItem>>(&self.pre_defined_rationale).map_err(
                |err| {
                    PersistenceError::InvalidData(format!(
                        "invalid pre_defined_rationale: {err}"
                    ))
                },
            )?
        };
        Ok(task)
    }
}

pub fn save_schedule_to_csv<P: AsRef<Path>>(
    schedule: &Schedule,
    path: P,
) -> PersistenceResult<()> {
    super::validate_schedule(schedule)?;
    let file = File::create(path)?;
    let mut writer = csv::Writer::from_writer(file);
    let df = schedule.dataframe();
    for row_idx in 0..df.height() {
        let task = Task::from_dataframe_row(df, row_idx)?;
        writer.serialize(TaskCsvRecord::from(&task))?;
    }
    writer.flush()?;
    Ok(())
}

pub fn load_schedule_from_csv<P: AsRef<Path>>(path: P) -> PersistenceResult<Schedule> {
    let file = File::open(path)?;
    let mut reader = csv::Reader::from_reader(file);
    let mut tasks = Vec::new();
    for record in reader.deserialize::<TaskCsvRecord>() {
        let record = record?;
        tasks.push(record.into_task()?);
    }

    if tasks.is_empty() {
        return Err(PersistenceError::InvalidData(
            "CSV file contained no tasks".into(),
        ));
    }

    super::validate_tasks(&tasks)?;

    // For CSV we do not store metadata, so default metadata is used.
    // Callers can adjust metadata after load if needed.
    let mut schedule = Schedule::new();
    for task in tasks {
        schedule.upsert_task_record(task)?;
    }
    Ok(schedule)
}

fn format_date(date: Option<NaiveDate>) -> String {
    date.map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_default()
}

fn parse_date(input: &str) -> PersistenceResult<Option<NaiveDate>> {
    if input.trim().is_empty() {
        return Ok(None);
    }
    NaiveDate::parse_from_str(input.trim(), "%Y-%m-%d")
        .map(Some)
        .map_err(|e| PersistenceError::InvalidData(format!("invalid date '{input}': {e}")))
}

fn format_option_f64(value: Option<f64>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

fn parse_f64(input: &str) -> PersistenceResult<Option<f64>> {
    if input.trim().is_empty() {
        return Ok(None);
    }
    input
        .trim()
        .parse::<f64>()
        .map(Some)
        .map_err(|e| PersistenceError::InvalidData(format!("invalid float '{input}': {e}")))
}

fn format_option_i64(value: Option<i64>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

fn parse_i64(input: &str) -> PersistenceResult<Option<i64>> {
    if input.trim().is_empty() {
        return Ok(None);
    }
    input
        .trim()
        .parse::<i64>()
        .map(Some)
        .map_err(|e| PersistenceError::InvalidData(format!("invalid integer '{input}': {e}")))
}

fn format_option_i32(value: Option<i32>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

fn parse_i32(input: &str) -> PersistenceResult<Option<i32>> {
    if input.trim().is_empty() {
        return Ok(None);
    }
    input
        .trim()
        .parse::<i32>()
        .map(Some)
        .map_err(|e| PersistenceError::InvalidData(format!("invalid integer '{input}': {e}")))
}

fn format_option_bool(value: Option<bool>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

fn parse_bool(input: &str) -> PersistenceResult<Option<bool>> {
    if input.trim().is_empty() {
        return Ok(None);
    }
    match input.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(Some(true)),
        "false" => Ok(Some(false)),
        other => Err(PersistenceError::InvalidData(format!(
            "invalid boolean '{other}'"
        ))),
    }
}

fn join_i32(values: &[i32]) -> String {
    values
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn split_i32(input: &str) -> PersistenceResult<Vec<i32>> {
    if input.trim().is_empty() {
        return Ok(Vec::new());
    }
    input
        .split(',')
        .map(|part| {
            part.trim()
                .parse::<i32>()
                .map_err(|e| PersistenceError::InvalidData(format!("invalid integer '{part}': {e}")))
        })
        .collect()
}

fn join_strings(values: &[String]) -> String {
    values.join(";")
}

fn split_strings(input: &str) -> Vec<String> {
    if input.trim().is_empty() {
        return Vec::new();
    }
    input.split(';').map(|s| s.trim().to_string()).collect()
}

fn parse_string_option(value: String) -> Option<String> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

