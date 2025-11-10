use super::{PersistenceError, PersistenceResult};
use crate::{
    Schedule, ScheduleMetadata, Task,
    calendar::{WorkCalendar, WorkCalendarConfig},
    resource::ResourceAllocation,
    task::{ProgressMeasurement, RationaleItem},
};
use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::Path;

#[derive(Serialize, Deserialize)]
struct ScheduleSnapshot {
    metadata: ScheduleMetadata,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    calendar: Option<WorkCalendarConfig>,
    #[serde(default)]
    calendar_is_custom: bool,
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
            calendar: Some(schedule.calendar_config()),
            calendar_is_custom: schedule.calendar_is_custom(),
            tasks,
        })
    }

    fn into_schedule(self) -> PersistenceResult<Schedule> {
        super::validate_tasks(&self.tasks)?;
        let calendar = self
            .calendar
            .map(|config| WorkCalendar::from_config(&config))
            .unwrap_or_else(|| {
                WorkCalendar::with_year_range(
                    self.metadata.project_start_date.year(),
                    self.metadata.project_end_date.year(),
                )
            });

        let mut schedule = Schedule::from_parts(self.metadata, calendar, self.calendar_is_custom);
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

#[derive(Default, Serialize, Deserialize)]
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
    #[serde(default)]
    resource_allocations: String,
    #[serde(default)]
    metadata_json: String,
    #[serde(default)]
    calendar_json: String,
    #[serde(default)]
    calendar_is_custom: String,
}

impl From<&Task> for TaskCsvRecord {
    fn from(task: &Task) -> Self {
        let mut record = TaskCsvRecord::default();
        record.id = task.id;
        record.name = task.name.clone();
        record.duration_days = task.duration_days;
        record.predecessors = join_i32(&task.predecessors);
        record.early_start = format_date(task.early_start);
        record.early_finish = format_date(task.early_finish);
        record.late_start = format_date(task.late_start);
        record.late_finish = format_date(task.late_finish);
        record.baseline_start = format_date(task.baseline_start);
        record.baseline_finish = format_date(task.baseline_finish);
        record.actual_start = format_date(task.actual_start);
        record.actual_finish = format_date(task.actual_finish);
        record.percent_complete = format_option_f64(task.percent_complete);
        record.progress_measurement = task.progress_measurement.as_str().to_string();
        record.pre_defined_rationale = serde_json::to_string(&task.pre_defined_rationale)
            .unwrap_or_else(|_| "[]".to_string());
        record.schedule_variance_days = format_option_i64(task.schedule_variance_days);
        record.total_float = format_option_i64(task.total_float);
        record.is_critical = format_option_bool(task.is_critical);
        record.successors = join_i32(&task.successors);
        record.parent_id = format_option_i32(task.parent_id);
        record.wbs_code = task.wbs_code.clone().unwrap_or_default();
        record.task_notes = task.task_notes.clone().unwrap_or_default();
        record.task_attachments = join_strings(&task.task_attachments);
        record.resource_allocations = serde_json::to_string(&task.resource_allocations)
            .unwrap_or_else(|_| "[]".to_string());
        record
    }
}

impl TaskCsvRecord {
    fn metadata_row(schedule: &Schedule) -> PersistenceResult<Self> {
        let metadata_json = serde_json::to_string(schedule.metadata())?;
        let calendar_json = serde_json::to_string(&schedule.calendar_config())?;
        let mut record = TaskCsvRecord::default();
        record.name = "__metadata__".to_string();
        record.metadata_json = metadata_json;
        record.calendar_json = calendar_json;
        record.calendar_is_custom = schedule.calendar_is_custom().to_string();
        Ok(record)
    }

    fn is_metadata_row(&self) -> bool {
        !self.metadata_json.trim().is_empty()
    }

    fn into_task(self) -> PersistenceResult<Task> {
        if self.is_metadata_row() {
            return Err(PersistenceError::InvalidData(
                "metadata row cannot be converted to task".into(),
            ));
        }
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
                    PersistenceError::InvalidData(format!("invalid pre_defined_rationale: {err}"))
                },
            )?
        };
        task.resource_allocations = if self.resource_allocations.trim().is_empty() {
            Vec::new()
        } else {
            serde_json::from_str::<Vec<ResourceAllocation>>(&self.resource_allocations).map_err(
                |err| {
                    PersistenceError::InvalidData(format!("invalid resource_allocations: {err}"))
                },
            )?
        };
        Ok(task)
    }
}

pub fn save_schedule_to_csv<P: AsRef<Path>>(schedule: &Schedule, path: P) -> PersistenceResult<()> {
    super::validate_schedule(schedule)?;
    let file = File::create(path)?;
    let mut writer = csv::Writer::from_writer(file);
    writer.serialize(TaskCsvRecord::metadata_row(schedule)?)?;
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
    let mut metadata: Option<ScheduleMetadata> = None;
    let mut calendar_config: Option<WorkCalendarConfig> = None;
    let mut calendar_is_custom = false;
    for record in reader.deserialize::<TaskCsvRecord>() {
        let record = record?;
        if record.is_metadata_row() {
            if metadata.is_some() {
                return Err(PersistenceError::InvalidData(
                    "CSV file contained multiple metadata rows".into(),
                ));
            }
            if !record.metadata_json.trim().is_empty() {
                metadata = Some(serde_json::from_str(&record.metadata_json).map_err(|err| {
                    PersistenceError::InvalidData(format!("invalid metadata json: {err}"))
                })?);
            }
            if !record.calendar_json.trim().is_empty() {
                calendar_config =
                    Some(serde_json::from_str(&record.calendar_json).map_err(|err| {
                        PersistenceError::InvalidData(format!("invalid calendar json: {err}"))
                    })?);
            }
            if !record.calendar_is_custom.trim().is_empty() {
                calendar_is_custom = record
                    .calendar_is_custom
                    .trim()
                    .parse::<bool>()
                    .unwrap_or(false);
            }
            continue;
        }
        tasks.push(record.into_task()?);
    }

    if tasks.is_empty() {
        return Err(PersistenceError::InvalidData(
            "CSV file contained no tasks".into(),
        ));
    }

    super::validate_tasks(&tasks)?;

    let mut schedule = if let Some(metadata) = metadata {
        let (calendar, has_custom_config) = if let Some(config) = calendar_config {
            (WorkCalendar::from_config(&config), true)
        } else {
            (
                WorkCalendar::with_year_range(
                    metadata.project_start_date.year(),
                    metadata.project_end_date.year(),
                ),
                false,
            )
        };
        Schedule::from_parts(metadata, calendar, calendar_is_custom && has_custom_config)
    } else {
        Schedule::new()
    };
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
            part.trim().parse::<i32>().map_err(|e| {
                PersistenceError::InvalidData(format!("invalid integer '{part}': {e}"))
            })
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
