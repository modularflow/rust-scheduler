use crate::calculations::backward_pass::BackwardPass as CalcBackwardPass;
use crate::calculations::forward_pass::ForwardPass as CalcForwardPass;
use crate::calendar::{WorkCalendar, WorkCalendarConfig};
use crate::metadata::ScheduleMetadata;
use crate::task::{ProgressRationaleTemplate, Task};
use crate::task_validation::{self, TaskValidationError};
use chrono::{Datelike, Duration, NaiveDate};
use polars::prelude::PlSmallStr;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshSummary {
    pub task_count: usize,
    pub critical_count: usize,
    pub critical_path: Vec<i32>,
    pub latest_finish: Option<NaiveDate>,
    pub positive_variance_count: usize,
    pub negative_variance_count: usize,
    pub on_track_variance_count: usize,
}

impl RefreshSummary {
    pub fn to_cli_summary(&self) -> String {
        let mut parts = Vec::new();
        parts.push(format!("tasks={}", self.task_count));
        parts.push(format!("critical={}", self.critical_count));
        if let Some(date) = self.latest_finish {
            parts.push(format!("finish={}", date));
        }
        if self.positive_variance_count > 0 {
            parts.push(format!("variance+={}", self.positive_variance_count));
        }
        if self.negative_variance_count > 0 {
            parts.push(format!("variance-={}", self.negative_variance_count));
        }
        if self.on_track_variance_count > 0 {
            parts.push(format!("variance0={}", self.on_track_variance_count));
        }
        if !self.critical_path.is_empty() {
            let chain = self
                .critical_path
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("->");
            parts.push(format!("crit_path={}", chain));
        }
        parts.join(", ")
    }
}

#[derive(Debug, Clone)]
pub enum ScheduleMetadataError {
    StartAfterEnd {
        start: NaiveDate,
        end: NaiveDate,
    },
    EndPrecedesScheduleFinish {
        project_end: NaiveDate,
        required_finish: NaiveDate,
    },
    Computation(String),
}

impl fmt::Display for ScheduleMetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScheduleMetadataError::StartAfterEnd { start, end } => write!(
                f,
                "project start date {start} must be on or before project end date {end}"
            ),
            ScheduleMetadataError::EndPrecedesScheduleFinish {
                project_end,
                required_finish,
            } => write!(
                f,
                "project end date {project_end} is before the current schedule finish {required_finish}"
            ),
            ScheduleMetadataError::Computation(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for ScheduleMetadataError {}

pub struct Schedule {
    df: DataFrame,
    metadata: ScheduleMetadata,
    calendar: WorkCalendar,
    calendar_is_custom: bool,
}

impl Schedule {
    pub(crate) fn from_parts(
        metadata: ScheduleMetadata,
        calendar: WorkCalendar,
        calendar_is_custom: bool,
    ) -> Self {
        let schema = Self::default_schema();
        let schedule = DataFrame::empty_with_schema(&schema);

        Self {
            df: schedule,
            metadata,
            calendar,
            calendar_is_custom,
        }
    }

    fn validate_metadata_dates(metadata: &ScheduleMetadata) -> Result<(), ScheduleMetadataError> {
        if metadata.project_start_date > metadata.project_end_date {
            return Err(ScheduleMetadataError::StartAfterEnd {
                start: metadata.project_start_date,
                end: metadata.project_end_date,
            });
        }
        Ok(())
    }

    fn validate_schedule_finish_against_metadata(
        &self,
        metadata: &ScheduleMetadata,
    ) -> Result<(), ScheduleMetadataError> {
        if self.df.height() == 0 {
            return Ok(());
        }
        let latest_finish = self
            .latest_early_finish()
            .map_err(|err| ScheduleMetadataError::Computation(err.to_string()))?;
        if let Some(required_finish) = latest_finish {
            if required_finish > metadata.project_end_date {
                return Err(ScheduleMetadataError::EndPrecedesScheduleFinish {
                    project_end: metadata.project_end_date,
                    required_finish,
                });
            }
        }
        Ok(())
    }

    fn validate_metadata(&self, metadata: &ScheduleMetadata) -> Result<(), ScheduleMetadataError> {
        Self::validate_metadata_dates(metadata)?;
        self.validate_schedule_finish_against_metadata(metadata)?;
        Ok(())
    }

    fn apply_metadata(&mut self, metadata: ScheduleMetadata) {
        self.metadata = metadata;
        if !self.calendar_is_custom {
            self.calendar = Self::calendar_for_metadata(&self.metadata);
        }
    }

    fn update_metadata_with<F>(&mut self, mutator: F) -> Result<(), ScheduleMetadataError>
    where
        F: FnOnce(&mut ScheduleMetadata),
    {
        let mut metadata = self.metadata.clone();
        mutator(&mut metadata);
        self.set_metadata(metadata)
    }

    pub fn new() -> Self {
        let metadata = ScheduleMetadata::default();
        let calendar = Self::calendar_for_metadata(&metadata);
        Self::from_parts(metadata, calendar, false)
    }

    pub fn new_with_metadata(metadata: ScheduleMetadata) -> Self {
        let calendar = Self::calendar_for_metadata(&metadata);
        Self::from_parts(metadata, calendar, false)
    }

    pub fn new_with_year_range(start_year: i32, end_year: i32) -> Self {
        let start =
            NaiveDate::from_ymd_opt(start_year, 1, 1).expect("invalid start year for schedule");
        let end = NaiveDate::from_ymd_opt(end_year, 12, 31).expect("invalid end year for schedule");
        let mut metadata = ScheduleMetadata::default();
        metadata.project_start_date = start;
        metadata.project_end_date = end;
        Self::new_with_metadata(metadata)
    }

    pub fn new_with_metadata_and_calendar(
        metadata: ScheduleMetadata,
        calendar: WorkCalendar,
    ) -> Self {
        Self::from_parts(metadata, calendar, true)
    }

    pub fn set_metadata(
        &mut self,
        metadata: ScheduleMetadata,
    ) -> Result<(), ScheduleMetadataError> {
        self.validate_metadata(&metadata)?;
        self.apply_metadata(metadata);
        Ok(())
    }

    pub fn dataframe(&self) -> &DataFrame {
        &self.df
    }

    pub fn metadata(&self) -> &ScheduleMetadata {
        &self.metadata
    }

    pub fn project_name(&self) -> &str {
        &self.metadata.project_name
    }

    pub fn project_description(&self) -> &str {
        &self.metadata.project_description
    }

    pub fn project_start_date(&self) -> NaiveDate {
        self.metadata.project_start_date
    }

    pub fn project_end_date(&self) -> NaiveDate {
        self.metadata.project_end_date
    }

    pub fn calendar(&self) -> &WorkCalendar {
        &self.calendar
    }

    pub fn calendar_is_custom(&self) -> bool {
        self.calendar_is_custom
    }

    pub fn calendar_config(&self) -> WorkCalendarConfig {
        self.calendar.to_config()
    }

    pub fn set_project_name(&mut self, name: impl Into<String>) {
        self.metadata.project_name = name.into();
    }

    pub fn set_project_description(&mut self, description: impl Into<String>) {
        self.metadata.project_description = description.into();
    }

    pub fn set_project_start_date(&mut self, date: NaiveDate) -> Result<(), ScheduleMetadataError> {
        self.update_metadata_with(|metadata| {
            metadata.project_start_date = date;
        })
    }

    pub fn set_project_end_date(&mut self, date: NaiveDate) -> Result<(), ScheduleMetadataError> {
        self.update_metadata_with(|metadata| {
            metadata.project_end_date = date;
        })
    }

    pub fn set_project_dates(
        &mut self,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<(), ScheduleMetadataError> {
        self.update_metadata_with(|metadata| {
            metadata.project_start_date = start;
            metadata.project_end_date = end;
        })
    }

    pub fn tasks(&self) -> Result<Vec<Task>, PolarsError> {
        let df = self.dataframe();
        let mut tasks = Vec::with_capacity(df.height());
        for idx in 0..df.height() {
            tasks.push(Task::from_dataframe_row(df, idx)?);
        }
        Ok(tasks)
    }

    pub fn find_task(&self, task_id: i32) -> Result<Option<Task>, PolarsError> {
        if self.df.height() == 0 {
            return Ok(None);
        }
        let ids = self.df.column("id")?.i32()?;
        for (idx, id_opt) in ids.into_iter().enumerate() {
            if id_opt == Some(task_id) {
                let task = Task::from_dataframe_row(self.dataframe(), idx)?;
                return Ok(Some(task));
            }
        }
        Ok(None)
    }

    pub fn delete_task(&mut self, task_id: i32) -> Result<bool, PolarsError> {
        if self.df.height() == 0 {
            return Ok(false);
        }
        let snapshot = self.df.clone();
        let mut tasks: Vec<Task> = Vec::with_capacity(snapshot.height());
        let mut found = false;
        for idx in 0..snapshot.height() {
            let mut task = Task::from_dataframe_row(&snapshot, idx)?;
            if task.id == task_id {
                found = true;
                continue;
            }
            task.predecessors.retain(|&pred| pred != task_id);
            task.successors.retain(|&succ| succ != task_id);
            tasks.push(task);
        }
        if !found {
            return Ok(false);
        }

        self.df = DataFrame::empty_with_schema(&Self::default_schema());
        for task in tasks {
            self.upsert_task_record(task)?;
        }
        self.refresh()?; // Recompute schedule after structural change
        Ok(true)
    }

    pub fn set_calendar_from_config(
        &mut self,
        config: &WorkCalendarConfig,
    ) -> Result<(), PolarsError> {
        let calendar = WorkCalendar::from_config(config);
        self.set_calendar(calendar)
    }

    fn calendar_for_metadata(metadata: &ScheduleMetadata) -> WorkCalendar {
        let start_year = metadata.project_start_date.year();
        let end_year = metadata.project_end_date.year();
        WorkCalendar::with_year_range(start_year, end_year)
    }

    fn default_schema() -> Schema {
        let schema = Schema::from_iter(vec![
            Field::new("id".into(), DataType::Int32),
            Field::new("name".into(), DataType::String),
            Field::new("duration_days".into(), DataType::Int64),
            Field::new(
                "predecessors".into(),
                DataType::List(Box::new(DataType::Int32)),
            ),
            Field::new("early_start".into(), DataType::Date),
            Field::new("early_finish".into(), DataType::Date),
            Field::new("late_start".into(), DataType::Date),
            Field::new("late_finish".into(), DataType::Date),
            Field::new("baseline_start".into(), DataType::Date),
            Field::new("baseline_finish".into(), DataType::Date),
            Field::new("actual_start".into(), DataType::Date),
            Field::new("actual_finish".into(), DataType::Date),
            Field::new("percent_complete".into(), DataType::Float64),
            Field::new("progress_measurement".into(), DataType::String),
            Field::new("pre_defined_rationale".into(), DataType::String),
            Field::new("schedule_variance_days".into(), DataType::Int64),
            Field::new("total_float".into(), DataType::Int64),
            Field::new("is_critical".into(), DataType::Boolean),
            Field::new(
                "successors".into(),
                DataType::List(Box::new(DataType::Int32)),
            ),
            Field::new("parent_id".into(), DataType::Int32),
            Field::new("wbs_code".into(), DataType::String),
            Field::new("task_notes".into(), DataType::String),
            Field::new(
                "task_attachments".into(),
                DataType::List(Box::new(DataType::String)),
            ),
            Field::new("resource_allocations".into(), DataType::String),
        ]);
        schema
    }

    fn update_string_column(
        &mut self,
        column_name: &str,
        task_id: i32,
        new_value: &str,
    ) -> Result<(), PolarsError> {
        let id_col = self.df.column("id")?;
        let target_col = self.df.column(column_name)?;

        // Create new series with conditional values
        let new_series = target_col
            .str()?
            .into_iter()
            .zip(id_col.i32()?.into_iter())
            .map(|(val, id)| {
                if id == Some(task_id) {
                    Some(new_value)
                } else {
                    val
                }
            })
            .collect::<StringChunked>()
            .into_series()
            .with_name(column_name.into());

        self.df.replace(column_name, new_series)?;
        Ok(())
    }

    fn update_i32_column(
        &mut self,
        column_name: &str,
        task_id: i32,
        new_value: i32,
    ) -> Result<(), PolarsError> {
        let id_col = self.df.column("id")?;
        let target_col = self.df.column(column_name)?;

        // Create new series with conditional values
        let new_series = target_col
            .i32()?
            .into_iter()
            .zip(id_col.i32()?.into_iter())
            .map(|(val, id)| {
                if id == Some(task_id) {
                    Some(new_value)
                } else {
                    val
                }
            })
            .collect::<Int32Chunked>()
            .into_series()
            .with_name(column_name.into());

        self.df.replace(column_name, new_series)?;
        Ok(())
    }

    fn update_i64_column(
        &mut self,
        column_name: &str,
        task_id: i32,
        new_value: i64,
    ) -> Result<(), PolarsError> {
        let id_col = self.df.column("id")?;
        let target_col = self.df.column(column_name)?;

        let new_series = target_col
            .i64()?
            .into_iter()
            .zip(id_col.i32()?.into_iter())
            .map(|(val, id)| {
                if id == Some(task_id) {
                    Some(new_value)
                } else {
                    val
                }
            })
            .collect::<Int64Chunked>()
            .into_series()
            .with_name(column_name.into());

        self.df.replace(column_name, new_series)?;
        Ok(())
    }

    fn update_list_i32_column(
        &mut self,
        column_name: &str,
        task_id: i32,
        new_values: Vec<i32>,
    ) -> Result<(), PolarsError> {
        let id_col = self.df.column("id")?;
        let target_col = self.df.column(column_name)?;

        let new_series = target_col
            .list()?
            .into_iter()
            .zip(id_col.i32()?.into_iter())
            .map(|(val, id)| {
                if id == Some(task_id) {
                    // Replace with new list value
                    Some(Series::new(PlSmallStr::from_static(""), new_values.clone()))
                } else {
                    val
                }
            })
            .collect::<ListChunked>()
            .into_series()
            .with_name(column_name.into());

        self.df.replace(column_name, new_series)?;
        Ok(())
    }

    fn update_list_str_column(
        &mut self,
        column_name: &str,
        task_id: i32,
        new_values: Vec<String>,
    ) -> Result<(), PolarsError> {
        let id_col = self.df.column("id")?;
        let target_col = self.df.column(column_name)?;

        let replacement = Series::new(PlSmallStr::from_static(""), new_values);
        let new_series = target_col
            .list()?
            .into_iter()
            .zip(id_col.i32()?.into_iter())
            .map(|(val, id)| {
                if id == Some(task_id) {
                    Some(replacement.clone())
                } else {
                    val
                }
            })
            .collect::<ListChunked>()
            .into_series()
            .with_name(column_name.into());

        self.df.replace(column_name, new_series)?;
        Ok(())
    }

    fn update_float_column(
        &mut self,
        column_name: &str,
        task_id: i32,
        new_value: f64,
    ) -> Result<(), PolarsError> {
        let id_col = self.df.column("id")?;
        let target_col = self.df.column(column_name)?;

        // Create new series with conditional values
        let new_series = target_col
            .f64()?
            .into_iter()
            .zip(id_col.i32()?.into_iter())
            .map(|(val, id)| {
                if id == Some(task_id) {
                    Some(new_value)
                } else {
                    val
                }
            })
            .collect::<Float64Chunked>()
            .into_series()
            .with_name(column_name.into());

        self.df.replace(column_name, new_series)?;
        Ok(())
    }

    fn update_bool_column(
        &mut self,
        column_name: &str,
        task_id: i32,
        new_value: bool,
    ) -> Result<(), PolarsError> {
        let id_col = self.df.column("id")?;
        let target_col = self.df.column(column_name)?;

        // Create new series with conditional values
        let new_series = target_col
            .bool()?
            .into_iter()
            .zip(id_col.i32()?.into_iter())
            .map(|(val, id)| {
                if id == Some(task_id) {
                    Some(new_value)
                } else {
                    val
                }
            })
            .collect::<BooleanChunked>()
            .into_series()
            .with_name(column_name.into());

        self.df.replace(column_name, new_series)?;
        Ok(())
    }

    fn update_date_column(
        &mut self,
        column_name: &str,
        task_id: i32,
        new_date: NaiveDate,
    ) -> Result<(), PolarsError> {
        self.df = self
            .df
            .clone()
            .lazy()
            .with_column(
                when(col("id").eq(lit(task_id)))
                    .then(lit(new_date).cast(DataType::Date))
                    .otherwise(col(column_name).cast(DataType::Date))
                    .alias(column_name),
            )
            .collect()?;
        Ok(())
    }

    fn update_duration_column(
        &mut self,
        task_id: i32,
        new_duration_days: i64,
    ) -> Result<(), PolarsError> {
        self.update_i64_column("duration_days", task_id, new_duration_days)?;
        // Duration changes ripple through schedule calculations; recompute dates.
        self.forward_pass()?;
        self.backward_pass()?;
        Ok(())
    }

    /// Convert NaiveDate to Polars i32 date
    fn date_to_i32(date: NaiveDate) -> i32 {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        (date - epoch).num_days() as i32
    }

    fn i32_to_date(days: i32) -> NaiveDate {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        epoch + Duration::days(days as i64)
    }

    fn date_from_chunk(chunk: &DateChunked, idx: usize) -> Option<NaiveDate> {
        chunk.get(idx).map(Self::i32_to_date)
    }

    fn working_days_diff(cal: &WorkCalendar, baseline: NaiveDate, actual: NaiveDate) -> i64 {
        if baseline == actual {
            0
        } else if actual > baseline {
            cal.count_available_days(baseline, actual) - 1
        } else {
            let days = cal.count_available_days(actual, baseline) - 1;
            -(days)
        }
    }

    fn latest_early_finish(&self) -> Result<Option<NaiveDate>, PolarsError> {
        if self.df.height() == 0 {
            return Ok(None);
        }
        let early_finish = self.df.column("early_finish")?.date()?;
        let mut latest: Option<NaiveDate> = None;
        for idx in 0..early_finish.len() {
            if let Some(days) = early_finish.get(idx) {
                let candidate = Self::i32_to_date(days);
                latest = Some(match latest {
                    Some(current) if current >= candidate => current,
                    _ => candidate,
                });
            }
        }
        Ok(latest)
    }

    fn set_schedule_variance(&mut self) -> Result<(), PolarsError> {
        let height = self.df.height();
        let baseline_finish = self.df.column("baseline_finish")?.date()?;
        let actual_finish = self.df.column("actual_finish")?.date()?;
        let baseline_start = self.df.column("baseline_start")?.date()?;
        let actual_start = self.df.column("actual_start")?.date()?;

        let mut values: Vec<Option<i64>> = Vec::with_capacity(height);
        for idx in 0..height {
            let variance = match (
                Self::date_from_chunk(&baseline_finish, idx),
                Self::date_from_chunk(&actual_finish, idx),
            ) {
                (Some(bf), Some(af)) => Some(Self::working_days_diff(&self.calendar, bf, af)),
                _ => match (
                    Self::date_from_chunk(&baseline_start, idx),
                    Self::date_from_chunk(&actual_start, idx),
                ) {
                    (Some(bs), Some(as_)) => Some(Self::working_days_diff(&self.calendar, bs, as_)),
                    _ => None,
                },
            };
            values.push(variance);
        }
        let series = Series::new(PlSmallStr::from_static("schedule_variance_days"), values);
        self.df.replace("schedule_variance_days", series)?;
        Ok(())
    }

    fn set_successors_column(&mut self) -> Result<(), PolarsError> {
        let id_col = self.df.column("id")?.i32()?;
        let predecessors = self.df.column("predecessors")?.list()?;

        let ids: Vec<Option<i32>> = id_col.into_iter().collect();
        let mut successors_map: HashMap<i32, Vec<i32>> = HashMap::new();
        for opt_id in ids.iter().flatten() {
            successors_map.entry(*opt_id).or_default();
        }

        for (idx, maybe_id) in ids.iter().enumerate() {
            if let Some(task_id) = maybe_id {
                if let Some(series) = predecessors.get_as_series(idx) {
                    let pred_col = series.i32()?;
                    for pred in pred_col.into_iter().flatten() {
                        successors_map.entry(pred).or_default().push(*task_id);
                    }
                }
            }
        }

        let successor_rows: Vec<Series> = ids
            .into_iter()
            .map(|maybe_id| {
                let list = if let Some(id) = maybe_id {
                    let mut list = successors_map.get(&id).cloned().unwrap_or_default();
                    list.sort_unstable();
                    list.dedup();
                    list
                } else {
                    Vec::new()
                };
                Series::new(PlSmallStr::from_static(""), list)
            })
            .collect();

        let list_chunked: ListChunked = successor_rows.into_iter().collect();
        self.df.replace("successors", list_chunked.into_series())?;
        Ok(())
    }

    fn validate_project_horizon(&self) -> Result<(), PolarsError> {
        if self.metadata.project_start_date > self.metadata.project_end_date {
            return Err(PolarsError::ComputeError(
                "project_end_date must be on or after project_start_date".into(),
            ));
        }

        if let Some(latest_finish) = self.latest_early_finish()? {
            if latest_finish > self.metadata.project_end_date {
                return Err(PolarsError::ComputeError(
                    format!(
                        "project_end_date {} precedes schedule finish {}",
                        self.metadata.project_end_date, latest_finish
                    )
                    .into(),
                ));
            }
        }
        Ok(())
    }

    pub fn forward_pass(&mut self) -> Result<(), PolarsError> {
        if self.df.height() == 0 {
            return Ok(());
        }
        let engine = CalcForwardPass::new(&self.df, &self.calendar);
        let results = engine.execute(self.metadata.project_start_date)?;

        // Persist results into early_start / early_finish
        let id_ca = self.df.column("id")?.i32()?;
        let height = self.df.height();
        let mut start_vals: Vec<Option<i32>> = vec![None; height];
        let mut finish_vals: Vec<Option<i32>> = vec![None; height];

        for (idx, id_opt) in id_ca.into_iter().enumerate() {
            if let Some(task_id) = id_opt {
                if let Some((es, ef)) = results.get(&task_id) {
                    start_vals[idx] = Some(Self::date_to_i32(*es));
                    finish_vals[idx] = Some(Self::date_to_i32(*ef));
                } else {
                    // Fallback compute for tasks not covered by the engine (e.g., join points)
                    let preds_lc = self.df.column("predecessors")?.list()?;
                    let duration = self
                        .df
                        .column("duration_days")?
                        .i64()?
                        .get(idx)
                        .unwrap_or(0);
                    let pred_ids: Vec<i32> = if let Some(series) = preds_lc.get_as_series(idx) {
                        series.i32()?.into_iter().flatten().collect()
                    } else {
                        Vec::new()
                    };
                    let project_start = self.metadata.project_start_date;
                    let early_start = if pred_ids.is_empty() {
                        project_start
                    } else {
                        let max_pred_finish = pred_ids
                            .iter()
                            .filter_map(|p| results.get(p).map(|(_, ef)| *ef))
                            .max()
                            .unwrap_or(project_start);
                        self.calendar.next_available(max_pred_finish)
                    };
                    let early_finish = self.calendar.find_next_available(early_start, duration);
                    start_vals[idx] = Some(Self::date_to_i32(early_start));
                    finish_vals[idx] = Some(Self::date_to_i32(early_finish));
                }
            }
        }

        let start_series = Series::new(PlSmallStr::from_static("early_start"), start_vals)
            .cast(&DataType::Date)?;
        let finish_series = Series::new(PlSmallStr::from_static("early_finish"), finish_vals)
            .cast(&DataType::Date)?;
        self.df.replace("early_start", start_series)?;
        self.df.replace("early_finish", finish_series)?;

        Ok(())
    }

    pub fn backward_pass(&mut self) -> Result<(), PolarsError> {
        if self.df.height() == 0 {
            return Ok(());
        }
        // Compute late dates using petgraph engine
        let engine = CalcBackwardPass::new(&self.df, &self.calendar);
        let results = engine.execute(self.metadata.project_end_date)?;

        // Persist late_start / late_finish
        let id_ca = self.df.column("id")?.i32()?;
        let height = self.df.height();
        let mut ls_vals: Vec<Option<i32>> = vec![None; height];
        let mut lf_vals: Vec<Option<i32>> = vec![None; height];
        for (idx, id_opt) in id_ca.into_iter().enumerate() {
            if let Some(task_id) = id_opt {
                if let Some((ls, lf)) = results.get(&task_id) {
                    ls_vals[idx] = Some(Self::date_to_i32(*ls));
                    lf_vals[idx] = Some(Self::date_to_i32(*lf));
                }
            }
        }
        // Fallback fill: if any late values remain None, use early counterparts to avoid nulls
        let es_dates = self.df.column("early_start")?.date()?;
        let ef_dates = self.df.column("early_finish")?.date()?;
        for i in 0..height {
            if ls_vals[i].is_none() {
                if let Some(es_i) = es_dates.get(i) {
                    ls_vals[i] = Some(es_i);
                }
            }
            if lf_vals[i].is_none() {
                if let Some(ef_i) = ef_dates.get(i) {
                    lf_vals[i] = Some(ef_i);
                }
            }
        }

        let ls_series =
            Series::new(PlSmallStr::from_static("late_start"), ls_vals).cast(&DataType::Date)?;
        let lf_series =
            Series::new(PlSmallStr::from_static("late_finish"), lf_vals).cast(&DataType::Date)?;
        self.df.replace("late_start", ls_series)?;
        self.df.replace("late_finish", lf_series)?;

        // Compute total_float and is_critical
        let es_col = self.df.column("early_start")?.date()?;
        let mut es_map: HashMap<i32, i32> = HashMap::new();
        for (i, id_opt) in self.df.column("id")?.i32()?.into_iter().enumerate() {
            if let Some(id) = id_opt {
                if let Some(es_days) = es_col.get(i) {
                    es_map.insert(id, es_days);
                }
            }
        }
        let id_ca2 = self.df.column("id")?.i32()?;
        let ls_col = self.df.column("late_start")?.date()?;
        let mut tf_vals: Vec<i64> = Vec::with_capacity(height);
        let mut crit_vals: Vec<bool> = Vec::with_capacity(height);
        for (i, id_opt) in id_ca2.into_iter().enumerate() {
            if let Some(id) = id_opt {
                let es_days = es_map.get(&id).copied().unwrap_or(0) as i64;
                let ls_days = ls_col.get(i).unwrap_or(0) as i64;
                let tf = ls_days - es_days;
                tf_vals.push(tf);
                crit_vals.push(tf == 0);
            } else {
                tf_vals.push(0);
                crit_vals.push(false);
            }
        }
        let tf_series = Series::new(PlSmallStr::from_static("total_float"), tf_vals);
        let crit_series = Series::new(PlSmallStr::from_static("is_critical"), crit_vals);
        self.df.replace("total_float", tf_series)?;
        self.df.replace("is_critical", crit_series)?;

        Ok(())
    }

    pub fn refresh(&mut self) -> Result<RefreshSummary, PolarsError> {
        if self.metadata.project_start_date > self.metadata.project_end_date {
            return Err(PolarsError::ComputeError(
                "project_end_date must be on or after project_start_date".into(),
            ));
        }

        self.forward_pass()?;
        self.validate_project_horizon()?;
        self.backward_pass()?;
        self.set_schedule_variance()?;
        self.set_successors_column()?;

        let task_count = self.df.height();
        let id_ca = self.df.column("id")?.i32()?;
        let tf_ca = self.df.column("total_float")?.i64()?;
        let variance_ca = self.df.column("schedule_variance_days")?.i64()?;
        let critical_ca = self.df.column("is_critical")?.bool()?;
        let early_start_ca = self.df.column("early_start")?.date()?;

        let mut critical_count = 0usize;
        let mut positive_variance_count = 0usize;
        let mut negative_variance_count = 0usize;
        let mut on_track_variance_count = 0usize;
        let mut critical_path: Vec<(NaiveDate, i32)> = Vec::new();

        for idx in 0..task_count {
            if let Some(true) = critical_ca.get(idx) {
                critical_count += 1;
            }
            match variance_ca.get(idx) {
                Some(v) if v > 0 => positive_variance_count += 1,
                Some(v) if v < 0 => negative_variance_count += 1,
                Some(_) => on_track_variance_count += 1,
                None => {}
            }
            if let (Some(id), Some(tf)) = (id_ca.get(idx), tf_ca.get(idx)) {
                if tf == 0 {
                    let start = Self::date_from_chunk(&early_start_ca, idx)
                        .unwrap_or(self.metadata.project_start_date);
                    critical_path.push((start, id));
                }
            }
        }

        critical_path.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        let critical_path_ids = critical_path.into_iter().map(|(_, id)| id).collect();

        let latest_finish = self.latest_early_finish()?;

        Ok(RefreshSummary {
            task_count,
            critical_count,
            critical_path: critical_path_ids,
            latest_finish,
            positive_variance_count,
            negative_variance_count,
            on_track_variance_count,
        })
    }

    fn validation_error(err: TaskValidationError) -> PolarsError {
        PolarsError::ComputeError(err.to_string().into())
    }

    pub fn upsert_task(
        &mut self,
        id: i32,
        name: &str,
        duration_days: i64,
        predecessors: Option<Vec<i32>>,
    ) -> Result<(), PolarsError> {
        if duration_days < 0 {
            return Err(PolarsError::ComputeError(
                format!("task {} has negative duration {}", id, duration_days).into(),
            ));
        }
        let id_exists = if self.df.height() == 0 {
            false
        } else {
            self.df
                .column("id")?
                .i32()?
                .into_iter()
                .any(|v| v == Some(id))
        };

        if id_exists {
            self.update_string_column("name", id, name)?;
            if let Some(preds) = predecessors {
                self.update_list_i32_column("predecessors", id, preds)?;
            }
            self.update_duration_column(id, duration_days)?;
            return Ok(());
        }

        let mut task = Task::new(id, name, duration_days);
        if let Some(preds) = predecessors {
            task.predecessors = preds;
        }
        task_validation::validate_task(&task).map_err(Self::validation_error)?;
        let new_row = task.to_dataframe_row()?;
        self.df = self.df.vstack(&new_row)?;
        Ok(())
    }

    pub fn apply_rationale_template(
        &mut self,
        task_id: i32,
        template: ProgressRationaleTemplate,
    ) -> Result<(), PolarsError> {
        let mut task = self
            .find_task(task_id)?
            .ok_or_else(|| PolarsError::ComputeError(format!("task {task_id} not found").into()))?;
        task.apply_rationale_template(template)
            .map_err(Self::validation_error)?;
        self.upsert_task_record(task)
    }

    pub fn update_task_duration(
        &mut self,
        task_id: i32,
        new_duration_days: i64,
    ) -> Result<(), PolarsError> {
        self.update_duration_column(task_id, new_duration_days)
    }

    pub fn upsert_task_record(&mut self, task: Task) -> Result<(), PolarsError> {
        task_validation::validate_task(&task).map_err(Self::validation_error)?;
        let id_exists = if self.df.height() == 0 {
            false
        } else {
            self.df
                .column("id")?
                .i32()?
                .into_iter()
                .any(|v| v == Some(task.id))
        };

        if id_exists {
            self.update_string_column("name", task.id, &task.name)?;
            self.update_list_i32_column("predecessors", task.id, task.predecessors.clone())?;
            self.update_duration_column(task.id, task.duration_days)?;

            if let Some(date) = task.early_start {
                self.update_date_column("early_start", task.id, date)?;
            }

            if let Some(date) = task.early_finish {
                self.update_date_column("early_finish", task.id, date)?;
            }

            if let Some(date) = task.late_start {
                self.update_date_column("late_start", task.id, date)?;
            }

            if let Some(date) = task.late_finish {
                self.update_date_column("late_finish", task.id, date)?;
            }

            if let Some(date) = task.baseline_start {
                self.update_date_column("baseline_start", task.id, date)?;
            }

            if let Some(date) = task.baseline_finish {
                self.update_date_column("baseline_finish", task.id, date)?;
            }

            if let Some(date) = task.actual_start {
                self.update_date_column("actual_start", task.id, date)?;
            }

            if let Some(date) = task.actual_finish {
                self.update_date_column("actual_finish", task.id, date)?;
            }

            if let Some(percent) = task.percent_complete {
                self.update_float_column("percent_complete", task.id, percent)?;
            }

            if let Some(variance) = task.schedule_variance_days {
                self.update_i64_column("schedule_variance_days", task.id, variance)?;
            }

            if let Some(total_float) = task.total_float {
                self.update_i64_column("total_float", task.id, total_float)?;
            }

            if let Some(is_critical) = task.is_critical {
                self.update_bool_column("is_critical", task.id, is_critical)?;
            }

            if !task.successors.is_empty() {
                self.update_list_i32_column("successors", task.id, task.successors.clone())?;
            }

            if let Some(parent) = task.parent_id {
                self.update_i32_column("parent_id", task.id, parent)?;
            }

            if let Some(ref wbs) = task.wbs_code {
                self.update_string_column("wbs_code", task.id, wbs)?;
            }

            if let Some(ref notes) = task.task_notes {
                self.update_string_column("task_notes", task.id, notes)?;
            }

            if !task.task_attachments.is_empty() {
                self.update_list_str_column(
                    "task_attachments",
                    task.id,
                    task.task_attachments.clone(),
                )?;
            }

            self.update_string_column(
                "progress_measurement",
                task.id,
                task.progress_measurement.as_str(),
            )?;

            let rationale_json = serde_json::to_string(&task.pre_defined_rationale)
                .map_err(|err| PolarsError::ComputeError(err.to_string().into()))?;
            self.update_string_column("pre_defined_rationale", task.id, rationale_json.as_str())?;

            let allocations_json = serde_json::to_string(&task.resource_allocations)
                .map_err(|err| PolarsError::ComputeError(err.to_string().into()))?;
            self.update_string_column("resource_allocations", task.id, allocations_json.as_str())?;

            return Ok(());
        }

        let new_row = task.to_dataframe_row()?;
        self.df = self.df.vstack(&new_row)?;
        Ok(())
    }

    // Public setters for common columns to enable CLI editing
    #[cfg(feature = "cli_api")]
    pub fn set_baseline_start(&mut self, task_id: i32, date: NaiveDate) -> Result<(), PolarsError> {
        self.update_date_column("baseline_start", task_id, date)
    }

    #[cfg(feature = "cli_api")]
    pub fn set_baseline_finish(
        &mut self,
        task_id: i32,
        date: NaiveDate,
    ) -> Result<(), PolarsError> {
        self.update_date_column("baseline_finish", task_id, date)
    }

    #[cfg(feature = "cli_api")]
    pub fn set_actual_start(&mut self, task_id: i32, date: NaiveDate) -> Result<(), PolarsError> {
        self.update_date_column("actual_start", task_id, date)
    }

    #[cfg(feature = "cli_api")]
    pub fn set_actual_finish(&mut self, task_id: i32, date: NaiveDate) -> Result<(), PolarsError> {
        self.update_date_column("actual_finish", task_id, date)
    }

    #[cfg(feature = "cli_api")]
    pub fn set_percent_complete(&mut self, task_id: i32, percent: f64) -> Result<(), PolarsError> {
        let mut task = self.find_task(task_id)?.ok_or_else(|| {
            PolarsError::ComputeError(format!("task {} not found", task_id).into())
        })?;
        task.percent_complete = Some(percent);
        task_validation::validate_task(&task).map_err(Self::validation_error)?;
        self.update_float_column("percent_complete", task_id, percent)
    }

    #[cfg(feature = "cli_api")]
    pub fn set_schedule_variance_days(
        &mut self,
        task_id: i32,
        days: i64,
    ) -> Result<(), PolarsError> {
        self.update_i64_column("schedule_variance_days", task_id, days)
    }

    #[cfg(feature = "cli_api")]
    pub fn set_is_critical(&mut self, task_id: i32, is_critical: bool) -> Result<(), PolarsError> {
        self.update_bool_column("is_critical", task_id, is_critical)
    }

    #[cfg(feature = "cli_api")]
    pub fn set_parent_id(&mut self, task_id: i32, parent_id: i32) -> Result<(), PolarsError> {
        self.update_i32_column("parent_id", task_id, parent_id)
    }

    #[cfg(feature = "cli_api")]
    pub fn set_wbs_code(&mut self, task_id: i32, wbs: &str) -> Result<(), PolarsError> {
        self.update_string_column("wbs_code", task_id, wbs)
    }

    #[cfg(feature = "cli_api")]
    pub fn set_task_notes(&mut self, task_id: i32, notes: &str) -> Result<(), PolarsError> {
        self.update_string_column("task_notes", task_id, notes)
    }

    #[cfg(feature = "cli_api")]
    pub fn set_successors(
        &mut self,
        task_id: i32,
        successors: Vec<i32>,
    ) -> Result<(), PolarsError> {
        self.update_list_i32_column("successors", task_id, successors)
    }

    pub fn set_calendar(&mut self, calendar: WorkCalendar) -> Result<(), PolarsError> {
        self.calendar = calendar;
        self.calendar_is_custom = true;
        if self.df.height() == 0 {
            return Ok(());
        }
        self.refresh().map(|_| ())
    }

    pub fn reset_calendar_to_default(&mut self) -> Result<(), PolarsError> {
        self.calendar = Self::calendar_for_metadata(&self.metadata);
        self.calendar_is_custom = false;
        if self.df.height() == 0 {
            return Ok(());
        }
        self.refresh().map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn default_schema_contains_expected_columns() {
        let schema = Schedule::default_schema();
        let expected = vec![
            "id",
            "name",
            "duration_days",
            "predecessors",
            "early_start",
            "early_finish",
            "late_start",
            "late_finish",
            "baseline_start",
            "baseline_finish",
            "actual_start",
            "actual_finish",
            "percent_complete",
            "progress_measurement",
            "pre_defined_rationale",
            "schedule_variance_days",
            "total_float",
            "is_critical",
            "successors",
            "parent_id",
            "wbs_code",
            "task_notes",
            "task_attachments",
            "resource_allocations",
        ];
        for name in expected {
            assert!(schema.contains(name.into()), "missing column {name}");
        }
    }

    #[test]
    fn upsert_task_inserts_and_updates() {
        let mut s = Schedule::new();
        s.upsert_task(1, "Task A", 5, None).unwrap();
        assert_eq!(s.dataframe().height(), 1);

        // Update name and duration, and set predecessors
        s.upsert_task(1, "Task A1", 7, Some(vec![2, 3])).unwrap();

        let df = s.dataframe();
        let name = df.column("name").unwrap().str().unwrap().get(0).unwrap();
        let dur = df
            .column("duration_days")
            .unwrap()
            .i64()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(name, "Task A1");
        assert_eq!(dur, 7);
    }
}
