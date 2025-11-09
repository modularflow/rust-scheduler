use chrono::{Duration, NaiveDate};
use polars::prelude::*;
use polars::prelude::PlSmallStr;

#[derive(Debug, Clone, PartialEq)]
pub struct Task {
    pub id: i32,
    pub name: String,
    pub duration_days: i64,
    pub predecessors: Vec<i32>,
    pub early_start: Option<NaiveDate>,
    pub early_finish: Option<NaiveDate>,
    pub late_start: Option<NaiveDate>,
    pub late_finish: Option<NaiveDate>,
    pub baseline_start: Option<NaiveDate>,
    pub baseline_finish: Option<NaiveDate>,
    pub actual_start: Option<NaiveDate>,
    pub actual_finish: Option<NaiveDate>,
    pub percent_complete: Option<f64>,
    pub schedule_variance_days: Option<i64>,
    pub total_float: Option<i64>,
    pub is_critical: Option<bool>,
    pub successors: Vec<i32>,
    pub parent_id: Option<i32>,
    pub wbs_code: Option<String>,
    pub task_notes: Option<String>,
    pub task_attachments: Vec<String>,
}

impl Task {
    pub fn new(id: i32, name: impl Into<String>, duration_days: i64) -> Self {
        Self {
            id,
            name: name.into(),
            duration_days,
            predecessors: Vec::new(),
            early_start: None,
            early_finish: None,
            late_start: None,
            late_finish: None,
            baseline_start: None,
            baseline_finish: None,
            actual_start: None,
            actual_finish: None,
            percent_complete: None,
            schedule_variance_days: None,
            total_float: None,
            is_critical: None,
            successors: Vec::new(),
            parent_id: None,
            wbs_code: None,
            task_notes: None,
            task_attachments: Vec::new(),
        }
    }

    pub fn to_dataframe_row(&self) -> PolarsResult<DataFrame> {
        let mut columns: Vec<Column> = Vec::with_capacity(20);

        let id_data: [i32; 1] = [self.id];
        columns.push(Series::new(PlSmallStr::from_static("id"), id_data).into_column());

        let name_data: [&str; 1] = [self.name.as_str()];
        columns.push(Series::new(PlSmallStr::from_static("name"), name_data).into_column());

        let duration_data: [i64; 1] = [self.duration_days];
        columns.push(
            Series::new(PlSmallStr::from_static("duration_days"), duration_data).into_column(),
        );

        columns.push(
            Self::series_from_i32_list("predecessors", &self.predecessors).into_column(),
        );
        columns.push(Self::series_from_date("early_start", self.early_start)?.into_column());
        columns.push(Self::series_from_date("early_finish", self.early_finish)?.into_column());
        columns.push(Self::series_from_date("late_start", self.late_start)?.into_column());
        columns.push(Self::series_from_date("late_finish", self.late_finish)?.into_column());
        columns.push(Self::series_from_date("baseline_start", self.baseline_start)?.into_column());
        columns.push(Self::series_from_date("baseline_finish", self.baseline_finish)?.into_column());
        columns.push(Self::series_from_date("actual_start", self.actual_start)?.into_column());
        columns.push(Self::series_from_date("actual_finish", self.actual_finish)?.into_column());

        let percent_complete: [Option<f64>; 1] = [self.percent_complete];
        columns.push(
            Series::new(PlSmallStr::from_static("percent_complete"), percent_complete)
                .into_column(),
        );

        let variance: [Option<i64>; 1] = [self.schedule_variance_days];
        columns.push(
            Series::new(PlSmallStr::from_static("schedule_variance_days"), variance)
                .into_column(),
        );

        let total_float: [Option<i64>; 1] = [self.total_float];
        columns.push(
            Series::new(PlSmallStr::from_static("total_float"), total_float).into_column(),
        );

        let is_critical: [Option<bool>; 1] = [self.is_critical];
        columns.push(
            Series::new(PlSmallStr::from_static("is_critical"), is_critical).into_column(),
        );

        columns.push(Self::series_from_i32_list("successors", &self.successors).into_column());
        let parent: [Option<i32>; 1] = [self.parent_id];
        columns.push(Series::new(PlSmallStr::from_static("parent_id"), parent).into_column());

        let wbs: [Option<&str>; 1] = [self.wbs_code.as_deref()];
        columns.push(Series::new(PlSmallStr::from_static("wbs_code"), wbs).into_column());

        let notes: [Option<&str>; 1] = [self.task_notes.as_deref()];
        columns.push(Series::new(PlSmallStr::from_static("task_notes"), notes).into_column());

        columns.push(
            Self::series_from_string_list("task_attachments", &self.task_attachments).into_column(),
        );

        DataFrame::new(columns)
    }

    pub fn from_dataframe_row(df: &DataFrame, row_idx: usize) -> PolarsResult<Self> {
        let id = df.column("id")?.i32()?.get(row_idx).ok_or_else(|| {
            PolarsError::ComputeError("task row missing id".into())
        })?;

        let name = df
            .column("name")?
            .str()?
            .get(row_idx)
            .unwrap_or("")
            .to_string();

        let duration_days = df
            .column("duration_days")?
            .i64()?
            .get(row_idx)
            .unwrap_or(0);

        let predecessors = Self::vec_from_i32_list(df.column("predecessors")?.list()?, row_idx)?;
        let successors = Self::vec_from_i32_list(df.column("successors")?.list()?, row_idx)?;
        let task_attachments =
            Self::vec_from_string_list(df.column("task_attachments")?.list()?, row_idx)?;

        Ok(Self {
            id,
            name,
            duration_days,
            predecessors,
            early_start: Self::date_from_series(df.column("early_start")?.date()?, row_idx),
            early_finish: Self::date_from_series(df.column("early_finish")?.date()?, row_idx),
            late_start: Self::date_from_series(df.column("late_start")?.date()?, row_idx),
            late_finish: Self::date_from_series(df.column("late_finish")?.date()?, row_idx),
            baseline_start: Self::date_from_series(df.column("baseline_start")?.date()?, row_idx),
            baseline_finish: Self::date_from_series(df.column("baseline_finish")?.date()?, row_idx),
            actual_start: Self::date_from_series(df.column("actual_start")?.date()?, row_idx),
            actual_finish: Self::date_from_series(df.column("actual_finish")?.date()?, row_idx),
            percent_complete: df.column("percent_complete")?.f64()?.get(row_idx),
            schedule_variance_days: df
                .column("schedule_variance_days")?
                .i64()?
                .get(row_idx),
            total_float: df.column("total_float")?.i64()?.get(row_idx),
            is_critical: df.column("is_critical")?.bool()?.get(row_idx),
            successors,
            parent_id: df.column("parent_id")?.i32()?.get(row_idx),
            wbs_code: df
                .column("wbs_code")?
                .str()?
                .get(row_idx)
                .map(ToOwned::to_owned),
            task_notes: df
                .column("task_notes")?
                .str()?
                .get(row_idx)
                .map(ToOwned::to_owned),
            task_attachments,
        })
    }

    fn series_from_i32_list(name: &str, values: &[i32]) -> Series {
        let inner = Series::new(PlSmallStr::from_static(""), values.to_vec());
        Series::new(name.into(), &[inner])
    }

    fn series_from_string_list(name: &str, values: &[String]) -> Series {
        let inner_values: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        let inner = Series::new(PlSmallStr::from_static(""), inner_values);
        Series::new(name.into(), &[inner])
    }

    fn series_from_date(name: &str, date: Option<NaiveDate>) -> PolarsResult<Series> {
        let data: [Option<i32>; 1] = [date.map(Self::date_to_i32)];
        Series::new(name.into(), data).cast(&DataType::Date)
    }

    fn date_from_series(chunked: &DateChunked, row_idx: usize) -> Option<NaiveDate> {
        chunked.get(row_idx).map(Self::date_from_i32)
    }

    fn vec_from_i32_list(list: &ListChunked, row_idx: usize) -> PolarsResult<Vec<i32>> {
        if let Some(series) = list.get_as_series(row_idx) {
            Ok(series
                .i32()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>())
        } else {
            Ok(Vec::new())
        }
    }

    fn vec_from_string_list(list: &ListChunked, row_idx: usize) -> PolarsResult<Vec<String>> {
        if let Some(series) = list.get_as_series(row_idx) {
            Ok(series
                .str()?
                .into_iter()
                .flatten()
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>())
        } else {
            Ok(Vec::new())
        }
    }

    fn date_to_i32(date: NaiveDate) -> i32 {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        (date - epoch).num_days() as i32
    }

    fn date_from_i32(days: i32) -> NaiveDate {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        epoch + Duration::days(days as i64)
    }
}


