use polars::prelude::*;
use polars::prelude::PlSmallStr;
use chrono::{NaiveDate, Datelike, Weekday, Duration};
use std::collections::HashSet;
use crate::calendar::WorkCalendar;
use crate::metadata::ScheduleMetadata;

struct DateColumns {
    start_date: NaiveDate,
    end_date: NaiveDate,
    duration: Duration,
    deadline: NaiveDate,
}


pub struct Schedule {
    df: DataFrame,
    metadata: ScheduleMetadata,
    calendar: WorkCalendar,
}

impl Schedule {
    pub fn new() -> Self {
        let schema = Self::default_schema();
        let schedule = DataFrame::empty_with_schema(&schema);

        Self {
            df: schedule,
            metadata: ScheduleMetadata::default(),
            calendar: WorkCalendar::default(),
        }
    }

    pub fn set_metadata(&mut self, metadata: ScheduleMetadata) {
        self.metadata = metadata;
    }

    fn set_calendar(&mut self, calendar: WorkCalendar) {
        self.calendar = calendar;
    }

    pub fn dataframe(&self) -> &DataFrame {
        &self.df
    }

    fn default_schema() -> Schema {
        let schema = Schema::from_iter(vec![
            Field::new("id".into(), DataType::Int32),
            Field::new("name".into(), DataType::String),
            Field::new("duration_days".into(), DataType::Int64),
            Field::new("predecessors".into(), DataType::List(Box::new(DataType::Int32))),
            Field::new("early_start".into(), DataType::Date),
            Field::new("early_finish".into(), DataType::Date),
            Field::new("late_start".into(), DataType::Date),
            Field::new("late_finish".into(), DataType::Date),
            Field::new("baseline_start".into(), DataType::Date),
            Field::new("baseline_finish".into(), DataType::Date),
            Field::new("actual_start".into(), DataType::Date),
            Field::new("actual_finish".into(), DataType::Date),
            Field::new("percent_complete".into(), DataType::Float64),
            Field::new("schedule_variance_days".into(), DataType::Int64),
            Field::new("total_float".into(), DataType::Int64),
            Field::new("is_critical".into(), DataType::Boolean),
            Field::new("successors".into(), DataType::List(Box::new(DataType::Int32))),
            Field::new("parent_id".into(), DataType::Int32),
            Field::new("wbs_code".into(), DataType::String),
            Field::new("task_notes".into(), DataType::String),
            Field::new("task_attachments".into(), DataType::List(Box::new(DataType::String))),
        ]);
        schema
    }

    fn get_predecessors(&self, task_id: i32) -> Result<Vec<i32>, PolarsError> {
        let row = self.df.clone().lazy()
            .filter(col("id").eq(lit(task_id)))
            .select([col("predecessors")])
            .collect()?;
        
        if row.height() == 0 {
            return Err(PolarsError::NoData(format!("Task ID {} not found", task_id).into()));
        }
        
        let predecessors_col = row.column("predecessors")?;
        let list_chunked = predecessors_col.list()?;
        
        // get_as_series returns Option<Series>, not Option<Option<Series>>
        let preds = match list_chunked.get_as_series(0) {
            Some(series) => {
                series.i32()?.into_iter()
                    .filter_map(|x| x)
                    .collect()
            }
            None => Vec::new()
        };
        
        Ok(preds)
    }

    fn get_early_finish_dates(&self, task_ids: Vec<i32>) -> Result<Vec<NaiveDate>, PolarsError> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }
        
        // Use lazy evaluation with filter on multiple IDs
        let mut filters = col("id").eq(lit(task_ids[0]));
        for &id in &task_ids[1..] {
            filters = filters.or(col("id").eq(lit(id)));
        }
        
        let filtered = self.df.clone().lazy()
            .filter(filters)
            .select([col("early_finish")])
            .collect()?;
        
        let early_finish_col = filtered.column("early_finish")?;
        let dates: Vec<NaiveDate> = early_finish_col.date()?
            .into_iter()
            .filter_map(|days_opt| {
                days_opt.map(Self::i32_to_date)
            })
            .collect();
        
        Ok(dates)
    }

    /// Calculate and update the early start and early finish for a task based on its predecessors
    fn calculate_task_early_dates(&mut self, task_id: i32) -> Result<(NaiveDate, NaiveDate), PolarsError> {
        // Get predecessors
        let predecessors = self.get_predecessors(task_id)?;
        
        // Determine early start date
        let early_start = if predecessors.is_empty() {
            // No predecessors: use project start date
            self.metadata.project_start_date
        } else {
            // Get early finish dates of predecessors
            let pred_finish_dates = self.get_early_finish_dates(predecessors)?;
            
            if pred_finish_dates.is_empty() {
                return Err(PolarsError::NoData(
                    format!("No early finish dates found for predecessors of task {}", task_id).into()
                ));
            }
            
            // Early start is the day after the latest predecessor finishes
            let max_pred_finish = pred_finish_dates.iter().max().unwrap();
            self.calendar.next_available(*max_pred_finish)
        };
        
        // Get task duration
        let row = self.df.clone().lazy()
            .filter(col("id").eq(lit(task_id)))
            .select([col("duration_days")])
            .collect()?;
        
        let duration = row.column("duration_days")?.i64()?.get(0)
            .ok_or_else(|| PolarsError::NoData("duration_days is null".into()))?;
        
        // Calculate early finish using the calendar
        let early_finish = self.calendar.find_next_available(early_start, duration);
        
        // Update the dataframe
        self.update_date_column("early_start", task_id, early_start)?;
        self.update_date_column("early_finish", task_id, early_finish)?;
        
        Ok((early_start, early_finish))
    }
        
    fn total_float_expr() -> Expr {
        (col("late_start") - col("early_start")).alias("total_float")
    }
    
    /// Expression to calculate free float
    fn free_float_expr() -> Expr {
        // Free float = min(successor early starts) - task early finish - 1
        // This is simplified - you'd need actual successor logic
        (col("successor_early_start") - col("early_finish") - lit(1))
            .alias("free_float")
    }
    
    /// Expression to identify critical path tasks (total float = 0)
    fn is_critical_expr() -> Expr {
        Self::total_float_expr().eq(lit(0)).alias("is_critical")
    }
    
    /// Expression to calculate early finish from early start + duration
    fn early_finish_expr() -> Expr {
        (col("early_start") + col("duration_days")).alias("early_finish")
    }
    
    /// Expression to calculate late start from late finish - duration
    fn late_start_expr() -> Expr {
        (col("late_finish") - col("duration_days")).alias("late_start")
    }
    
    /// Filter expression for tasks with no predecessors
    fn no_predecessors_filter() -> Expr {
        col("predecessors").list().len().eq(lit(0))
    }
    
    /// Filter expression for specific status (placeholder)
    fn status_filter(status: &str) -> Expr {
        col("status").eq(lit(status))
    }
    
    /// Filter expression for active tasks (started but not finished)
    fn active_tasks_filter() -> Expr {
        col("actual_start").is_not_null()
            .and(col("actual_finish").is_null())
    }
    
    /// Expression to calculate percent complete
    fn percent_complete_expr() -> Expr {
        when(col("duration_days").eq(lit(0)))
            .then(lit(100.0))
            .otherwise(
                col("actual_duration") / col("duration_days") * lit(100.0)
            )
            .alias("percent_complete")
    }
    
    /// Filter for overdue tasks
    fn overdue_filter(current_date: NaiveDate) -> Expr {
        let current_date_days = Self::date_to_i32(current_date);
        col("late_finish")
            .lt(lit(current_date_days))
            .and(col("actual_finish").is_null())
    }
    
    /// Expression to calculate schedule variance (Earned Value)
    fn schedule_variance_expr() -> Expr {
        (col("planned_finish") - col("actual_finish"))
            .alias("schedule_variance_days")
    }

    fn add_empty_rows(&mut self, num_rows: usize) -> Result<(), PolarsError> {
        let empty_series: Vec<Column> = self.df.get_columns()
            .iter()
            .map(|col| {
                let null_series = Series::new_null(col.name().clone(), num_rows);
                null_series.into_column()
            })
            .collect();
        
        let empty_df = DataFrame::new(empty_series)?;
        // Consumes old Dataframe in self.df and returns a new DataFrame as self.df
        self.df = self.df.vstack(&empty_df)?;
        Ok(())
    }

    fn update_string_column(&mut self, column_name: &str, task_id: i32, new_value: &str) -> Result<(), PolarsError> {
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

    fn update_i32_column(&mut self, column_name: &str, task_id: i32, new_value: i32) -> Result<(), PolarsError> {
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

    fn update_i64_column(&mut self, column_name: &str, task_id: i32, new_value: i64) -> Result<(), PolarsError> {
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

    fn update_list_i32_column(&mut self, column_name: &str, task_id: i32, new_values: Vec<i32>) -> Result<(), PolarsError> {
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

    fn update_float_column(&mut self, column_name: &str, task_id: i32, new_value: f64) -> Result<(), PolarsError> {
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

    fn update_bool_column(&mut self, column_name: &str, task_id: i32, new_value: bool) -> Result<(), PolarsError> {
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

    fn update_date_column(&mut self, column_name: &str, task_id: i32, new_date: NaiveDate) -> Result<(), PolarsError> {
        self.df = self.df.clone().lazy()
            .with_column(
                when(col("id").eq(lit(task_id)))
                    .then(lit(new_date).cast(DataType::Date))
                    .otherwise(col(column_name).cast(DataType::Date))
                    .alias(column_name)
            )
            .collect()?;
        Ok(())
    }

    fn update_duration_column(&mut self, column_name: &str, task_id: i32, new_duration: Duration) -> Result<(), PolarsError> {
        self.df = self.df.clone().lazy()
        .with_column(
            when(col("id").eq(lit(task_id)))
                .then(lit(new_duration))
                .otherwise(col(column_name))
                .alias(column_name)
        )
        .collect()?;
        Ok(())
    }

    fn i32_to_date(days: i32) -> NaiveDate {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        epoch + Duration::days(days as i64)
    }
    
    /// Convert NaiveDate to Polars i32 date
    fn date_to_i32(date: NaiveDate) -> i32 {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        (date - epoch).num_days() as i32
    }

    fn calculate_finish_date(&mut self, task_id: i32) -> Result<NaiveDate, PolarsError> {
        // Filter to just the row with this task_id
        let row = self.df.clone().lazy()
            .filter(col("id").eq(lit(task_id)))
            .select([
                col("early_start"),
                col("duration_days"),
            ])
            .collect()?;
        
        if row.height() == 0 {
            return Err(PolarsError::NoData(format!("Task ID {} not found", task_id).into()));
        }
        
        let start_date_days = row.column("early_start")?.date()?.get(0)
            .ok_or_else(|| PolarsError::NoData("start_date is null".into()))?;
        let duration = row.column("duration_days")?.i64()?.get(0)
            .ok_or_else(|| PolarsError::NoData("duration is null".into()))?;
        
        let start_date = Self::i32_to_date(start_date_days);
        let finish_date = self.calendar.find_next_available(start_date, duration);
        
        self.update_date_column("early_finish", task_id, finish_date)?;
        Ok(finish_date)
    }

    fn calculate_critical_path(&mut self) -> Result<(), PolarsError> {
        self.df = self.df.clone().lazy()
            .with_columns([
                Self::early_finish_expr(),
                Self::late_start_expr(),
                Self::total_float_expr(),
                Self::is_critical_expr(),
            ])
            .collect()?;
        Ok(())
    }
    
    pub fn forward_pass(&mut self) -> Result<(), PolarsError> {
        // Calculate early start/finish for all tasks
        // Start with tasks that have no predecessors
        let _start_tasks = self.df.clone().lazy()
            .filter(Self::no_predecessors_filter())
            .with_column(
                lit(Self::date_to_i32(self.metadata.project_start_date))
                    .alias("early_start")
            )
            .with_column(Self::early_finish_expr())
            .collect()?;
        
        // Then propagate through successors...
        Ok(())
    }

    pub fn upsert_task(&mut self, id: i32, name: &str, duration_days: i64, predecessors: Option<Vec<i32>>) -> Result<(), PolarsError> {
        let id_exists = if self.df.height() == 0 {
            false
        } else {
            self.df.column("id")?
                .i32()? 
                .into_iter()
                .any(|v| v == Some(id))
        };

        if id_exists {
            self.update_string_column("name", id, name)?;
            self.update_i64_column("duration_days", id, duration_days)?;
            if let Some(preds) = predecessors {
                self.update_list_i32_column("predecessors", id, preds)?;
            }
            return Ok(());
        }

        // Build a single-row DataFrame matching schema, mostly nulls
        let mut cols: Vec<Column> = self.df.get_columns()
            .iter()
            .map(|c| Series::new_null(c.name().clone(), 1).into_column())
            .collect();

        // helper to set/replace a column by name
        let mut set_col = |name: &str, series: Series| {
            if let Some(idx) = cols.iter().position(|c| c.name() == name) {
                cols[idx] = series.into_column();
            }
        };

        set_col("id", Series::new(PlSmallStr::from_static("id"), [id]));
        set_col("name", Series::new(PlSmallStr::from_static("name"), [name]));
        set_col("duration_days", Series::new(PlSmallStr::from_static("duration_days"), [duration_days]));
        let preds_vec = predecessors.unwrap_or_default();
        // Build a List series containing one element (the predecessors for this task)
        let preds_elem = Series::new(PlSmallStr::from_static(""), preds_vec);
        let preds_list = Series::new(PlSmallStr::from_static("predecessors"), &[preds_elem]);
        set_col("predecessors", preds_list);

        let new_row = DataFrame::new(cols)?;
        self.df = self.df.vstack(&new_row)?;
        Ok(())
    }

    // Public setters for common columns to enable CLI editing
    #[cfg(feature = "cli_api")]
    pub fn set_baseline_start(&mut self, task_id: i32, date: NaiveDate) -> Result<(), PolarsError> {
        self.update_date_column("baseline_start", task_id, date)
    }

    #[cfg(feature = "cli_api")]
    pub fn set_baseline_finish(&mut self, task_id: i32, date: NaiveDate) -> Result<(), PolarsError> {
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
        self.update_float_column("percent_complete", task_id, percent)
    }

    #[cfg(feature = "cli_api")]
    pub fn set_schedule_variance_days(&mut self, task_id: i32, days: i64) -> Result<(), PolarsError> {
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
    pub fn set_successors(&mut self, task_id: i32, successors: Vec<i32>) -> Result<(), PolarsError> {
        self.update_list_i32_column("successors", task_id, successors)
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn default_schema_contains_expected_columns() {
        let schema = Schedule::default_schema();
        let expected = vec![
            "id", "name", "duration_days", "predecessors", "early_start", "early_finish",
            "late_start", "late_finish", "baseline_start", "baseline_finish", "actual_start",
            "actual_finish", "percent_complete", "schedule_variance_days", "total_float",
            "is_critical", "successors", "parent_id", "wbs_code", "task_notes",
            "task_attachments",
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
        let dur = df.column("duration_days").unwrap().i64().unwrap().get(0).unwrap();
        assert_eq!(name, "Task A1");
        assert_eq!(dur, 7);
    }

    #[test]
    fn get_predecessors_and_update_list_columns() {
        let mut s = Schedule::new();
        s.upsert_task(1, "Task 1", 2, None).unwrap();
        s.upsert_task(2, "Task 2", 3, Some(vec![1])).unwrap();

        let preds = s.get_predecessors(2).unwrap();
        assert_eq!(preds, vec![1]);

        // Replace successors list via setter
        s.update_list_i32_column("successors", 1, vec![2]).unwrap();
        let row = s.df.clone().lazy()
            .filter(col("id").eq(lit(1)))
            .select([col("successors")])
            .collect()
            .unwrap();
        let list = row.column("successors").unwrap().list().unwrap();
        let inner = list.get_as_series(0).unwrap();
        let vals: Vec<i32> = inner.i32().unwrap().into_iter().flatten().collect();
        assert_eq!(vals, vec![2]);
    }

    #[test]
    fn calculate_task_early_dates_for_chain() {
        let mut s = Schedule::new();
        // Make project start a known Monday
        let mut md = ScheduleMetadata::default();
        md.project_start_date = date(2025, 1, 6);
        s.set_metadata(md);

        // Two tasks in a chain: 1 -> 2
        s.upsert_task(1, "T1", 2, None).unwrap();
        s.upsert_task(2, "T2", 3, Some(vec![1])).unwrap();

        // Calculate for task 1 then task 2
        let (_s1, f1) = s.calculate_task_early_dates(1).unwrap();
        let (s2, _f2) = s.calculate_task_early_dates(2).unwrap();

        // Task 2 early start must be next available after task 1 finish
        assert_eq!(s2, s.calendar.next_available(f1));
    }

    #[test]
    fn calculate_finish_date_uses_calendar() {
        let mut s = Schedule::new();
        let mut md = ScheduleMetadata::default();
        md.project_start_date = date(2025, 1, 6); // Monday
        s.set_metadata(md);

        s.upsert_task(1, "T1", 4, None).unwrap();
        // Set early_start explicitly
        s.update_date_column("early_start", 1, date(2025, 1, 6)).unwrap();

        let finish = s.calculate_finish_date(1).unwrap();
        // 4 working days after Monday should be Friday (skipping no holidays here)
        assert_eq!(finish.weekday(), chrono::Weekday::Fri);
    }
}

