use polars::prelude::*;
use polars::prelude::PlSmallStr;
use chrono::{NaiveDate, Duration};
use std::collections::HashMap;
use crate::calendar::WorkCalendar;
use crate::calculations::forward_pass::ForwardPass as CalcForwardPass;
use crate::calculations::backward_pass::BackwardPass as CalcBackwardPass;
use crate::metadata::ScheduleMetadata;

// (removed unused DateColumns struct)


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

    /// Convert NaiveDate to Polars i32 date
    fn date_to_i32(date: NaiveDate) -> i32 {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        (date - epoch).num_days() as i32
    }
    
    
    pub fn forward_pass(&mut self) -> Result<(), PolarsError> {
        if self.df.height() == 0 { return Ok(()); }
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
                    let duration = self.df.column("duration_days")?.i64()?.get(idx).unwrap_or(0);
                    let pred_ids: Vec<i32> = if let Some(series) = preds_lc.get_as_series(idx) {
                        series.i32()?.into_iter().flatten().collect()
                    } else { Vec::new() };
                    let project_start = self.metadata.project_start_date;
                    let early_start = if pred_ids.is_empty() {
                        project_start
                    } else {
                        let max_pred_finish = pred_ids.iter()
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

        let start_series = Series::new(PlSmallStr::from_static("early_start"), start_vals).cast(&DataType::Date)?;
        let finish_series = Series::new(PlSmallStr::from_static("early_finish"), finish_vals).cast(&DataType::Date)?;
        self.df.replace("early_start", start_series)?;
        self.df.replace("early_finish", finish_series)?;

        Ok(())
    }

    pub fn backward_pass(&mut self) -> Result<(), PolarsError> {
        if self.df.height() == 0 { return Ok(()); }
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
                if let Some(es_i) = es_dates.get(i) { ls_vals[i] = Some(es_i); }
            }
            if lf_vals[i].is_none() {
                if let Some(ef_i) = ef_dates.get(i) { lf_vals[i] = Some(ef_i); }
            }
        }

        let ls_series = Series::new(PlSmallStr::from_static("late_start"), ls_vals).cast(&DataType::Date)?;
        let lf_series = Series::new(PlSmallStr::from_static("late_finish"), lf_vals).cast(&DataType::Date)?;
        self.df.replace("late_start", ls_series)?;
        self.df.replace("late_finish", lf_series)?;

        // Compute total_float and is_critical
        let es_col = self.df.column("early_start")?.date()?;
        let mut es_map: HashMap<i32, i32> = HashMap::new();
        for (i, id_opt) in self.df.column("id")?.i32()?.into_iter().enumerate() {
            if let Some(id) = id_opt {
                if let Some(es_days) = es_col.get(i) { es_map.insert(id, es_days); }
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
    use chrono::{NaiveDate, Datelike};

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

    
    
}

