use crate::{
    task::ProgressMeasurement,
    Schedule,
    Task,
};
use polars::prelude::PolarsError;
use serde_json::Error as SerdeJsonError;
use std::fmt;
use std::io;
use std::collections::HashSet;

#[derive(Debug)]
pub enum PersistenceError {
    Serialization(SerdeJsonError),
    DataFrame(PolarsError),
    Io(io::Error),
    #[cfg(feature = "sqlite")]
    Sqlite(rusqlite::Error),
    Csv(csv::Error),
    InvalidData(String),
    NotFound,
}

impl fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PersistenceError::Serialization(err) => write!(f, "serialization error: {err}"),
            PersistenceError::DataFrame(err) => write!(f, "dataframe conversion error: {err}"),
            PersistenceError::Io(err) => write!(f, "io error: {err}"),
            PersistenceError::Sqlite(err) => write!(f, "sqlite error: {err}"),
            PersistenceError::Csv(err) => write!(f, "csv error: {err}"),
            PersistenceError::InvalidData(msg) => write!(f, "invalid data: {msg}"),
            PersistenceError::NotFound => write!(f, "no schedule stored"),
        }
    }
}

impl std::error::Error for PersistenceError {}

impl From<SerdeJsonError> for PersistenceError {
    fn from(value: SerdeJsonError) -> Self {
        Self::Serialization(value)
    }
}

impl From<PolarsError> for PersistenceError {
    fn from(value: PolarsError) -> Self {
        Self::DataFrame(value)
    }
}

impl From<io::Error> for PersistenceError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

#[cfg(feature = "sqlite")]
impl From<rusqlite::Error> for PersistenceError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Sqlite(value)
    }
}

impl From<csv::Error> for PersistenceError {
    fn from(value: csv::Error) -> Self {
        Self::Csv(value)
    }
}

pub type PersistenceResult<T> = Result<T, PersistenceError>;

pub trait ScheduleStore {
    fn save_schedule(&self, schedule: &Schedule) -> PersistenceResult<()>;
    fn load_schedule(&self) -> PersistenceResult<Option<Schedule>>;
}

const EPSILON: f64 = 1e-6;

fn approx_equal(a: f64, b: f64) -> bool {
    (a - b).abs() <= EPSILON
}

pub fn validate_tasks(tasks: &[Task]) -> PersistenceResult<()> {
    let mut seen_ids = HashSet::with_capacity(tasks.len());
    for task in tasks {
        if task.duration_days < 0 {
            return Err(PersistenceError::InvalidData(format!(
                "task {} has negative duration {}",
                task.id, task.duration_days
            )));
        }
        if !seen_ids.insert(task.id) {
            return Err(PersistenceError::InvalidData(format!(
                "duplicate task id {}",
                task.id
            )));
        }

        if let Some(pct) = task.percent_complete {
            if !pct.is_finite() || pct < -EPSILON || pct > 1.0 + EPSILON {
                return Err(PersistenceError::InvalidData(format!(
                    "task {} has invalid percent_complete {} (must be between 0 and 1)",
                    task.id, pct
                )));
            }
        }

        match task.progress_measurement {
            ProgressMeasurement::ZeroOneHundred => {
                if let Some(pct) = task.percent_complete {
                    if !(approx_equal(pct, 0.0) || approx_equal(pct, 1.0)) {
                        return Err(PersistenceError::InvalidData(format!(
                            "task {} progress_measurement=0_100 requires percent_complete of 0 or 1 (got {})",
                            task.id, pct
                        )));
                    }
                }
            }
            ProgressMeasurement::FiftyFifty => {
                if let Some(pct) = task.percent_complete {
                    let allowed = [0.0, 0.5, 1.0];
                    if !allowed.iter().any(|v| approx_equal(*v, pct)) {
                        return Err(PersistenceError::InvalidData(format!(
                            "task {} progress_measurement=50_50 requires percent_complete of 0, 0.5, or 1 (got {})",
                            task.id, pct
                        )));
                    }
                }
            }
            ProgressMeasurement::TwentyFiveSeventyFive | ProgressMeasurement::SeventyFiveTwentyFive => {
                if let Some(pct) = task.percent_complete {
                    let allowed = [0.0, 0.25, 0.75, 1.0];
                    if !allowed.iter().any(|v| approx_equal(*v, pct)) {
                        return Err(PersistenceError::InvalidData(format!(
                            "task {} progress_measurement={} requires percent_complete of 0, 0.25, 0.75, or 1 (got {})",
                            task.id,
                            task.progress_measurement.as_str(),
                            pct
                        )));
                    }
                }
            }
            ProgressMeasurement::PercentComplete => {
                // already ensured 0..=1 above
            }
            ProgressMeasurement::PreDefinedRationale => {
                if task.pre_defined_rationale.is_empty() {
                    return Err(PersistenceError::InvalidData(format!(
                        "task {} progress_measurement=pre_defined_rationale requires at least one rationale item",
                        task.id
                    )));
                }
                let mut total = 0.0;
                let mut rationale_ids = HashSet::with_capacity(task.pre_defined_rationale.len());
                for rationale in &task.pre_defined_rationale {
                    if !rationale.weight.is_finite() {
                        return Err(PersistenceError::InvalidData(format!(
                            "task {} has non-finite rationale weight for '{}'",
                            task.id, rationale.name
                        )));
                    }
                    if rationale.weight < 0.0 {
                        return Err(PersistenceError::InvalidData(format!(
                            "task {} has negative rationale weight for '{}'",
                            task.id, rationale.name
                        )));
                    }
                    if !rationale_ids.insert(rationale.id) {
                        return Err(PersistenceError::InvalidData(format!(
                            "task {} has duplicate rationale id {}",
                            task.id, rationale.id
                        )));
                    }
                    total += rationale.weight;
                    // is_complete is a bool; no extra check needed beyond existence.
                }
                if !approx_equal(total, 1.0) {
                    return Err(PersistenceError::InvalidData(format!(
                        "task {} pre_defined_rationale weights must sum to 1.0 (got {:.4})",
                        task.id, total
                    )));
                }
            }
        }
    }
    Ok(())
}

pub fn validate_schedule(schedule: &Schedule) -> PersistenceResult<()> {
    let df = schedule.dataframe();
    let mut tasks = Vec::with_capacity(df.height());
    for idx in 0..df.height() {
        tasks.push(Task::from_dataframe_row(df, idx)?);
    }
    validate_tasks(&tasks)
}

#[cfg(feature = "sqlite")]
pub mod sqlite;
pub mod file;

pub use file::{
    load_schedule_from_csv, load_schedule_from_json, save_schedule_to_csv, save_schedule_to_json,
};

