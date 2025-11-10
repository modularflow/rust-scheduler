use crate::task_validation;
use crate::{Schedule, Task};
use polars::prelude::PolarsError;
use serde_json::Error as SerdeJsonError;
use std::fmt;
use std::io;

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

pub fn validate_tasks(tasks: &[Task]) -> PersistenceResult<()> {
    task_validation::validate_task_collection(tasks)
        .map_err(|err| PersistenceError::InvalidData(err.to_string()))
}

pub fn validate_schedule(schedule: &Schedule) -> PersistenceResult<()> {
    let df = schedule.dataframe();
    let mut tasks = Vec::with_capacity(df.height());
    for idx in 0..df.height() {
        tasks.push(Task::from_dataframe_row(df, idx)?);
    }
    validate_tasks(&tasks)
}

pub mod file;
#[cfg(feature = "sqlite")]
pub mod sqlite;

pub use file::{
    load_schedule_from_csv, load_schedule_from_json, save_schedule_to_csv, save_schedule_to_json,
};
