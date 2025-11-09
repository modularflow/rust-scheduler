pub mod calendar;
pub mod metadata;
pub mod schedule;
pub mod graph;
pub mod calculations;
pub mod task;
pub mod persistence;

pub use calendar::WorkCalendar;
pub use metadata::ScheduleMetadata;
pub use schedule::{Schedule, RefreshSummary};
pub use task::{ProgressMeasurement, RationaleItem, Task};
pub use persistence::{
    load_schedule_from_csv,
    load_schedule_from_json,
    validate_schedule,
    validate_tasks,
    save_schedule_to_csv,
    save_schedule_to_json,
    ScheduleStore,
    PersistenceError,
};
#[cfg(feature = "sqlite")]
pub use persistence::sqlite::SqliteScheduleStore;