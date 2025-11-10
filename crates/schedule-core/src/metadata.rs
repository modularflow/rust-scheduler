use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleMetadata {
    pub project_name: String,
    pub project_description: String,
    pub project_start_date: NaiveDate,
    pub project_end_date: NaiveDate,
}

impl Default for ScheduleMetadata {
    fn default() -> Self {
        Self {
            project_name: "New Project".to_string(),
            project_description: "No description".to_string(),
            project_start_date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            project_end_date: NaiveDate::from_ymd_opt(2025, 12, 31).unwrap(),
        }
    }
}
