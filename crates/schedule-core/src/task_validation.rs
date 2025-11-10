use crate::task::{ProgressMeasurement, Task};
use std::collections::HashSet;
use std::fmt;

const EPSILON: f64 = 1e-6;

#[derive(Debug, Clone)]
pub struct TaskValidationError {
    message: String,
}

impl TaskValidationError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for TaskValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for TaskValidationError {}

fn approx_equal(a: f64, b: f64) -> bool {
    (a - b).abs() <= EPSILON
}

pub fn validate_task(task: &Task) -> Result<(), TaskValidationError> {
    if task.duration_days < 0 {
        return Err(TaskValidationError::new(format!(
            "task {} has negative duration {}",
            task.id, task.duration_days
        )));
    }

    if let Some(pct) = task.percent_complete {
        if !pct.is_finite() || pct < -EPSILON || pct > 1.0 + EPSILON {
            return Err(TaskValidationError::new(format!(
                "task {} has invalid percent_complete {} (must be between 0 and 1)",
                task.id, pct
            )));
        }
    }

    match task.progress_measurement {
        ProgressMeasurement::ZeroOneHundred => {
            if let Some(pct) = task.percent_complete {
                if !(approx_equal(pct, 0.0) || approx_equal(pct, 1.0)) {
                    return Err(TaskValidationError::new(format!(
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
                    return Err(TaskValidationError::new(format!(
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
                    return Err(TaskValidationError::new(format!(
                        "task {} progress_measurement={} requires percent_complete of 0, 0.25, 0.75, or 1 (got {})",
                        task.id,
                        task.progress_measurement.as_str(),
                        pct
                    )));
                }
            }
        }
        ProgressMeasurement::PercentComplete => {}
        ProgressMeasurement::PreDefinedRationale => {
            if task.pre_defined_rationale.is_empty() {
                return Err(TaskValidationError::new(format!(
                    "task {} progress_measurement=pre_defined_rationale requires at least one rationale item",
                    task.id
                )));
            }
            let mut total = 0.0;
            let mut rationale_ids = HashSet::with_capacity(task.pre_defined_rationale.len());
            for rationale in &task.pre_defined_rationale {
                if !rationale.weight.is_finite() {
                    return Err(TaskValidationError::new(format!(
                        "task {} has non-finite rationale weight for '{}'",
                        task.id, rationale.name
                    )));
                }
                if rationale.weight < 0.0 {
                    return Err(TaskValidationError::new(format!(
                        "task {} has negative rationale weight for '{}'",
                        task.id, rationale.name
                    )));
                }
                if !rationale_ids.insert(rationale.id) {
                    return Err(TaskValidationError::new(format!(
                        "task {} has duplicate rationale id {}",
                        task.id, rationale.id
                    )));
                }
                total += rationale.weight;
            }
            if !approx_equal(total, 1.0) {
                return Err(TaskValidationError::new(format!(
                    "task {} pre_defined_rationale weights must sum to 1.0 (got {:.4})",
                    task.id, total
                )));
            }
        }
    }

    for (idx, allocation) in task.resource_allocations.iter().enumerate() {
        if allocation.resource_id.trim().is_empty() {
            return Err(TaskValidationError::new(format!(
                "task {} resource allocation #{} requires a non-empty resource_id",
                task.id, idx
            )));
        }
        if !allocation.allocation_units.is_finite() || allocation.allocation_units < -EPSILON {
            return Err(TaskValidationError::new(format!(
                "task {} allocation for '{}' has invalid allocation_units {}",
                task.id, allocation.resource_id, allocation.allocation_units
            )));
        }
        if let Some(cost_rate) = allocation.cost_rate {
            if !cost_rate.is_finite() || cost_rate < -EPSILON {
                return Err(TaskValidationError::new(format!(
                    "task {} allocation for '{}' has invalid cost_rate {}",
                    task.id, allocation.resource_id, cost_rate
                )));
            }
        }
    }

    Ok(())
}

pub fn validate_task_collection(tasks: &[Task]) -> Result<(), TaskValidationError> {
    let mut seen_ids = HashSet::with_capacity(tasks.len());
    for task in tasks {
        if !seen_ids.insert(task.id) {
            return Err(TaskValidationError::new(format!(
                "duplicate task id {}",
                task.id
            )));
        }
        validate_task(task)?;
    }
    Ok(())
}
