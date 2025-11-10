use serde::{Deserialize, Serialize};

/// Represents an allocation of a resource (person, equipment, cost bucket) to a task.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResourceAllocation {
    /// Identifier for the resource. This can be a person id, crew name, or equipment tag.
    pub resource_id: String,
    /// Optional role or description for the resource while working on the task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// Planned units for the allocation (e.g., hours or FTE-days). Must be non-negative.
    pub allocation_units: f64,
    /// Optional cost rate per unit (e.g., hourly rate). Non-negative when provided.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_rate: Option<f64>,
    /// Optional free-form notes about the allocation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

impl ResourceAllocation {
    pub fn new(resource_id: impl Into<String>, allocation_units: f64) -> Self {
        Self {
            resource_id: resource_id.into(),
            role: None,
            allocation_units,
            cost_rate: None,
            notes: None,
        }
    }
}
