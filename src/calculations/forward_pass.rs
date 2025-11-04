use crate::calendar::WorkCalendar;
use crate::graph::schedule_dag::ScheduleDag;
use polars::prelude::*;
use chrono::NaiveDate;
use std::collections::HashMap;
use petgraph::algo::toposort;
use petgraph::Direction;

pub struct ForwardPass<'a> {
    df: &'a DataFrame,
    calendar: &'a WorkCalendar,
}

impl<'a> ForwardPass<'a> {
    pub fn new(df: &'a DataFrame, calendar: &'a WorkCalendar) -> Self {
        Self { df, calendar }
    }
    
    pub fn execute(&self, project_start: NaiveDate) -> Result<HashMap<i32, (NaiveDate, NaiveDate)>, PolarsError> {
        let dag = ScheduleDag::build(self.df)?;

        // ES/EF maps keyed by task id
        let mut early_starts: HashMap<i32, NaiveDate> = HashMap::new();
        let mut early_finishes: HashMap<i32, NaiveDate> = HashMap::new();

        // Topological order over nodes
        let order = toposort(&dag.graph, None)
            .map_err(|_| PolarsError::ComputeError("Cycle detected in schedule DAG".into()))?;

        for node_ix in order {
            let task_id = dag.graph[node_ix];

            // Determine early start from predecessors' early finishes
            let mut es = project_start;
            let mut has_pred = false;
            for pred_ix in dag.graph.neighbors_directed(node_ix, Direction::Incoming) {
                let pred_id = dag.graph[pred_ix];
                if let Some(ef) = early_finishes.get(&pred_id).copied() {
                    has_pred = true;
                    if ef > es { es = ef; }
                }
            }
            if has_pred {
                es = self.calendar.next_available(es);
            }

            let duration = *dag.durations.get(&task_id).unwrap_or(&0);
            let ef = self.calendar.find_next_available(es, duration);

            early_starts.insert(task_id, es);
            early_finishes.insert(task_id, ef);
        }

        // Pack results
        let mut results = HashMap::new();
        for (task_id, es) in early_starts.into_iter() {
            if let Some(&ef) = early_finishes.get(&task_id) {
                results.insert(task_id, (es, ef));
            }
        }
        Ok(results)
    }
    

    
    // Note: branch-based processing replaced by petgraph traversal.
}