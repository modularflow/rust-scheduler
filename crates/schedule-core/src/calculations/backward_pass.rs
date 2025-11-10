use crate::calendar::WorkCalendar;
use crate::graph::schedule_dag::ScheduleDag;
use chrono::NaiveDate;
use petgraph::Direction;
use petgraph::algo::toposort;
use polars::prelude::*;
use std::collections::HashMap;

pub struct BackwardPass<'a> {
    df: &'a DataFrame,
    calendar: &'a WorkCalendar,
}

impl<'a> BackwardPass<'a> {
    pub fn new(df: &'a DataFrame, calendar: &'a WorkCalendar) -> Self {
        Self { df, calendar }
    }

    pub fn execute(
        &self,
        project_end: NaiveDate,
    ) -> Result<HashMap<i32, (NaiveDate, NaiveDate)>, PolarsError> {
        let dag = ScheduleDag::build(self.df)?;

        // LS/LF maps keyed by task id
        let mut late_starts: HashMap<i32, NaiveDate> = HashMap::new();
        let mut late_finishes: HashMap<i32, NaiveDate> = HashMap::new();

        // Reverse topological order
        let mut order = toposort(&dag.graph, None)
            .map_err(|_| PolarsError::ComputeError("Cycle detected in schedule DAG".into()))?;
        order.reverse();

        for node_ix in order {
            let task_id = dag.graph[node_ix];

            // Determine allowed late finish from successors' late starts
            let mut lf = project_end;
            let mut has_succ = false;
            for succ_ix in dag.graph.neighbors_directed(node_ix, Direction::Outgoing) {
                let succ_id = dag.graph[succ_ix];
                if let Some(ls_succ) = late_starts.get(&succ_id).copied() {
                    has_succ = true;
                    let prev = self.calendar.prev_available(ls_succ);
                    if prev < lf {
                        lf = prev;
                    }
                }
            }
            if !has_succ {
                lf = project_end;
            }

            let duration = *dag.durations.get(&task_id).unwrap_or(&0);
            let ls = self.calendar.find_prev_available(lf, duration);

            late_finishes.insert(task_id, lf);
            late_starts.insert(task_id, ls);
        }

        // Pack results
        let mut results = HashMap::new();
        for (task_id, ls) in late_starts.into_iter() {
            if let Some(&lf) = late_finishes.get(&task_id) {
                results.insert(task_id, (ls, lf));
            }
        }
        Ok(results)
    }
}
