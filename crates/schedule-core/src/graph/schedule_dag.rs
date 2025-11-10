use petgraph::graph::{DiGraph, NodeIndex};
use polars::prelude::*;
use std::collections::HashMap;

pub struct ScheduleDag {
    pub graph: DiGraph<i32, ()>,
    pub id_to_index: HashMap<i32, NodeIndex>,
    pub durations: HashMap<i32, i64>,
}

impl ScheduleDag {
    pub fn build(df: &DataFrame) -> Result<Self, PolarsError> {
        let ids_ca = df.column("id")?.i32()?;
        let durations_ca = df.column("duration_days")?.i64()?;
        let preds_lc = df.column("predecessors")?.list()?;

        let mut graph: DiGraph<i32, ()> = DiGraph::new();
        let mut id_to_index: HashMap<i32, NodeIndex> = HashMap::new();
        let mut durations: HashMap<i32, i64> = HashMap::new();

        // Add nodes first
        for (idx, id_opt) in ids_ca.into_iter().enumerate() {
            if let Some(task_id) = id_opt {
                let node_ix = graph.add_node(task_id);
                id_to_index.insert(task_id, node_ix);
                let dur = durations_ca.get(idx).unwrap_or(0);
                durations.insert(task_id, dur);
            }
        }

        // Add edges: pred -> task
        let ids_ca = df.column("id")?.i32()?;
        for (idx, id_opt) in ids_ca.into_iter().enumerate() {
            if let Some(task_id) = id_opt {
                if let Some(series) = preds_lc.get_as_series(idx) {
                    for pred_opt in series.i32()?.into_iter() {
                        if let Some(pred_id) = pred_opt {
                            if let (Some(&u), Some(&v)) =
                                (id_to_index.get(&pred_id), id_to_index.get(&task_id))
                            {
                                graph.add_edge(u, v, ());
                            }
                        }
                    }
                }
            }
        }

        Ok(Self {
            graph,
            id_to_index,
            durations,
        })
    }
}
