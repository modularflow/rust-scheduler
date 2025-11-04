use crate::calendar::WorkCalendar;
use crate::graph::{MetaDependencyTree, ExecutionNode, Branch};
use polars::prelude::*;
use chrono::NaiveDate;
use std::collections::HashMap;
use rayon::prelude::*;

pub struct ForwardPass<'a> {
    df: &'a DataFrame,
    calendar: &'a WorkCalendar,
    tree: &'a MetaDependencyTree,
}

impl<'a> ForwardPass<'a> {
    pub fn new(df: &'a DataFrame, calendar: &'a WorkCalendar, tree: &'a MetaDependencyTree) -> Self {
        Self { df, calendar, tree }
    }
    
    pub fn execute(&self, project_start: NaiveDate) -> Result<HashMap<i32, (NaiveDate, NaiveDate)>, PolarsError> {
        let mut early_starts: HashMap<i32, NaiveDate> = HashMap::new();
        let mut early_finishes: HashMap<i32, NaiveDate> = HashMap::new();
        
        // Get task data for fast access
        let task_to_idx = self.build_task_index()?;
        let durations = self.df.column("duration_days")?.i64()?;
        
        // Execute according to the optimal plan
        for execution_node in &self.tree.execution_order {
            match execution_node {
                ExecutionNode::ParallelBranches(branch_ids) => {
                    // Process branches in parallel
                    let branch_results: Vec<Vec<(i32, NaiveDate, NaiveDate)>> = branch_ids
                        .par_iter()
                        .map(|branch_id| {
                            let branch = &self.tree.branches[branch_id];
                            self.process_branch(
                                branch,
                                &early_finishes,
                                &task_to_idx,
                                &durations,
                                project_start,
                            )
                        })
                        .collect();
                    
                    // Merge results
                    for results in branch_results {
                        for (task_id, early_start, early_finish) in results {
                            early_starts.insert(task_id, early_start);
                            early_finishes.insert(task_id, early_finish);
                        }
                    }
                }
                
                ExecutionNode::SequentialBranch(branch_id) => {
                    let branch = &self.tree.branches[branch_id];
                    let results = self.process_branch(
                        branch,
                        &early_finishes,
                        &task_to_idx,
                        &durations,
                        project_start,
                    );
                    
                    for (task_id, early_start, early_finish) in results {
                        early_starts.insert(task_id, early_start);
                        early_finishes.insert(task_id, early_finish);
                    }
                }
                
                ExecutionNode::JoinPoint(task_id) => {
                    if let Some(join) = self.tree.join_points.get(task_id) {
                        let max_pred_finish = join.incoming_branches.iter()
                            .filter_map(|branch_id| {
                                let branch = &self.tree.branches[branch_id];
                                early_finishes.get(&branch.exit_point)
                            })
                            .max()
                            .copied()
                            .unwrap_or(project_start);
                        
                        let idx = task_to_idx[task_id];
                        let duration = durations.get(idx).unwrap();
                        let early_start = self.calendar.next_available(max_pred_finish);
                        let early_finish = self.calendar.find_next_available(early_start, duration);
                        
                        early_starts.insert(*task_id, early_start);
                        early_finishes.insert(*task_id, early_finish);
                    }
                }
            }
        }
        
        // Combine into result
        let mut results = HashMap::new();
        for (task_id, early_start) in early_starts {
            if let Some(early_finish) = early_finishes.get(&task_id) {
                results.insert(task_id, (early_start, *early_finish));
            }
        }
        
        Ok(results)
    }
    
    fn build_task_index(&self) -> Result<HashMap<i32, usize>, PolarsError> {
        let mut task_to_idx = HashMap::new();
        let task_ids = self.df.column("id")?.i32()?;
        
        for (idx, task_id) in task_ids.into_iter().enumerate() {
            if let Some(id) = task_id {
                task_to_idx.insert(id, idx);
            }
        }
        
        Ok(task_to_idx)
    }
    
    fn process_branch(
        &self,
        branch: &Branch,
        early_finishes: &HashMap<i32, NaiveDate>,
        task_to_idx: &HashMap<i32, usize>,
        durations: &Int64Chunked,
        project_start: NaiveDate,
    ) -> Vec<(i32, NaiveDate, NaiveDate)> {
        let mut results = Vec::new();
        let mut branch_finishes: HashMap<i32, NaiveDate> = HashMap::new();
        
        let predecessors_col = self.df.column("predecessors").unwrap();
        let list_chunked = predecessors_col.list().unwrap();
        
        for &task_id in &branch.tasks {
            let idx = task_to_idx[&task_id];
            let duration = durations.get(idx).unwrap();
            
            // Get predecessors
            let pred_ids: Vec<i32> = if let Some(preds_series) = list_chunked.get_as_series(idx) {
                preds_series.i32().unwrap()
                    .into_iter()
                    .filter_map(|x| x)
                    .collect()
            } else {
                Vec::new()
            };
            
            // Early start is max of all predecessor finishes
            let early_start = if pred_ids.is_empty() {
                project_start
            } else {
                pred_ids.iter()
                    .filter_map(|pred_id| {
                        branch_finishes.get(pred_id)
                            .or_else(|| early_finishes.get(pred_id))
                    })
                    .max()
                    .map(|date| self.calendar.next_available(*date))
                    .unwrap_or(project_start)
            };
            
            let early_finish = self.calendar.find_next_available(early_start, duration);
            branch_finishes.insert(task_id, early_finish);
            results.push((task_id, early_start, early_finish));
        }
        
        results
    }
}