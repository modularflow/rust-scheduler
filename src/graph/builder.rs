use super::{Branch, JoinPoint, MetaDependencyTree, ExecutionNode};
use std::collections::{HashMap, HashSet, VecDeque};
use polars::prelude::*;

pub struct GraphBuilder<'a> {
    df: &'a DataFrame,
}

impl<'a> GraphBuilder<'a> {
    pub fn new(df: &'a DataFrame) -> Self {
        Self { df }
    }
    
    pub fn build(&self) -> Result<MetaDependencyTree, PolarsError> {
        // Step 1: Extract graph structure
        let (graph, reverse_graph, in_degree, out_degree) = self.build_graph_structure()?;
        
        // Step 2: Identify branches and join points
        let (branches, join_points) = self.identify_branches_and_joins(
            &graph,
            &reverse_graph,
            &in_degree,
            &out_degree,
        )?;
        
        // Step 3: Build branch dependencies
        let branch_dependencies = self.build_branch_dependencies(&branches)?;
        
        // Step 4: Determine execution order
        let execution_order = self.determine_execution_order(&branches, &branch_dependencies)?;
        
        // Step 5: Build task-to-branch lookup
        let mut task_to_branch = HashMap::new();
        for (branch_id, branch) in &branches {
            for &task_id in &branch.tasks {
                task_to_branch.insert(task_id, *branch_id);
            }
        }
        
        Ok(MetaDependencyTree {
            branches,
            join_points,
            branch_dependencies,
            execution_order,
            task_to_branch,
        })
    }
    
    fn build_graph_structure(&self) -> Result<(
        HashMap<i32, Vec<i32>>,
        HashMap<i32, Vec<i32>>,
        HashMap<i32, usize>,
        HashMap<i32, usize>,
    ), PolarsError> {
        let task_ids: Vec<i32> = self.df.column("id")?
            .i32()?
            .into_iter()
            .filter_map(|x| x)
            .collect();
        
        let mut graph: HashMap<i32, Vec<i32>> = HashMap::new();
        let mut reverse_graph: HashMap<i32, Vec<i32>> = HashMap::new();
        let mut in_degree: HashMap<i32, usize> = HashMap::new();
        let mut out_degree: HashMap<i32, usize> = HashMap::new();
        
        // Initialize
        for task_id in &task_ids {
            graph.insert(*task_id, Vec::new());
            reverse_graph.insert(*task_id, Vec::new());
            in_degree.insert(*task_id, 0);
            out_degree.insert(*task_id, 0);
        }
        
        // Build edges from predecessors column
        let predecessors_col = self.df.column("predecessors")?;
        let list_chunked = predecessors_col.list()?;
        
        for (idx, task_id) in task_ids.iter().enumerate() {
            if let Some(preds_series) = list_chunked.get_as_series(idx) {
                let preds: Vec<i32> = preds_series.i32()?
                    .into_iter()
                    .filter_map(|x| x)
                    .collect();
                
                *in_degree.get_mut(task_id).unwrap() = preds.len();
                
                for pred_id in preds {
                    graph.entry(pred_id).or_default().push(*task_id);
                    reverse_graph.entry(*task_id).or_default().push(pred_id);
                    *out_degree.entry(pred_id).or_default() += 1;
                }
            }
        }
        
        Ok((graph, reverse_graph, in_degree, out_degree))
    }
    
    fn identify_branches_and_joins(
        &self,
        graph: &HashMap<i32, Vec<i32>>,
        reverse_graph: &HashMap<i32, Vec<i32>>,
        in_degree: &HashMap<i32, usize>,
        out_degree: &HashMap<i32, usize>,
    ) -> Result<(HashMap<usize, Branch>, HashMap<i32, JoinPoint>), PolarsError> {
        let mut join_points = HashMap::new();
        let task_ids: Vec<i32> = graph.keys().copied().collect();
        
        // Identify join points: tasks with multiple predecessors or successors,
        // or tasks at boundaries (no predecessors or successors)
        for task_id in &task_ids {
            let in_deg = in_degree[task_id];
            let out_deg = out_degree[task_id];
            
            if in_deg > 1 || out_deg > 1 || in_deg == 0 || out_deg == 0 {
                join_points.insert(*task_id, JoinPoint {
                    task_id: *task_id,
                    incoming_branches: Vec::new(),
                    outgoing_branches: Vec::new(),
                });
            }
        }
        
        // Build branches: linear sequences between join points
        let mut branches = HashMap::new();
        let mut branch_id = 0;
        let mut visited = HashSet::new();
        
        let join_tasks: Vec<i32> = join_points.keys().copied().collect();
        
        for start_task in &join_tasks {
            if let Some(successors) = graph.get(start_task) {
                for &successor in successors {
                    if visited.contains(&successor) {
                        continue;
                    }
                    
                    // Trace linear path until next join point
                    let mut branch_tasks = vec![successor];
                    let mut current = successor;
                    visited.insert(current);
                    
                    loop {
                        let succs = &graph[&current];
                        
                        // Stop at join point or end
                        if succs.is_empty() || join_points.contains_key(&current) || succs.len() > 1 {
                            break;
                        }
                        
                        let next = succs[0];
                        let preds = &reverse_graph[&next];
                        
                        // Stop if next has multiple predecessors
                        if preds.len() > 1 {
                            break;
                        }
                        
                        if visited.contains(&next) {
                            break;
                        }
                        
                        branch_tasks.push(next);
                        visited.insert(next);
                        current = next;
                    }
                    
                    if !branch_tasks.is_empty() {
                        let branch = Branch {
                            id: branch_id,
                            tasks: branch_tasks.clone(),
                            entry_point: branch_tasks[0],
                            exit_point: *branch_tasks.last().unwrap(),
                            is_critical: false,
                        };
                        
                        branches.insert(branch_id, branch);
                        branch_id += 1;
                    }
                }
            }
        }
        
        Ok((branches, join_points))
    }
    
    fn build_branch_dependencies(
        &self,
        branches: &HashMap<usize, Branch>,
    ) -> Result<HashMap<usize, Vec<usize>>, PolarsError> {
        let mut dependencies: HashMap<usize, Vec<usize>> = HashMap::new();
        
        // Build reverse lookup: task_id -> branch_id
        let mut task_to_branch: HashMap<i32, usize> = HashMap::new();
        for (branch_id, branch) in branches {
            for &task_id in &branch.tasks {
                task_to_branch.insert(task_id, *branch_id);
            }
        }
        
        // For each branch, find predecessor branches
        let predecessors_col = self.df.column("predecessors")?;
        let list_chunked = predecessors_col.list()?;
        
        for (branch_id, branch) in branches {
            let mut pred_branches = HashSet::new();
            
            // Get predecessors of the entry point
            let entry_task = branch.entry_point;
            
            // Find the row index for this task
            let task_ids = self.df.column("id")?.i32()?;
            if let Some(idx) = task_ids.into_iter()
                .position(|id| id == Some(entry_task))
            {
                if let Some(preds_series) = list_chunked.get_as_series(idx) {
                    let preds: Vec<i32> = preds_series.i32()?
                        .into_iter()
                        .filter_map(|x| x)
                        .collect();
                    
                    // Find which branches contain these predecessors
                    for pred_id in preds {
                        if let Some(&pred_branch_id) = task_to_branch.get(&pred_id) {
                            pred_branches.insert(pred_branch_id);
                        }
                    }
                }
            }
            
            dependencies.insert(*branch_id, pred_branches.into_iter().collect());
        }
        
        Ok(dependencies)
    }
    
    fn determine_execution_order(
        &self,
        branches: &HashMap<usize, Branch>,
        dependencies: &HashMap<usize, Vec<usize>>,
    ) -> Result<Vec<ExecutionNode>, PolarsError> {
        let mut execution_order = Vec::new();
        let mut completed = HashSet::new();
        let mut in_degree: HashMap<usize, usize> = HashMap::new();
        
        // Calculate in-degrees
        for (branch_id, deps) in dependencies {
            in_degree.insert(*branch_id, deps.len());
        }
        
        // Topological sort with level detection
        while completed.len() < branches.len() {
            // Find all branches with no pending dependencies
            let ready: Vec<usize> = in_degree
                .iter()
                .filter_map(|(branch_id, deg)| {
                    if *deg == 0 && !completed.contains(branch_id) {
                        Some(*branch_id)
                    } else {
                        None
                    }
                })
                .collect();
            
            if ready.is_empty() {
                break; // Cycle or done
            }
            
            // These can run in parallel
            if ready.len() > 1 {
                execution_order.push(ExecutionNode::ParallelBranches(ready.clone()));
            } else {
                execution_order.push(ExecutionNode::SequentialBranch(ready[0]));
            }
            
            // Mark as completed and update dependencies
            for branch_id in ready {
                completed.insert(branch_id);
                
                // Reduce in-degree of dependent branches
                for (other_id, deps) in dependencies {
                    if deps.contains(&branch_id) {
                        *in_degree.get_mut(other_id).unwrap() -= 1;
                    }
                }
            }
        }
        
        Ok(execution_order)
    }
}