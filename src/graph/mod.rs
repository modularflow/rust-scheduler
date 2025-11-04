use std::collections::HashMap;
use chrono::NaiveDate;

#[derive(Debug, Clone)]
pub struct Branch {
    pub id: usize,
    pub tasks: Vec<i32>,
    pub entry_point: i32,
    pub exit_point: i32,
    pub is_critical: bool,
}

#[derive(Debug, Clone)]
pub struct JoinPoint {
    pub task_id: i32,
    pub incoming_branches: Vec<usize>,
    pub outgoing_branches: Vec<usize>,
}

#[derive(Debug, Clone)]
pub enum ExecutionNode {
    ParallelBranches(Vec<usize>),
    SequentialBranch(usize),
    JoinPoint(i32),
}

#[derive(Debug, Clone)]
pub struct MetaDependencyTree {
    pub branches: HashMap<usize, Branch>,
    pub join_points: HashMap<i32, JoinPoint>,
    pub branch_dependencies: HashMap<usize, Vec<usize>>,
    pub execution_order: Vec<ExecutionNode>,
    pub task_to_branch: HashMap<i32, usize>,
}

impl MetaDependencyTree {
    pub fn new() -> Self {
        Self {
            branches: HashMap::new(),
            join_points: HashMap::new(),
            branch_dependencies: HashMap::new(),
            execution_order: Vec::new(),
            task_to_branch: HashMap::new(),
        }
    }
}

pub mod branch;
pub mod builder;
pub mod analyzer;