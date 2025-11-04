use polars::prelude::*;
use schedule_tool::graph::builder::GraphBuilder;

fn df_with_preds(ids: &[i32], preds: &[Vec<i32>]) -> DataFrame {
    let id_series = Series::new("id".into(), ids.to_vec());
    let preds_series_list: Vec<Series> = preds
        .iter()
        .map(|v| Series::new("".into(), v.clone()))
        .collect();
    let preds_series = Series::new("predecessors".into(), preds_series_list);
    DataFrame::new(vec![id_series.into_column(), preds_series.into_column()]).unwrap()
}

#[test]
fn simple_fan_out_produces_two_branches() {
    // 1 -> {2, 3}
    let df = df_with_preds(&[1, 2, 3], &vec![vec![], vec![1], vec![1]]);
    let builder = GraphBuilder::new(&df);
    let tree = builder.build().unwrap();

    assert_eq!(tree.branches.len(), 2);
    assert!(tree.join_points.contains_key(&1));
    assert!(tree.task_to_branch.contains_key(&2));
    assert!(tree.task_to_branch.contains_key(&3));
}


