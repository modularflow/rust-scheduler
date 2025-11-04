use polars::prelude::*;
use schedule_tool::graph::schedule_dag::ScheduleDag;

fn df_with_preds(ids: &[i32], preds: &[Vec<i32>]) -> DataFrame {
    let id_series = Series::new("id".into(), ids.to_vec());
    let duration_series = Series::new("duration_days".into(), vec![1_i64; ids.len()]);
    let preds_series_list: Vec<Series> = preds
        .iter()
        .map(|v| Series::new("".into(), v.clone()))
        .collect();
    let preds_series = Series::new("predecessors".into(), preds_series_list);
    DataFrame::new(vec![
        id_series.into_column(),
        duration_series.into_column(),
        preds_series.into_column(),
    ]).unwrap()
}

#[test]
fn schedule_dag_builds_edges_from_predecessors() {
    // 1 -> {2, 3}
    let df = df_with_preds(&[1, 2, 3], &vec![vec![], vec![1], vec![1]]);
    let dag = ScheduleDag::build(&df).unwrap();

    // Expect 3 nodes and 2 edges
    assert_eq!(dag.graph.node_count(), 3);
    assert_eq!(dag.graph.edge_count(), 2);
}


