use chrono::NaiveDate;
use schedule_tool::{Schedule, ScheduleMetadata};

fn d(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

#[test]
fn backward_pass_sets_late_dates_and_floats() {
    let mut s = Schedule::new();
    // Set project start and end to match forward finish
    let mut md = ScheduleMetadata::default();
    md.project_start_date = d(2025, 1, 6);
    md.project_end_date = d(2025, 1, 17);
    s.set_metadata(md).unwrap();

    // Graph: 1 -> {2,3} -> 4 with durations 2,3,1,2
    s.upsert_task(1, "T1", 2, None).unwrap();
    s.upsert_task(2, "T2", 3, Some(vec![1])).unwrap();
    s.upsert_task(3, "T3", 1, Some(vec![1])).unwrap();
    s.upsert_task(4, "T4", 2, Some(vec![2, 3])).unwrap();

    // First compute earlys
    s.forward_pass().unwrap();
    // Then compute lates
    s.backward_pass().unwrap();

    let df = s.dataframe();
    let ids = df.column("id").unwrap().i32().unwrap();
    let es = df.column("early_start").unwrap().date().unwrap();
    let ef = df.column("early_finish").unwrap().date().unwrap();
    let ls = df.column("late_start").unwrap().date().unwrap();
    let lf = df.column("late_finish").unwrap().date().unwrap();
    let tf = df.column("total_float").unwrap().i64().unwrap();
    let crit = df.column("is_critical").unwrap().bool().unwrap();

    let mut m = std::collections::HashMap::new();
    for (i, id_opt) in ids.into_iter().enumerate() {
        if let Some(id) = id_opt {
            m.insert(
                id,
                (
                    es.get(i).unwrap(),
                    ef.get(i).unwrap(),
                    ls.get(i).unwrap(),
                    lf.get(i).unwrap(),
                    tf.get(i).unwrap(),
                    crit.get(i).unwrap(),
                ),
            );
        }
    }

    let epoch = d(1970, 1, 1);
    let td = |x: NaiveDate| (x - epoch).num_days() as i32;

    // T4 should match project end
    assert_eq!(
        m.get(&4).map(|v| (v.2, v.3)),
        Some((td(d(2025, 1, 15)), td(d(2025, 1, 17))))
    );
    // T2 on critical path
    assert_eq!(
        m.get(&2).map(|v| (v.2, v.3, v.4, v.5)),
        Some((td(d(2025, 1, 9)), td(d(2025, 1, 14)), 0, true))
    );
    // T3 has slack
    assert_eq!(m.get(&3).map(|v| (v.2, v.3, v.4, v.5)).unwrap().3, false);
    assert!(m.get(&3).unwrap().4 > 0);
    // T1 is critical
    assert_eq!(m.get(&1).unwrap().4, 0);
}
