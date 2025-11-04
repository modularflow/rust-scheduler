use schedule_tool::Schedule;
use chrono::NaiveDate;

fn d(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

#[test]
fn forward_pass_computes_early_dates_across_dag() {
    let mut s = Schedule::new();
    // Set project start to Monday 2025-01-06
    let mut md = schedule_tool::ScheduleMetadata::default();
    md.project_start_date = d(2025, 1, 6);
    s.set_metadata(md);

    // Graph:
    // 1(2d) -> {2(3d), 3(1d)} -> 4(2d)
    s.upsert_task(1, "T1", 2, None).unwrap();
    s.upsert_task(2, "T2", 3, Some(vec![1])).unwrap();
    s.upsert_task(3, "T3", 1, Some(vec![1])).unwrap();
    s.upsert_task(4, "T4", 2, Some(vec![2, 3])).unwrap();

    s.forward_pass().unwrap();

    let df = s.dataframe();
    let ids = df.column("id").unwrap().i32().unwrap();
    let es = df.column("early_start").unwrap().date().unwrap();
    let ef = df.column("early_finish").unwrap().date().unwrap();

    // Build quick lookup
    let mut m: std::collections::HashMap<i32, (i32, i32)> = std::collections::HashMap::new();
    for (i, id_opt) in ids.into_iter().enumerate() {
        if let Some(id) = id_opt {
            let esd = es.get(i).unwrap();
            let efd = ef.get(i).unwrap();
            m.insert(id, (esd, efd));
        }
    }

    // Expected:
    // T1: start 2025-01-06, finish 2025-01-08
    // T2: start 2025-01-09, finish 2025-01-14 (exclusive add)
    // T3: start 2025-01-09, finish 2025-01-10
    // T4: start 2025-01-15, finish 2025-01-17 (exclusive add)

    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let to_days = |dt: NaiveDate| (dt - epoch).num_days() as i32;

    assert_eq!(m.get(&1).copied(), Some((to_days(d(2025,1,6)), to_days(d(2025,1,8)))));
    assert_eq!(m.get(&2).copied(), Some((to_days(d(2025,1,9)), to_days(d(2025,1,14)))));
    assert_eq!(m.get(&3).copied(), Some((to_days(d(2025,1,9)), to_days(d(2025,1,10)))));
    assert_eq!(m.get(&4).copied(), Some((to_days(d(2025,1,15)), to_days(d(2025,1,17)))));
}


