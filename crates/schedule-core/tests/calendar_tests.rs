use chrono::{Datelike, NaiveDate, Weekday};
use schedule_tool::calendar::WorkCalendar;

#[test]
fn default_calendar_weekends_unavailable() {
    let cal = WorkCalendar::default();
    // 2025-01-04 is a Saturday, 2025-01-05 is a Sunday
    let sat = NaiveDate::from_ymd_opt(2025, 1, 4).unwrap();
    let sun = NaiveDate::from_ymd_opt(2025, 1, 5).unwrap();
    assert!(!cal.is_available(sat));
    assert!(!cal.is_available(sun));
}

#[test]
fn default_calendar_weekday_available_except_holidays() {
    let cal = WorkCalendar::default();
    // 2025-01-02 is a Thursday and not a holiday by default list
    let date = NaiveDate::from_ymd_opt(2025, 1, 2).unwrap();
    assert!(cal.is_available(date));
}

#[test]
fn next_available_skips_weekend() {
    let cal = WorkCalendar::default();
    // From Friday 2025-01-03, next available should be Monday 2025-01-06 (since 1/1 and weekends)
    let fri = NaiveDate::from_ymd_opt(2025, 1, 3).unwrap();
    let next = cal.next_available(fri);
    assert_eq!(next.weekday(), Weekday::Mon);
    assert_eq!(next, NaiveDate::from_ymd_opt(2025, 1, 6).unwrap());
}

#[test]
fn find_next_available_counts_only_workdays() {
    let cal = WorkCalendar::default();
    let mon = NaiveDate::from_ymd_opt(2025, 1, 6).unwrap(); // Monday
    let four_ahead = cal.find_next_available(mon, 4);
    // 4 working days ahead of Monday should land on Friday
    assert_eq!(four_ahead.weekday(), Weekday::Fri);
}

#[test]
fn available_days_in_range_and_count_match() {
    let cal = WorkCalendar::default();
    let start = NaiveDate::from_ymd_opt(2025, 1, 6).unwrap(); // Mon
    let end = NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(); // Fri
    let days = cal.available_days_in_range(start, end);
    let count = cal.count_available_days(start, end);
    assert_eq!(days.len() as i64, count);
    assert_eq!(days.first().copied().unwrap(), start);
    assert_eq!(days.last().copied().unwrap(), end);
}

#[test]
fn set_working_days_includes_saturday() {
    let mut cal = WorkCalendar::default();
    // Make Mon-Sat working days (exclude only Sunday)
    cal.set_working_days(vec![
        Weekday::Mon,
        Weekday::Tue,
        Weekday::Wed,
        Weekday::Thu,
        Weekday::Fri,
        Weekday::Sat,
    ]);
    let sat = NaiveDate::from_ymd_opt(2025, 1, 4).unwrap();
    assert!(cal.is_available(sat));
}

#[test]
fn add_and_recurring_holidays_block_days() {
    let mut cal = WorkCalendar::default();
    let custom = NaiveDate::from_ymd_opt(2025, 2, 4).unwrap();
    cal.add_holiday(custom);
    assert!(!cal.is_available(custom));

    // Add Dec 24 for 2025-2026
    cal.add_recurring_holiday(12, 24, 2025, 2026);
    assert!(!cal.is_available(NaiveDate::from_ymd_opt(2025, 12, 24).unwrap()));
    assert!(!cal.is_available(NaiveDate::from_ymd_opt(2026, 12, 24).unwrap()));
}

#[test]
fn custom_calendar_builds_non_standard_week_and_round_trips() {
    let mut working = vec![
        Weekday::Mon,
        Weekday::Tue,
        Weekday::Wed,
        Weekday::Thu,
        Weekday::Sat,
    ];
    let holidays = vec![
        NaiveDate::from_ymd_opt(2025, 6, 19).unwrap(),
        NaiveDate::from_ymd_opt(2025, 7, 3).unwrap(),
    ];
    let cal = WorkCalendar::custom(working.clone(), holidays.clone());

    // Fridays and Sundays should be unavailable
    let friday = NaiveDate::from_ymd_opt(2025, 6, 20).unwrap();
    assert!(!cal.is_available(friday));
    let sunday = NaiveDate::from_ymd_opt(2025, 6, 22).unwrap();
    assert!(!cal.is_available(sunday));

    // Saturday should be available because we included it as working
    let saturday = NaiveDate::from_ymd_opt(2025, 6, 21).unwrap();
    assert!(cal.is_available(saturday));

    // Ensure holidays are respected
    for holiday in &holidays {
        assert!(!cal.is_available(*holiday));
    }

    // Round trip through config (and dedup order)
    let config = cal.to_config();
    working.sort_by_key(|wd| wd.num_days_from_monday());
    working.dedup_by(|a, b| a.num_days_from_monday() == b.num_days_from_monday());
    assert_eq!(config.working_days(), working.as_slice());
    assert_eq!(config.holidays(), holidays.as_slice());

    let recreated = WorkCalendar::from_config(&config);
    assert_eq!(recreated.to_config(), config);
}
