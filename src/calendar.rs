use chrono::{NaiveDate, Datelike, Weekday, Duration};
use std::collections::HashSet;

pub struct WorkCalendar {
    holidays: HashSet<NaiveDate>,
    non_working_days: HashSet<Weekday>,
}

impl Default for WorkCalendar {
    fn default() -> Self {
        Self::defaults(2025)
    }
}

impl WorkCalendar {
    /// Create a new calendar with US federal holidays and Mon-Fri work week for a single year
    fn defaults(year: i32) -> Self {
        let mut calendar = Self {
            holidays: HashSet::new(),
            non_working_days: HashSet::from([Weekday::Sat, Weekday::Sun]),
        };
        
        calendar.add_us_holidays(year);
        calendar
    }
    
    /// Create a new calendar with US federal holidays and Mon-Fri work week for a range of years
    fn defaults_range(start_year: i32, end_year: i32) -> Self {
        let mut calendar = Self {
            holidays: HashSet::new(),
            non_working_days: HashSet::from([Weekday::Sat, Weekday::Sun]),
        };
        
        calendar.add_us_holidays_range(start_year, end_year);
        calendar
    }
    
    /// Create an empty calendar
    fn new() -> Self {
        Self {
            holidays: HashSet::new(),
            non_working_days: HashSet::new(),
        }
    }
    
    /// Add standard US federal holidays for a given year
    fn add_us_holidays(&mut self, year: i32) {
        // New Year's Day
        self.holidays.insert(NaiveDate::from_ymd_opt(year, 1, 1).unwrap());
        
        // Martin Luther King Jr. Day (3rd Monday in January)
        self.holidays.insert(Self::nth_weekday(year, 1, Weekday::Mon, 3));
        
        // Presidents' Day (3rd Monday in February)
        self.holidays.insert(Self::nth_weekday(year, 2, Weekday::Mon, 3));
        
        // Memorial Day (last Monday in May)
        self.holidays.insert(Self::last_weekday(year, 5, Weekday::Mon));
        
        // Independence Day
        self.holidays.insert(NaiveDate::from_ymd_opt(year, 7, 4).unwrap());
        
        // Labor Day (1st Monday in September)
        self.holidays.insert(Self::nth_weekday(year, 9, Weekday::Mon, 1));
        
        // Columbus Day (2nd Monday in October)
        self.holidays.insert(Self::nth_weekday(year, 10, Weekday::Mon, 2));
        
        // Veterans Day
        self.holidays.insert(NaiveDate::from_ymd_opt(year, 11, 11).unwrap());
        
        // Thanksgiving (4th Thursday in November)
        self.holidays.insert(Self::nth_weekday(year, 11, Weekday::Thu, 4));
        
        // Christmas
        self.holidays.insert(NaiveDate::from_ymd_opt(year, 12, 25).unwrap());
    }
    
    /// Add US federal holidays for a range of years (inclusive)
    fn add_us_holidays_range(&mut self, start_year: i32, end_year: i32) {
        for year in start_year..=end_year {
            self.add_us_holidays(year);
        }
    }
    
    /// Helper: Find the nth occurrence of a weekday in a month
    fn nth_weekday(year: i32, month: u32, weekday: Weekday, n: u32) -> NaiveDate {
        let mut date = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
        let mut count = 0;
        
        while date.month() == month {
            if date.weekday() == weekday {
                count += 1;
                if count == n {
                    return date;
                }
            }
            date = date + Duration::days(1);
        }
        panic!("Could not find {}th {} in {}/{}", n, weekday, month, year);
    }
    
    /// Helper: Find the last occurrence of a weekday in a month
    fn last_weekday(year: i32, month: u32, weekday: Weekday) -> NaiveDate {
        let mut date = if month == 12 {
            NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap()
        } else {
            NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap()
        };
        date = date - Duration::days(1); // Last day of the month
        
        while date.weekday() != weekday {
            date = date - Duration::days(1);
        }
        date
    }
    
    /// Add a single holiday
    pub fn add_holiday(&mut self, date: NaiveDate) {
        self.holidays.insert(date);
    }
    
    /// Add multiple holidays at once
    pub fn add_holidays(&mut self, dates: &[NaiveDate]) {
        self.holidays.extend(dates);
    }
    
    /// Add the same custom holiday for multiple years
    /// Example: Add Dec 24 (Christmas Eve) for 2025-2030
    pub fn add_recurring_holiday(&mut self, month: u32, day: u32, start_year: i32, end_year: i32) {
        for year in start_year..=end_year {
            if let Some(date) = NaiveDate::from_ymd_opt(year, month, day) {
                self.holidays.insert(date);
            }
        }
    }
    
    /// Add recurring holidays that fall on a specific weekday
    /// Example: Add "Black Friday" (day after Thanksgiving) for multiple years
    pub fn add_recurring_weekday_holiday(&mut self, month: u32, weekday: Weekday, n: u32, start_year: i32, end_year: i32) {
        for year in start_year..=end_year {
            self.holidays.insert(Self::nth_weekday(year, month, weekday, n));
        }
    }
    
    /// Set custom working days (e.g., Mon-Sat for 6-day weeks)
    pub fn set_working_days(&mut self, days: Vec<Weekday>) {
        self.non_working_days.clear();
        for day in [Weekday::Mon, Weekday::Tue, Weekday::Wed, 
                    Weekday::Thu, Weekday::Fri, Weekday::Sat, Weekday::Sun] {
            if !days.contains(&day) {
                self.non_working_days.insert(day);
            }
        }
    }
    
    /// Check if a date is available for scheduling
    pub fn is_available(&self, date: NaiveDate) -> bool {
        !self.holidays.contains(&date) 
            && !self.non_working_days.contains(&date.weekday())
    }
    
    /// Find the next available date after a given date
    pub fn next_available(&self, from: NaiveDate) -> NaiveDate {
        let mut current = from + Duration::days(1);
        while !self.is_available(current) {
            current = current + Duration::days(1);
        }
        current
    }
    
    /// Find a date N available days ahead
    pub fn find_next_available(&self, from: NaiveDate, days_ahead: i64) -> NaiveDate {
        let mut current = from;
        let mut count = 0;
        
        while count < days_ahead {
            current = current + Duration::days(1);
            if self.is_available(current) {
                count += 1;
            }
        }
        current
    }
    
    /// Get all available days in a date range
    pub fn available_days_in_range(&self, start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
        let mut days = Vec::new();
        let mut current = start;
        
        while current <= end {
            if self.is_available(current) {
                days.push(current);
            }
            current = current + Duration::days(1);
        }
        days
    }
    
    /// Count available days in a date range
    pub fn count_available_days(&self, start: NaiveDate, end: NaiveDate) -> i64 {
        let mut count = 0;
        let mut current = start;
        
        while current <= end {
            if self.is_available(current) {
                count += 1;
            }
            current = current + Duration::days(1);
        }
        count
    }
}
