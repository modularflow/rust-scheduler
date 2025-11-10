use chrono::NaiveDate;
use polars::prelude::{AnyValue, DataFrame};
use schedule_tool::{
    ProgressRationaleTemplate, Schedule, ScheduleMetadataError, WorkCalendarConfig,
    load_schedule_from_csv, load_schedule_from_json, save_schedule_to_csv, save_schedule_to_json,
};
use serde_json;
use std::fs;
use std::io::{self, Write};
use std::str::FromStr;

fn parse_pred_list(s: &str) -> Vec<i32> {
    s.split(',')
        .filter_map(|p| p.trim().parse::<i32>().ok())
        .collect()
}

fn render_df_as_text_table(df: &DataFrame) -> String {
    // Compute column widths
    let columns = df.get_columns();
    let col_names: Vec<String> = columns.iter().map(|c| c.name().to_string()).collect();

    let mut widths: Vec<usize> = col_names.iter().map(|n| n.len()).collect();
    for (ci, col) in columns.iter().enumerate() {
        for row_idx in 0..df.height() {
            if let Ok(ref av) = col.get(row_idx) {
                let s = match av {
                    AnyValue::Null => String::new(),
                    AnyValue::Int32(v) => v.to_string(),
                    AnyValue::Int64(v) => v.to_string(),
                    AnyValue::String(s) => s.to_string(),
                    AnyValue::List(inner) if col.name() == "predecessors" => {
                        if let Ok(ca) = inner.i32() {
                            ca.into_iter()
                                .flatten()
                                .map(|v| v.to_string())
                                .collect::<Vec<_>>()
                                .join(",")
                        } else {
                            av.to_string()
                        }
                    }
                    _ => av.to_string(),
                };
                if s.len() > widths[ci] {
                    widths[ci] = s.len();
                }
            }
        }
    }

    // Build horizontal separator
    let mut sep = String::new();
    sep.push('+');
    for w in &widths {
        sep.push_str(&"-".repeat(*w + 2));
        sep.push('+');
    }

    // Build output
    let mut out = String::new();
    out.push_str(&sep);
    out.push('\n');

    // Header
    out.push('|');
    for (i, name) in col_names.iter().enumerate() {
        out.push(' ');
        out.push_str(name);
        let pad = widths[i] - name.len();
        if pad > 0 {
            out.push_str(&" ".repeat(pad));
        }
        out.push(' ');
        out.push('|');
    }
    out.push('\n');
    out.push_str(&sep);
    out.push('\n');

    // Rows
    for row_idx in 0..df.height() {
        out.push('|');
        for (ci, col) in columns.iter().enumerate() {
            let mut s = String::new();
            if let Ok(ref av) = col.get(row_idx) {
                s = match av {
                    AnyValue::Null => String::new(),
                    AnyValue::Int32(v) => v.to_string(),
                    AnyValue::Int64(v) => v.to_string(),
                    AnyValue::String(st) => st.to_string(),
                    AnyValue::List(inner) if col.name() == "predecessors" => {
                        if let Ok(ca) = inner.i32() {
                            ca.into_iter()
                                .flatten()
                                .map(|v| v.to_string())
                                .collect::<Vec<_>>()
                                .join(",")
                        } else {
                            av.to_string()
                        }
                    }
                    _ => av.to_string(),
                };
            }
            out.push(' ');
            out.push_str(&s);
            let pad = widths[ci].saturating_sub(s.len());
            if pad > 0 {
                out.push_str(&" ".repeat(pad));
            }
            out.push(' ');
            out.push('|');
        }
        out.push('\n');
    }

    out.push_str(&sep);
    out.push('\n');
    out
}

fn print_help() {
    println!(
        "Commands:\n  help                               Show this help\n  show                               Show current schedule\n  new                                Append empty task with next id\n  add <id> <name> <duration_days> [preds_csv]\n                                     Upsert a task (preds like 1,2,3)\n  delete <id>                        Delete a task and clean up dependencies\n  bstart  <id> <YYYY-MM-DD>          Set baseline_start\n  bfinish <id> <YYYY-MM-DD>          Set baseline_finish\n  astart  <id> <YYYY-MM-DD>          Set actual_start\n  afinish <id> <YYYY-MM-DD>          Set actual_finish\n  pct     <id> <float>               Set percent_complete\n  var     <id> <i64>                 Set schedule_variance_days\n  crit    <id> <true|false>          Set is_critical\n  parent  <id> <i32>                 Set parent_id\n  wbs     <id> <code>                Set wbs_code\n  notes   <id> <text...>             Set task_notes (rest of line)\n  succ    <id> <csv>                 Set successors (e.g. 2,3)\n  rationale templates                List available rationale templates\n  rationale template <id> <name>     Apply rationale template to task\n  meta show                          Show project metadata\n  meta name <text...>                Update project name\n  meta desc <text...>                Update project description\n  meta dates <start> <end>           Update project start/end dates (YYYY-MM-DD)\n  calendar show                      Display calendar configuration summary\n  calendar default                   Reset to default calendar for metadata span\n  calendar set <json_path>           Load calendar config from JSON file\n  calendar save <json_path>          Save current calendar config to JSON file\n  save <json|csv> <path>             Persist schedule to disk\n  load <json|csv> <path>             Load schedule from disk\n  compute                            Refresh schedule (forward + backward passes)\n  quit|exit                          Exit"
    );
}

fn print_rationale_templates() {
    println!("Available rationale templates:");
    for (key, description) in ProgressRationaleTemplate::variants() {
        println!("  {:<24} {}", key, description);
    }
}

fn print_metadata(schedule: &Schedule) {
    let metadata = schedule.metadata();
    println!("Project name       : {}", metadata.project_name);
    println!("Project description: {}", metadata.project_description);
    println!("Project start date : {}", metadata.project_start_date);
    println!("Project end date   : {}", metadata.project_end_date);
}

fn print_calendar_info(schedule: &Schedule) {
    let config = schedule.calendar_config();
    let working_days = config
        .working_days()
        .iter()
        .map(|wd| wd.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let holidays = config
        .holidays()
        .iter()
        .map(|d| d.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    println!("Calendar custom    : {}", schedule.calendar_is_custom());
    println!("Working days       : {}", working_days);
    println!("Holidays           : {}", holidays);
}

fn next_id(schedule: &Schedule) -> i32 {
    if schedule.dataframe().height() == 0 {
        return 1;
    }
    schedule
        .dataframe()
        .column("id")
        .ok()
        .and_then(|s| s.i32().ok())
        .and_then(|ca| ca.into_iter().flatten().max())
        .map(|m| m + 1)
        .unwrap_or(1)
}

fn main() {
    let mut schedule = Schedule::new();
    if schedule.dataframe().height() == 0 {
        let _ = schedule.upsert_task(1, "", 0, None);
    }

    println!("Schedule Tool (CLI) - type 'help' for commands\n");
    println!("{}", render_df_as_text_table(schedule.dataframe()));

    let stdin = io::stdin();
    let mut line = String::new();
    loop {
        print!("> ");
        let _ = io::stdout().flush();
        line.clear();
        if stdin.read_line(&mut line).is_err() {
            break;
        }
        let input = line.trim();
        if input.is_empty() {
            continue;
        }

        let mut parts = input.split_whitespace();
        let cmd = parts.next().unwrap_or("");

        match cmd {
            "help" => {
                print_help();
            }
            "quit" | "exit" => break,
            "show" => {
                println!("{}", render_df_as_text_table(schedule.dataframe()));
            }
            "new" => {
                let id = next_id(&schedule);
                let _ = schedule.upsert_task(id, "", 0, None);
                println!("Added empty task id={}", id);
                println!("{}", render_df_as_text_table(schedule.dataframe()));
            }
            "delete" => {
                let id_s = parts.next();
                match id_s {
                    Some(id_s) => match id_s.parse::<i32>() {
                        Ok(id) => match schedule.delete_task(id) {
                            Ok(true) => {
                                println!("Deleted task {id}.");
                                println!("{}", render_df_as_text_table(schedule.dataframe()));
                            }
                            Ok(false) => println!("Task {id} not found."),
                            Err(e) => println!("Error deleting task: {}", e),
                        },
                        Err(_) => println!("Invalid id"),
                    },
                    None => println!("Usage: delete <id>"),
                }
            }
            "add" => {
                let id_s = parts.next();
                let name_s = parts.next();
                let dur_s = parts.next();
                let preds_s = parts.next();
                match (id_s, name_s, dur_s) {
                    (Some(id_s), Some(name), Some(dur_s)) => {
                        let id: i32 = match id_s.parse() {
                            Ok(v) => v,
                            Err(_) => {
                                println!("Invalid id");
                                continue;
                            }
                        };
                        let duration_days: i64 = match dur_s.parse() {
                            Ok(v) => v,
                            Err(_) => {
                                println!("Invalid duration_days");
                                continue;
                            }
                        };
                        let preds = preds_s.map(parse_pred_list);
                        match schedule.upsert_task(id, name, duration_days, preds) {
                            Ok(_) => {
                                println!("Task upserted.");
                                println!("{}", render_df_as_text_table(schedule.dataframe()));
                            }
                            Err(e) => println!("Error: {}", e),
                        }
                    }
                    _ => {
                        println!("Usage: add <id> <name> <duration_days> [preds_csv]");
                    }
                }
            }
            "compute" => match schedule.refresh() {
                Ok(summary) => {
                    println!(
                        "Refreshed ({})\n{}",
                        summary.to_cli_summary(),
                        render_df_as_text_table(schedule.dataframe())
                    );
                }
                Err(e) => println!("Refresh error: {}", e),
            },
            "bstart" | "bfinish" | "astart" | "afinish" => {
                let id_s = parts.next();
                let date_s = parts.next();
                match (id_s, date_s) {
                    (Some(id_s), Some(date_s)) => {
                        let id: i32 = match id_s.parse() {
                            Ok(v) => v,
                            Err(_) => {
                                println!("Invalid id");
                                continue;
                            }
                        };
                        let date = match NaiveDate::parse_from_str(date_s, "%Y-%m-%d") {
                            Ok(d) => d,
                            Err(_) => {
                                println!("Invalid date (YYYY-MM-DD)");
                                continue;
                            }
                        };
                        let res = match cmd {
                            "bstart" => schedule.set_baseline_start(id, date),
                            "bfinish" => schedule.set_baseline_finish(id, date),
                            "astart" => schedule.set_actual_start(id, date),
                            _ => schedule.set_actual_finish(id, date),
                        };
                        match res {
                            Ok(_) => println!(
                                "{} set.\n{}",
                                cmd,
                                render_df_as_text_table(schedule.dataframe())
                            ),
                            Err(e) => println!("Error: {}", e),
                        }
                    }
                    _ => println!("Usage: {} <id> <YYYY-MM-DD>", cmd),
                }
            }
            "pct" => {
                let id_s = parts.next();
                let val_s = parts.next();
                match (id_s, val_s) {
                    (Some(id_s), Some(val_s)) => {
                        let id: i32 = match id_s.parse() {
                            Ok(v) => v,
                            Err(_) => {
                                println!("Invalid id");
                                continue;
                            }
                        };
                        let val: f64 = match val_s.parse() {
                            Ok(v) => v,
                            Err(_) => {
                                println!("Invalid float");
                                continue;
                            }
                        };
                        match schedule.set_percent_complete(id, val) {
                            Ok(_) => println!(
                                "percent_complete set.\n{}",
                                render_df_as_text_table(schedule.dataframe())
                            ),
                            Err(e) => println!("Error: {}", e),
                        }
                    }
                    _ => println!("Usage: pct <id> <float>"),
                }
            }
            "var" => {
                let id_s = parts.next();
                let val_s = parts.next();
                match (id_s, val_s) {
                    (Some(id_s), Some(val_s)) => {
                        let id: i32 = match id_s.parse() {
                            Ok(v) => v,
                            Err(_) => {
                                println!("Invalid id");
                                continue;
                            }
                        };
                        let val: i64 = match val_s.parse() {
                            Ok(v) => v,
                            Err(_) => {
                                println!("Invalid i64");
                                continue;
                            }
                        };
                        match schedule.set_schedule_variance_days(id, val) {
                            Ok(_) => println!(
                                "schedule_variance_days set.\n{}",
                                render_df_as_text_table(schedule.dataframe())
                            ),
                            Err(e) => println!("Error: {}", e),
                        }
                    }
                    _ => println!("Usage: var <id> <i64>"),
                }
            }
            "crit" => {
                let id_s = parts.next();
                let val_s = parts.next();
                match (id_s, val_s) {
                    (Some(id_s), Some(val_s)) => {
                        let id: i32 = match id_s.parse() {
                            Ok(v) => v,
                            Err(_) => {
                                println!("Invalid id");
                                continue;
                            }
                        };
                        let val = match val_s.to_ascii_lowercase().as_str() {
                            "true" => true,
                            "false" => false,
                            _ => {
                                println!("Invalid bool (true|false)");
                                continue;
                            }
                        };
                        match schedule.set_is_critical(id, val) {
                            Ok(_) => println!(
                                "is_critical set.\n{}",
                                render_df_as_text_table(schedule.dataframe())
                            ),
                            Err(e) => println!("Error: {}", e),
                        }
                    }
                    _ => println!("Usage: crit <id> <true|false>"),
                }
            }
            "parent" => {
                let id_s = parts.next();
                let parent_s = parts.next();
                match (id_s, parent_s) {
                    (Some(id_s), Some(parent_s)) => {
                        let id: i32 = match id_s.parse() {
                            Ok(v) => v,
                            Err(_) => {
                                println!("Invalid id");
                                continue;
                            }
                        };
                        let parent_id: i32 = match parent_s.parse() {
                            Ok(v) => v,
                            Err(_) => {
                                println!("Invalid parent_id");
                                continue;
                            }
                        };
                        match schedule.set_parent_id(id, parent_id) {
                            Ok(_) => println!(
                                "parent_id set.\n{}",
                                render_df_as_text_table(schedule.dataframe())
                            ),
                            Err(e) => println!("Error: {}", e),
                        }
                    }
                    _ => println!("Usage: parent <id> <i32>"),
                }
            }
            "wbs" => {
                let id_s = parts.next();
                let code = parts.next();
                match (id_s, code) {
                    (Some(id_s), Some(code)) => {
                        let id: i32 = match id_s.parse() {
                            Ok(v) => v,
                            Err(_) => {
                                println!("Invalid id");
                                continue;
                            }
                        };
                        match schedule.set_wbs_code(id, code) {
                            Ok(_) => println!(
                                "wbs_code set.\n{}",
                                render_df_as_text_table(schedule.dataframe())
                            ),
                            Err(e) => println!("Error: {}", e),
                        }
                    }
                    _ => println!("Usage: wbs <id> <code>"),
                }
            }
            "notes" => {
                let id_s = parts.next();
                let rest: Vec<&str> = parts.collect();
                match (id_s, !rest.is_empty()) {
                    (Some(id_s), true) => {
                        let id: i32 = match id_s.parse() {
                            Ok(v) => v,
                            Err(_) => {
                                println!("Invalid id");
                                continue;
                            }
                        };
                        let text = rest.join(" ");
                        match schedule.set_task_notes(id, &text) {
                            Ok(_) => println!(
                                "task_notes set.\n{}",
                                render_df_as_text_table(schedule.dataframe())
                            ),
                            Err(e) => println!("Error: {}", e),
                        }
                    }
                    _ => println!("Usage: notes <id> <text...>"),
                }
            }
            "succ" => {
                let id_s = parts.next();
                let csv = parts.next();
                match (id_s, csv) {
                    (Some(id_s), Some(csv)) => {
                        let id: i32 = match id_s.parse() {
                            Ok(v) => v,
                            Err(_) => {
                                println!("Invalid id");
                                continue;
                            }
                        };
                        let successors = parse_pred_list(csv);
                        match schedule.set_successors(id, successors) {
                            Ok(_) => println!(
                                "successors set.\n{}",
                                render_df_as_text_table(schedule.dataframe())
                            ),
                            Err(e) => println!("Error: {}", e),
                        }
                    }
                    _ => println!("Usage: succ <id> <csv>"),
                }
            }
            "rationale" => match parts.next() {
                Some("templates") | Some("list") => print_rationale_templates(),
                Some("template") => {
                    let id_s = parts.next();
                    let template_name = parts.next();
                    match (id_s, template_name) {
                        (Some(id_s), Some(name)) => {
                            let id: i32 = match id_s.parse() {
                                Ok(v) => v,
                                Err(_) => {
                                    println!("Invalid id");
                                    continue;
                                }
                            };
                            match ProgressRationaleTemplate::from_str(name) {
                                Ok(template) => {
                                    let key = template.key();
                                    match schedule.apply_rationale_template(id, template) {
                                        Ok(_) => {
                                            println!(
                                                "Applied rationale template '{}' to task {}.",
                                                key, id
                                            );
                                            println!(
                                                "{}",
                                                render_df_as_text_table(schedule.dataframe())
                                            );
                                        }
                                        Err(e) => println!("Error applying template: {}", e),
                                    }
                                }
                                Err(_) => {
                                    println!(
                                        "Unknown rationale template '{}'. Use 'rationale templates' to list options.",
                                        name
                                    );
                                }
                            }
                        }
                        _ => println!("Usage: rationale template <id> <name>"),
                    }
                }
                Some(other) => {
                    println!("Unknown rationale command '{}'.", other);
                    println!("Usage: rationale templates|template <id> <name>");
                }
                None => {
                    println!("Usage: rationale templates|template <id> <name>");
                }
            },
            "meta" => match parts.next() {
                Some("show") | None => print_metadata(&schedule),
                Some("name") => {
                    let rest: Vec<&str> = parts.collect();
                    if rest.is_empty() {
                        println!("Usage: meta name <text...>");
                        continue;
                    }
                    let name = rest.join(" ");
                    schedule.set_project_name(name);
                    println!("Project name updated.");
                    print_metadata(&schedule);
                }
                Some("desc") => {
                    let rest: Vec<&str> = parts.collect();
                    if rest.is_empty() {
                        println!("Usage: meta desc <text...>");
                        continue;
                    }
                    let desc = rest.join(" ");
                    schedule.set_project_description(desc);
                    println!("Project description updated.");
                    print_metadata(&schedule);
                }
                Some("dates") => {
                    let start_s = parts.next();
                    let end_s = parts.next();
                    match (start_s, end_s) {
                        (Some(start_s), Some(end_s)) => {
                            let start = match NaiveDate::parse_from_str(start_s, "%Y-%m-%d") {
                                Ok(d) => d,
                                Err(_) => {
                                    println!("Invalid start date (YYYY-MM-DD)");
                                    continue;
                                }
                            };
                            let end = match NaiveDate::parse_from_str(end_s, "%Y-%m-%d") {
                                Ok(d) => d,
                                Err(_) => {
                                    println!("Invalid end date (YYYY-MM-DD)");
                                    continue;
                                }
                            };
                            match schedule.set_project_dates(start, end) {
                                Ok(_) => match schedule.refresh() {
                                    Ok(summary) => {
                                        println!(
                                            "Metadata dates updated ({}).",
                                            summary.to_cli_summary()
                                        );
                                        print_metadata(&schedule);
                                    }
                                    Err(e) => println!("Refresh error: {}", e),
                                },
                                Err(ScheduleMetadataError::StartAfterEnd { .. }) => {
                                    println!(
                                        "Project start date must be on or before project end date."
                                    );
                                }
                                Err(ScheduleMetadataError::EndPrecedesScheduleFinish {
                                    project_end,
                                    required_finish,
                                }) => {
                                    println!(
                                        "Project end date {} is before current schedule finish {}.",
                                        project_end, required_finish
                                    );
                                }
                                Err(ScheduleMetadataError::Computation(message)) => {
                                    println!("Metadata update error: {}", message);
                                }
                            }
                        }
                        _ => println!("Usage: meta dates <YYYY-MM-DD> <YYYY-MM-DD>"),
                    }
                }
                Some(other) => {
                    println!("Unknown meta command '{}'.", other);
                    println!("Usage: meta show|name|desc|dates ...");
                }
            },
            "calendar" => match parts.next() {
                Some("show") | None => print_calendar_info(&schedule),
                Some("default") => match schedule.reset_calendar_to_default() {
                    Ok(_) => {
                        println!("Calendar reset to default.");
                        print_calendar_info(&schedule);
                    }
                    Err(e) => println!("Error resetting calendar: {}", e),
                },
                Some("set") => {
                    let path = parts.next();
                    match path {
                        Some(path) => match fs::read_to_string(path) {
                            Ok(contents) => {
                                match serde_json::from_str::<WorkCalendarConfig>(&contents) {
                                    Ok(config) => {
                                        match schedule.set_calendar_from_config(&config) {
                                            Ok(_) => {
                                                println!("Calendar updated from {}.", path);
                                                print_calendar_info(&schedule);
                                            }
                                            Err(e) => println!("Error applying calendar: {}", e),
                                        }
                                    }
                                    Err(e) => println!("Invalid calendar JSON: {}", e),
                                }
                            }
                            Err(e) => println!("Error reading {}: {}", path, e),
                        },
                        None => println!("Usage: calendar set <json_path>"),
                    }
                }
                Some("save") => {
                    let path = parts.next();
                    match path {
                        Some(path) => {
                            let config = schedule.calendar_config();
                            match serde_json::to_string_pretty(&config) {
                                Ok(json) => match fs::write(path, json) {
                                    Ok(_) => println!("Calendar saved to {}.", path),
                                    Err(e) => println!("Error writing {}: {}", path, e),
                                },
                                Err(e) => println!("Error serializing calendar: {}", e),
                            }
                        }
                        None => println!("Usage: calendar save <json_path>"),
                    }
                }
                Some(other) => {
                    println!("Unknown calendar command '{}'.", other);
                    println!("Usage: calendar show|default|set <json_path>|save <json_path>");
                }
            },
            "save" => {
                let fmt = parts.next();
                let path = parts.next();
                match (fmt, path) {
                    (Some("json"), Some(path)) => match save_schedule_to_json(&schedule, path) {
                        Ok(_) => println!("Schedule saved to {}.", path),
                        Err(e) => println!("Error saving schedule: {}", e),
                    },
                    (Some("csv"), Some(path)) => match save_schedule_to_csv(&schedule, path) {
                        Ok(_) => println!("Schedule saved to {}.", path),
                        Err(e) => println!("Error saving schedule: {}", e),
                    },
                    _ => println!("Usage: save <json|csv> <path>"),
                }
            }
            "load" => {
                let fmt = parts.next();
                let path = parts.next();
                match (fmt, path) {
                    (Some("json"), Some(path)) => match load_schedule_from_json(path) {
                        Ok(loaded) => {
                            schedule = loaded;
                            if let Err(e) = schedule.refresh() {
                                println!("Loaded schedule but refresh failed: {}", e);
                            }
                            println!("Schedule loaded from {}.", path);
                            println!("{}", render_df_as_text_table(schedule.dataframe()));
                        }
                        Err(e) => println!("Error loading schedule: {}", e),
                    },
                    (Some("csv"), Some(path)) => match load_schedule_from_csv(path) {
                        Ok(mut loaded) => {
                            if let Err(e) = loaded.refresh() {
                                println!("Loaded schedule but refresh failed: {}", e);
                            }
                            schedule = loaded;
                            println!("Schedule loaded from {}.", path);
                            println!("{}", render_df_as_text_table(schedule.dataframe()));
                        }
                        Err(e) => println!("Error loading schedule: {}", e),
                    },
                    _ => println!("Usage: load <json|csv> <path>"),
                }
            }
            _ => {
                println!("Unknown command. Type 'help'.");
            }
        }
    }
}
