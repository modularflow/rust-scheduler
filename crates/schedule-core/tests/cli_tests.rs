#![cfg(feature = "cli_api")]

use assert_cmd::Command;
use predicates::str::contains as str_contains;
use tempfile::NamedTempFile;

#[allow(deprecated)]
fn run_cli(script: &str) -> assert_cmd::assert::Assert {
    let mut cmd = Command::cargo_bin("cli").expect("cli binary");
    cmd.write_stdin(script.to_string()).assert()
}

#[test]
fn cli_reports_metadata_validation_errors() {
    run_cli("meta dates 2025-01-10 2025-01-05\nquit\n")
        .success()
        .stdout(str_contains(
            "Project start date must be on or before project end date.",
        ));
}

#[test]
fn cli_delete_command_removes_task() {
    run_cli("add 1 TaskA 5\nadd 2 TaskB 3 1\ndelete 2\nquit\n")
        .success()
        .stdout(str_contains("Deleted task 2."));
}

#[test]
fn cli_save_and_load_json_round_trip() {
    let tmp = NamedTempFile::new().expect("create temp file");
    let path = tmp.path().to_string_lossy().replace('\\', "\\\\");
    let script = format!(
        "add 1 TaskPersist 4\nsave json {}\nadd 2 Temp 1\nload json {}\nshow\nquit\n",
        path, path
    );
    let assert = run_cli(&script).success();
    let output = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        output.contains("Schedule loaded from"),
        "expected output to mention load completion"
    );
    assert!(
        output.contains("TaskPersist"),
        "expected persisted task to remain"
    );
    let after_reload = output
        .split("Schedule loaded from")
        .last()
        .unwrap_or_default();
    assert!(
        !after_reload.contains("Temp"),
        "temporary task should not appear after reload:\n{}",
        after_reload
    );
}

#[test]
fn cli_applies_rationale_template() {
    run_cli("add 1 TaskA 5\nrationale template 1 fifty_fifty\nshow\nquit\n")
        .success()
        .stdout(str_contains(
            "Applied rationale template 'fifty_fifty' to task 1.",
        ))
        .stdout(str_contains("pre_defined_rationale"));
}
