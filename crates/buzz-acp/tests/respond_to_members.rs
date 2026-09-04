use std::fs::File;
use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const TEST_PRIVATE_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000001";

/// Run `buzz-acp` to completion (help / config-rejection paths that exit on
/// their own) and return `(status, stdout, stderr)`.
fn run_to_exit(args: &[&str]) -> (std::process::ExitStatus, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_buzz-acp"))
        .args(args)
        .output()
        .expect("run buzz-acp");
    (
        output.status,
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// Spawn `buzz-acp` with stdout redirected to a temp file (tracing-subscriber
/// writes the startup log to stdout), then poll that file until `marker`
/// appears, the process exits, or `timeout` elapses. Always kills the child.
/// Returns whether the marker was observed.
fn wait_for_startup_log(
    args: &[&str],
    envs: &[(&str, &str)],
    marker: &str,
    log_suffix: &str,
) -> bool {
    let log_path = std::env::temp_dir().join(format!(
        "buzz-acp-respond-members-{}-{log_suffix}.log",
        std::process::id()
    ));
    let file = File::create(&log_path).expect("create temp log file");

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_buzz-acp"));
    cmd.args(args)
        .stdout(Stdio::from(file))
        .stderr(Stdio::null());
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let mut child: Child = cmd.spawn().expect("spawn buzz-acp");

    let deadline = Instant::now() + Duration::from_secs(20);
    let mut found = false;
    loop {
        if let Ok(mut f) = File::open(&log_path) {
            let mut contents = String::new();
            if f.read_to_string(&mut contents).is_ok() && contents.contains(marker) {
                found = true;
                break;
            }
        }
        if let Ok(Some(_)) = child.try_wait() {
            break;
        }
        if Instant::now() > deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    if !found {
        // Final read after the process exited — catches a marker written in
        // the last moment before exit (e.g. config error after startup log).
        if let Ok(mut f) = File::open(&log_path) {
            let mut contents = String::new();
            if f.read_to_string(&mut contents).is_ok() && contents.contains(marker) {
                found = true;
            }
        }
    }

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_file(&log_path);
    found
}

#[test]
fn help_documents_members_mode() {
    let (status, stdout, stderr) = run_to_exit(&["--help"]);
    assert!(status.success());
    let help = format!("{stdout}\n{stderr}");
    assert!(
        help.contains("members"),
        "help must list the members respond-to mode"
    );
    assert!(
        help.contains("owner-only") && help.contains("nobody"),
        "help must still list the pre-existing respond-to modes"
    );
    assert!(
        help.contains("BUZZ_ACP_RESPOND_TO"),
        "help must document the BUZZ_ACP_RESPOND_TO env var"
    );
}

#[test]
fn members_mode_selected_by_flag() {
    assert!(
        wait_for_startup_log(
            &["--private-key", TEST_PRIVATE_KEY, "--respond-to", "members"],
            &[],
            "respond_to=members",
            "flag",
        ),
        "startup log must report respond_to=members when --respond-to members is passed"
    );
}

#[test]
fn members_mode_selected_by_env() {
    assert!(
        wait_for_startup_log(
            &["--private-key", TEST_PRIVATE_KEY],
            &[("BUZZ_ACP_RESPOND_TO", "members")],
            "respond_to=members",
            "env",
        ),
        "startup log must report respond_to=members when BUZZ_ACP_RESPOND_TO=members is set"
    );
}

#[test]
fn members_mode_rejected_when_not_in_allowed_set() {
    let (status, _stdout, stderr) = run_to_exit(&[
        "--private-key",
        TEST_PRIVATE_KEY,
        "--respond-to",
        "members",
        "--allowed-respond-to",
        "owner-only,allowlist",
    ]);
    assert!(
        !status.success(),
        "startup must fail when respond_to=members is not in the allowed set"
    );
    assert!(
        stderr.contains("not permitted"),
        "rejection must mention 'not permitted', got: {stderr}"
    );
}
