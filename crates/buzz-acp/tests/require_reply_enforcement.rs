use std::process::Command;

#[test]
fn require_reply_is_exposed_as_an_explicit_opt_in() {
    let output = Command::new(env!("CARGO_BIN_EXE_buzz-acp"))
        .arg("--help")
        .output()
        .expect("run buzz-acp --help");
    assert!(output.status.success());

    let help = String::from_utf8(output.stdout).expect("help is UTF-8");
    assert!(help.contains("--require-reply"));
    assert!(help.contains("BUZZ_ACP_REQUIRE_REPLY"));
    assert!(
        !help.contains("--no-require-reply"),
        "reply enforcement must remain an explicit default-off opt-in"
    );
}
