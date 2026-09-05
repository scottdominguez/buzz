//! Integration tests for buzz-acp's mention-loop guards (`loop_guard.rs`).
//!
//! The guards protect multi-agent channels from agent-to-agent mention
//! cascades:
//!   1. Per-thread agent-chain depth (`BUZZ_ACP_MAX_AGENT_CHAIN`, default 3) —
//!      refuse to auto-respond past the depth, log and stay silent.
//!   2. Two-pubkey ping-pong detection (`BUZZ_ACP_PINGPONG_LIMIT`, default 2) —
//!      more than N consecutive alternating rounds in one thread enters a
//!      per-thread cooldown and logs loudly.
//!   3. Per-channel agent auto-reply rate limit
//!      (`BUZZ_ACP_AGENT_REPLY_RATE`, default 6 per minute) — over the limit
//!      events are queued with a cooldown, never dropped.
//!
//! Human-authored triggers are NEVER limited by these guards; the regression
//! tests prove their handling is byte-identical to running without a guard.

use std::process::Command;
use std::time::{Duration, Instant};

use buzz_acp::test_api::{GuardDecision, LoopGuard, LoopGuardConfig};
use uuid::Uuid;

#[test]
fn help_documents_the_mention_loop_guards() {
    let output = Command::new(env!("CARGO_BIN_EXE_buzz-acp"))
        .arg("--help")
        .output()
        .expect("run buzz-acp --help");
    assert!(output.status.success());
    let help = String::from_utf8_lossy(&output.stdout);
    for (flag, env) in [
        ("--max-agent-chain", "BUZZ_ACP_MAX_AGENT_CHAIN"),
        ("--pingpong-limit", "BUZZ_ACP_PINGPONG_LIMIT"),
        (
            "--pingpong-cooldown-secs",
            "BUZZ_ACP_PINGPONG_COOLDOWN_SECS",
        ),
        ("--agent-reply-rate", "BUZZ_ACP_AGENT_REPLY_RATE"),
        (
            "--agent-reply-cooldown-secs",
            "BUZZ_ACP_AGENT_REPLY_COOLDOWN_SECS",
        ),
    ] {
        assert!(help.contains(flag), "help must document the {flag} flag");
        assert!(help.contains(env), "help must document the {env} env var");
    }
}

fn guard() -> LoopGuard {
    LoopGuard::new(LoopGuardConfig::default())
}

#[test]
fn human_triggers_are_byte_identical_to_a_no_guard_baseline() {
    let mut g = guard();
    let channel = Uuid::new_v4();
    let t0 = Instant::now();

    // A human-only channel consults the guard only via `on_human_trigger`,
    // which must leave every dispatch-affecting knob untouched: chain depth
    // stays 0, no rate ticks, no ping-pong cooldown, no rate cooldown.
    for _ in 0..50 {
        g.on_human_trigger("thread-root");
    }
    assert_eq!(g.chain_depth("thread-root"), 0);
    assert_eq!(g.agent_reply_tick_count(channel), 0);
    assert!(!g.is_rate_cooldown(channel, t0));
    assert!(g.pingpong_cooldown_until("thread-root").is_none());
    assert!(g.rate_cooldown_until(channel).is_none());

    // Identical to a freshly-constructed guard's state.
    let baseline = guard();
    assert_eq!(
        baseline.chain_depth("thread-root"),
        g.chain_depth("thread-root")
    );
    assert_eq!(
        baseline.agent_reply_tick_count(channel),
        g.agent_reply_tick_count(channel)
    );

    // A human interjection resets the chain: the next agent trigger starts a
    // fresh chain at depth 1 rather than inheriting a stale counter.
    assert_eq!(
        g.evaluate_agent_trigger(channel, "thread-root", "agent-b", t0),
        GuardDecision::Respond
    );
    assert_eq!(g.chain_depth("thread-root"), 1);
}

#[test]
fn agent_chain_depth_caps_a_cascade_and_human_resets_it() {
    let cfg = LoopGuardConfig {
        max_agent_chain: 3,
        ..Default::default()
    };
    let mut g = LoopGuard::new(cfg);
    let channel = Uuid::new_v4();
    let t0 = Instant::now();
    let thread = "root-chain";

    for (i, author) in ["agent-a", "agent-b", "agent-c"].iter().enumerate() {
        assert_eq!(
            g.evaluate_agent_trigger(channel, thread, author, t0),
            GuardDecision::Respond,
            "agent trigger {} within the default depth must respond",
            i + 1
        );
    }
    assert_eq!(g.chain_depth(thread), 3);

    // The fourth consecutive agent trigger is refused: log and stay silent.
    assert_eq!(
        g.evaluate_agent_trigger(channel, thread, "agent-d", t0),
        GuardDecision::ChainRefused
    );
    assert_eq!(
        g.evaluate_agent_trigger(channel, thread, "agent-e", t0),
        GuardDecision::ChainRefused,
        "repeated agent triggers stay refused until a human resets the chain"
    );

    // A human message resets the chain; the next agent trigger is accepted.
    g.on_human_trigger(thread);
    assert_eq!(g.chain_depth(thread), 0);
    assert_eq!(
        g.evaluate_agent_trigger(channel, thread, "agent-f", t0),
        GuardDecision::Respond
    );
    assert_eq!(g.chain_depth(thread), 1);
}

#[test]
fn pingpong_alternation_enters_thread_cooldown() {
    let cfg = LoopGuardConfig {
        max_agent_chain: 100, // isolate ping-pong detection from the chain cap
        pingpong_limit: 2,
        ..Default::default()
    };
    let mut g = LoopGuard::new(cfg);
    let channel = Uuid::new_v4();
    let t0 = Instant::now();
    let thread = "root-pingpong";

    // A B A B — exactly two rounds of alternation is allowed.
    for author in ["agent-a", "agent-b", "agent-a", "agent-b"] {
        assert_eq!(
            g.evaluate_agent_trigger(channel, thread, author, t0),
            GuardDecision::Respond
        );
    }
    assert!(g.pingpong_cooldown_until(thread).is_none());

    // The next alternation (5th position) crosses the limit: cooldown fires
    // and the event is refused.
    assert_eq!(
        g.evaluate_agent_trigger(channel, thread, "agent-a", t0),
        GuardDecision::PingPongCooldown
    );
    assert!(g.pingpong_cooldown_until(thread).is_some());

    // During the cooldown the whole thread stays silent — even from a
    // different channel, because the cooldown is thread-scoped.
    let other_channel = Uuid::new_v4();
    assert_eq!(
        g.evaluate_agent_trigger(other_channel, thread, "agent-b", t0),
        GuardDecision::PingPongCooldown
    );

    // A different thread is unaffected.
    assert_eq!(
        g.evaluate_agent_trigger(channel, "root-other", "agent-c", t0),
        GuardDecision::Respond
    );

    // After the cooldown expires the thread may respond again.
    let after = t0 + Duration::from_secs(120);
    assert_eq!(
        g.evaluate_agent_trigger(other_channel, thread, "agent-b", after),
        GuardDecision::Respond
    );
}

#[test]
fn agent_reply_rate_limits_per_channel_and_defers_until_cooldown() {
    let cfg = LoopGuardConfig {
        max_agent_chain: 100, // isolate the rate limit from the chain cap
        agent_reply_rate: 6,
        ..Default::default()
    };
    let mut g = LoopGuard::new(cfg);
    let channel = Uuid::new_v4();
    let other_channel = Uuid::new_v4();
    let t0 = Instant::now();
    let thread = "root-rate";

    for i in 0..6 {
        assert_eq!(
            g.evaluate_agent_trigger(channel, thread, &format!("agent-{i}"), t0),
            GuardDecision::Respond,
            "agent trigger {} within the default per-minute rate must respond",
            i + 1
        );
    }
    assert_eq!(g.agent_reply_tick_count(channel), 6);

    // The 7th in the same minute is rate-limited: queued with a cooldown.
    assert_eq!(
        g.evaluate_agent_trigger(channel, thread, "agent-6", t0),
        GuardDecision::RateLimited
    );
    assert!(g.is_rate_cooldown(channel, t0));
    assert!(g.rate_cooldown_until(channel).is_some());

    // During the cooldown further agent triggers stay deferred (never dropped
    // — the event is queued, dispatch is held).
    assert_eq!(
        g.evaluate_agent_trigger(channel, thread, "agent-7", t0),
        GuardDecision::RateLimited
    );

    // A different channel is unaffected by this channel's cooldown.
    assert_eq!(
        g.evaluate_agent_trigger(other_channel, "root-other", "agent-x", t0),
        GuardDecision::Respond
    );

    // Human events are never rate limited and never add rate ticks.
    g.on_human_trigger(thread);
    assert_eq!(g.agent_reply_tick_count(channel), 6);

    // After the cooldown expires the channel may dispatch agent triggers again.
    let after = t0 + Duration::from_secs(120);
    assert_eq!(
        g.evaluate_agent_trigger(channel, thread, "agent-8", after),
        GuardDecision::Respond
    );
    assert!(!g.is_rate_cooldown(channel, after));
}

#[test]
fn zero_config_disables_every_guard() {
    let cfg = LoopGuardConfig {
        max_agent_chain: 0,
        pingpong_limit: 0,
        agent_reply_rate: 0,
        ..Default::default()
    };
    let channel = Uuid::new_v4();
    let t0 = Instant::now();
    let thread = "root-disabled";

    // Chain guard disabled: an arbitrarily deep agent cascade all responds.
    let mut g = LoopGuard::new(cfg);
    for i in 0..10 {
        assert_eq!(
            g.evaluate_agent_trigger(channel, thread, &format!("agent-{i}"), t0),
            GuardDecision::Respond,
            "disabled chain guard must respond to agent trigger {i}"
        );
    }

    // Ping-pong guard disabled: strict alternation never cooldowns.
    let mut g = LoopGuard::new(cfg);
    for (i, author) in ["agent-a", "agent-b"].iter().cycle().take(12).enumerate() {
        assert_eq!(
            g.evaluate_agent_trigger(channel, thread, author, t0),
            GuardDecision::Respond,
            "disabled ping-pong guard must respond to alternation step {i}"
        );
    }
    assert!(g.pingpong_cooldown_until(thread).is_none());

    // Rate guard disabled: no per-minute cap, no rate ticks recorded.
    let mut g = LoopGuard::new(cfg);
    for i in 0..50 {
        assert_eq!(
            g.evaluate_agent_trigger(channel, thread, &format!("agent-{i}"), t0),
            GuardDecision::Respond
        );
    }
    assert_eq!(g.agent_reply_tick_count(channel), 0);
}

#[test]
fn chain_and_pingpong_state_is_per_thread() {
    let cfg = LoopGuardConfig {
        max_agent_chain: 2,
        pingpong_limit: 0, // disabled — isolate the chain cap
        ..Default::default()
    };
    let mut g = LoopGuard::new(cfg);
    let channel = Uuid::new_v4();
    let t0 = Instant::now();

    assert_eq!(
        g.evaluate_agent_trigger(channel, "thread-a", "agent-1", t0),
        GuardDecision::Respond
    );
    assert_eq!(
        g.evaluate_agent_trigger(channel, "thread-a", "agent-2", t0),
        GuardDecision::Respond
    );
    assert_eq!(
        g.evaluate_agent_trigger(channel, "thread-a", "agent-3", t0),
        GuardDecision::ChainRefused,
        "thread-a has exhausted its chain depth"
    );

    // thread-b starts its own chain fresh.
    assert_eq!(
        g.evaluate_agent_trigger(channel, "thread-b", "agent-4", t0),
        GuardDecision::Respond
    );
    assert_eq!(g.chain_depth("thread-b"), 1);
}

#[test]
fn rate_limiter_sliding_window_recovers_after_ticks_age_out() {
    let cfg = LoopGuardConfig {
        max_agent_chain: 100,
        agent_reply_rate: 6,
        ..Default::default()
    };
    let mut g = LoopGuard::new(cfg);
    let channel = Uuid::new_v4();
    let t0 = Instant::now();
    let thread = "root-window";

    for i in 0..6 {
        assert_eq!(
            g.evaluate_agent_trigger(
                channel,
                thread,
                &format!("agent-{i}"),
                t0 + Duration::from_secs(i)
            ),
            GuardDecision::Respond
        );
    }
    // All 6 ticks are inside the 60s window -> the 7th is limited.
    assert_eq!(
        g.evaluate_agent_trigger(channel, thread, "agent-6", t0 + Duration::from_secs(10)),
        GuardDecision::RateLimited
    );
    // Once the two oldest ticks age past 60s, the window has room again.
    assert_eq!(
        g.evaluate_agent_trigger(channel, thread, "agent-7", t0 + Duration::from_secs(61)),
        GuardDecision::Respond,
        "sliding window must recover once the oldest ticks are older than 60s"
    );
}
