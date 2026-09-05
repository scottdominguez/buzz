//! Red-first regression test for the busy-channel ingest crash (the lorelei
//! defect).
//!
//! Repro basis: pointing a bridge at a channel with a large backlog (hundreds
//! of messages, ~28 members) crashed the process 4-12s after start with the
//! error swallowed (zero output).
//!
//! This test drives the bounded snapshot/backlog ingestion
//! (`ingest_snapshot` + `BacklogLimiter`) against a synthetic 500-message
//! backlog channel and proves it starts cleanly:
//!   - no panic, no unbounded retention — the configured `context_message_limit`
//!     is a hard cap (bounded above by `INGEST_ABSOLUTE_CAP`),
//!   - oversized events are clamped, never fatal,
//!   - ingestion beyond the cap is skipped (INFO line, not fatal),
//!   - the per-channel backlog limiter bounds startup ingestion and releases
//!     after the first dispatch so live events flow unbounded.

use buzz_acp::test_api::{
    effective_cap, ingest_snapshot, BacklogLimiter, INGEST_ABSOLUTE_CAP, INGEST_MAX_EVENT_BYTES,
};
use serde_json::json;
use uuid::Uuid;

fn synthetic_event(i: usize, content: &str) -> serde_json::Value {
    json!({
        "id": format!("{:064x}", i),
        "pubkey": format!("{:064x}", i + 1),
        "created_at": 1_700_000_000 + i as u64,
        "content": content,
    })
}

#[test]
fn five_hundred_message_backlog_starts_cleanly() {
    let events: Vec<serde_json::Value> = (0..500)
        .map(|i| synthetic_event(i, &format!("message {i}")))
        .collect();

    let (messages, report) = ingest_snapshot(events.iter(), 12);

    // The whole backlog is seen, but only the configured context limit is
    // retained; everything beyond the cap is skipped, never fatal.
    assert_eq!(report.total_seen, 500);
    assert_eq!(report.retained, 12);
    assert_eq!(report.skipped, 488);
    assert!(report.truncated);
    assert_eq!(messages.len(), 12);

    // Relay backlog order (newest-first) is preserved: the first `cap`
    // messages are the retained snapshot.
    assert_eq!(messages[0].content, "message 0");
    assert_eq!(messages[11].content, "message 11");
}

#[test]
fn oversized_events_are_clamped_not_fatal() {
    let huge = "x".repeat(2 * 1024 * 1024);
    let events = [
        synthetic_event(0, &huge),
        synthetic_event(1, "normal"),
        synthetic_event(2, &huge),
    ];

    let (messages, report) = ingest_snapshot(events.iter(), 100);

    assert_eq!(report.oversized, 2);
    assert_eq!(messages.len(), 3);
    for message in &messages {
        assert!(
            message.content.len() <= INGEST_MAX_EVENT_BYTES,
            "oversized content must be clamped to INGEST_MAX_EVENT_BYTES"
        );
    }
    assert!(messages[0].oversized);
    assert!(
        messages[0].content.contains("truncated"),
        "clamped content must carry an elision marker"
    );
    assert!(!messages[1].oversized);
    assert_eq!(messages[1].content, "normal");
}

#[test]
fn context_limit_above_the_absolute_cap_is_clamped() {
    assert_eq!(effective_cap(1_000_000), INGEST_ABSOLUTE_CAP);
    assert_eq!(effective_cap(0), INGEST_ABSOLUTE_CAP);

    let events: Vec<serde_json::Value> = (0..2000).map(|i| synthetic_event(i, "x")).collect();

    // A misconfigured (huge) context limit cannot drive unbounded retention.
    let (_messages, report) = ingest_snapshot(events.iter(), 1_000_000);
    assert_eq!(report.retained, INGEST_ABSOLUTE_CAP);
    assert_eq!(report.skipped, 2000 - INGEST_ABSOLUTE_CAP);

    // Disabled context (0) still bounds at the absolute cap.
    let (_messages, report) = ingest_snapshot(events.iter(), 0);
    assert_eq!(report.retained, INGEST_ABSOLUTE_CAP);
}

#[test]
fn malformed_events_are_skipped_without_panicking() {
    let events = [
        json!({"id": "no-content"}),
        json!({"content": 42}), // non-string content
        synthetic_event(0, "ok"),
        serde_json::Value::Null,
        json!({}),
    ];

    let (messages, report) = ingest_snapshot(events.iter(), 100);

    assert_eq!(report.total_seen, 5);
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].content, "ok");
}

#[test]
fn synthetic_500_message_channel_starts_cleanly_end_to_end() {
    // Mirrors the production startup flow: the relay replays a 500-message
    // backlog newest-first; the per-channel backlog limiter admits at most
    // `context_limit` events (skipping the rest with an INFO line), and the
    // admitted snapshot ingests cleanly without panicking.
    let channel = Uuid::new_v4();
    let mut limiter = BacklogLimiter::new(12);

    let mut admitted = Vec::new();
    let mut skipped = 0usize;
    for i in (0..500).rev() {
        let event = synthetic_event(i, &format!("message {i}"));
        if limiter.should_ingest(channel) {
            admitted.push(event);
        } else {
            skipped += 1;
        }
    }

    assert_eq!(admitted.len(), 12);
    assert_eq!(skipped, 488);
    assert_eq!(limiter.ingested(channel), 12);

    let (_messages, report) = ingest_snapshot(admitted.iter(), 12);
    assert_eq!(report.retained, 12);
    assert_eq!(report.total_seen, 12);
    assert!(!report.truncated, "the limiter already bounded admission");

    // The limiter released after the first dispatch lets live events flow
    // unbounded for the same channel.
    limiter.release(channel);
    for i in 0..500 {
        assert!(
            limiter.should_ingest(channel),
            "post-release event {i} must ingest — the snapshot cap only bounds startup"
        );
    }
}

#[test]
fn backlog_limiter_is_per_channel() {
    let mut limiter = BacklogLimiter::new(5);
    let channel_a = Uuid::new_v4();
    let channel_b = Uuid::new_v4();

    for _ in 0..5 {
        assert!(limiter.should_ingest(channel_a));
    }
    assert!(
        !limiter.should_ingest(channel_a),
        "channel a exhausted its cap"
    );
    assert!(
        limiter.should_ingest(channel_b),
        "channel b has its own independent snapshot budget"
    );
}
