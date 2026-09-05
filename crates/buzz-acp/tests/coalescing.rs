use std::time::{Duration, Instant};

use buzz_acp::test_api::{
    format_prompt, CancelReason, DedupMode, EventQueue, FormatPromptArgs, QueuedEvent,
};
use nostr::{EventBuilder, Keys, Kind};
use uuid::Uuid;

fn queued(channel_id: Uuid, content: &str, received_at: Instant) -> QueuedEvent {
    QueuedEvent {
        channel_id,
        event: EventBuilder::new(Kind::Custom(9), content)
            .sign_with_keys(&Keys::generate())
            .expect("event"),
        received_at,
        prompt_tag: "@mention".into(),
    }
}

#[test]
fn bounded_window_coalesces_only_events_arriving_before_work_begins() {
    let channel = Uuid::new_v4();
    let start = Instant::now();
    let window = Duration::from_millis(80);
    let mut queue = EventQueue::new(DedupMode::Queue).with_coalescing_window(window);

    assert!(queue.push(queued(channel, "first", start)));
    assert!(queue
        .flush_next_at(start + Duration::from_millis(79))
        .is_none());
    assert!(queue.push(queued(channel, "second", start + Duration::from_millis(30),)));

    let first_turn = queue.flush_next_at(start + window).expect("window elapsed");
    assert_eq!(
        first_turn
            .events
            .iter()
            .map(|event| event.event.content.as_str())
            .collect::<Vec<_>>(),
        ["first", "second"]
    );

    // Once the channel is active, later events retain their own envelope and
    // do not wait for or merge into the already-dispatched turn.
    assert!(queue.push(queued(
        channel,
        "during-work",
        start + Duration::from_millis(90),
    )));
    assert!(queue
        .flush_next_at(start + Duration::from_secs(1))
        .is_none());
    queue.mark_complete(channel);
    let second_turn = queue
        .flush_next_at(start + Duration::from_secs(1))
        .expect("queued after active turn");
    assert_eq!(second_turn.events.len(), 1);
    assert_eq!(second_turn.events[0].event.content, "during-work");
}

#[test]
fn zero_window_disables_coalescing() {
    let channel = Uuid::new_v4();
    let now = Instant::now();
    let mut queue = EventQueue::new(DedupMode::Queue).with_coalescing_window(Duration::ZERO);
    assert!(queue.push(queued(channel, "now", now)));
    assert!(queue.flush_next_at(now).is_some());
}

#[test]
fn next_deadline_wakes_a_quiet_queue_without_another_relay_event() {
    let channel = Uuid::new_v4();
    let now = Instant::now();
    let window = Duration::from_millis(100);
    let mut queue = EventQueue::new(DedupMode::Queue).with_coalescing_window(window);
    assert!(queue.push(queued(channel, "quiet", now)));
    assert_eq!(queue.next_flush_deadline(), Some(now + window));
    assert!(!queue.has_flushable_work_at(now + Duration::from_millis(99)));
    assert!(queue.has_flushable_work_at(now + window));
}

#[test]
fn passive_context_cannot_wake_queue_and_attaches_to_next_turn() {
    let channel = Uuid::new_v4();
    let now = Instant::now();
    let mut queue = EventQueue::new(DedupMode::Queue);

    queue.attach_context(queued(channel, "FYI only", now));
    assert!(!queue.has_undispatched_work());
    assert!(queue.flush_next_at(now + Duration::from_secs(1)).is_none());

    assert!(queue.push(queued(channel, "Please act", now + Duration::from_secs(2),)));
    let batch = queue
        .flush_next_at(now + Duration::from_secs(2))
        .expect("admitted work carries context");
    assert_eq!(batch.events.len(), 1);
    assert_eq!(batch.cancel_reason, Some(CancelReason::PassiveContext));
    assert_eq!(batch.cancelled_events.len(), 1);
    assert_eq!(batch.cancelled_events[0].event.content, "FYI only");
    let prompt = format_prompt(&batch, &FormatPromptArgs::default()).join("\n");
    assert!(prompt.contains("[Passive context / FYI — no response requested]"));
    assert!(prompt.contains("Content: FYI only"));
    assert!(prompt.contains("Content: Please act"));
}
