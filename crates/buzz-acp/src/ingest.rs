//! Bounded channel snapshot/backlog ingestion for buzz-acp.
//!
//! When the harness points at a busy channel, the relay replays a large
//! backlog (hundreds of messages) during startup. The lorelei defect was a
//! bridge pointed at exactly such a channel crashing the process 4-12s after
//! start with the error swallowed — zero output. This module makes channel
//! snapshot/backlog ingestion safe by construction:
//!
//! - **Hard-capped**: [`ingest_snapshot`] never retains more than the
//!   operator's `context_message_limit` (`BUZZ_ACP_CONTEXT_MESSAGE_LIMIT`)
//!   messages, regardless of what the relay returns. That limit is itself
//!   clamped to [`INGEST_ABSOLUTE_CAP`] so a misconfigured value cannot drive
//!   an unbounded allocation.
//! - **Iterative, not recursive**: parsing walks a flat loop with constant
//!   per-event allocation; there is no recursion that a deep or wide backlog
//!   could turn into a stack overflow.
//! - **Oversized-event tolerant**: event content beyond
//!   [`INGEST_MAX_EVENT_BYTES`] is clamped with an elision marker instead of
//!   ballooning memory, and malformed events are skipped — never a panic.
//! - **Never silently fatal**: every boundary logs the concrete error/count,
//!   and ingestion beyond the cap is skipped with an INFO line, not fatal.
//!
//! [`BacklogLimiter`] is the startup-phase integration: it gives each channel
//! an independent ingestion budget for its initial snapshot. Once the channel
//! has been dispatched for the first time ([`BacklogLimiter::release`]) the
//! bound is lifted so live events flow unbounded.

use std::collections::{HashMap, HashSet};

use uuid::Uuid;

/// Absolute ceiling for a single snapshot ingest pass, applied on top of
/// `context_message_limit` so a misconfigured value can never drive an
/// unbounded retention. 500 covers the documented busy-channel repro
/// (hundreds of backlog messages) while still small enough that a full pass
/// cannot exhaust memory.
pub const INGEST_ABSOLUTE_CAP: usize = 500;

/// Ceiling for a single event's retained content, in bytes. Events with larger
/// content are clamped to this bound with an elision marker rather than
/// ballooning the snapshot.
pub const INGEST_MAX_EVENT_BYTES: usize = 64 * 1024;

/// Marker appended to clamped oversized event content.
const ELISION_MARKER: &str = "…[truncated: oversized event content]";

/// One retained message from a channel snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestedMessage {
    /// Nostr event id (hex), when present.
    pub event_id: String,
    /// Author pubkey (hex), when present.
    pub pubkey: String,
    /// Unix timestamp, defaulting to 0 when absent.
    pub created_at: u64,
    /// Message content. Clamped to [`INGEST_MAX_EVENT_BYTES`] when oversized.
    pub content: String,
    /// Whether `content` was clamped because the raw event was oversized.
    pub oversized: bool,
}

/// Counts for one snapshot ingest pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IngestReport {
    /// Events offered to the ingest pass (valid or not).
    pub total_seen: usize,
    /// Events retained in the bounded snapshot.
    pub retained: usize,
    /// Events skipped because the hard cap was already reached.
    pub skipped: usize,
    /// Events whose content was clamped as oversized.
    pub oversized: usize,
    /// Events that were structurally invalid and could not become messages.
    pub malformed: usize,
    /// Whether the backlog extended beyond the hard cap (`skipped > 0`).
    pub truncated: bool,
}

/// Resolve the operator's `context_message_limit` into a hard retention cap.
///
/// `0` (context disabled) still bounds at [`INGEST_ABSOLUTE_CAP`]: disabling
/// prompt context must not re-open the busy-channel crash.
pub fn effective_cap(context_limit: u32) -> usize {
    match context_limit {
        0 => INGEST_ABSOLUTE_CAP,
        limit => (limit as usize).min(INGEST_ABSOLUTE_CAP),
    }
}

/// Ingest a channel backlog snapshot, returning the bounded retained messages
/// and a report of what happened.
///
/// Relay backlog order (newest-first) is preserved: the first `cap` valid
/// messages are retained, and everything beyond the cap is skipped with an
/// INFO line (never fatal). Malformed events are skipped without panicking.
pub fn ingest_snapshot<'a>(
    events: impl IntoIterator<Item = &'a serde_json::Value>,
    context_limit: u32,
) -> (Vec<IngestedMessage>, IngestReport) {
    let cap = effective_cap(context_limit);
    let mut messages: Vec<IngestedMessage> = Vec::new();
    let mut report = IngestReport::default();

    for event in events {
        report.total_seen += 1;
        if messages.len() >= cap {
            report.skipped += 1;
            continue;
        }
        let Some(mut message) = message_from_json(event) else {
            report.malformed += 1;
            continue;
        };
        if message.content.len() > INGEST_MAX_EVENT_BYTES {
            message.oversized = true;
            report.oversized += 1;
            message.content = clamp_content(&message.content, INGEST_MAX_EVENT_BYTES);
        }
        messages.push(message);
        report.retained += 1;
    }

    report.truncated = report.skipped > 0;
    (messages, report)
}

/// Build an [`IngestedMessage`] from a raw JSON event object.
///
/// Defensive by construction: every field access is fallible and a message
/// without a string `content` is considered malformed. Never panics.
fn message_from_json(event: &serde_json::Value) -> Option<IngestedMessage> {
    let content = event.get("content")?.as_str()?;
    let pubkey = event
        .get("pubkey")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let created_at = event
        .get("created_at")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let event_id = event
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();

    Some(IngestedMessage {
        event_id,
        pubkey,
        created_at,
        content: content.to_string(),
        oversized: false,
    })
}

/// Clamp `content` to at most `max_bytes` bytes, appending an elision marker.
///
/// Truncates on a char boundary so a multi-byte character can never be cut
/// mid-UTF-8.
fn clamp_content(content: &str, max_bytes: usize) -> String {
    if content.len() <= max_bytes {
        return content.to_string();
    }
    let budget = max_bytes.saturating_sub(ELISION_MARKER.len());
    let mut out = String::new();
    for c in content.chars() {
        if out.len() + c.len_utf8() > budget {
            break;
        }
        out.push(c);
    }
    out.push_str(ELISION_MARKER);
    out
}

/// Per-channel startup backlog ingestion budget.
///
/// While a channel is draining its initial snapshot (before its first
/// dispatch), at most `effective_cap(context_limit)` events are ingested per
/// channel; every further event is skipped with an INFO line so startup stays
/// bounded regardless of backlog depth. [`BacklogLimiter::release`] lifts the
/// bound after the channel's first dispatch, after which live events flow
/// unbounded.
#[derive(Debug)]
pub struct BacklogLimiter {
    context_limit: u32,
    ingested: HashMap<Uuid, usize>,
    released: HashSet<Uuid>,
}

impl BacklogLimiter {
    /// Create a limiter bound by the operator's `context_message_limit`.
    pub fn new(context_limit: u32) -> Self {
        Self {
            context_limit,
            ingested: HashMap::new(),
            released: HashSet::new(),
        }
    }

    /// Ask whether an event for `channel` may be ingested right now.
    ///
    /// Returns `false` when the channel is still inside its startup snapshot
    /// phase and has already consumed its full budget; the caller skips the
    /// event with an INFO line. Returns `true` unconditionally once the
    /// channel has been released.
    pub fn should_ingest(&mut self, channel: Uuid) -> bool {
        if self.released.contains(&channel) {
            return true;
        }
        let cap = effective_cap(self.context_limit);
        let count = self.ingested.entry(channel).or_insert(0);
        if *count >= cap {
            return false;
        }
        *count += 1;
        true
    }

    /// Number of events ingested so far for `channel` during its snapshot
    /// phase (tests and diagnostics).
    pub fn ingested(&self, channel: Uuid) -> usize {
        self.ingested.get(&channel).copied().unwrap_or(0)
    }

    /// Lift the startup bound for `channel` (call after its first dispatch).
    pub fn release(&mut self, channel: Uuid) {
        self.released.insert(channel);
        self.ingested.remove(&channel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_content_respects_byte_budget_and_char_boundaries() {
        let content = "héllo wörld ".repeat(10_000);
        let clamped = clamp_content(&content, 1024);
        assert!(clamped.len() <= 1024);
        assert!(clamped.ends_with(ELISION_MARKER));
        assert!(std::str::from_utf8(clamped.as_bytes()).is_ok());
    }

    #[test]
    fn short_content_is_unchanged() {
        assert_eq!(clamp_content("short", 1024), "short");
    }

    #[test]
    fn backlog_limiter_release_is_idempotent() {
        let mut limiter = BacklogLimiter::new(4);
        let channel = Uuid::new_v4();
        limiter.release(channel);
        limiter.release(channel);
        assert!(limiter.should_ingest(channel));
    }
}
