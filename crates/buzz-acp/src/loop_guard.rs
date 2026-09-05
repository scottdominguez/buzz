//! Mention-loop guards for multi-agent channels.
//!
//! When several agents (and their shared owner) inhabit one channel, an
//! agent-to-agent mention cascade can spin a prompt loop: A replies to B, B
//! replies to A, A replies again … until the owner steps in or the harness
//! burns tokens. These guards protect against that loop while leaving
//! human-authored triggers completely untouched.
//!
//! Three independent, configurable guards, all ON by default:
//!
//! 1. **Agent-chain depth** — when a turn is triggered by an event authored by
//!    another agent (a NIP-OA-verified same-owner sibling), a per-thread chain
//!    counter inherited from the triggering thread is incremented. Beyond
//!    `BUZZ_ACP_MAX_AGENT_CHAIN` (default 3) the harness refuses to
//!    auto-respond and stays silent. A human message resets the chain.
//! 2. **Ping-pong detection** — if the same two pubkeys alternate mentions in
//!    one thread for more than `BUZZ_ACP_PINGPONG_LIMIT` consecutive rounds
//!    (default 2), the thread enters a cooldown (`BUZZ_ACP_PINGPONG_COOLDOWN_SECS`,
//!    default 60) and the harness logs loudly instead of responding.
//! 3. **Agent auto-reply rate limit** — at most `BUZZ_ACP_AGENT_REPLY_RATE`
//!    agent-triggered auto-replies per channel per minute (default 6). Over the
//!    limit the channel enters a cooldown (`BUZZ_ACP_AGENT_REPLY_COOLDOWN_SECS`,
//!    default 30) during which agent-triggered events are queued but dispatch
//!    is deferred — events are never dropped, and human events are never
//!    limited by any of these guards.
//!
//! A configured value of `0` disables the corresponding guard. The guards are
//! per-session companions to [`crate::turn_state`] — they gate *whether* an
//! auto-response may happen; the state machine still decides *how* a second
//! event is routed once a turn is in flight.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use uuid::Uuid;

/// Default maximum agent-to-agent chain depth per thread.
pub const DEFAULT_MAX_AGENT_CHAIN: u32 = 3;
/// Default ping-pong detection limit: consecutive alternating rounds before
/// the thread enters cooldown.
pub const DEFAULT_PINGPONG_LIMIT: u32 = 2;
/// Default per-thread ping-pong cooldown duration.
pub const DEFAULT_PINGPONG_COOLDOWN_SECS: u64 = 60;
/// Default maximum agent-triggered auto-replies per channel per minute.
pub const DEFAULT_AGENT_REPLY_RATE: u32 = 6;
/// Default per-channel cooldown when the agent reply rate is exceeded.
pub const DEFAULT_AGENT_REPLY_COOLDOWN_SECS: u64 = 30;

/// Sliding-window width for the per-channel agent auto-reply rate limit.
pub const AGENT_REPLY_WINDOW: Duration = Duration::from_secs(60);

/// Upper bound on tracked thread state before the oldest entries are dropped.
/// Guards hold at most one small struct per active thread/channel; the cap
/// prevents a hostile multi-thread flood from growing the maps without bound.
const MAX_TRACKED_THREADS: usize = 1024;
/// Upper bound on tracked channel state (rate ticks + cooldowns).
const MAX_TRACKED_CHANNELS: usize = 1024;

/// Tunable guard limits. `0` disables the corresponding guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoopGuardConfig {
    /// Maximum agent-to-agent chain depth per thread. `0` disables the guard.
    pub max_agent_chain: u32,
    /// Ping-pong detection limit (consecutive alternating rounds). `0` disables
    /// the guard.
    pub pingpong_limit: u32,
    /// Per-thread cooldown after a ping-pong is detected.
    pub pingpong_cooldown: Duration,
    /// Maximum agent-triggered auto-replies per channel per
    /// [`AGENT_REPLY_WINDOW`]. `0` disables the guard.
    pub agent_reply_rate: u32,
    /// Per-channel cooldown after the agent reply rate is exceeded.
    pub agent_reply_cooldown: Duration,
}

impl Default for LoopGuardConfig {
    fn default() -> Self {
        Self {
            max_agent_chain: DEFAULT_MAX_AGENT_CHAIN,
            pingpong_limit: DEFAULT_PINGPONG_LIMIT,
            pingpong_cooldown: Duration::from_secs(DEFAULT_PINGPONG_COOLDOWN_SECS),
            agent_reply_rate: DEFAULT_AGENT_REPLY_RATE,
            agent_reply_cooldown: Duration::from_secs(DEFAULT_AGENT_REPLY_COOLDOWN_SECS),
        }
    }
}

/// What the harness should do with an agent-authored trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardDecision {
    /// The trigger may proceed: it is within every guard's limits.
    Respond,
    /// The agent chain depth is exhausted — log and stay silent.
    ChainRefused,
    /// Ping-pong alternation was detected or the thread is in its cooldown —
    /// log loudly and stay silent.
    PingPongCooldown,
    /// The per-channel agent auto-reply rate is exceeded — the event stays
    /// queued but dispatch is deferred until the channel cooldown expires.
    RateLimited,
}

/// Per-thread/per-channel mention-loop guard state.
///
/// All decisions are made against the `now` the caller injects so tests and
/// the main loop share one deterministic clock discipline. The guard only
/// ever gates **agent-triggered** auto-responses; human triggers call
/// [`LoopGuard::on_human_trigger`], which resets thread-local cascade state
/// and is never rate-limited.
#[derive(Debug)]
pub struct LoopGuard {
    config: LoopGuardConfig,
    /// Current agent-chain depth per thread key.
    chains: HashMap<String, u32>,
    /// Recent agent-trigger author sequence per thread key, for ping-pong
    /// detection. Bounded per entry to the ping-pong horizon.
    pingpong_streaks: HashMap<String, VecDeque<String>>,
    /// Per-thread ping-pong cooldown deadlines.
    pingpong_cooldown_until: HashMap<String, Instant>,
    /// Per-channel sliding window of agent auto-reply ticks.
    reply_ticks: HashMap<Uuid, VecDeque<Instant>>,
    /// Per-channel agent-reply cooldown deadlines.
    rate_cooldown_until: HashMap<Uuid, Instant>,
}

impl Default for LoopGuard {
    fn default() -> Self {
        Self::new(LoopGuardConfig::default())
    }
}

impl LoopGuard {
    /// Create a guard with the given configuration.
    pub fn new(config: LoopGuardConfig) -> Self {
        Self {
            config,
            chains: HashMap::new(),
            pingpong_streaks: HashMap::new(),
            pingpong_cooldown_until: HashMap::new(),
            reply_ticks: HashMap::new(),
            rate_cooldown_until: HashMap::new(),
        }
    }

    /// Evaluate an **agent-authored** trigger and record the auto-response.
    ///
    /// `thread` is the thread key (root event id, or the event's own id for a
    /// top-level message); `author` is the triggering agent's pubkey hex;
    /// `now` is the caller's clock.
    ///
    /// Returns the decision the harness must honor:
    /// - [`GuardDecision::Respond`] — dispatch normally and count this as an
    ///   agent auto-reply for the channel rate window.
    /// - [`GuardDecision::ChainRefused`] — stay silent; the thread's agent
    ///   chain is deeper than `max_agent_chain`.
    /// - [`GuardDecision::PingPongCooldown`] — stay silent; ping-pong was
    ///   detected or the thread is still in its cooldown.
    /// - [`GuardDecision::RateLimited`] — do not dispatch now; keep the event
    ///   queued until the channel cooldown expires.
    pub fn evaluate_agent_trigger(
        &mut self,
        channel: Uuid,
        thread: &str,
        author: &str,
        now: Instant,
    ) -> GuardDecision {
        if self
            .pingpong_cooldown_until
            .get(thread)
            .is_some_and(|until| *until > now)
        {
            return GuardDecision::PingPongCooldown;
        }
        if self.is_rate_cooldown(channel, now) {
            return GuardDecision::RateLimited;
        }

        // 1. Agent-chain depth. The counter is inherited from the triggering
        //    thread and incremented by this turn; a human message resets it.
        if self.config.max_agent_chain > 0 {
            let depth = self.chains.get(thread).copied().unwrap_or(0) + 1;
            if depth > self.config.max_agent_chain {
                return GuardDecision::ChainRefused;
            }
            bounded_insert(
                &mut self.chains,
                thread.to_string(),
                depth,
                MAX_TRACKED_THREADS,
            );
        }

        // 2. Ping-pong detection. Only meaningful for a trigger we are about
        //    to respond to (the chain check already passed).
        if self.config.pingpong_limit > 0 {
            if self.pingpong_streaks.len() >= MAX_TRACKED_THREADS {
                tracing::debug!(
                    cap = MAX_TRACKED_THREADS,
                    "loop-guard ping-pong streak cap reached — evicting all"
                );
                self.pingpong_streaks.clear();
            }
            // The streak needs at most `2*limit + 1` entries to prove one
            // round past the detection horizon; clamp so a large configured
            // limit can never grow a streak without bound.
            let horizon = ((self.config.pingpong_limit as usize) * 2 + 1).min(64);
            let streak = self.pingpong_streaks.entry(thread.to_string()).or_default();
            if streak.len() >= horizon {
                streak.pop_front();
            }
            streak.push_back(author.to_string());
            if alternating_len(streak) > (self.config.pingpong_limit as usize) * 2 {
                let partner = streak_first_other(streak, author);
                bounded_insert(
                    &mut self.pingpong_cooldown_until,
                    thread.to_string(),
                    now + self.config.pingpong_cooldown,
                    MAX_TRACKED_THREADS,
                );
                streak.clear();
                tracing::warn!(
                    thread,
                    partner = partner.as_deref().unwrap_or("?"),
                    author,
                    limit = self.config.pingpong_limit,
                    cooldown_secs = self.config.pingpong_cooldown.as_secs(),
                    "mention-loop guard: agent ping-pong detected in thread — entering cooldown"
                );
                return GuardDecision::PingPongCooldown;
            }
        }

        // 3. Per-channel agent auto-reply rate limit. A responded trigger is a
        //    tick in the channel's sliding window; over the limit the channel
        //    enters a cooldown and the event is queued, not dispatched.
        if self.config.agent_reply_rate > 0 {
            let ticks = self.reply_ticks.entry(channel).or_default();
            prune_window(ticks, now);
            if ticks.len() >= self.config.agent_reply_rate as usize {
                bounded_insert(
                    &mut self.rate_cooldown_until,
                    channel,
                    now + self.config.agent_reply_cooldown,
                    MAX_TRACKED_CHANNELS,
                );
                return GuardDecision::RateLimited;
            }
            ticks.push_back(now);
        }

        GuardDecision::Respond
    }

    /// Record a **human-authored** trigger in `thread`.
    ///
    /// Humans are never limited by these guards; this call only resets the
    /// thread-local cascade state (chain depth + ping-pong streak) so the next
    /// agent-triggered turn starts a fresh chain.
    pub fn on_human_trigger(&mut self, thread: &str) {
        self.chains.remove(thread);
        self.pingpong_streaks.remove(thread);
    }

    /// Whether dispatch for `channel` is currently deferred by the agent
    /// auto-reply rate cooldown.
    pub fn is_rate_cooldown(&self, channel: Uuid, now: Instant) -> bool {
        self.rate_cooldown_until
            .get(&channel)
            .is_some_and(|until| *until > now)
    }

    /// Current agent-chain depth for `thread` (tests and diagnostics).
    pub fn chain_depth(&self, thread: &str) -> u32 {
        self.chains.get(thread).copied().unwrap_or(0)
    }

    /// Ping-pong cooldown deadline for `thread`, if active.
    pub fn pingpong_cooldown_until(&self, thread: &str) -> Option<Instant> {
        self.pingpong_cooldown_until.get(thread).copied()
    }

    /// Agent-reply cooldown deadline for `channel`, if active.
    pub fn rate_cooldown_until(&self, channel: Uuid) -> Option<Instant> {
        self.rate_cooldown_until.get(&channel).copied()
    }

    /// Number of agent auto-reply ticks currently inside the channel's
    /// sliding window (tests and diagnostics).
    pub fn agent_reply_tick_count(&self, channel: Uuid) -> usize {
        self.reply_ticks.get(&channel).map_or(0, VecDeque::len)
    }

    /// The active guard configuration.
    pub fn config(&self) -> &LoopGuardConfig {
        &self.config
    }
}

/// Drop rate ticks older than the sliding window.
fn prune_window(ticks: &mut VecDeque<Instant>, now: Instant) {
    while ticks
        .front()
        .is_some_and(|tick| now.duration_since(*tick) >= AGENT_REPLY_WINDOW)
    {
        ticks.pop_front();
    }
}

/// Insert `(key, value)` into `map`, evicting everything when the entry cap is
/// reached so a hostile multi-thread flood cannot grow the guard's state
/// without bound.
fn bounded_insert<K: std::cmp::Eq + std::hash::Hash, V>(
    map: &mut HashMap<K, V>,
    key: K,
    value: V,
    cap: usize,
) {
    if map.len() >= cap {
        tracing::debug!(cap, "loop-guard state cap reached — evicting all entries");
        map.clear();
    }
    map.insert(key, value);
}

/// Length of the longest suffix of `authors` that strictly alternates between
/// exactly two distinct pubkeys (a ping-pong pattern `A B A B …`).
///
/// Returns `0` when fewer than two distinct keys appear in the suffix or when
/// consecutive entries repeat.
fn alternating_len(authors: &VecDeque<String>) -> usize {
    let mut keys: Vec<&str> = Vec::with_capacity(2);
    let mut len = 0usize;
    let mut prev: Option<&str> = None;
    for author in authors.iter().rev() {
        if let Some(prev) = prev {
            if author == prev {
                break; // consecutive entries must strictly alternate
            }
        }
        if !keys.contains(&author.as_str()) {
            if keys.len() >= 2 {
                break; // a third distinct key ends the alternation
            }
            keys.push(author.as_str());
        }
        prev = Some(author.as_str());
        len += 1;
    }
    if keys.len() == 2 {
        len
    } else {
        0
    }
}

/// Best-effort partner name for the ping-pong warn line. `author` is the
/// current trigger; returns the other pubkey from the (just-cleared) streak.
fn streak_first_other(streak: &VecDeque<String>, author: &str) -> Option<String> {
    streak
        .iter()
        .find(|candidate| *candidate != author)
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alternating_len_detects_two_key_alternation() {
        let mut streak = VecDeque::new();
        streak.extend(["a", "b", "a", "b", "a"].map(String::from));
        assert_eq!(alternating_len(&streak), 5);

        streak.clear();
        streak.extend(["a", "b", "a", "b"].map(String::from));
        assert_eq!(alternating_len(&streak), 4);

        streak.clear();
        streak.extend(["a", "a", "b"].map(String::from));
        assert_eq!(alternating_len(&streak), 2);

        streak.clear();
        streak.extend(["a", "b", "c"].map(String::from));
        assert_eq!(alternating_len(&streak), 2);

        streak.clear();
        streak.extend(["a"].map(String::from));
        assert_eq!(alternating_len(&streak), 0);
    }
}
