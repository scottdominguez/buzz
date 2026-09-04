//! Per-session active-turn state machine.
//!
//! Buzz owns at most one ACP session for a channel at a time, so the channel
//! id is the stable session key while a worker is checked out. Each tracked
//! session has its own mutex. The relay branch, prompt-result branch, and steer
//! acknowledgement branch therefore make atomic decisions against the same
//! state even if their work is moved onto concurrent tasks later.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

use uuid::Uuid;

/// Lifecycle of one channel session's active turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnState {
    /// A prompt is active and may accept one capability-approved steer.
    Running,
    /// A non-cancelling steer is awaiting its ACP acknowledgement.
    Steering,
    /// Events are waiting for a future prompt; no prompt is active.
    Queued,
    /// An explicit owner stop or supersede policy is cancelling the prompt.
    Cancelling,
    /// The completed prompt is committing delivery state and publishing.
    Publishing,
}

/// Policy requested for a newly accepted event on an in-flight channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventIntent {
    /// Preserve FIFO order and deliver after the active turn.
    Queue,
    /// Inject into the active turn when its ACP session advertised support.
    Steer,
    /// Explicit supersede policy; cancellation is allowed.
    Cancel,
}

/// Atomic decision returned for a newly accepted event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventDisposition {
    /// Leave the event in the ordered queue.
    Queue,
    /// Attempt a true, non-cancelling steer. The state is already `Steering`.
    Steer,
    /// Send the explicit cancellation. The state is already `Cancelling`.
    Cancel,
}

#[derive(Debug)]
struct SessionTurn {
    state: TurnState,
    /// Copied only from `_meta.steering.supported` during dispatch. A run id,
    /// adapter name, or later notification never changes this value.
    steering_supported: bool,
}

type SessionLock = Arc<Mutex<SessionTurn>>;

/// Turn states keyed by channel-session, with a distinct lock per session.
pub struct TurnStateMachine {
    sessions: RwLock<HashMap<Uuid, SessionLock>>,
}

impl Default for TurnStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl TurnStateMachine {
    /// Create an empty per-session state registry.
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Record queued work without disturbing a live turn.
    pub fn on_event_queued(&self, channel: Uuid) {
        if self.session(channel).is_some() {
            return;
        }
        self.sessions_write().entry(channel).or_insert_with(|| {
            Arc::new(Mutex::new(SessionTurn {
                state: TurnState::Queued,
                steering_supported: false,
            }))
        });
    }

    /// A prompt has been dispatched to this channel's current ACP session.
    ///
    /// `steering_supported` must be the exact boolean parsed from
    /// `_meta.steering.supported`; callers must not synthesize it.
    pub fn on_dispatch(&self, channel: Uuid, steering_supported: bool) {
        let session = self.session_or_insert(channel);
        let mut turn = lock_session(&session);
        turn.state = TurnState::Running;
        turn.steering_supported = steering_supported;
    }

    /// Atomically decide the fate of an event that arrived during a turn.
    ///
    /// This method is the single source of truth for second-event routing.
    /// Claiming `Steer` or `Cancel` performs the corresponding transition
    /// before returning, so concurrent arrivals cannot claim the same running
    /// turn twice.
    pub fn on_event_arrived(&self, channel: Uuid, intent: EventIntent) -> EventDisposition {
        let Some(session) = self.session(channel) else {
            return EventDisposition::Queue;
        };
        let mut turn = lock_session(&session);
        match (turn.state, intent) {
            (TurnState::Running, EventIntent::Steer) if turn.steering_supported => {
                turn.state = TurnState::Steering;
                EventDisposition::Steer
            }
            (TurnState::Running | TurnState::Steering, EventIntent::Cancel) => {
                turn.state = TurnState::Cancelling;
                EventDisposition::Cancel
            }
            _ => EventDisposition::Queue,
        }
    }

    /// A steer ack resolved, or a claimed steer could not be sent.
    pub fn on_steer_resolved(&self, channel: Uuid) {
        let Some(session) = self.session(channel) else {
            return;
        };
        let mut turn = lock_session(&session);
        if turn.state == TurnState::Steering {
            turn.state = TurnState::Running;
        }
    }

    /// An owner control command initiated cancellation outside event routing.
    pub fn on_cancel_started(&self, channel: Uuid) {
        let Some(session) = self.session(channel) else {
            return;
        };
        let mut turn = lock_session(&session);
        if matches!(turn.state, TurnState::Running | TurnState::Steering) {
            turn.state = TurnState::Cancelling;
        }
    }

    /// Restore a failed cancellation claim while the original turn is alive.
    pub fn on_cancel_not_sent(&self, channel: Uuid) {
        let Some(session) = self.session(channel) else {
            return;
        };
        let mut turn = lock_session(&session);
        if turn.state == TurnState::Cancelling {
            turn.state = TurnState::Running;
        }
    }

    /// The prompt returned and its result is being committed/published.
    pub fn on_publishing(&self, channel: Uuid) {
        let Some(session) = self.session(channel) else {
            return;
        };
        let mut turn = lock_session(&session);
        if matches!(
            turn.state,
            TurnState::Running | TurnState::Steering | TurnState::Cancelling
        ) {
            turn.state = TurnState::Publishing;
        }
    }

    /// The turn fully settled. Queued events may now dispatch in FIFO order.
    pub fn on_turn_completed(&self, channel: Uuid) {
        let session = self.session_or_insert(channel);
        let mut turn = lock_session(&session);
        turn.state = TurnState::Queued;
        turn.steering_supported = false;
    }

    /// Current state for tests and diagnostics.
    pub fn state(&self, channel: Uuid) -> Option<TurnState> {
        self.session(channel)
            .map(|session| lock_session(&session).state)
    }

    fn session(&self, channel: Uuid) -> Option<SessionLock> {
        self.sessions_read().get(&channel).cloned()
    }

    fn session_or_insert(&self, channel: Uuid) -> SessionLock {
        if let Some(session) = self.session(channel) {
            return session;
        }
        self.sessions_write()
            .entry(channel)
            .or_insert_with(|| {
                Arc::new(Mutex::new(SessionTurn {
                    state: TurnState::Queued,
                    steering_supported: false,
                }))
            })
            .clone()
    }

    fn sessions_read(&self) -> RwLockReadGuard<'_, HashMap<Uuid, SessionLock>> {
        self.sessions
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn sessions_write(&self) -> RwLockWriteGuard<'_, HashMap<Uuid, SessionLock>> {
        self.sessions
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn lock_session(session: &SessionLock) -> MutexGuard<'_, SessionTurn> {
    session
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    #[test]
    fn capability_is_required_for_steer_claim() {
        let channel = Uuid::new_v4();
        let machine = TurnStateMachine::new();
        machine.on_dispatch(channel, false);
        assert_eq!(
            machine.on_event_arrived(channel, EventIntent::Steer),
            EventDisposition::Queue
        );
        assert_eq!(machine.state(channel), Some(TurnState::Running));
    }

    #[test]
    fn only_one_concurrent_arrival_claims_a_steer() {
        const ARRIVALS: usize = 12;
        let channel = Uuid::new_v4();
        let machine = Arc::new(TurnStateMachine::new());
        machine.on_dispatch(channel, true);
        let barrier = Arc::new(Barrier::new(ARRIVALS));
        let mut threads = Vec::new();
        for _ in 0..ARRIVALS {
            let machine = Arc::clone(&machine);
            let barrier = Arc::clone(&barrier);
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                machine.on_event_arrived(channel, EventIntent::Steer)
            }));
        }
        let claims = threads
            .into_iter()
            .filter_map(|thread| thread.join().ok())
            .filter(|disposition| *disposition == EventDisposition::Steer)
            .count();
        assert_eq!(claims, 1, "one per-session lock permits one steer claim");
        assert_eq!(machine.state(channel), Some(TurnState::Steering));
    }

    #[test]
    fn explicit_cancel_claim_wins_from_running_or_steering() {
        for start_steering in [false, true] {
            let channel = Uuid::new_v4();
            let machine = TurnStateMachine::new();
            machine.on_dispatch(channel, true);
            if start_steering {
                assert_eq!(
                    machine.on_event_arrived(channel, EventIntent::Steer),
                    EventDisposition::Steer
                );
            }
            assert_eq!(
                machine.on_event_arrived(channel, EventIntent::Cancel),
                EventDisposition::Cancel
            );
            assert_eq!(machine.state(channel), Some(TurnState::Cancelling));
        }
    }

    #[test]
    fn queued_cancelling_and_publishing_never_steer() {
        let channel = Uuid::new_v4();
        let machine = TurnStateMachine::new();
        machine.on_event_queued(channel);
        assert_eq!(machine.state(channel), Some(TurnState::Queued));
        assert_eq!(
            machine.on_event_arrived(channel, EventIntent::Steer),
            EventDisposition::Queue
        );

        machine.on_dispatch(channel, true);
        machine.on_cancel_started(channel);
        assert_eq!(machine.state(channel), Some(TurnState::Cancelling));
        assert_eq!(
            machine.on_event_arrived(channel, EventIntent::Steer),
            EventDisposition::Queue
        );

        machine.on_publishing(channel);
        assert_eq!(machine.state(channel), Some(TurnState::Publishing));
        assert_eq!(
            machine.on_event_arrived(channel, EventIntent::Steer),
            EventDisposition::Queue
        );
    }

    #[test]
    fn stale_ack_cannot_resurrect_completed_turn() {
        let channel = Uuid::new_v4();
        let machine = TurnStateMachine::new();
        machine.on_dispatch(channel, true);
        assert_eq!(
            machine.on_event_arrived(channel, EventIntent::Steer),
            EventDisposition::Steer
        );
        machine.on_turn_completed(channel);
        machine.on_steer_resolved(channel);
        assert_eq!(machine.state(channel), Some(TurnState::Queued));
    }
}
