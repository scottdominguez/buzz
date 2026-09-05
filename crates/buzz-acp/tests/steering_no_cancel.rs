//! Integration test: capability-aware true active-turn steering in buzz-acp.
//!
//! Drives the real ACP wire protocol against a fake agent subprocess and the
//! real pool/queue/read-loop machinery:
//!
//! 1. The initial prompt is active.
//! 2. A second eligible event arrives.
//! 3. The harness sends a steering prompt to the SAME ACP session with NO
//!    `session/cancel`.
//! 4. The original prompt stays alive and incorporates the steering text.
//! 5. Both Buzz event IDs are acknowledged exactly once.
//!
//! A second test proves the no-capability fallback: when the agent does not
//! advertise `_meta.steering.supported` (and has no goose run id), a second
//! event enters the ordered queue — zero `session/cancel`, zero cancel-and-
//! re-prompt.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use buzz_acp::test_api::*;
use nostr::{EventBuilder, Keys, Kind};
use uuid::Uuid;

const CHANNEL_SESSION_ID: &str = "live-session";

// ── fake agent ───────────────────────────────────────────────────────────────

/// Build a `bash` script that speaks enough ACP to exercise the steer path:
///
/// - `initialize` advertises `_meta.steering.supported` only when `capable`.
/// - `session/new` returns `live-session`.
/// - `session/prompt` emits a tool_call update (keeps the turn alive) and
///   HOLDs the response.
/// - `_session/steering` emits an `agent_message_chunk` (the steer text is
///   "incorporated" into the still-open turn), answers `{outcome:"injected"}`,
///   then completes the held prompt.
/// - `session/cancel` writes a `CANCEL_OBSERVED` marker to the wire log — the
///   tests assert this never happens.
///
/// Every received line is appended to `wire_log` so the test can assert exact
/// wire behavior. When `respond_file` appears, a held prompt is completed
/// (used by the no-capability test to end the turn after the steer attempt was
/// rejected).
fn fake_agent_script(wire_log: &Path, capable: bool, respond_file: &Path) -> String {
    let wire = wire_log.to_string_lossy().replace('\'', "'\\''");
    let respond = respond_file.to_string_lossy().replace('\'', "'\\''");
    // Bash variable holding the optional `_meta` capability fragment. The `$id`
    // must NOT live inside a variable (bash does not re-expand variables in
    // variable values) — it is written directly in the printf format string.
    let meta_fragment = if capable {
        ",\"_meta\":{\"steering\":{\"supported\":true}}"
    } else {
        ""
    };
    format!(
        r#"#!/bin/bash
wire='{wire}'
respond='{respond}'
meta='{meta_fragment}'
prompt_id=""
out() {{
  printf '%s\n' "$1"
  printf '%s\n' "$1" >> "$wire"
}}
while true; do
  if IFS= read -r -t 0.2 line; then
    printf '%s\n' "$line" >> "$wire"
    id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
    method=$(printf '%s' "$line" | sed -n 's/.*"method":"\([^"]*\)".*/\1/p')
    case "$method" in
      initialize)
        out "{{\"jsonrpc\":\"2.0\",\"id\":$id,\"result\":{{\"protocolVersion\":1,\"agentName\":\"fake-steering-agent\"$meta}}}}"
        ;;
      session/new)
        out "{{\"jsonrpc\":\"2.0\",\"id\":$id,\"result\":{{\"sessionId\":\"live-session\"}}}}"
        ;;
      session/prompt)
        prompt_id="$id"
        out '{{"jsonrpc":"2.0","method":"session/update","params":{{"sessionId":"live-session","update":{{"sessionUpdate":"tool_call","toolCallId":"t1","title":"get_messages","kind":"execute"}}}}}}'
        ;;
      _session/steering)
        out '{{"jsonrpc":"2.0","method":"session/update","params":{{"sessionId":"live-session","update":{{"sessionUpdate":"agent_message_chunk","content":{{"text":"STEER_INTO_TURN"}}}}}}}}'
        out "{{\"jsonrpc\":\"2.0\",\"id\":$id,\"result\":{{\"outcome\":\"injected\"}}}}"
        if [ -n "$prompt_id" ]; then
          out "{{\"jsonrpc\":\"2.0\",\"id\":$prompt_id,\"result\":{{\"stopReason\":\"end_turn\"}}}}"
          prompt_id=""
        fi
        ;;
      session/cancel)
        printf '%s\n' "CANCEL_OBSERVED $line" >> "$wire"
        ;;
      *)
        if [ -n "$id" ]; then
          out "{{\"jsonrpc\":\"2.0\",\"id\":$id,\"error\":{{\"code\":-32601,\"message\":\"Method not found: $method\"}}}}"
        fi
        ;;
    esac
  else
    if [ -n "$prompt_id" ] && [ -f "$respond" ]; then
      out "{{\"jsonrpc\":\"2.0\",\"id\":$prompt_id,\"result\":{{\"stopReason\":\"end_turn\"}}}}"
      exit 0
    fi
    if [ -n "$prompt_id" ]; then
      out '{{"jsonrpc":"2.0","method":"session/update","params":{{"sessionId":"live-session","update":{{"sessionUpdate":"tool_call_update","toolCallId":"t1","status":"running"}}}}}}'
    fi
  fi
done
"#,
        meta_fragment = meta_fragment,
    )
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn unique_temp(prefix: &str, suffix: &str) -> PathBuf {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "buzz-acp-steering-{prefix}-{}-{suffix}-{n}",
        std::process::id()
    ))
}

fn make_event(content: &str) -> nostr::Event {
    let keys = Keys::generate();
    EventBuilder::new(Kind::Custom(9), content)
        .tags([])
        .sign_with_keys(&keys)
        .unwrap()
}

/// Start a minimal HTTP server that answers every request with `[]` so the
/// pool's context fetches (channel info, canvas, profile lookup) complete
/// fast instead of hanging against a dead port until the 3s timeout.
/// Returns `(base_url, abort_handle)`.
async fn start_fake_rest_server() -> (String, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake REST server");
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = socket.read(&mut buf).await;
                let body = "[]";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });
    (base_url, handle)
}

fn make_context(keys: Keys, base_url: String) -> PromptContext {
    let rest = RestClient {
        http: reqwest::Client::new(),
        base_url,
        keys: keys.clone(),
        auth_tag_json: None,
    };
    PromptContext {
        mcp_servers: vec![],
        initial_message: None,
        idle_timeout: Duration::from_secs(60),
        max_turn_duration: Duration::from_secs(120),
        turn_liveness_interval: Duration::ZERO,
        dedup_mode: DedupMode::Queue,
        require_reply: false,
        system_prompt: None,
        session_title: None,
        team_instructions: None,
        heartbeat_prompt: None,
        base_prompt: None,
        cwd: ".".to_string(),
        rest_client: rest.clone(),
        channel_info: ChannelInfoResolver::new(std::collections::HashMap::new(), rest.clone()),
        member_resolver: MemberResolver::new(rest),
        context_message_limit: 0,
        max_turns_per_session: 0,
        permission_mode: PermissionMode::Default,
        agent_keys: keys,
        agent_owner_pubkey: None,
        memory_enabled: false,
        harness_name: "fake-steering-agent".to_string(),
        relay_url: "ws://127.0.0.1:3000".to_string(),
    }
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();
}

async fn spawn_agent(wire_log: &Path, capable: bool, respond_file: &Path) -> AcpClient {
    let script = fake_agent_script(wire_log, capable, respond_file);
    let mut acp = AcpClient::spawn("bash", &["-c".into(), script], &[], false)
        .await
        .expect("spawn fake steering agent");
    acp.initialize().await.expect("initialize fake agent");
    if capable {
        assert!(
            acp.steering_supported(),
            "fake agent must advertise the steering capability"
        );
    } else {
        assert!(
            !acp.steering_supported(),
            "no-capability fake agent must NOT advertise steering support"
        );
    }
    acp
}

/// Poll `wire_log` until `marker` appears; return the full log content.
async fn wait_for_wire(wire_log: &Path, marker: &str, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(contents) = std::fs::read_to_string(wire_log) {
            if contents.contains(marker) {
                return contents;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {marker:?} in {}",
            wire_log.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Dispatch the first event and start a real prompt task against the fake
/// agent. Returns `(pool, queue, event1_id, task_id, channel)`.
async fn start_first_turn(
    wire_log: &Path,
    respond_file: &Path,
    capable: bool,
) -> (AgentPool, EventQueue, String, tokio::task::Id, Uuid) {
    let channel = Uuid::new_v4();
    let agent = OwnedAgent {
        index: 0,
        acp: spawn_agent(wire_log, capable, respond_file).await,
        state: SessionState::default(),
        model_capabilities: None,
        desired_model: None,
        model_overridden: false,
        desired_model_request_id: None,
        desired_model_pending_ack: false,
        startup_effort: None,
        agent_name: "fake-steering-agent".into(),
        goose_system_prompt_supported: None,
        protocol_version: 1,
    };

    let (base_url, _rest_server) = start_fake_rest_server().await;
    let ctx = Arc::new(make_context(Keys::generate(), base_url));

    let mut queue = EventQueue::new(DedupMode::Queue);
    let event1 = make_event("original-request-unique");
    let event1_id = event1.id.to_hex();
    queue.push(QueuedEvent {
        channel_id: channel,
        event: event1,
        received_at: Instant::now(),
        prompt_tag: "mention".into(),
    });
    let batch = queue.flush_next().expect("first event flushes immediately");
    queue.mark_running(channel, agent.acp.steering_supported());
    assert_eq!(
        queue.turn_state(channel),
        Some(TurnState::Running),
        "a freshly dispatched prompt must be running"
    );

    let mut pool = AgentPool::from_slots(vec![Some(agent)]);
    let result_tx = pool.result_tx();
    let (control_tx, control_rx) = tokio::sync::oneshot::channel::<ControlSignal>();
    let (steer_tx, steer_rx) = tokio::sync::mpsc::channel::<SteerRequest>(1);

    // Install the steer receiver on the client BEFORE the agent moves into the
    // spawned task; the sender is recorded in TaskMeta for `pool.send_steer`.
    let mut claimed = pool.try_claim(Some(channel)).expect("idle agent");
    claimed.acp.install_steer_rx(steer_rx);

    let ctx_clone = Arc::clone(&ctx);
    let handle = pool.join_set.spawn(async move {
        run_prompt_task(
            claimed,
            Some(batch),
            None,
            ctx_clone,
            result_tx,
            Some(control_rx),
            "steering-test-turn".into(),
        )
        .await;
    });
    let task_id = handle.id();
    pool.task_map_mut().insert(
        task_id,
        TaskMeta {
            agent_index: 0,
            channel_id: Some(channel),
            turn_id: "steering-test-turn".into(),
            recoverable_batch: None,
            control_tx: Some(control_tx),
            steer_tx: Some(steer_tx),
            successful_steer_deliveries: Default::default(),
        },
    );

    // The initial prompt must be active before the second event is considered.
    wait_for_wire(wire_log, "session/prompt", Duration::from_secs(10)).await;
    (pool, queue, event1_id, task_id, channel)
}

/// Block until the prompt task posts a result, polling the pool's result
/// channel without holding a long-lived borrow (so `&mut pool` stays usable for
/// `send_steer` / `task_map_mut` between polls).
async fn await_prompt_result(pool: &mut AgentPool, timeout: Duration) -> PromptResult {
    let deadline = Instant::now() + timeout;
    loop {
        match pool.result_rx_try_recv() {
            Ok(result) => return result,
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for the prompt result"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                panic!("result channel closed while awaiting the prompt result")
            }
        }
    }
}

// ── tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn steering_capable_agent_gets_true_non_cancelling_steer() {
    init_tracing();
    let wire_log = unique_temp("capable", "wire");
    let respond_file = unique_temp("capable", "respond");
    let (mut pool, mut queue, event1_id, task_id, channel) =
        start_first_turn(&wire_log, &respond_file, true).await;

    // A second eligible event arrives while the initial prompt is active.
    let event2 = make_event("steer-target-unique");
    let event2_id = event2.id.to_hex();
    assert!(queue.push(QueuedEvent {
        channel_id: channel,
        event: event2,
        received_at: Instant::now(),
        prompt_tag: "mention".into(),
    }));
    assert_eq!(
        queue.route_second_event(channel, EventIntent::Steer),
        EventDisposition::Steer,
        "an advertised, running session must atomically claim a steer"
    );
    assert_eq!(queue.turn_state(channel), Some(TurnState::Steering));

    // Send a steering prompt into the SAME live ACP session — no session/cancel.
    let body = "[New message — arrived while you were working]\n\n[Buzz event: mention]\n\
                original-request-unique not involved here — new content: steer-target-unique"
        .to_string();
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel::<SteerAck>();
    assert!(
        queue.mark_native_steer_pending(channel, &event2_id),
        "the event must be withheld before its steer can reach the wire"
    );
    pool.send_steer(
        channel,
        SteerRequest {
            prompt_blocks: vec![body],
            ack_tx,
        },
    )
    .expect("read loop accepts the steer request");
    assert_eq!(
        queue.route_second_event(channel, EventIntent::Steer),
        EventDisposition::Queue,
        "a second event during an in-flight steer must wait in the ordered queue"
    );

    // The steer is delivered to the live turn: outcome `injected`.
    let ack = tokio::time::timeout(Duration::from_secs(10), ack_rx)
        .await
        .expect("steer ack must arrive")
        .expect("steer ack channel must not drop");
    let SteerAck::Success { session_id } = ack else {
        panic!("steer must succeed for a capable agent, got: {ack:?}");
    };
    assert_eq!(
        session_id, CHANNEL_SESSION_ID,
        "steer targets the SAME session"
    );

    // Mirror the main loop's `PoolEvent::SteerAck::Success` arm: record the
    // successful delivery and drop the withheld event so it is never
    // redelivered via normal dispatch.
    assert!(pool.record_successful_steer(
        channel,
        event2_id.clone(),
        CHANNEL_SESSION_ID.to_string(),
    ));
    queue.remove_event(channel, &event2_id);
    assert_eq!(
        queue.turn_state(channel),
        Some(TurnState::Running),
        "after the steer resolves the active turn is running again"
    );

    // The original prompt stays alive (no cancel) and completes, incorporating
    // the steering text.
    let mut result = await_prompt_result(&mut pool, Duration::from_secs(15)).await;
    assert!(
        matches!(&result.outcome, PromptOutcome::Ok(_)),
        "the original turn must complete normally"
    );

    // Merge the staged successful steer delivery into the returned agent's
    // ledger (mirrors `handle_prompt_result`).
    let staged = pool
        .task_map_mut()
        .remove(&task_id)
        .expect("task meta present")
        .successful_steer_deliveries;
    let delivered = result
        .agent
        .state
        .deliveries
        .entry(channel)
        .or_default()
        .delivered_event_ids
        .clone();
    let mut all = delivered;
    for d in &staged {
        if d.session_id == CHANNEL_SESSION_ID {
            all.insert(d.event_id.clone());
        }
    }

    let wire = wait_for_wire(&wire_log, "end_turn", Duration::from_secs(10)).await;

    // Zero session/cancel on this path.
    assert!(
        !wire.contains("session/cancel"),
        "ordinary peer steering must never cancel the turn, wire:\n{wire}"
    );

    // The steering prompt went to the SAME ACP session as the original prompt.
    let steer_line = wire
        .lines()
        .find(|l| l.contains("_session/steering"))
        .expect("steer request must appear on the wire");
    assert!(
        steer_line.contains("\"sessionId\":\"live-session\""),
        "steer must target the live session: {steer_line}"
    );
    assert!(
        steer_line.contains("steer-target-unique"),
        "the steering text must reach the agent: {steer_line}"
    );

    // The original prompt was already active when the steer arrived (steer line
    // after the session/prompt line) and stayed alive through it (STEER_INTO_TURN
    // chunk emitted by the live turn before the prompt response).
    let prompt_pos = wire.find("session/prompt").expect("prompt on wire");
    let steer_pos = wire.find("_session/steering").expect("steer on wire");
    let chunk_pos = wire.find("STEER_INTO_TURN").expect("steer incorporation");
    let response_pos = wire
        .rfind("\"stopReason\":\"end_turn\"")
        .expect("prompt response");
    assert!(
        prompt_pos < steer_pos && steer_pos < chunk_pos && chunk_pos < response_pos,
        "wire order must be: prompt → steer → incorporation → prompt response, wire:\n{wire}"
    );

    // Exactly one prompt and one steer were sent.
    assert_eq!(
        wire.matches("session/prompt").count(),
        1,
        "exactly one original prompt, wire:\n{wire}"
    );
    assert_eq!(
        wire.matches("_session/steering").count(),
        1,
        "exactly one steer, wire:\n{wire}"
    );

    // Both Buzz event IDs acknowledged exactly once.
    assert!(
        all.contains(&event1_id) && all.contains(&event2_id),
        "both event IDs must be acked, have: {all:?}"
    );
    assert_eq!(
        all.len(),
        2,
        "exactly two distinct event IDs acknowledged, have: {all:?}"
    );
    for event_id in [&event1_id, &event2_id] {
        assert_eq!(
            all.iter().filter(|acked| *acked == event_id).count(),
            1,
            "Buzz event {event_id} must appear in the acknowledgement ledger exactly once"
        );
    }

    // Nothing is left in the queue for redelivery — the steered event was
    // consumed, not double-delivered.
    assert!(
        !queue.has_undispatched_work(),
        "after a successful steer the queue must hold no undispatched work"
    );
    queue.mark_complete(channel);
    assert!(
        queue.flush_next().is_none(),
        "no second dispatch: the steered event was delivered exactly once"
    );

    result.agent.acp.shutdown().await;
    let _ = std::fs::remove_file(&wire_log);
    let _ = std::fs::remove_file(&respond_file);
}

#[tokio::test]
async fn no_capability_agent_falls_back_to_ordered_queue_without_cancel() {
    init_tracing();
    let wire_log = unique_temp("nocap", "wire");
    let respond_file = unique_temp("nocap", "respond");
    let (mut pool, mut queue, event1_id, task_id, channel) =
        start_first_turn(&wire_log, &respond_file, false).await;

    // A second eligible event arrives while the turn is active.
    let event2 = make_event("queued-target-unique");
    let event2_id = event2.id.to_hex();
    assert!(queue.push(QueuedEvent {
        channel_id: channel,
        event: event2.clone(),
        received_at: Instant::now(),
        prompt_tag: "mention".into(),
    }));
    assert_eq!(
        queue.route_second_event(channel, EventIntent::Steer),
        EventDisposition::Queue,
        "without _meta.steering.supported the state machine must choose FIFO queue immediately"
    );
    assert_eq!(
        queue.turn_state(channel),
        Some(TurnState::Running),
        "queue fallback leaves the original turn alive"
    );

    // No steering method was ever written to the wire, and no cancel either.
    let wire_so_far = std::fs::read_to_string(&wire_log).unwrap_or_default();
    assert!(
        !wire_so_far.contains("session/cancel"),
        "no-capability fallback must never cancel, wire:\n{wire_so_far}"
    );
    assert!(
        !wire_so_far.contains("_session/steering")
            && !wire_so_far.contains("_goose/unstable/session/steer"),
        "no steer transport may be written or even probed without the capability, wire:\n{wire_so_far}"
    );

    // The turn completes WITHOUT the second event (it was not injected).
    std::fs::write(&respond_file, "done").expect("signal the fake agent to finish");
    let mut result = await_prompt_result(&mut pool, Duration::from_secs(15)).await;
    assert!(
        matches!(&result.outcome, PromptOutcome::Ok(_)),
        "the original turn completes normally"
    );
    let wire_full = wait_for_wire(&wire_log, "end_turn", Duration::from_secs(10)).await;
    assert!(
        !wire_full.contains("session/cancel"),
        "zero session/cancel calls across the whole flow, wire:\n{wire_full}"
    );
    assert_eq!(
        wire_full.matches("session/prompt").count(),
        1,
        "the second event must NOT be re-prompted mid-turn, wire:\n{wire_full}"
    );
    assert!(
        !wire_full.contains("queued-target-unique"),
        "the second event's content must not reach the active turn, wire:\n{wire_full}"
    );

    // Only event1 was delivered to this turn.
    let delivered = result
        .agent
        .state
        .deliveries
        .entry(channel)
        .or_default()
        .delivered_event_ids
        .clone();
    assert!(
        delivered.contains(&event1_id) && !delivered.contains(&event2_id),
        "only the original event is acked so far, have: {delivered:?}"
    );
    assert_eq!(delivered.len(), 1, "exactly one acked event on turn one");

    // The second event waits in the ORDERED QUEUE and is delivered by the next
    // dispatch after the turn completes — the coalesce path, never cancel+merge.
    pool.task_map_mut().remove(&task_id);
    queue.mark_complete(channel);
    let next_batch = queue
        .flush_next()
        .expect("queued second event flushes next");
    assert_eq!(next_batch.channel_id, channel);
    assert_eq!(next_batch.events.len(), 1);
    assert_eq!(
        next_batch.events[0].event.id.to_hex(),
        event2_id,
        "the second event is delivered after the turn, in order"
    );

    result.agent.acp.shutdown().await;
    let _ = std::fs::remove_file(&wire_log);
    let _ = std::fs::remove_file(&respond_file);
}
