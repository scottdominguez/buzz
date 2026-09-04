//! End-to-end regression for `BUZZ_ACP_REQUIRE_REPLY` reply enforcement.
//!
//! The `buzz-acp` library modules are private, so this integration target
//! compiles the relevant source modules directly (the same `#[path]` pattern
//! as `tests/pool_lifecycle_state.rs`) and drives the real `run_prompt_task`
//! pool flow against a mock ACP agent subprocess and a mock relay HTTP bridge.
//!
//! The regression reproduces the observed production failure mode:
//!   1. ACP completes a turn with assistant text but no `buzz_reply` tool use.
//!   2. The harness's deterministic reply publish (POST /events) fails
//!      (nonzero / timeout) and the immediate readback (POST /query) finds no
//!      exact signed kind-9 reply linked to the triggering event.
//!   3. Exactly one bounded delivery-only recovery turn runs.
//!   4. The turn terminates in a visible `ReplyDeliveryFailed` outcome with the
//!      batch preserved — the completed operational work is never replayed.

#![allow(dead_code)]

#[path = "../src/acp.rs"]
mod acp;
#[path = "../src/config.rs"]
mod config;
#[path = "../src/engram_fetch.rs"]
mod engram_fetch;
#[path = "../src/filter.rs"]
mod filter;
#[path = "../src/observer.rs"]
mod observer;
#[path = "../src/pool.rs"]
mod pool;
#[path = "../src/queue.rs"]
mod queue;
#[path = "../src/relay.rs"]
mod relay;
#[path = "../src/usage.rs"]
mod usage;

use std::io::{Read, Write};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use acp::AcpClient;
use pool::{
    run_prompt_task, ChannelDeliveryState, ChannelInfoResolver, OwnedAgent, PromptContext,
    PromptOutcome, PromptSource, SessionState,
};
use queue::{BatchEvent, FlushBatch};
use relay::{ChannelInfo, RestClient};
use uuid::Uuid;

/// Spin up a mock relay HTTP bridge on a background thread.
///
/// - `POST /query` → `[]` (the exact signed reply event never appears).
/// - `POST /events` → HTTP 500 (reply send returns nonzero, like the observed
///   production failure where the agent's reply MCP was misconfigured).
fn spawn_mock_relay() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind mock relay");
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = match stream {
                Ok(s) => s,
                Err(_) => continue,
            };
            let mut buf = vec![0; 64 * 1024];
            let n = match stream.read(&mut buf) {
                Ok(0) => continue,
                Ok(n) => n,
                Err(_) => continue,
            };
            let request = String::from_utf8_lossy(&buf[..n]);
            let (status_line, body) = if request.starts_with("POST /events") {
                (
                    "HTTP/1.1 500 Internal Server Error",
                    b"{\"ok\":false}".to_vec(),
                )
            } else {
                ("HTTP/1.1 200 OK", b"[]".to_vec())
            };
            let mut out = format!(
                "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .into_bytes();
            out.extend_from_slice(&body);
            let _ = stream.write_all(&out);
        }
    });
    base_url
}

/// Spawn a mock ACP agent subprocess that:
/// - appends every inbound request line to `capture_path`,
/// - on the FIRST prompt emits an `agent_message_chunk` (assistant text) and
///   then `end_turn` — the completed operational work with no `buzz_reply`
///   tool invocation,
/// - on every later prompt (the single delivery-only recovery) returns
///   `end_turn` silently.
async fn spawn_mock_agent(capture_path: &std::path::Path) -> AcpClient {
    let quoted = capture_path.to_string_lossy().replace('\'', "'\\''");
    let script = format!(
        r#"count=0
while IFS= read -r line; do
  printf '%s\n' "$line" >> '{quoted}'
  if [ "$count" -eq 0 ]; then
    printf '%s\n' '{{"jsonrpc":"2.0","method":"session/update","params":{{"update":{{"sessionUpdate":"agent_message_chunk","content":{{"text":"the completed operational result"}}}}}}}}'
  fi
  printf '%s\n' "{{\"jsonrpc\":\"2.0\",\"id\":$count,\"result\":{{\"stopReason\":\"end_turn\"}}}}"
  count=$((count + 1))
done"#
    );
    AcpClient::spawn("bash", &["-c".into(), script], &[], false)
        .await
        .expect("spawn mock ACP agent")
}

#[tokio::test]
async fn reply_send_failure_never_requeues_and_recovers_exactly_once() {
    let capture = std::env::temp_dir().join(format!(
        "buzz-acp-reply-enforcement-wire-{}.ndjson",
        Uuid::new_v4()
    ));

    let agent_keys = nostr::Keys::generate();
    let channel_id = Uuid::new_v4();
    let base_url = spawn_mock_relay();

    // The triggering event — a signed kind-9 stream message in the channel.
    let trigger = nostr::EventBuilder::new(nostr::Kind::Custom(9), "trigger the agent")
        .tags([nostr::Tag::parse(["h", &channel_id.to_string()]).expect("h tag")])
        .sign_with_keys(&agent_keys)
        .expect("sign trigger");
    let trigger_event_id = trigger.id.to_hex();

    let batch = FlushBatch {
        channel_id,
        events: vec![BatchEvent {
            event: trigger,
            prompt_tag: "test".into(),
            received_at: std::time::Instant::now(),
        }],
        cancelled_events: vec![],
        cancel_reason: None,
    };

    let rest_client = RestClient {
        http: reqwest::Client::new(),
        base_url: base_url.clone(),
        keys: agent_keys.clone(),
        auth_tag_json: None,
    };
    let ctx = Arc::new(PromptContext {
        mcp_servers: vec![],
        initial_message: None,
        idle_timeout: Duration::from_secs(60),
        max_turn_duration: Duration::from_secs(120),
        turn_liveness_interval: Duration::ZERO,
        dedup_mode: config::DedupMode::Drop,
        require_reply: true,
        system_prompt: None,
        session_title: None,
        team_instructions: None,
        heartbeat_prompt: None,
        base_prompt: None,
        cwd: ".".to_string(),
        rest_client: rest_client.clone(),
        channel_info: ChannelInfoResolver::new(
            std::collections::HashMap::from([(
                channel_id,
                ChannelInfo {
                    name: "test-channel".into(),
                    channel_type: "group".into(),
                    description: None,
                },
            )]),
            rest_client,
        ),
        context_message_limit: 0,
        max_turns_per_session: 0,
        permission_mode: config::PermissionMode::Default,
        agent_keys,
        agent_owner_pubkey: None,
        memory_enabled: false,
        harness_name: "goose".to_string(),
        relay_url: "ws://127.0.0.1:3000".to_string(),
    });

    let acp = spawn_mock_agent(&capture).await;
    let mut agent = OwnedAgent {
        index: 0,
        acp,
        state: SessionState::default(),
        model_capabilities: None,
        desired_model: None,
        model_overridden: false,
        desired_model_request_id: None,
        desired_model_pending_ack: false,
        startup_effort: None,
        agent_name: "reply-enforcement-test-agent".into(),
        goose_system_prompt_supported: None,
        protocol_version: 2,
    };
    agent
        .state
        .sessions
        .insert(channel_id, "live-session".into());
    agent
        .state
        .deliveries
        .insert(channel_id, ChannelDeliveryState::default());

    let (result_tx, mut result_rx) = mpsc::unbounded_channel();
    run_prompt_task(
        agent,
        Some(batch),
        None,
        Arc::clone(&ctx),
        result_tx,
        None,
        "reply-enforcement-turn".into(),
    )
    .await;
    let result = result_rx.recv().await.expect("prompt result");

    // The turn must be marked failed — terminal and visible — with the batch
    // preserved so the queue layer can never requeue (replay) completed work.
    let (recovery_attempts, notice_published, reason) = match result.outcome {
        PromptOutcome::ReplyDeliveryFailed {
            trigger_event_id,
            recovery_attempts,
            notice_published,
            reason,
        } => {
            assert_eq!(
                trigger_event_id, trigger_event_id,
                "failure must name the exact triggering event"
            );
            (recovery_attempts, notice_published, reason)
        }
        other => panic!(
            "expected ReplyDeliveryFailed, got {}",
            outcome_label_for(&other)
        ),
    };
    assert_eq!(
        recovery_attempts, 1,
        "recovery must be bounded to exactly one attempt"
    );
    assert!(
        !notice_published,
        "reply send failed, so no visible notice was published"
    );
    assert!(
        reason.contains("recovery completed without a matching signed kind-9 reply"),
        "failure reason must record the unproven recovery: {reason}"
    );
    assert!(
        matches!(result.source, PromptSource::Channel(cid) if cid == channel_id),
        "failure must be scoped to the triggering channel"
    );
    let returned_batch = result
        .batch
        .expect("batch must be preserved so it is never requeued or dropped");
    assert_eq!(returned_batch.channel_id, channel_id);
    let delivered = result
        .agent
        .state
        .deliveries
        .get(&channel_id)
        .is_some_and(|d| d.delivered_event_ids.contains(&trigger_event_id));
    assert!(
        !delivered,
        "the failed turn must not mark the triggering event as delivered"
    );

    // Exactly two ACP prompts hit the wire: the original operational turn and
    // the single delivery-only recovery. The original work is never replayed.
    let wire = std::fs::read_to_string(&capture).expect("read mock agent capture");
    let lines: Vec<&str> = wire.lines().collect();
    assert_eq!(
        lines.len(),
        2,
        "expected exactly original turn + one recovery turn, got: {wire}"
    );
    assert!(
        !lines[0].contains("DELIVERY RECOVERY ONLY"),
        "first prompt must be the original operational turn"
    );
    assert!(
        lines[1].contains("DELIVERY RECOVERY ONLY"),
        "second prompt must be the delivery-only recovery: {}",
        lines[1]
    );
    assert!(
        lines[1].contains("Do not repeat the operational work"),
        "recovery prompt must forbid repeating completed work: {}",
        lines[1]
    );
    assert!(
        lines[1].contains(&trigger_event_id),
        "recovery prompt must name the exact triggering event"
    );

    let _ = std::fs::remove_file(&capture);
}

fn outcome_label_for(outcome: &PromptOutcome) -> &'static str {
    match outcome {
        PromptOutcome::Ok(_) => "Ok",
        PromptOutcome::Error(_) => "Error",
        PromptOutcome::AgentExited => "AgentExited",
        PromptOutcome::Timeout(_) => "Timeout",
        PromptOutcome::ReplyDeliveryFailed { .. } => "ReplyDeliveryFailed",
        PromptOutcome::Cancelled => "Cancelled",
        PromptOutcome::CancelDrainTimeout(_) => "CancelDrainTimeout",
    }
}
