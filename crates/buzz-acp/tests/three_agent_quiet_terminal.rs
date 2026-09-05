use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use buzz_acp::test_api::{
    Admission, AuthorClass, DedupMode, EventQueue, GuardDecision, InboundClassifier, LoopGuard,
    LoopGuardConfig, QueuedEvent,
};
use nostr::{Event, EventBuilder, Keys, Kind, Tag};
use uuid::Uuid;

struct SimAgent {
    classifier: InboundClassifier,
    queue: EventQueue,
    guards: LoopGuard,
    delivered: HashSet<String>,
    replies: usize,
}

fn message(author: &Keys, content: &str, recipients: &[&Keys], thread: Option<&str>) -> Event {
    let mut tags: Vec<Tag> = recipients
        .iter()
        .map(|key| Tag::parse(["p", &key.public_key().to_hex()]).expect("p tag"))
        .collect();
    if let Some(root) = thread {
        tags.push(Tag::parse(["e", root, "", "reply"]).expect("reply tag"));
    }
    EventBuilder::new(Kind::Custom(9), content)
        .tags(tags)
        .sign_with_keys(author)
        .expect("event")
}

fn ingest(
    agent: &mut SimAgent,
    channel: Uuid,
    event: &Event,
    author: AuthorClass,
    received_at: Instant,
) {
    let classified = agent.classifier.classify(event, author);
    if classified.admission == Admission::AttachContext {
        agent.queue.attach_context(QueuedEvent {
            channel_id: channel,
            event: event.clone(),
            received_at,
            prompt_tag: classified.class.to_string(),
        });
        return;
    } else if classified.admission != Admission::Admit {
        return;
    }
    if author == AuthorClass::AgentPeer {
        let thread = event
            .tags
            .iter()
            .find_map(|tag| {
                let fields = tag.as_slice();
                (fields.first().map(String::as_str) == Some("e"))
                    .then(|| fields.get(1).cloned())
                    .flatten()
            })
            .unwrap_or_else(|| event.id.to_hex());
        assert_eq!(
            agent.guards.evaluate_agent_trigger(
                channel,
                &thread,
                &event.pubkey.to_hex(),
                received_at,
            ),
            GuardDecision::Respond
        );
    }
    assert!(agent.queue.push(QueuedEvent {
        channel_id: channel,
        event: event.clone(),
        received_at,
        prompt_tag: classified.class.to_string(),
    }));
}

#[test]
fn scripted_three_agent_conversation_reaches_a_quiet_terminal_state() {
    let owner = Keys::generate();
    let alpha = Keys::generate();
    let beta = Keys::generate();
    let gamma = Keys::generate();
    let channel = Uuid::new_v4();
    let start = Instant::now();
    let window = Duration::from_millis(50);
    let guard_cfg = LoopGuardConfig {
        max_agent_chain: 3,
        pingpong_limit: 2,
        agent_reply_rate: 6,
        ..LoopGuardConfig::default()
    };

    let mut agents: HashMap<String, SimAgent> =
        [(&alpha, "Alpha"), (&beta, "Beta"), (&gamma, "Gamma")]
            .into_iter()
            .map(|(keys, name)| {
                (
                    keys.public_key().to_hex(),
                    SimAgent {
                        classifier: InboundClassifier::new(keys.public_key().to_hex(), [name]),
                        queue: EventQueue::new(DedupMode::Queue).with_coalescing_window(window),
                        guards: LoopGuard::new(guard_cfg),
                        delivered: HashSet::new(),
                        replies: 0,
                    },
                )
            })
            .collect();

    // Simultaneous owner work plus a follow-up inside the pre-work window must
    // become one turn per agent, with both signed IDs preserved.
    let initial = message(
        &owner,
        "Please inspect the incident",
        &[&alpha, &beta, &gamma],
        None,
    );
    let follow_up = message(
        &owner,
        "Also verify the rollback plan",
        &[&alpha, &beta, &gamma],
        None,
    );
    for key in [&alpha, &beta, &gamma] {
        let sim = agents.get_mut(&key.public_key().to_hex()).expect("agent");
        ingest(sim, channel, &initial, AuthorClass::Owner, start);
        ingest(
            sim,
            channel,
            &follow_up,
            AuthorClass::Owner,
            start + Duration::from_millis(20),
        );
        // Duplicate relay delivery is classified silent before queue admission.
        ingest(
            sim,
            channel,
            &follow_up,
            AuthorClass::Owner,
            start + Duration::from_millis(25),
        );
    }

    for sim in agents.values_mut() {
        let batch = sim
            .queue
            .flush_next_at(start + window)
            .expect("coalesced owner turn");
        assert_eq!(batch.events.len(), 2);
        for event in batch.events {
            assert!(sim.delivered.insert(event.event.id.to_hex()));
        }
        sim.replies += 1;
        sim.queue.mark_complete(channel);
    }

    // Crossed, simultaneous explicit assignments cause exactly one further
    // turn at each target. A tagged FYI remains context, and delayed
    // duplicates remain silent.
    let alpha_to_beta = message(
        &alpha,
        "Please check the database timeline",
        &[&beta],
        Some(&initial.id.to_hex()),
    );
    let beta_to_alpha = message(
        &beta,
        "Can you verify the client logs?",
        &[&alpha],
        Some(&initial.id.to_hex()),
    );
    let gamma_fyi = message(
        &gamma,
        "FYI: metrics are stable; no action needed",
        &[&alpha, &beta],
        Some(&initial.id.to_hex()),
    );
    ingest(
        agents.get_mut(&beta.public_key().to_hex()).expect("beta"),
        channel,
        &alpha_to_beta,
        AuthorClass::AgentPeer,
        start + Duration::from_millis(100),
    );
    ingest(
        agents.get_mut(&alpha.public_key().to_hex()).expect("alpha"),
        channel,
        &beta_to_alpha,
        AuthorClass::AgentPeer,
        start + Duration::from_millis(100),
    );
    for key in [&alpha, &beta] {
        ingest(
            agents.get_mut(&key.public_key().to_hex()).expect("target"),
            channel,
            &gamma_fyi,
            AuthorClass::AgentPeer,
            start + Duration::from_millis(110),
        );
    }

    for key in [&alpha, &beta] {
        let sim = agents.get_mut(&key.public_key().to_hex()).expect("target");
        let batch = sim
            .queue
            .flush_next_at(start + Duration::from_millis(150))
            .expect("explicit peer assignment");
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.cancelled_events.len(), 1, "FYI attached as context");
        assert!(sim.delivered.insert(batch.events[0].event.id.to_hex()));
        sim.replies += 1;
        sim.queue.mark_complete(channel);
    }

    ingest(
        agents.get_mut(&beta.public_key().to_hex()).expect("beta"),
        channel,
        &alpha_to_beta,
        AuthorClass::AgentPeer,
        start + Duration::from_secs(30),
    );

    let delayed_assignment = message(
        &gamma,
        "Please verify the final health check",
        &[&beta],
        Some(&initial.id.to_hex()),
    );
    ingest(
        agents.get_mut(&beta.public_key().to_hex()).expect("beta"),
        channel,
        &delayed_assignment,
        AuthorClass::AgentPeer,
        start + Duration::from_secs(30),
    );
    // A delayed eligible delivery still runs once; its replay does not.
    ingest(
        agents.get_mut(&beta.public_key().to_hex()).expect("beta"),
        channel,
        &delayed_assignment,
        AuthorClass::AgentPeer,
        start + Duration::from_secs(31),
    );
    let beta_sim = agents.get_mut(&beta.public_key().to_hex()).expect("beta");
    let delayed_batch = beta_sim
        .queue
        .flush_next_at(start + Duration::from_secs(30) + window)
        .expect("delayed assignment");
    assert_eq!(delayed_batch.events.len(), 1);
    assert!(beta_sim
        .delivered
        .insert(delayed_batch.events[0].event.id.to_hex()));
    beta_sim.replies += 1;
    beta_sim.queue.mark_complete(channel);

    // No response events mention another agent, and FYI/duplicate deliveries
    // did not enqueue work: all three bridges are quiet.
    for sim in agents.values_mut() {
        assert!(sim
            .queue
            .flush_next_at(start + Duration::from_secs(60))
            .is_none());
    }
    assert_eq!(agents[&alpha.public_key().to_hex()].delivered.len(), 3);
    assert_eq!(agents[&beta.public_key().to_hex()].delivered.len(), 4);
    assert_eq!(agents[&gamma.public_key().to_hex()].delivered.len(), 2);
    assert_eq!(agents[&alpha.public_key().to_hex()].replies, 2);
    assert_eq!(agents[&beta.public_key().to_hex()].replies, 3);
    assert_eq!(agents[&gamma.public_key().to_hex()].replies, 1);
}
