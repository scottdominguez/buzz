use buzz_acp::test_api::{
    Admission, AuthorClass, InboundClass, InboundClassifier, RecipientEvidence,
};
use nostr::{Event, EventBuilder, Keys, Kind, Tag, ToBech32};

fn event(author: &Keys, content: &str, tags: Vec<Tag>) -> Event {
    EventBuilder::new(Kind::Custom(9), content)
        .tags(tags)
        .sign_with_keys(author)
        .expect("signed event")
}

fn p(hex: &str) -> Tag {
    Tag::parse(["p", hex]).expect("p tag")
}

fn mention(hex: &str) -> Tag {
    Tag::parse(["mention", hex]).expect("mention tag")
}

fn reply(root: &str) -> Tag {
    Tag::parse(["e", root, "", "reply"]).expect("reply tag")
}

#[test]
fn classification_matrix_prefers_recipient_metadata() {
    let agent = Keys::generate();
    let owner = Keys::generate();
    let peer = Keys::generate();
    let other = Keys::generate();
    let agent_hex = agent.public_key().to_hex();
    let mut classifier = InboundClassifier::new(agent_hex.clone(), ["Vector"]);

    let cases = [
        (
            event(&owner, "!cancel", vec![p(&agent_hex)]),
            AuthorClass::Owner,
            InboundClass::OwnerCommand,
            Admission::Admit,
            RecipientEvidence::Metadata,
        ),
        (
            event(
                &owner,
                "@Vector please continue with the review",
                vec![p(&agent_hex)],
            ),
            AuthorClass::Owner,
            InboundClass::OwnerCommand,
            Admission::Admit,
            RecipientEvidence::Metadata,
        ),
        (
            event(&peer, "Please review the patch", vec![p(&agent_hex)]),
            AuthorClass::AgentPeer,
            InboundClass::ExplicitPeerTaskOrQuestion,
            Admission::Admit,
            RecipientEvidence::Metadata,
        ),
        (
            event(
                &peer,
                "Can you confirm the result?",
                vec![mention(&agent_hex)],
            ),
            AuthorClass::AgentPeer,
            InboundClass::ExplicitPeerTaskOrQuestion,
            Admission::Admit,
            RecipientEvidence::Metadata,
        ),
        (
            event(
                &peer,
                "Vector, also verify the rollback",
                vec![p(&agent_hex)],
            ),
            AuthorClass::AgentPeer,
            InboundClass::ExplicitPeerTaskOrQuestion,
            Admission::Admit,
            RecipientEvidence::Metadata,
        ),
        (
            event(
                &peer,
                "FYI: CI is green; no action needed",
                vec![p(&agent_hex)],
            ),
            AuthorClass::AgentPeer,
            InboundClass::PeerContextOrFyi,
            Admission::AttachContext,
            RecipientEvidence::Metadata,
        ),
        (
            event(&other, "The deploy completed", vec![reply(&"1".repeat(64))]),
            AuthorClass::Other,
            InboundClass::OrdinaryThreadReply,
            Admission::Ignore,
            RecipientEvidence::None,
        ),
    ];

    for (event, author, expected_class, expected_admission, expected_recipient) in cases {
        let result = classifier.classify(&event, author);
        assert_eq!(result.class, expected_class, "content={}", event.content);
        assert_eq!(
            result.admission, expected_admission,
            "content={}",
            event.content
        );
        assert_eq!(
            result.recipient, expected_recipient,
            "content={}",
            event.content
        );
    }

    // A p-tag explicitly naming somebody else is authoritative. The plain-text
    // @Vector token must not override signed recipient metadata.
    let metadata_for_other = event(
        &peer,
        "@Vector please review this",
        vec![p(&other.public_key().to_hex())],
    );
    let result = classifier.classify(&metadata_for_other, AuthorClass::AgentPeer);
    assert_eq!(result.class, InboundClass::PeerContextOrFyi);
    assert_eq!(result.admission, Admission::AttachContext);
    assert_eq!(result.recipient, RecipientEvidence::MetadataForOther);
}

#[test]
fn textual_recipient_is_a_legacy_fallback_only_without_p_tags() {
    let agent = Keys::generate();
    let peer = Keys::generate();
    let agent_hex = agent.public_key().to_hex();
    let agent_npub = agent.public_key().to_bech32().expect("npub");
    let mut classifier = InboundClassifier::new(agent_hex, ["Vector"]);

    for content in [
        "@Vector can you investigate the timeout?",
        &format!("nostr:{agent_npub} please run the focused tests"),
    ] {
        let result = classifier.classify(&event(&peer, content, vec![]), AuthorClass::AgentPeer);
        assert_eq!(result.class, InboundClass::ExplicitPeerTaskOrQuestion);
        assert_eq!(result.admission, Admission::Admit);
        assert_eq!(result.recipient, RecipientEvidence::TextFallback);
    }
}

#[test]
fn uncertain_events_preserve_pre_lane_admission_and_duplicates_are_silent() {
    let agent = Keys::generate();
    let peer = Keys::generate();
    let agent_hex = agent.public_key().to_hex();
    let mut classifier = InboundClassifier::new(agent_hex.clone(), ["Vector"]);

    let ambiguous = event(&peer, "@Vector hello there", vec![p(&agent_hex)]);
    let first = classifier.classify(&ambiguous, AuthorClass::Other);
    assert_eq!(first.class, InboundClass::LegacyFallback);
    assert_eq!(first.admission, Admission::Admit);

    let duplicate = classifier.classify(&ambiguous, AuthorClass::Other);
    assert_eq!(duplicate.class, InboundClass::SelfAuthoredOrDuplicate);
    assert_eq!(duplicate.admission, Admission::Ignore);

    let authored_by_self = event(&agent, "Please review", vec![p(&agent_hex)]);
    let own = classifier.classify(&authored_by_self, AuthorClass::SelfAuthored);
    assert_eq!(own.class, InboundClass::SelfAuthoredOrDuplicate);
    assert_eq!(own.admission, Admission::Ignore);
}
