//! Metadata-first classification for inbound channel events.
//!
//! Subscription rules decide which relay events are visible to the harness;
//! this module makes the narrower decision of whether a visible event starts
//! work. Signed `p` recipient tags are authoritative. Human-readable `@Name`
//! and NIP-27 `nostr:npub...` references are used only when no `p` recipient
//! metadata exists, preserving compatibility with older senders.

use std::collections::HashSet;
use std::fmt;

use nostr::{Event, PublicKey, ToBech32};

const SEEN_GENERATION_CAP: usize = 2_048;

/// Relationship between the event author and this harness identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorClass {
    /// The event was signed by this agent.
    SelfAuthored,
    /// The event was signed by the configured human owner.
    Owner,
    /// The event was signed by a cryptographically verified sibling agent.
    AgentPeer,
    /// An allowed channel member whose owner/agent status is not special.
    Other,
}

/// Stable inbound event classes used by admission and structured logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundClass {
    OwnerCommand,
    ExplicitPeerTaskOrQuestion,
    PeerContextOrFyi,
    OrdinaryThreadReply,
    SelfAuthoredOrDuplicate,
    /// The classifier could not make a confident new-policy decision. The
    /// caller must preserve the pre-classification admission behavior.
    LegacyFallback,
}

impl fmt::Display for InboundClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::OwnerCommand => "owner-command",
            Self::ExplicitPeerTaskOrQuestion => "explicit-peer-task-or-question",
            Self::PeerContextOrFyi => "peer-context-or-fyi",
            Self::OrdinaryThreadReply => "ordinary-thread-reply",
            Self::SelfAuthoredOrDuplicate => "self-authored-or-duplicate",
            Self::LegacyFallback => "legacy-fallback",
        })
    }
}

/// Effect of a classification on the work queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// Start, steer, or queue an agent turn.
    Admit,
    /// Retain as passive context for a later admitted turn, if supported.
    AttachContext,
    /// Stay silent and do not create work.
    Ignore,
}

/// Evidence used to determine whether this agent was addressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecipientEvidence {
    /// A signed `p` tag names this agent.
    Metadata,
    /// Signed `p` tags are present, but all name other recipients.
    MetadataForOther,
    /// No recipient metadata existed; a textual agent reference matched.
    TextFallback,
    /// No recipient evidence existed.
    None,
}

/// Complete classification result for one inbound event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Classification {
    pub class: InboundClass,
    pub admission: Admission,
    pub recipient: RecipientEvidence,
    /// True only when this exact signed event ID was classified previously.
    pub duplicate: bool,
}

/// Bounded, stateful inbound classifier.
///
/// The two-generation event-id set rejects relay duplicates without a periodic
/// all-at-once amnesia window. At most `2 * SEEN_GENERATION_CAP` IDs are held.
#[derive(Debug)]
pub struct InboundClassifier {
    agent_pubkey: String,
    agent_npub: Option<String>,
    agent_names: Vec<String>,
    seen_current: HashSet<String>,
    seen_previous: HashSet<String>,
}

impl InboundClassifier {
    /// Create a classifier for an agent pubkey and its known display names.
    pub fn new(
        agent_pubkey: impl Into<String>,
        agent_names: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Self {
        let agent_pubkey = agent_pubkey.into().to_ascii_lowercase();
        let agent_npub = PublicKey::from_hex(&agent_pubkey)
            .ok()
            .and_then(|key| key.to_bech32().ok());
        let agent_names = agent_names
            .into_iter()
            .map(|name| name.as_ref().trim().to_ascii_lowercase())
            .filter(|name| !name.is_empty())
            .collect();
        Self {
            agent_pubkey,
            agent_npub,
            agent_names,
            seen_current: HashSet::new(),
            seen_previous: HashSet::new(),
        }
    }

    /// Classify an event exactly once.
    pub fn classify(&mut self, event: &Event, author: AuthorClass) -> Classification {
        let duplicate = !self.remember(event.id.to_hex());
        if author == AuthorClass::SelfAuthored || duplicate {
            return Classification {
                class: InboundClass::SelfAuthoredOrDuplicate,
                admission: Admission::Ignore,
                recipient: RecipientEvidence::None,
                duplicate,
            };
        }

        let recipient = self.recipient_evidence(event);
        let directed = matches!(
            recipient,
            RecipientEvidence::Metadata | RecipientEvidence::TextFallback
        );
        let content = strip_leading_recipients(&event.content, &self.agent_names);
        let passive = is_explicitly_passive(content);
        let requests_work = requests_work(content);
        let is_thread_reply = buzz_core::nip10::parse_thread_markers(&event.tags)
            .resolve()
            .is_some();

        let (class, admission) = match author {
            AuthorClass::SelfAuthored => (InboundClass::SelfAuthoredOrDuplicate, Admission::Ignore),
            AuthorClass::Owner if is_owner_control(content) => {
                (InboundClass::OwnerCommand, Admission::Admit)
            }
            AuthorClass::Owner if directed && passive => {
                (InboundClass::PeerContextOrFyi, Admission::AttachContext)
            }
            AuthorClass::Owner if directed || requests_work => {
                (InboundClass::OwnerCommand, Admission::Admit)
            }
            AuthorClass::AgentPeer if directed && requests_work && !passive => {
                (InboundClass::ExplicitPeerTaskOrQuestion, Admission::Admit)
            }
            AuthorClass::AgentPeer => (InboundClass::PeerContextOrFyi, Admission::AttachContext),
            AuthorClass::Other if directed && requests_work && !passive => {
                (InboundClass::ExplicitPeerTaskOrQuestion, Admission::Admit)
            }
            _ if passive => (InboundClass::PeerContextOrFyi, Admission::AttachContext),
            _ if is_thread_reply && !requests_work => {
                (InboundClass::OrdinaryThreadReply, Admission::Ignore)
            }
            // This is intentionally admitted. It is the default-safe escape
            // hatch: uncertain inputs retain the pre-lane rule-match behavior.
            _ => (InboundClass::LegacyFallback, Admission::Admit),
        };

        Classification {
            class,
            admission,
            recipient,
            duplicate: false,
        }
    }

    fn remember(&mut self, event_id: String) -> bool {
        if self.seen_current.contains(&event_id) || self.seen_previous.contains(&event_id) {
            return false;
        }
        if self.seen_current.len() >= SEEN_GENERATION_CAP {
            self.seen_previous = std::mem::take(&mut self.seen_current);
        }
        self.seen_current.insert(event_id)
    }

    fn recipient_evidence(&self, event: &Event) -> RecipientEvidence {
        let recipients: Vec<&str> = event
            .tags
            .iter()
            .filter_map(|tag| {
                let fields = tag.as_slice();
                (matches!(fields.first().map(String::as_str), Some("p" | "mention")))
                    .then(|| fields.get(1).map(String::as_str))
                    .flatten()
            })
            .collect();
        if !recipients.is_empty() {
            return if recipients
                .iter()
                .any(|recipient| recipient.eq_ignore_ascii_case(&self.agent_pubkey))
            {
                RecipientEvidence::Metadata
            } else {
                RecipientEvidence::MetadataForOther
            };
        }

        let lower = event.content.to_ascii_lowercase();
        let npub_match = self
            .agent_npub
            .as_deref()
            .is_some_and(|npub| lower.contains(&format!("nostr:{npub}")));
        let name_match = self.agent_names.iter().any(|name| {
            let needle = format!("@{name}");
            lower.match_indices(&needle).any(|(start, _)| {
                let end = start + needle.len();
                lower[end..]
                    .chars()
                    .next()
                    .is_none_or(|c| !is_mention_char(c))
            })
        });
        if npub_match || name_match {
            RecipientEvidence::TextFallback
        } else {
            RecipientEvidence::None
        }
    }
}

fn is_mention_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_')
}

fn strip_leading_recipients<'a>(content: &'a str, agent_names: &[String]) -> &'a str {
    let mut rest = content.trim();
    loop {
        if let Some(consumed) = agent_names.iter().find_map(|name| {
            let at_name = format!("@{name}");
            let match_candidate = |candidate: &str| {
                let prefix = rest.get(..candidate.len())?;
                if !prefix.eq_ignore_ascii_case(candidate) {
                    return None;
                }
                let suffix = rest.get(candidate.len()..)?;
                suffix
                    .chars()
                    .next()
                    .is_none_or(|c| c.is_whitespace() || matches!(c, ':' | ',' | '-'))
                    .then_some(candidate.len())
            };
            match_candidate(&at_name).or_else(|| match_candidate(name))
        }) {
            rest = rest[consumed..]
                .trim_start_matches([':', ',', '-'])
                .trim_start();
            continue;
        }
        let Some(first) = rest.split_whitespace().next() else {
            return rest;
        };
        if first.starts_with('@')
            || first.starts_with("nostr:npub1")
            || first.starts_with("nostr:nprofile1")
        {
            rest = rest[first.len()..].trim_start();
        } else {
            return rest;
        }
    }
}

fn is_owner_control(content: &str) -> bool {
    let lower = content.trim().to_ascii_lowercase();
    [
        "!shutdown",
        "!cancel",
        "!rotate",
        "/stop",
        "/cancel",
        "/supersede",
        "/interrupt",
        "stop",
        "supersede",
    ]
    .iter()
    .any(|command| lower == *command || lower.starts_with(&format!("{command} ")))
}

fn is_explicitly_passive(content: &str) -> bool {
    let lower = content.trim().to_ascii_lowercase();
    [
        "fyi",
        "for your information",
        "for context",
        "context:",
        "heads up",
        "no action",
        "nothing needed",
        "just letting you know",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
}

fn requests_work(content: &str) -> bool {
    let mut lower = content
        .trim()
        .trim_start_matches([':', ',', '-'])
        .trim()
        .to_ascii_lowercase();
    loop {
        let trimmed = ["also ", "and ", "then ", "next "]
            .iter()
            .find_map(|prefix| lower.strip_prefix(prefix).map(str::to_owned));
        match trimmed {
            Some(value) => lower = value,
            None => break,
        }
    }
    if lower.is_empty() {
        return false;
    }
    if lower.contains('?')
        || lower.starts_with('/')
        || matches!(
            lower.as_str(),
            "thoughts" | "any thoughts" | "your thoughts"
        )
        || lower.starts_with("what do you think")
    {
        return true;
    }
    const REQUEST_PREFIXES: &[&str] = &[
        "please ",
        "can you ",
        "could you ",
        "would you ",
        "will you ",
        "i need you to ",
        "need you to ",
        "let me know ",
        "add ",
        "analyze ",
        "answer ",
        "build ",
        "change ",
        "check ",
        "confirm ",
        "continue ",
        "create ",
        "diagnose ",
        "do ",
        "explain ",
        "find ",
        "fix ",
        "handle ",
        "help ",
        "implement ",
        "inspect ",
        "investigate ",
        "look into ",
        "own ",
        "prepare ",
        "remove ",
        "reply ",
        "review ",
        "run ",
        "send ",
        "ship ",
        "summarize ",
        "take ",
        "test ",
        "update ",
        "verify ",
        "write ",
    ];
    REQUEST_PREFIXES
        .iter()
        .any(|prefix| lower.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_request_heuristic_is_conservative() {
        assert!(requests_work("Can you check this?"));
        assert!(requests_work("review the patch"));
        assert!(!requests_work("CI completed successfully"));
        assert!(is_explicitly_passive("FYI: no action needed"));
    }
}
