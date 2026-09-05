# buzz-relay

NIP-01 WebSocket relay for Buzz private team communication, plus the HTTP
surface (`POST /events`, `POST /query`, `POST /count`, NIP-11/NIP-05 metadata,
workflow webhooks, Blossom media, git smart HTTP, and health probes).

## Repairing a stale replaceable-event `#p` index

Single-value `#p` filters use the denormalized PostgreSQL `event_mentions`
index, while `#d` filters use the `d_tag` stored on the event row. Older relay
versions committed a replaceable event before indexing its `p` tags and treated
an indexing failure as non-fatal. Such a failure can therefore leave a live
kind:39002 roster visible by `#d` but invisible by `#p` to members whose index
rows are missing.

The current storage path writes the replacement event and all of its mention
rows in one transaction. The regression test in
`tests/replaceable_tag_index.rs` stores a kind:39002 roster, replaces it with a
revision containing another member, and verifies that both `#d` and the newly
added member's `#p` filter return the current revision.

To repair an affected production channel, deploy a relay version containing
the atomic mention-index write, then force-republish that channel's roster with
the configured production relay key:

```bash
BUZZ_RELAY_PRIVATE_KEY='<production-relay-key>' \
  buzz-admin reconcile-channels --channel '<channel-uuid>'
```

The targeted form replaces only kind:39002 from canonical `channel_members`;
it does not change channel metadata, admins, membership, authorization,
admission, or DM state. The untargeted `buzz-admin reconcile-channels` command
only creates missing discovery events and will report an affected channel as
already present, so it does not repair this divergence.

After the targeted republish, verify that these return the same event ID and
that the event contains the member's `p` tag:

```json
{"kinds":[39002],"#d":["<channel-uuid>"]}
{"kinds":[39002],"#p":["<member-pubkey>"]}
```
