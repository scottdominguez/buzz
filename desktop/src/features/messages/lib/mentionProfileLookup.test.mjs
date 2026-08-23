import assert from "node:assert/strict";
import test from "node:test";

import { mergeMentionProfileLookups } from "./mentionProfileLookup.ts";

test("hydrates mention avatars from the member profile batch", () => {
  const merged = mergeMentionProfileLookups({
    agent: { displayName: "Agent", avatarUrl: "https://relay/media/agent.png" },
  });

  assert.equal(merged.agent.avatarUrl, "https://relay/media/agent.png");
});

test("caller profile data wins over an older member batch result", () => {
  const merged = mergeMentionProfileLookups(
    {
      agent: { displayName: "Agent", avatarUrl: "https://relay/media/old.png" },
    },
    {
      agent: { displayName: "Agent", avatarUrl: "https://relay/media/new.png" },
    },
  );

  assert.equal(merged.agent.avatarUrl, "https://relay/media/new.png");
});
