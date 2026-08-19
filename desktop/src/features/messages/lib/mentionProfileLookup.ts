import type { UserProfileLookup } from "@/features/profile/lib/identity";

/**
 * Merge profile data fetched specifically for mention candidates with any
 * profiles already hydrated by the caller. Caller data wins because it can
 * include fresher live-event state than the batch query cache.
 */
export function mergeMentionProfileLookups(
  fetched?: UserProfileLookup,
  caller?: UserProfileLookup,
): UserProfileLookup {
  return { ...(fetched ?? {}), ...(caller ?? {}) };
}
