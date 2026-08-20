import type { ListVirtualizer } from "@/shared/ui/VirtualizedList";

type MessageIdentity = { id: string };
type ScrollToIndexApi = Pick<ListVirtualizer, "scrollToIndex">;

export function createMessageIndex(
  messages: readonly MessageIdentity[],
): ReadonlyMap<string, number> {
  return new Map(messages.map((message, index) => [message.id, index]));
}

export function scrollVirtualizedMessageIntoView(
  virtualizer: ScrollToIndexApi | null,
  indexById: ReadonlyMap<string, number>,
  messageId: string,
  behavior: ScrollBehavior = "auto",
): boolean {
  const index = indexById.get(messageId);
  if (!virtualizer || index === undefined) return false;
  virtualizer.scrollToIndex(index, { align: "center", behavior });
  return true;
}
