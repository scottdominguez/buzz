import assert from "node:assert/strict";
import { after, afterEach, before, test } from "node:test";

import { JSDOM } from "jsdom";

import {
  createMessageIndex,
  scrollVirtualizedMessageIntoView,
} from "./conversationVirtualization.ts";

const dom = new JSDOM("<!doctype html><html><body></body></html>", {
  pretendToBeVisual: true,
  url: "http://localhost",
});
const resizeObservers = new Set();

class TestResizeObserver {
  constructor(callback) {
    this.callback = callback;
    this.elements = new Set();
    resizeObservers.add(this);
  }
  observe(element) {
    this.elements.add(element);
  }
  unobserve(element) {
    this.elements.delete(element);
  }
  disconnect() {
    this.elements.clear();
    resizeObservers.delete(this);
  }
  notify() {
    this.callback(
      [...this.elements].map((target) => ({
        borderBoxSize: [{ blockSize: target.clientHeight, inlineSize: 400 }],
        contentRect: target.getBoundingClientRect(),
        target,
      })),
      this,
    );
  }
}

before(() => {
  Object.assign(globalThis, {
    document: dom.window.document,
    Element: dom.window.Element,
    HTMLElement: dom.window.HTMLElement,
    IS_REACT_ACT_ENVIRONMENT: true,
    MutationObserver: dom.window.MutationObserver,
    ResizeObserver: TestResizeObserver,
    window: dom.window,
  });
  Object.defineProperties(dom.window.HTMLElement.prototype, {
    clientHeight: { configurable: true, get: () => 240 },
    offsetHeight: { configurable: true, get: () => 240 },
  });
  dom.window.HTMLElement.prototype.getBoundingClientRect = () => ({
    bottom: 240,
    height: 240,
    left: 0,
    right: 400,
    top: 0,
    width: 400,
    x: 0,
    y: 0,
    toJSON() {},
  });
});

afterEach(async () => {
  const { cleanup } = await import("@testing-library/react");
  cleanup();
});

after(() => dom.window.close());

test("658 messages mount only a bounded virtual range", async () => {
  const React = await import("react");
  const { act, render, screen } = await import("@testing-library/react");
  const { VirtualizedList } = await import("@/shared/ui/VirtualizedList");
  const items = Array.from({ length: 658 }, (_, index) => ({
    id: `message-${index}`,
  }));
  let virtualizer = null;

  const view = render(
    React.createElement(VirtualizedList, {
      className: "test-scroll",
      estimateSize: 40,
      getItemKey: (item) => item.id,
      items,
      onVirtualizer: (instance) => {
        virtualizer = instance;
      },
      overscan: 3,
      renderItem: (item) =>
        React.createElement("div", { "data-testid": "message-row" }, item.id),
    }),
  );

  const scrollElement = view.container.querySelector(".test-scroll");
  assert.ok(scrollElement);
  Object.defineProperties(scrollElement, {
    clientHeight: { configurable: true, value: 240 },
    offsetHeight: { configurable: true, value: 240 },
    scrollHeight: { configurable: true, value: items.length * 40 },
  });
  scrollElement.getBoundingClientRect = () => ({
    bottom: 240,
    height: 240,
    left: 0,
    right: 400,
    top: 0,
    width: 400,
    x: 0,
    y: 0,
    toJSON() {},
  });
  scrollElement.scrollTo = (first) => {
    scrollElement.scrollTop = typeof first === "number" ? first : first.top;
    scrollElement.dispatchEvent(new dom.window.Event("scroll"));
  };

  await act(async () => {
    for (const observer of resizeObservers) observer.notify();
    virtualizer?.measure();
    await new Promise((resolve) => dom.window.requestAnimationFrame(resolve));
  });

  const initiallyMounted = screen.getAllByTestId("message-row");
  assert.ok(initiallyMounted.length > 0);
  assert.ok(
    initiallyMounted.length < 30,
    `expected a small virtual range, mounted ${initiallyMounted.length}`,
  );
  assert.equal(screen.queryByText("message-512"), null);
});

test("virtualized message navigation resolves an offscreen row by stable id", () => {
  const messages = Array.from({ length: 658 }, (_, index) => ({
    id: `message-${index}`,
  }));
  const indexById = createMessageIndex(messages);
  const calls = [];
  const virtualizer = {
    scrollToIndex(index, options) {
      calls.push([index, options]);
    },
  };

  assert.equal(
    scrollVirtualizedMessageIntoView(
      virtualizer,
      indexById,
      "message-512",
      "smooth",
    ),
    true,
  );
  assert.deepEqual(calls, [[512, { align: "center", behavior: "smooth" }]]);
  assert.equal(
    scrollVirtualizedMessageIntoView(virtualizer, indexById, "missing"),
    false,
  );
});
