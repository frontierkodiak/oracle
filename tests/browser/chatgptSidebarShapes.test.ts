import { readFileSync } from "node:fs";
import { Window } from "happy-dom";
import { afterEach, describe, expect, test } from "vitest";
import { buildChatModeProbeExpressionForTest } from "../../src/browser/actions/navigation.js";

// ChatGPT restyled its sidebar history rows on 2026-09-25 (PL-168). The fixture holds two rows
// recorded read-only from the live bridge (a Work and a Chat conversation; titles scrubbed, ids
// zeroed). The anchor lost its `__menu-item` class and now carries `data-interactive-row-link`,
// and the Work badge sits beside it in the `[data-thread-title-trigger]` row. The old shape is
// rebuilt as the probe was written for it: a `.__menu-item` anchor with the badge inside.

const ROWS = readFileSync(
  new URL("../fixtures/chatgpt-sidebar/2026-09-25-history-rows.html", import.meta.url),
  "utf8",
);
const WORK_ID = "00000000-0000-0000-0000-00000000000a";
const CHAT_ID = "00000000-0000-0000-0000-00000000000b";

const windows: Window[] = [];
afterEach(async () => {
  for (const window of windows.splice(0)) await window.happyDOM.close();
});

function probe(url: string, body: string): { status: string } {
  const window = new Window({ url });
  windows.push(window);
  window.document.body.innerHTML = body;
  return window.eval(buildChatModeProbeExpressionForTest()) as { status: string };
}

const oldRow = (id: string, title: string, work: boolean) =>
  `<a class="__menu-item group" href="/c/${id}" aria-label="${title}"><span class="flex items-center">` +
  `<span dir="auto">${title}</span>${work ? '<span class="shrink-0 text-xs">Work</span>' : ""}</span></a>`;

describe("Chat/Work mode of the resumed conversation", () => {
  test("new rows: a Work conversation is recognised by the badge beside its anchor", () => {
    expect(probe(`https://chatgpt.com/c/${WORK_ID}`, ROWS)).toEqual({
      status: "work-conversation",
    });
  });

  test("new rows: an ordinary conversation resolves as Chat", () => {
    expect(probe(`https://chatgpt.com/c/${CHAT_ID}`, ROWS)).toEqual({
      status: "chat-conversation",
    });
  });

  test("new rows: a project conversation URL matches its sidebar row by id", () => {
    expect(
      probe(`https://chatgpt.com/g/g-p-00000000000000000000000000000000-quiet/c/${CHAT_ID}`, ROWS),
    ).toEqual({ status: "chat-conversation" });
  });

  test("a message link to the current thread is not sidebar evidence", () => {
    const body = `<main><a href="/c/${CHAT_ID}" aria-label="Linked">Linked</a></main>`;
    expect(probe(`https://chatgpt.com/c/${CHAT_ID}`, body)).toEqual({
      status: "conversation-unresolved",
    });
  });

  test("old rows: the badge inside the menu-item anchor still decides", () => {
    const body = `<nav>${oldRow(WORK_ID, "Harbor lantern survey", true)}${oldRow(CHAT_ID, "Orchard ledger notes", false)}</nav>`;
    expect(probe(`https://chatgpt.com/c/${WORK_ID}`, body)).toEqual({
      status: "work-conversation",
    });
    expect(probe(`https://chatgpt.com/c/${CHAT_ID}`, body)).toEqual({
      status: "chat-conversation",
    });
  });
});
