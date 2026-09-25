import { readFileSync } from "node:fs";
import { Window } from "happy-dom";
import { afterEach, describe, expect, test } from "vitest";
import { ensureModelSelection } from "../../src/browser/actions/modelSelection.js";
import { ensureThinkingTime } from "../../src/browser/actions/thinkingTime.js";
import { buildTabInspectionExpressionForTest } from "../../src/browser/liveTabs.js";
import type { BrowserLogger, ChromeClient } from "../../src/browser/types.js";

// ChatGPT replaced its model/effort picker on 2026-09-25 (PL-168). The view-shape fixtures are the
// closed trigger and the open menu recorded read-only from the live bridge (the project id is
// redacted). The slider-shape document is rebuilt in the markup the picker code was written for
// (a `.__composer-pill` trigger and test-id menu content). `installPicker` plays Radix's part on
// both: the trigger toggles the menu, the power slider answers arrow keys, the view toggle swaps
// panes, and a radio choice checks the radio and closes the menu. Inert panes ignore clicks.

const FIXTURE_DIR = new URL("../fixtures/chatgpt-picker/", import.meta.url);
const manifest = JSON.parse(readFileSync(new URL("manifest.json", FIXTURE_DIR), "utf8")) as Record<
  string,
  { url: string }
>;
type Recording = { trigger: string; menu: string; menuAdvanced?: string };
const recording = (name: string): Recording =>
  JSON.parse(readFileSync(new URL(`${name}.json`, FIXTURE_DIR), "utf8")) as Recording;

const HOME = "2026-09-25-home-latest-pro";
const PROJECT = "2026-09-25-project-latest-instant";
const CONVERSATION = "2026-09-25-conversation-sol-pro";
const TIERS = ["Instant", "Medium", "High", "Extra High", "Pro"];

const windows: Window[] = [];
afterEach(async () => {
  for (const window of windows.splice(0)) await window.happyDOM.close();
});

function open(url: string): Window {
  const window = new Window({ url });
  // happy-dom nodes fail `instanceof EventTarget` inside window.eval, which makes the production
  // click dispatcher decline them; a real page has no such gap.
  Object.defineProperty(window.EventTarget, Symbol.hasInstance, {
    value: (value: unknown) =>
      typeof (value as { dispatchEvent?: unknown } | null)?.dispatchEvent === "function",
  });
  // happy-dom has no layout. On the live page every picker part has a box, inactive panes included.
  window.HTMLElement.prototype.getBoundingClientRect = () =>
    ({ x: 0, y: 0, top: 0, left: 0, right: 100, bottom: 20, width: 100, height: 20 }) as ReturnType<
      Window["HTMLElement"]["prototype"]["getBoundingClientRect"]
    >;
  windows.push(window);
  return window;
}

function runtimeFor(window: Window): ChromeClient["Runtime"] {
  return {
    evaluate: async ({ expression }: { expression: string }) => {
      const value = await window.eval(expression);
      return { result: { type: typeof value, value } };
    },
  } as unknown as ChromeClient["Runtime"];
}

// happy-dom's own node and event types; the DOM lib's do not line up with them.
type HElement = NonNullable<ReturnType<Window["document"]["querySelector"]>>;
type HEvent = { target: unknown; key?: string };

const logger = Object.assign((_message: string) => {}, { verbose: false }) as BrowserLogger;

interface Picker {
  window: Window;
  events: string[];
  tier: () => string;
  model: () => string;
  menuOpen: () => boolean;
  triggerLabel: () => string;
}

/** The live view-shape picker: the recorded trigger in a composer form, the recorded menu on open. */
function installViewPicker(
  name: string,
  options: { ignoreKeys?: boolean; ignoreRadios?: boolean } = {},
): Picker {
  const window = open(manifest[name].url);
  const document = window.document;
  const rec = recording(name);
  document.body.innerHTML = `<main><form>${rec.trigger}</form></main>`;
  const trigger = document.querySelector("button[data-codex-intelligence-trigger]")!;
  const events: string[] = [];
  const menuTemplate = document.createElement("template");
  menuTemplate.innerHTML = rec.menu;
  const recordedMenu = menuTemplate.content.firstElementChild!;
  let tierIndex = Number(
    recordedMenu.querySelector('[role="slider"]')!.getAttribute("aria-valuenow"),
  );
  let model = recordedMenu
    .querySelector('[role="menuitemradio"][aria-checked="true"]')!
    .textContent!.trim();
  const labelSpan = () => {
    const spans = Array.from(trigger.querySelectorAll("span")).filter(
      (span) => !span.closest('[aria-hidden="true"]') && span.children.length === 0,
    );
    return spans.at(-1)!.parentElement!;
  };
  const setTriggerLabel = (text: string) => {
    labelSpan().textContent = text;
  };
  const closedLabel = () =>
    model === "Latest"
      ? TIERS[tierIndex]!
      : `${model.replace(/^GPT-| Sol$/g, "")} ${TIERS[tierIndex]}`;
  let menu: HElement | null = null;

  const setView = (view: "simple" | "advanced") => {
    const root = menu!.querySelector("[data-model-picker-view]")!;
    root.setAttribute("data-model-picker-view", view);
    const panes = Array.from(root.children);
    panes.forEach((pane, index) => {
      const active = (index === 0) === (view === "simple");
      pane.setAttribute("data-active", String(active));
      pane.setAttribute("aria-hidden", String(!active));
      if (active) pane.removeAttribute("inert");
      else pane.setAttribute("inert", "");
    });
  };
  const syncMenu = () => {
    if (!menu) return;
    const thumb = menu.querySelector('[role="slider"]')!;
    thumb.setAttribute("aria-valuenow", String(tierIndex));
    menu.querySelector('[role="status"]')!.textContent =
      `${TIERS[tierIndex]}, ${tierIndex + 1} of 5.`;
    for (const radio of Array.from(menu.querySelectorAll('[role="menuitemradio"]'))) {
      const checked = radio.textContent!.trim().startsWith(model);
      radio.setAttribute("aria-checked", String(checked));
      if (checked) radio.setAttribute("data-model-selected", "true");
      else radio.removeAttribute("data-model-selected");
    }
  };
  const closeMenu = () => {
    menu?.remove();
    menu = null;
    trigger.setAttribute("aria-expanded", "false");
    trigger.setAttribute("data-state", "closed");
    trigger.removeAttribute("aria-controls");
    setTriggerLabel(closedLabel());
  };
  const openMenu = () => {
    menu = recordedMenu.cloneNode(true) as HElement;
    document.body.appendChild(menu);
    setView("simple");
    syncMenu();
    trigger.setAttribute("aria-expanded", "true");
    trigger.setAttribute("data-state", "open");
    trigger.setAttribute("aria-controls", menu.id);
    // The open trigger shows its "Thinking effort" caption instead of the tier.
    setTriggerLabel("Thinking effort");
  };

  document.addEventListener("click", (event) => {
    const target = (event as unknown as HEvent).target as HElement;
    if (target.closest?.("[inert]")) {
      events.push("inert-click");
      return;
    }
    if (trigger.contains(target)) {
      events.push(menu ? "trigger-close" : "trigger-open");
      if (menu) closeMenu();
      else openMenu();
      return;
    }
    if (!menu?.contains(target)) return;
    if (target.closest("[data-model-picker-view-toggle]")) {
      events.push("view-toggle");
      setView("advanced");
      return;
    }
    const radio = target.closest('[role="menuitemradio"]');
    if (radio) {
      events.push(`radio:${radio.textContent!.trim()}`);
      if (options.ignoreRadios) return;
      model = radio.querySelector("span")!.textContent!.trim();
      closeMenu();
    }
  });
  document.addEventListener("keydown", (event) => {
    const key = (event as unknown as HEvent).key;
    const target = (event as unknown as HEvent).target as HElement;
    if (key === "Escape") {
      events.push("escape");
      if (menu) closeMenu();
      return;
    }
    if (
      (key === "ArrowRight" || key === "ArrowLeft") &&
      target.closest?.("[data-reasoning-slider]")
    ) {
      events.push(key);
      if (options.ignoreKeys) return;
      tierIndex = Math.max(0, Math.min(4, tierIndex + (key === "ArrowRight" ? 1 : -1)));
      syncMenu();
    }
  });

  return {
    window,
    events,
    tier: () => TIERS[tierIndex]!,
    model: () => model,
    menuOpen: () => Boolean(menu),
    triggerLabel: () => labelSpan().textContent ?? "",
  };
}

/**
 * The slider shape, rebuilt: a `.__composer-pill` reading "6 High" whose menu holds the test-id
 * picker content, the simple view with the power slider, and the Advanced toggle.
 */
function installSliderPicker(initialTier = 2): Picker {
  const window = open("https://chatgpt.com/");
  const document = window.document;
  let tierIndex = initialTier;
  document.body.innerHTML = `<main><form><div data-testid="composer-footer-actions">
    <button type="button" class="__composer-pill" aria-haspopup="menu" aria-expanded="false">6 ${TIERS[tierIndex]}</button>
  </div></form></main>`;
  const pill = document.querySelector("button.__composer-pill")!;
  const events: string[] = [];
  let menu: HElement | null = null;
  const render = () => {
    if (!menu) return;
    menu.querySelector('[role="slider"]')!.setAttribute("aria-valuenow", String(tierIndex));
    menu.querySelector("#slider-announcement")!.textContent =
      `${TIERS[tierIndex]}, ${tierIndex + 1} of 5.`;
  };
  const closeMenu = () => {
    menu?.remove();
    menu = null;
    pill.setAttribute("aria-expanded", "false");
    pill.removeAttribute("aria-controls");
    pill.textContent = `6 ${TIERS[tierIndex]}`;
  };
  const openMenu = () => {
    const wrapper = document.createElement("div");
    wrapper.innerHTML = `<div role="menu" id="slider-menu" data-radix-menu-content="">
      <div data-testid="composer-intelligence-picker-content" role="group">
        <div data-model-selection-view="true">
          <div data-testid="composer-model-picker-slider-simple-view" data-active="true">
            <span id="slider-announcement" role="status"></span>
            <div role="menuitem" aria-label="Power" aria-describedby="slider-announcement">
              <div data-model-reasoning-effort-slider="">
                <span role="slider" aria-hidden="true" aria-valuemin="0" aria-valuemax="4"></span>
              </div>
            </div>
          </div>
        </div>
        <div role="menuitem" aria-label="Show advanced options" aria-expanded="false">Advanced</div>
      </div>
    </div>`;
    menu = wrapper.firstElementChild!;
    document.body.appendChild(menu);
    render();
    pill.setAttribute("aria-expanded", "true");
    pill.setAttribute("aria-controls", "slider-menu");
  };
  document.addEventListener("click", (event) => {
    if (!pill.contains((event as unknown as HEvent).target as HElement)) return;
    events.push(menu ? "trigger-close" : "trigger-open");
    if (menu) closeMenu();
    else openMenu();
  });
  document.addEventListener("keydown", (event) => {
    const key = (event as unknown as HEvent).key;
    if (key === "Escape") {
      events.push("escape");
      if (menu) closeMenu();
      return;
    }
    if (key === "ArrowRight" || key === "ArrowLeft") {
      events.push(key);
      tierIndex = Math.max(0, Math.min(4, tierIndex + (key === "ArrowRight" ? 1 : -1)));
      render();
    }
  });
  return {
    window,
    events,
    tier: () => TIERS[tierIndex]!,
    model: () => "Latest",
    menuOpen: () => Boolean(menu),
    triggerLabel: () => pill.textContent ?? "",
  };
}

const selectModel = (picker: Picker, model: string) =>
  ensureModelSelection(runtimeFor(picker.window), model, logger, "select", {
    buttonWaitMs: 0,
  });
const selectEffort = (picker: Picker, level: "pro" | "extended", model = "Latest") =>
  ensureThinkingTime(runtimeFor(picker.window), level, logger, model);

describe("ChatGPT picker, view shape (recorded 2026-09-25)", () => {
  test("the recorded advanced view is what the view toggle produces", () => {
    const picker = installViewPicker(HOME);
    const holder = picker.window.document.createElement("div");
    holder.innerHTML = recording(HOME).menuAdvanced!;
    const panes = (root: HElement) =>
      Array.from(root.querySelector("[data-model-picker-view]")!.children).map((pane) => [
        pane.getAttribute("data-active"),
        pane.getAttribute("aria-hidden"),
        pane.hasAttribute("inert"),
      ]);
    const recorded = holder.firstElementChild!;
    expect(
      recorded.querySelector("[data-model-picker-view]")!.getAttribute("data-model-picker-view"),
    ).toBe("advanced");
    expect(panes(recorded)).toEqual([
      ["false", "true", true],
      ["true", "false", false],
    ]);
  });

  test("Latest and Pro already selected: verified by the checked radio and the slider, no changes", async () => {
    const picker = installViewPicker(HOME);
    expect(picker.triggerLabel()).toBe("Pro");

    const model = await selectModel(picker, "Latest");
    expect(model).toMatchObject({
      status: "already-selected",
      resolvedLabel: "Latest",
      verified: true,
    });
    expect(picker.menuOpen()).toBe(false);

    const effort = await selectEffort(picker, "pro");
    expect(effort).toMatchObject({
      status: "already-selected",
      resolvedLabel: "Pro",
      verified: true,
    });
    expect(picker.menuOpen()).toBe(false);
    expect(picker.events.filter((e) => e.startsWith("radio") || e.startsWith("Arrow"))).toEqual([]);
  });

  test("project composer on Instant: Pro is reached with the slider and proven by its announcement", async () => {
    const picker = installViewPicker(PROJECT);
    expect(picker.triggerLabel()).toBe("Instant");

    expect(await selectModel(picker, "Latest")).toMatchObject({
      status: "already-selected",
      verified: true,
    });
    const effort = await selectEffort(picker, "pro");
    expect(effort).toMatchObject({ status: "switched", resolvedLabel: "Pro", verified: true });
    expect(picker.events.filter((e) => e.startsWith("Arrow"))).toEqual(Array(4).fill("ArrowRight"));
    expect(picker.tier()).toBe("Pro");
    expect(picker.menuOpen()).toBe(false);
    expect(picker.triggerLabel()).toBe("Pro");
  });

  test("a GPT-5.6 Sol composer: Latest is chosen in the model view and read back from its radio", async () => {
    const picker = installViewPicker(CONVERSATION);
    expect(picker.triggerLabel()).toBe("5.6 Pro");

    const model = await selectModel(picker, "Latest");
    expect(model).toMatchObject({ status: "switched", resolvedLabel: "Latest", verified: true });
    expect(picker.model()).toBe("Latest");
    expect(picker.events).toContain("view-toggle");
    expect(picker.events).not.toContain("inert-click");
    expect(picker.menuOpen()).toBe(false);
    expect(picker.triggerLabel()).toBe("Pro");

    expect(await selectEffort(picker, "pro")).toMatchObject({ status: "already-selected" });
  });

  test("selects a versioned model radio", async () => {
    const picker = installViewPicker(HOME);
    const model = await selectModel(picker, "gpt-5.6-sol");
    expect(model).toMatchObject({
      status: "switched",
      resolvedLabel: "GPT-5.6 Sol",
      verified: true,
    });
    expect(picker.model()).toBe("GPT-5.6 Sol");
  });

  test("refuses Pro effort for Latest while the composer is on GPT-5.6 Sol", async () => {
    const picker = installViewPicker(CONVERSATION);
    await expect(selectEffort(picker, "pro")).rejects.toThrow(/refusing to submit/);
    expect(picker.events.filter((e) => e.startsWith("Arrow"))).toEqual([]);
  });

  test("fails closed when the slider ignores the keyboard", async () => {
    const picker = installViewPicker(PROJECT, { ignoreKeys: true });
    await expect(selectEffort(picker, "pro")).rejects.toThrow(
      /refusing to submit without confirmed Pro/,
    );
    expect(picker.tier()).toBe("Instant");
    expect(picker.menuOpen()).toBe(false);
  }, 30_000);

  test("fails closed when a model radio click does not take", async () => {
    const picker = installViewPicker(CONVERSATION, { ignoreRadios: true });
    await expect(selectModel(picker, "Latest")).rejects.toThrow(/did not confirm "Latest"/);
    expect(picker.model()).toBe("GPT-5.6 Sol");
    expect(picker.menuOpen()).toBe(false);
  }, 30_000);

  test("lists the model radios when the target is not one of them", async () => {
    const picker = installViewPicker(HOME);
    await expect(selectModel(picker, "gpt-5.4")).rejects.toThrow(
      /Available: Latest, GPT-5.6 Sol, GPT-5.5\./,
    );
    expect(picker.menuOpen()).toBe(false);
  });

  test("live-tab inspection reads the trigger's visible label", async () => {
    const picker = installViewPicker(CONVERSATION);
    const value = (await picker.window.eval(buildTabInspectionExpressionForTest())) as {
      currentModelLabel?: string;
    };
    expect(value.currentModelLabel).toBe("5.6 Pro");
  });
});

describe("ChatGPT picker, slider shape (rebuilt)", () => {
  test("Latest is read from the 6-prefixed pill and Pro is set with the slider", async () => {
    const picker = installSliderPicker(2);
    expect(await selectModel(picker, "Latest")).toMatchObject({
      status: "already-selected",
      resolvedLabel: "Latest",
      verified: true,
    });
    const effort = await selectEffort(picker, "pro");
    expect(effort).toMatchObject({ status: "switched", resolvedLabel: "Pro", verified: true });
    expect(picker.events.filter((e) => e.startsWith("Arrow"))).toEqual([
      "ArrowRight",
      "ArrowRight",
    ]);
    expect(picker.menuOpen()).toBe(false);
  });

  test("Pro already selected needs no keys", async () => {
    const picker = installSliderPicker(4);
    expect(await selectEffort(picker, "pro")).toMatchObject({ status: "already-selected" });
    expect(picker.events.filter((e) => e.startsWith("Arrow"))).toEqual([]);
  });
});
