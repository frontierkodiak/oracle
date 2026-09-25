import { VIEW_PICKER_TRIGGER_SELECTOR } from "../constants.js";

/**
 * In-page helpers that read ChatGPT's model/effort picker the same way for both of its current
 * shapes. Declares one `const pickerDom` in the enclosing scope; inject it once per function body.
 *
 * - Slider shape (August to 2026-09-24): a `button.__composer-pill` trigger; menu content
 *   `[data-testid="composer-intelligence-picker-content"]`; a simple view
 *   `[data-model-selection-view="true"] [data-testid="composer-model-picker-slider-simple-view"]`
 *   holding `[data-model-reasoning-effort-slider]`; models behind Advanced -> Model.
 * - View shape (2026-09-25): a `button[data-codex-intelligence-trigger]` trigger and no test ids.
 *   The menu holds `[data-model-picker-view="simple"|"advanced"]` with two panes. The active pane
 *   has `data-active="true"`; the other is `aria-hidden` and `inert` but keeps a real layout box,
 *   so geometry cannot tell them apart. The simple pane holds the "Select model" view toggle
 *   `[data-model-picker-view-toggle]` and the power slider `[data-reasoning-slider]`, whose
 *   `[data-model-picker-power-slider]` wraps the `role="slider"` thumb. The advanced pane holds
 *   the model radios ("Latest", "GPT-5.6 Sol", ...). The trigger shows only the effort tier
 *   ("Pro", or "5.6 Pro" off Latest), so the checked radio is the model evidence.
 *
 * - `viewTrigger()`, `isViewTrigger(node)`: the view-shape trigger, or null; and the test for it.
 * - `label(node)`: the node's text without `aria-hidden` subtrees. The view trigger keeps a hidden
 *   "Thinking effort" measurement span next to its visible label; textContent would merge them.
 * - `viewMenu(trigger)`: the open view-shape menu (the trigger's `aria-controls` first), or null.
 * - `sliderPane(menu)`: the pane that holds the power slider, in either shape. It exists before
 *   the slider mounts, so callers can wait on `sliderControl` without guessing the layout.
 * - `sliderControl(pane)`: `{ control, thumb }`: the keyboard-owner menuitem and its thumb.
 * - `inActivePane(node)`: false inside a view-shape pane that is not `data-active="true"`.
 * - `modelRadios(menu)`, `radioLabel(radio)`, `viewToggle(menu)`, `view(menu)`: the view-shape
 *   model list, a radio's name without its subtitle ("GPT-5.5" + "Leaving on October 14"), the
 *   "Select model" toggle, and the active view name.
 */
export function buildPickerDomHelpersJs(): string {
  return `const pickerDom = (() => {
    const TRIGGER_SELECTOR = ${JSON.stringify(VIEW_PICKER_TRIGGER_SELECTOR)};
    const VIEW_ROOT = '[data-model-picker-view]';
    const viewTrigger = () => document.querySelector(TRIGGER_SELECTOR);
    const isViewTrigger = (node) =>
      node?.getAttribute?.('data-codex-intelligence-trigger') != null ||
      node?.getAttribute?.('aria-label') === 'Select ChatGPT model';
    const label = (node) => {
      if (!node) return '';
      if (typeof node.childNodes === 'undefined') return String(node.textContent ?? '').trim();
      const parts = [];
      const walk = (current) => {
        for (const child of Array.from(current.childNodes || [])) {
          if (child.nodeType === 3) parts.push(child.textContent || '');
          else if (child.nodeType === 1 && child.getAttribute('aria-hidden') !== 'true') walk(child);
        }
      };
      walk(node);
      return parts.join('').replace(/\\s+/g, ' ').trim();
    };
    const isViewMenu = (menu) => Boolean(menu?.querySelector?.(VIEW_ROOT));
    const viewMenu = (trigger) => {
      const id = trigger?.getAttribute?.('aria-controls');
      const controlled = id ? document.getElementById?.(id) : null;
      if (isViewMenu(controlled)) return controlled;
      return Array.from(document.querySelectorAll('[role="menu"]')).find(isViewMenu) ?? null;
    };
    const view = (menu) => menu?.querySelector?.(VIEW_ROOT)?.getAttribute('data-model-picker-view') ?? null;
    const paneOf = (node) => {
      let current = node;
      while (current?.parentElement) {
        if (current.parentElement.hasAttribute?.('data-model-picker-view')) return current;
        current = current.parentElement;
      }
      return null;
    };
    const inActivePane = (node) => {
      const pane = paneOf(node);
      return !pane || pane.getAttribute('data-active') === 'true';
    };
    const sliderPane = (menu) => {
      const simple = menu
        ?.querySelector?.('[data-model-selection-view="true"]')
        ?.querySelector?.('[data-testid="composer-model-picker-slider-simple-view"]');
      if (simple) return simple;
      const root = menu?.querySelector?.(VIEW_ROOT);
      if (!root) return null;
      return (
        Array.from(root.children).find((pane) =>
          pane.querySelector('[data-reasoning-slider], [data-model-picker-view-toggle]'),
        ) ?? null
      );
    };
    const sliderControl = (pane) => {
      const slider =
        pane?.querySelector?.('[data-model-reasoning-effort-slider]') ||
        pane?.querySelector?.('[data-model-picker-power-slider]');
      const control = slider?.closest?.('[role="menuitem"]');
      const thumb = slider?.querySelector?.('[role="slider"]');
      return control && thumb ? { control, thumb } : null;
    };
    const modelRadios = (menu) =>
      Array.from(menu?.querySelector?.(VIEW_ROOT)?.querySelectorAll('[role="menuitemradio"]') ?? []);
    const radioLabel = (radio) => {
      const walker = document.createTreeWalker(radio, 4);
      for (let node = walker.nextNode(); node; node = walker.nextNode()) {
        const text = (node.textContent || '').trim();
        if (text) return text;
      }
      return '';
    };
    const viewToggle = (menu) => menu?.querySelector?.('[data-model-picker-view-toggle]') ?? null;
    return {
      viewTrigger,
      isViewTrigger,
      label,
      isViewMenu,
      viewMenu,
      view,
      inActivePane,
      sliderPane,
      sliderControl,
      modelRadios,
      radioLabel,
      viewToggle,
    };
  })();`;
}
