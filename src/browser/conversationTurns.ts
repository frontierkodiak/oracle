import {
  CONVERSATION_TURN_CONTAINER_SELECTOR,
  CONVERSATION_TURN_SELECTOR,
  COPY_BUTTON_SELECTOR,
  FINISHED_ACTIONS_SELECTOR,
  MESSAGE_ID_SELECTOR,
  MESSAGE_UNIT_KEY_ATTRIBUTE,
  MESSAGE_UNIT_SELECTOR,
  TURN_GROUP_COPY_BUTTON_SELECTOR,
  TURN_GROUP_FINISHED_ACTIONS_SELECTOR,
  TURN_GROUP_SELECTOR,
} from "./constants.js";

/**
 * Build a browser-context expression that returns one DOM node per conversation message, for
 * either ChatGPT DOM shape: `article[data-testid^="conversation-turn"]` (old) or, through the
 * fallback selector, the `[data-content-search-unit-key]` message unit (September 2026).
 */
export function buildConversationTurnListExpression(rootExpression = "document"): string {
  const containerSelector = JSON.stringify(CONVERSATION_TURN_CONTAINER_SELECTOR);
  const fallbackSelector = JSON.stringify(CONVERSATION_TURN_SELECTOR);
  return `(() => {
    const root = ${rootExpression};
    const containers = Array.from(root.querySelectorAll(${containerSelector}));
    return containers.length > 0
      ? containers
      : Array.from(root.querySelectorAll(${fallbackSelector}));
  })()`;
}

export function buildConversationTurnCountExpression(rootExpression = "document"): string {
  return `(${buildConversationTurnListExpression(rootExpression)}).length`;
}

/**
 * In-page helpers that read a turn node the same way for both DOM shapes. Declares one
 * `const turnDom` in the enclosing scope; inject it once per function body.
 *
 * - `role(node)`: `'user' | 'assistant' | ''`, from `data-message-author-role` / `data-turn`
 *   (old) or the unit key's role suffix (new).
 * - `turnNumber(node)`: the old `conversation-turn-N` ordinal, or for a new-shape unit its
 *   1-based position among mounted units. The old ordinal survives culling; the new one is
 *   positional. Unmounting earlier units lowers it, so the anchor rejects the new answer (fails
 *   closed). Remounting two or more earlier units after the baseline would raise it enough to
 *   pass an older answer; resume hydration waits for a stable count to keep that out.
 * - `messageId(node)`: `data-message-id` (old) or `data-chatgpt-selection-message-id`, falling
 *   back to the enclosing `data-chatgpt-search-message-ids` for a new-shape user unit.
 * - `isUnit(node)`: whether the node is a new-shape message unit.
 * - `hasFinishedActions(turn)`: the old in-turn action bar, or for a new-shape unit the turn
 *   group's bar that follows the unit and sits outside it (code blocks inside the answer have
 *   their own "Copy" button, which must not read as completion).
 * - `copyButton(turn)`: the turn's copy-response control under the same scoping.
 */
export function buildTurnDomHelpersJs(): string {
  return `const turnDom = (() => {
    const UNIT_KEY = ${JSON.stringify(MESSAGE_UNIT_KEY_ATTRIBUTE)};
    const UNIT_SELECTOR = ${JSON.stringify(MESSAGE_UNIT_SELECTOR)};
    const GROUP_SELECTOR = ${JSON.stringify(TURN_GROUP_SELECTOR)};
    const MESSAGE_ID_SELECTOR = ${JSON.stringify(MESSAGE_ID_SELECTOR)};
    const FINISHED_SELECTOR = ${JSON.stringify(FINISHED_ACTIONS_SELECTOR)};
    const GROUP_FINISHED_SELECTOR = ${JSON.stringify(TURN_GROUP_FINISHED_ACTIONS_SELECTOR)};
    const COPY_SELECTOR = ${JSON.stringify(COPY_BUTTON_SELECTOR)};
    const GROUP_COPY_SELECTOR = ${JSON.stringify(TURN_GROUP_COPY_BUTTON_SELECTOR)};
    const isUnit = (node) => Boolean(node && typeof node.getAttribute === 'function' && node.getAttribute(UNIT_KEY));
    const role = (node) => {
      if (!node || typeof node.getAttribute !== 'function') return '';
      const unitKey = node.getAttribute(UNIT_KEY);
      if (unitKey) return unitKey.slice(unitKey.lastIndexOf(':') + 1).toLowerCase();
      return String(
        node.getAttribute('data-message-author-role') ||
          node.getAttribute('data-turn') ||
          node.dataset?.turn ||
          node.dataset?.messageAuthorRole ||
          '',
      ).toLowerCase();
    };
    const turnNumber = (node) => {
      if (!node || typeof node.getAttribute !== 'function') return null;
      const match = /^conversation-turn-(\\d+)$/.exec(node.getAttribute('data-testid') || '');
      if (match) return Number(match[1]);
      if (!isUnit(node)) return null;
      const index = Array.from(document.querySelectorAll(UNIT_SELECTOR)).indexOf(node);
      return index >= 0 ? index + 1 : null;
    };
    const messageId = (node) => {
      if (!node || typeof node.getAttribute !== 'function') return null;
      const carrier = node.matches?.(MESSAGE_ID_SELECTOR) ? node : node.querySelector?.(MESSAGE_ID_SELECTOR);
      if (carrier) {
        return carrier.getAttribute('data-message-id') || carrier.getAttribute('data-chatgpt-selection-message-id');
      }
      if (!isUnit(node)) return null;
      const wrapper = node.closest?.('[data-chatgpt-search-message-ids]');
      const ids = String(wrapper?.getAttribute('data-chatgpt-search-message-ids') || '').trim();
      return ids ? ids.split(/\\s+/)[0] : null;
    };
    const groupControlsAfter = (turn, selector) => {
      const group = turn.closest?.(GROUP_SELECTOR);
      if (!group) return [];
      return Array.from(group.querySelectorAll(selector)).filter(
        (button) => !turn.contains(button) && Boolean(turn.compareDocumentPosition(button) & 4),
      );
    };
    const hasFinishedActions = (turn) => {
      if (!turn || typeof turn.querySelector !== 'function') return false;
      if (!isUnit(turn)) return Boolean(turn.querySelector(FINISHED_SELECTOR));
      return groupControlsAfter(turn, GROUP_FINISHED_SELECTOR).length > 0;
    };
    const copyButton = (turn) => {
      if (!turn || typeof turn.querySelectorAll !== 'function') return null;
      const buttons = isUnit(turn)
        ? groupControlsAfter(turn, GROUP_COPY_SELECTOR)
        : Array.from(turn.querySelectorAll(COPY_SELECTOR));
      return buttons.at(-1) ?? null;
    };
    return { isUnit, role, turnNumber, messageId, hasFinishedActions, copyButton };
  })();`;
}

/**
 * In-page statement for an `isAssistantTurn`/`isUserTurn` body: a new-shape message unit answers
 * from its key's role suffix and returns early; any other node falls through to the old checks.
 */
export function buildUnitRoleGuardJs(nodeName: string, role: "assistant" | "user"): string {
  return `{ const unitKey = ${nodeName}.getAttribute(${JSON.stringify(MESSAGE_UNIT_KEY_ATTRIBUTE)}); if (unitKey) return unitKey.endsWith(':${role}'); }`;
}
