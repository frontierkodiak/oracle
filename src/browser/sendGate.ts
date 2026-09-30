import type { BrowserRunOptions } from "./types.js";

/**
 * Reserve each provider send attempt before it happens (PL-229).
 *
 * A run can send several prompts: the initial prompt and each follow-up. The
 * caller's `beforeSend` hook runs before every attempt, inside the pre-click
 * fence, so a refusal stops that attempt before anything reaches the
 * provider. A recovery retry may follow an ambiguous dispatch, so it is a new
 * attempt and is reserved again; the ledger settles over-reservation from the
 * provider's own record.
 */
export interface BrowserSendGate {
  setOrdinal(ordinal: number): void;
  beforeAttempt(markAttempt: () => Promise<void>): Promise<void>;
}

export function createSendGate(beforeSend?: BrowserRunOptions["beforeSend"]): BrowserSendGate {
  let ordinal = 0;
  const attempts = new Map<number, number>();
  return {
    setOrdinal(next) {
      ordinal = next;
    },
    async beforeAttempt(markAttempt) {
      if (beforeSend) {
        const attempt = attempts.get(ordinal) ?? 0;
        await beforeSend({ ordinal, attempt });
        attempts.set(ordinal, attempt + 1);
      }
      await markAttempt();
    },
  };
}
