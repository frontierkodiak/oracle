import type { BrowserRunOptions } from "./types.js";

/**
 * Reserve each provider send before it happens (PL-229).
 *
 * A run can send several prompts: the initial prompt and each follow-up. The
 * caller's `beforeSend` hook runs once per send ordinal, inside the pre-click
 * fence, so a refusal stops that send before anything reaches the provider.
 * Retrying the same send (the recovery path) does not reserve it again.
 */
export interface BrowserSendGate {
  setOrdinal(ordinal: number): void;
  beforeAttempt(markAttempt: () => Promise<void>): Promise<void>;
}

export function createSendGate(beforeSend?: BrowserRunOptions["beforeSend"]): BrowserSendGate {
  let ordinal = 0;
  const reserved = new Set<number>();
  return {
    setOrdinal(next) {
      ordinal = next;
    },
    async beforeAttempt(markAttempt) {
      if (beforeSend && !reserved.has(ordinal)) {
        await beforeSend({ ordinal });
        reserved.add(ordinal);
      }
      await markAttempt();
    },
  };
}
