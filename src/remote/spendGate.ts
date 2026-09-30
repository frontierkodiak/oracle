import { execFile } from "node:child_process";

/** One provider send the bridge is about to make (PL-229). */
export interface SpendGateSubmission {
  /** Durable queue run ID: the server's own identity for this run. */
  runId: string;
  /** Client-chosen session slug; the ledger uses it to find wrapper/lane coverage. */
  sessionId?: string;
  /** 0 for the initial prompt, n for the n-th follow-up in the same run. */
  ordinal: number;
  model?: string;
  effort?: string;
}

/** Resolves when the send is reserved; rejects to refuse it before it happens. */
export type SpendGate = (submission: SpendGateSubmission) => Promise<void>;

export class SpendGateRefusedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SpendGateRefusedError";
  }
}

type ExecFile = (
  file: string,
  args: string[],
  options: { timeout: number },
  callback: (error: Error | null, stdout: string, stderr: string) => void,
) => unknown;

/**
 * A gate that asks an external ledger command, e.g. Polli's `pro-lane charge
 * --at-submit`. The ledger decides whether an existing reservation covers this
 * exact send or charges a new one; any failure, including a missing command or
 * ledger, refuses the send.
 */
export function createCommandSpendGate(options: {
  command: string;
  oracleHome: string;
  route?: string;
  timeoutMs?: number;
  exec?: ExecFile;
}): SpendGate {
  const exec = options.exec ?? (execFile as unknown as ExecFile);
  return (submission) =>
    new Promise<void>((resolve, reject) => {
      const args = [
        "charge",
        "--at-submit",
        "--route",
        options.route ?? "oracle-serve",
        "--session",
        submission.sessionId || `run-${submission.runId}`,
        "--oracle-home",
        options.oracleHome,
        "--run-id",
        submission.runId,
        "--ordinal",
        String(submission.ordinal),
      ];
      if (submission.model) args.push("--model", submission.model);
      if (submission.effort) args.push("--effort", submission.effort);
      exec(
        options.command,
        args,
        { timeout: options.timeoutMs ?? 60_000 },
        (error, _stdout, stderr) => {
          if (error) {
            const detail = String(stderr || error.message)
              .trim()
              .slice(0, 500);
            reject(new SpendGateRefusedError(`spend gate refused the send: ${detail}`));
            return;
          }
          resolve();
        },
      );
    });
}

/** A required gate with nothing configured refuses every send. */
export const refuseAllSpendGate: SpendGate = async () => {
  throw new SpendGateRefusedError("spend gate required but not configured");
};
