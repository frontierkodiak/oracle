export const ORACLE_MIN_NODE_MAJOR = 24;

export interface OracleRuntimeIdentity {
  name: "node";
  version: string;
  major: number;
  minimumMajor: number;
}

export function parseNodeMajor(version: string): number | undefined {
  const match = /^(\d+)(?:\.\d+){0,2}(?:[-+].*)?$/.exec(version.trim());
  if (!match) return undefined;
  const major = Number(match[1]);
  return Number.isSafeInteger(major) ? major : undefined;
}

export function assertSupportedNodeVersion(
  version: string,
  minimumMajor = ORACLE_MIN_NODE_MAJOR,
): number {
  const major = parseNodeMajor(version);
  if (major === undefined || major < minimumMajor) {
    throw new Error(`Oracle remote service requires Node.js >= ${minimumMajor}; found ${version}`);
  }
  return major;
}

export function getOracleRuntimeIdentity(version = process.versions.node): OracleRuntimeIdentity {
  const major = assertSupportedNodeVersion(version);
  return { name: "node", version, major, minimumMajor: ORACLE_MIN_NODE_MAJOR };
}
