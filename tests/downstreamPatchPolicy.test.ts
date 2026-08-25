import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

const manifestUrl = new URL("../downstream/patch-manifest.json", import.meta.url);

const forbiddenAuthorityKeys = new Set([
  "command",
  "commands",
  "controlleroverride",
  "controlleroverrides",
  "deploy",
  "deployment",
  "deployments",
  "executable",
  "executablepath",
  "gate",
  "gates",
  "installdir",
  "installpath",
  "packagecommand",
  "packagemanager",
  "promote",
  "promotion",
  "receipt",
  "receipts",
  "requiredcommands",
  "restart",
  "rollback",
  "rungates",
  "runtime",
  "script",
  "scripts",
  "servicecommand",
]);

const commandPrefix =
  /^\s*(?:\.{0,2}\/|\/(?:bin|sbin|usr\/bin|usr\/sbin|opt\/homebrew\/bin)\/|(?:bash|corepack|git|node|npm|npx|oxfmt|oxlint|pnpm|python3?|sh|tsx|vitest|zsh)\s)/i;
const shellOperator = /(?:&&|\|\||\$\(|`|;)/;

function requireObject(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${label} must be an object`);
  }
  return value as Record<string, unknown>;
}

function requireArray(value: unknown, label: string): unknown[] {
  if (!Array.isArray(value)) {
    throw new Error(`${label} must be an array`);
  }
  return value;
}

function requireString(value: unknown, label: string): string {
  if (typeof value !== "string") {
    throw new Error(`${label} must be a string`);
  }
  return value;
}

function requireStringArray(value: unknown, label: string): string[] {
  return requireArray(value, label).map((entry, index) =>
    requireString(entry, `${label}[${index}]`),
  );
}

async function readManifest(): Promise<Record<string, unknown>> {
  const parsed: unknown = JSON.parse(await readFile(manifestUrl, "utf8"));
  return requireObject(parsed, "manifest");
}

function collectAuthorityViolations(value: unknown, path: readonly string[] = []): string[] {
  if (typeof value === "string") {
    if (commandPrefix.test(value) || shellOperator.test(value)) {
      return [`${path.join(".")}: command-shaped value`];
    }
    return [];
  }
  if (Array.isArray(value)) {
    return value.flatMap((entry, index) =>
      collectAuthorityViolations(entry, [...path, String(index)]),
    );
  }
  if (typeof value !== "object" || value === null) {
    return [];
  }

  return Object.entries(value).flatMap(([key, entry]) => {
    const normalizedKey = key.replaceAll(/[-_]/g, "").toLowerCase();
    const keyViolations = forbiddenAuthorityKeys.has(normalizedKey)
      ? [`${[...path, key].join(".")}: forbidden authority key`]
      : [];
    return [...keyViolations, ...collectAuthorityViolations(entry, [...path, key])];
  });
}

describe("downstream patch policy", () => {
  it("contains declarations without executable or deployment authority", async () => {
    const manifest = await readManifest();
    expect(collectAuthorityViolations(manifest)).toEqual([]);
  });

  it("pins the trusted upstream identity and excludes untrusted sources", async () => {
    const manifest = await readManifest();
    expect(manifest).toMatchObject({
      schema: "oracle.downstream-patch-policy/v2",
      trustedUpstream: {
        repository: "https://github.com/steipete/oracle.git",
        mainRef: "refs/heads/main",
        identityMatching: "authenticated-github-account-and-exact-git-identity",
        trustedReleaseIdentity: {
          name: "Peter Steinberger",
          email: "steipete@gmail.com",
          githubLogin: "steipete",
        },
        candidateClasses: [
          {
            kind: "tagged-release",
            requiresReachableFromMain: true,
            requiresTrustedTagger: true,
            requiresTrustedAuthor: true,
            requiresTrustedCommitter: true,
            requiresVerifiedSignature: true,
          },
          {
            kind: "main-commit",
            requiresReachableFromMain: true,
            requiresTrustedAuthor: true,
            requiresTrustedCommitter: true,
            requiresVerifiedSignature: true,
          },
        ],
        excludedCandidateClasses: [
          "open-pull-request",
          "unmerged-branch",
          "third-party-authored-commit",
          "third-party-committed-commit",
        ],
      },
    });
  });

  it("accounts for every downstream first-parent commit exactly once", async () => {
    const manifest = await readManifest();
    const lineage = requireObject(manifest.downstreamLineage, "downstreamLineage");
    const lineageCommits = requireStringArray(
      lineage.firstParentCommits,
      "downstreamLineage.firstParentCommits",
    );
    expect(lineage).toMatchObject({
      upstreamBaseCommit: "083bba7e61f487ad3d99b42039d9f603f61dc4ff",
      lastReviewedRuntimeHeadCommit: "60b4919eb55723305a25288988affc9cc5236d8f",
    });
    expect(lineageCommits).toHaveLength(25);

    const familyValues = requireArray(manifest.semanticPatchFamilies, "semanticPatchFamilies");
    const families = familyValues.map((value, index) =>
      requireObject(value, `semanticPatchFamilies[${index}]`),
    );
    expect(families).toHaveLength(7);

    const familyCommits = families.flatMap((family, index) =>
      requireStringArray(
        family.firstParentCommits,
        `semanticPatchFamilies[${index}].firstParentCommits`,
      ),
    );
    expect(familyCommits).toHaveLength(new Set(familyCommits).size);
    expect([...familyCommits].sort()).toEqual([...lineageCommits].sort());

    for (const [index, family] of families.entries()) {
      expect(
        requireStringArray(
          family.verificationInvariants,
          `semanticPatchFamilies[${index}].verificationInvariants`,
        ).length,
      ).toBeGreaterThan(0);
      expect(
        requireStringArray(
          family.verificationTests,
          `semanticPatchFamilies[${index}].verificationTests`,
        ),
      ).toEqual(expect.arrayContaining([expect.stringMatching(/^tests\/.+\.test\.ts$/)]));
    }
  });

  it("maps every advertised capability to a semantic patch family", async () => {
    const manifest = await readManifest();
    const familyValues = requireArray(manifest.semanticPatchFamilies, "semanticPatchFamilies");
    const familyIds = new Set(
      familyValues.map((value, index) => {
        const family = requireObject(value, `semanticPatchFamilies[${index}]`);
        return requireString(family.id, `semanticPatchFamilies[${index}].id`);
      }),
    );

    const capabilityValues = requireArray(
      manifest.advertisedCapabilities,
      "advertisedCapabilities",
    );
    expect(capabilityValues).toHaveLength(8);
    for (const [index, value] of capabilityValues.entries()) {
      const capability = requireObject(value, `advertisedCapabilities[${index}]`);
      const verifiedByFamilies = requireStringArray(
        capability.verifiedByFamilies,
        `advertisedCapabilities[${index}].verifiedByFamilies`,
      );
      expect(verifiedByFamilies.length).toBeGreaterThan(0);
      expect(verifiedByFamilies.every((familyId) => familyIds.has(familyId))).toBe(true);
    }
  });
});
