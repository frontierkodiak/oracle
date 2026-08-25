# Downstream upkeep policy

`downstream/patch-manifest.json` is a declarative record of Oracle's trusted
upstream boundary, downstream lineage, semantic patch families, verification
invariants, and advertised capabilities.

The trusted upstream boundary is the `steipete/oracle` main branch and Peter
Steinberger's exact release identity. Candidates are limited to releases tagged
and signed by that identity or signed main-branch commits whose Git author,
committer, and authenticated GitHub accounts all match it. Open pull requests,
unmerged branches, and third-party commits are excluded.

The manifest accounts for every reviewed first-parent runtime commit from the
`0.18.0` upstream base through the maintenance-safe capture substrate. The
policy commits that carry the manifest are deliberately not presented as
runtime heads. Its seven semantic families describe the behavior that must
remain present when the patch stack changes. The advertised capability list is
the corresponding public contract, including the authenticated maintenance
drain and one-conversation capture-grant protocols.

This repository does not define execution authority for upkeep. In particular,
the manifest contains no commands, executable paths, gate definitions, receipt
decisions, controller overrides, or deployment actions. A trusted external
controller may consume these declarations as evidence, but cannot obtain
authority from them.
