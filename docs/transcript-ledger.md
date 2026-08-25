# Private transcript ledger

Oracle can keep a local, provider-faithful archive of ChatGPT conversations
without sending transcript bodies to Calyx, QMD, the vault, or another index.
The ledger is independent of the session log and is disabled as a discovery
surface: nothing is crawled until a watch is explicitly seeded and synced.

The default root is `$ORACLE_HOME_DIR/transcript-ledger` (normally
`~/.oracle/transcript-ledger`). Set `ORACLE_TRANSCRIPT_LEDGER_DIR` or pass
`--root` to choose another private root. The root, its owned directories, the
SQLite index, and object files are owner-only. Symlinked ledger descendants are
rejected. The index uses SQLite WAL mode. Raw provider JSON and evidence JSON
are immutable, content-addressed objects; the raw bytes are authoritative.
Ingest validates the provider mapping graph (including exactly one connected
root), selected current-node chain, and evidence-to-turn digest correspondence
before publication. The independent-fetch descriptor is closed and requires a
real 32-byte decimal digest, byte count, and timestamp; the materialized
descriptor binds the authoritative raw bytes. Bounded JSON depth, node count,
turn count, body size, and artifact size prevent untrusted captures from
exhausting the process. Publication uses a state-root lock and generation file;
startup recovery only removes orphans while holding that lock, then fails closed
if an indexed object is missing or altered. Reopened objects are repaired to
owner-only permissions.

## Commands

```sh
oracle transcript seed https://chatgpt.com/c/<conversation-id> --profile my-profile
oracle transcript ingest --raw raw.json --evidence evidence.json --profile my-profile
oracle transcript status
oracle transcript sync <conversation-id> --profile my-profile --profile-dir ~/.oracle/browser-profile
oracle transcript sync --all --profile-dir ~/.oracle/browser-profile
oracle transcript schedule <conversation-id> --interval 3600
```

`seed`/`watch` only records watch state. `sync` invokes the existing browser
capture-only path with an empty prompt, provider-native capture enabled, and the
canonical conversation URL. It never types or submits a prompt. A failed,
challenged, or unavailable-auth attempt is an observation, not deletion. The
`schedule` command records an interval for an external scheduler; Oracle does
not start an internal daemon. `sync <thread>` must name one watched thread;
`sync --all` is the explicit opt-in for all enabled watches. A bare `sync` is
rejected. Intervals must be finite and strictly positive.

Each conversation identity is `(provider, opaque provider-profile id,
provider conversation id)`. Every successful observation retains both raw and
evidence hashes. A revision is keyed by a deterministic hash of the selected
current-node branch's normalized turn sequence, so volatile provider metadata
creates a new observation while leaving the logical revision unchanged. Body
changes create a new immutable revision. Normalized turn rows contain role,
content type, body hash/length, parent/node identity, and attachment metadata;
the raw object remains the source of truth.

Automatic best-effort ingest runs after the CLI receives a successful local or
remote browser result containing the paired provider-native artifacts. Ledger
failure produces a body-free typed warning and does not fail or discard the
completed provider result. Remote capture artifacts are ingested on the client
after the existing authenticated transfer verifies them, so the caller's
ledger remains the durable local archive.
