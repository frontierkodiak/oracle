# Interrupted-run collection

When capture-only is enabled on `oracle serve`, interrupted durable runs (`unknown`)
are automatically considered for read-only collection at startup and every five
seconds. The startup discovery batch and each sweep are limited to 32 records.
Existing capture-only and maintenance-grant runs are excluded. Canceled originals
and capture children pause collection until an explicit operator resume.

```
oracle remote reconcile <run-id> --json
oracle remote collect <run-id> --inspect --json
oracle remote reconcile <run-id> --resume --json
oracle remote reconcile <historical-run-id> --use-current-profile --json
oracle remote recover --session-id <id> --json
```

Both reconcile aliases use the same scheduler as automatic collection. The authenticated
`GET /v1/runs/:id/reconciliation` endpoint reads the receipt (or `null` before
scheduling); `POST` with an empty body or `{}` creates an idempotent intent. A structured `{ "action": "resume" }` requests a new bounded retry budget after
attention; adding `"useCurrentProfile": true` deliberately binds missing historical
profile provenance. No conversation, browser-path, or prompt overrides are accepted. Health advertises
`oracle.remote.reconciliation` version 1. Ordinary v1 run snapshots are unchanged.

## Resolving a receipt with no run ID

A submission that times out before its run ID is recorded leaves a durable receipt
with a session, an idempotency key, and a payload hash but no run ID. The client
must not guess whether the service accepted it, and it must not reconstruct and
repost the payload to find out. Instead it asks the service read-only by
idempotency key:

```
GET /v1/runs/by-idempotency-key/:key
```

The run ID is the queue's unique idempotency key, so this returns the ordinary run
snapshot, or `404 { "error": "run_not_found" }` when no run was committed under
that key. Only that documented body is a definite miss. It is operator-authenticated
and `GET`-only; it never admits, dispatches, or cancels work. Health advertises
`oracle.remote.idempotency-lookup` version 1 and a stable `queueId`.

Identity matters: before the first POST, the client records the accepting queue's
`queueId` (and the host as a minimum) in the receipt. A 404 is definite only when
the responding service's `queueId` matches the record; a different queue returns
`identity_mismatch`, and a receipt with no recorded identity returns
`identity_unverified`. Both are non-definite and never permit a resubmit, so a
receipt pointed at the wrong queue — for example `~/.oracle` on 9473 versus
`~/.disprove/oracle` on 9483 — can never dispatch the prompt twice. A `queueId`
survives restarts; the host:port fallback does not, so prefer the advertised ID.

Health classification is deliberate. Only a _healthy_ service that lacks the
capability is `unsupported`; a timeout, 401, 5xx, or refused connection is
`unreachable`, never `unsupported`.

`oracle remote recover --session-id <id>` reads a local receipt and reports one of
(each with its own exit code):

- **found** (0) — a run exists; its state and run ID are printed, and nothing is sent.
- **not_found** (2) — a documented miss from the same queue; the payload may be resubmitted.
- **missing_run** (3) — the receipt records a run ID, but that run is absent from this
  queue. This is never reported as "never accepted".
- **unreachable** (4) — the service could not be reached, so the outcome stays unknown.
- **unsupported** (5) — a healthy service predates the lookup capability; upgrade the bridge.
- **identity_mismatch** (6) — the receipt belongs to a different queue.
- **identity_unverified** (7) — the receipt records no queue identity.
- **not_found_unverified** (8) — a 404 that was not the documented run_not_found body.

Recovery of any pre-existing receipt without a run ID — a timeout, a crash mid-POST,
or the explicit `submission: "unknown"` flag — uses the same lookup first: a committed
run is adopted, every non-definite outcome (unreachable, unsupported, mismatch,
unverified) fails closed without resubmitting, and only a definite not-found from the
same queue permits the same-key idempotent POST. A brand-new receipt whose first POST
has not happened is submitted directly, so an older service keeps accepting new work.
Messaging distinguishes "not found" from "unreachable" so an operator never treats a
network failure as proof the work was never accepted.

## Collection semantics

Collection only uses `runtimeHint.conversationId` stored by the original browser
run. A URL in the submitted request is not evidence of the resulting conversation.
Missing identity yields `missing_identity`. New browser dispatches persist the host profile identity privately before access;
a changed profile yields `profile_mismatch`. Historical jobs without a dispatch
binding stay `profile_unbound`. `--use-current-profile` explicitly selects the
current host profile and records `operator_current_profile` provenance; plain
resume does not create a binding.
Neither state is repaired by guessing or by sending the original prompt again.

Each capture has a persisted observation identity before admission. Its ordinary
queue run is admitted using that identity as an idempotency key, with an empty
prompt, no attachments, no follow-ups, and `captureOnly: true`. The ordinary queue
owns its browser lease and applies capacity and maintenance-drain rules. No
maintenance grant is acquired or reused. A live lease is never expired by a timer;
existing service restart reconciliation makes interrupted child leases `unknown`.
The collection sweep itself is serialized inside the service process.

The collector verifies a child's immutable manifest and provider-native raw,
evidence, and independent documents before publishing to the transcript ledger.
A crash after artifact publication can therefore resume without another browser
read. A crash after ledger publication replays the same observation identity;
that identity can only be reused with the same conversation and exact three
source hashes. A partial artifact directory belongs to its failed capture and
cannot wedge a fresh retry, which receives a new capture identity.

Capture failures have at most three attempts, with persisted exponential backoff
starting at 60 seconds and respecting any longer provider throttle. Auth and
challenge failures appear as `auth_unavailable` and `challenged`; exhaustion is
`retry_exhausted`. Ledger publication failures retry the verified existing capture
with the same observation identity, also bounded to three attempts. Repeated plain POSTs do not bypass these limits or reset a failure. Explicit
`--resume` grants another three attempts while preserving history and reusing the
same observation on ledger-publication retry. Receipts retain failed capture
identities, the final observation ID, revision ID, and exact source/manifest hashes and verified artifact descriptors. Descriptors enable
retrieval through existing artifact endpoints even if the child remains `unknown`.
The CLI currently prints receipts; it does not download the artifact set.

A successful receipt is **`captured_unattributed`**: the conversation was collected,
but its assistant messages have not been attributed to the interrupted request.
Historical runs lack a submitted user-node anchor. The original run remains
`unknown`, and neither the latest assistant turn nor answer digest membership is
used to declare provider completion. This implementation does not monitor a
particular answer to completion, resend prompts, use an API fallback, or repair
historical attribution.

Tests use a fake browser and isolated queue/ledger stores. They cover process loss,
artifact and ledger crash boundaries, idempotent publication, wrong conversation,
backoff, capacity/drains, and compatibility with existing v1 clients. Live provider
behavior and cross-process concurrent service ownership are not established by
these tests; the collector follows the existing queue's single-service ownership
model.
