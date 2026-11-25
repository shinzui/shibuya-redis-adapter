---
id: 2
slug: harden-the-redis-adapter-for-a-bug-free-first-release
title: "Harden the Redis adapter for a bug-free first release"
kind: master-plan
created_at: 2026-11-25T13:50:20Z
intention: "intention_01m89fh99nex0r6hchenpa2c1b"
provenance:
  created_by:
    model: "claude-fable-5-2"
    harness: "claude-code"
    at: 2026-11-25T13:50:20Z
  revisions:
    - model: "gpt-6.2-sol"
      harness: "codex-cli"
      at: 2026-11-25T14:01:47Z
      mode: "implement"
      note: "Coordinate release hardening and global Jaeger verification"
---

# Harden the Redis adapter for a bug-free first release

This MasterPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.
If durable project context changes, update or create ADRs in docs/adr/ in the same change.


## Vision & Scope


The adapter in this repository connects Shibuya processors to Redis Streams in two modes: an independent reader (`newReader`) and a consumer-group worker (`newGroupWorker`). A pre-publication review on 2026-11-25 found one confirmed delivery bug, missing OpenTelemetry trace propagation, a package Hackage would reject, and a set of smaller correctness, API and efficiency problems. This initiative fixes every finding so that version 0.1.0.0 can be published without known defects.

When the initiative is complete:

- A worker that returns `AckRetry`, or whose handler throws, receives the entry again itself after the requested delay. Today a single-worker group never sees a retried entry again.
- A message that exhausts its retries is written to the configured dead-letter hash with Shibuya's `MaxRetriesExceeded` reason and the processor keeps running. Today it stops each worker that touches it, one after another.
- A consumer span links to, or is a child of, the span that produced the stream entry, and carries `messaging.system = "redis"`. Today the producer's `traceparent` is never read.
- The exposed module exports only what an application needs, every export is documented, and internals live in a module with no compatibility promise.
- `cabal check` passes, bounds follow Shibuya's conventions, and all suites pass on both qualified Redis builds.

Out of scope: uploading to Hackage and tagging the release (use the `agents/skills/release` skill after the owner approves), a decoder for dead-letter records, a Redis-side index of retry due times, Redis Cluster, and any new delivery mode.

The sixteen review findings, with the child plan that owns each:

1. A consumer never reclaims its own retried entry (`src/Shibuya/Adapter/Redis/Group/Scripts.hs`, the self-claim guard). EP-5.
2. `Envelope.traceContext` and `Envelope.attributes` are never set, and no test uses a real tracer. EP-7.
3. Packaging: no license, changelog, metadata or upper bounds; `effectful-core` range; examples built unconditionally. EP-9.
4. A group retry delay is floored at `minimumIdleMillis`. EP-5.
5. Retry exhaustion stops the processor even when a dead-letter destination exists. EP-5 for groups, EP-6 for readers.
6. Telemetry options live on `RuntimeOptions` instead of a per-mode `telemetry` field. EP-7.
7. The exposed module exports internals. EP-8.
8. `readerName` and `groupAdapterName` name the same concept differently. EP-8.
9. Default configurations are built positionally. EP-8.
10. Almost nothing has Haddock documentation. EP-8.
11. The pending scan runs one script call per row, including the worker's own in-flight rows. EP-5.
12. The full Lua script is sent on every call. EP-5.
13. A reader's rejection write for an undecodable entry has no connection retry. EP-6.
14. A stale `leaseExtend` does not change `deliveryStatus`. EP-5.
15. Dead-letter records carry no trace context from the failing consumer. EP-7.
16. One reader or worker per runtime is documented but not enforced. EP-8.


## Decomposition Strategy


The work is split by the behavior a user observes, and so that no two plans rewrite the same function. Group delivery (script, scan and finalizer) is one plan because its fixes all change `Group.hs` and the one Lua script, and each fix alters what the next one must test. Reader failure handling is separate because it touches only `Reader.hs` and has its own progress model. Telemetry is separate because it changes envelope construction and configuration for both modes but none of their retry logic. The API plan comes after the three behavior plans so that it renames, relocates and documents a surface that has stopped moving. Packaging and qualification is last because its evidence must describe the final code.

Two alternatives were rejected. Reshaping the API first would let later plans add fields under final names, but it would put a rename of every module in front of the one confirmed delivery bug. A single plan for "retry exhaustion" across both modes would split `Group.hs` between two plans.

Relevant local ADRs:

- [ADR 1](../adr/0001-stream-identity-and-rejection-records.md) defines the message identity and the dead-letter record format. EP-7 extends the record value with trace context and must amend this ADR.
- [ADR 2](../adr/0002-keep-command-connections-through-runner-drain.md) defines the read pool, command pool and shutdown boundary. No plan changes it; EP-8's attach check must respect it.
- [ADR 3](../adr/0003-commit-reader-checkpoints-in-stream-order.md) defines reader checkpoint order. EP-6 must write a dead-letter record before moving the checkpoint, as this ADR requires.
- [ADR 4](../adr/0004-fence-group-finalizers-with-redis-attempt-state.md) defines the group fencing protocol. EP-5 changes retry eligibility and must amend this ADR.

Cross-repository decisions come from the Shibuya core project, `mori://shinzui/shibuya`. Mori has no decision bundle for that project, so artifact-level URIs are pending; the records are at these project-relative paths:

- `docs/adr/0005-exclude-regressed-effectful-core-releases-in-every-package.md`: every Shibuya package declares `effectful-core (>=2.6.1 && <2.7) || (>=2.7.1.1 && <2.8)` in every component. EP-9 applies it.
- `docs/adr/0012-keep-telemetry-ownership-and-disabled-context-contracts-explicit.md`: adapters supply incoming context and attributes; the framework owns the consumer span.
- `docs/adr/0013-let-adapters-supersede-a-delivery-before-its-handler-runs.md`: `deliveryStatus` must be prompt and free of side effects. EP-5 relies on it.
- `docs/adr/0014-define-consumer-correlation-and-bounded-batch-link-accounting.md`: defines `ParentAndLink` and `LinkOnly`, and records that replaying sources choose `LinkOnly`. EP-7 follows it.

The adapter-author guidance for tracing is `docs/user/opentelemetry.md` in the same project.


## Exec-Plan Registry


| # | Title | Path | Hard Deps | Soft Deps | Status |
|---|-------|------|-----------|-----------|--------|
| 5 | Correct consumer-group retry, recovery and lease fencing | [docs/plans/5-correct-consumer-group-retry-recovery-and-lease-fencing.md](../plans/5-correct-consumer-group-retry-recovery-and-lease-fencing.md) | None | None | Complete |
| 6 | Dead-letter exhausted reader retries and retry reader rejection writes | [docs/plans/6-dead-letter-exhausted-reader-retries-and-retry-reader-rejection-writes.md](../plans/6-dead-letter-exhausted-reader-retries-and-retry-reader-rejection-writes.md) | None | None | Complete |
| 7 | Propagate trace context and Redis span attributes | [docs/plans/7-propagate-trace-context-and-redis-span-attributes.md](../plans/7-propagate-trace-context-and-redis-span-attributes.md) | None | EP-5, EP-6 | Complete |
| 8 | Narrow, name and document the public API | [docs/plans/8-narrow-name-and-document-the-public-api.md](../plans/8-narrow-name-and-document-the-public-api.md) | EP-5, EP-6, EP-7 | None | Complete |
| 9 | Package and qualify the 0.1.0.0 release | [docs/plans/9-package-and-qualify-the-0-1-0-0-release.md](../plans/9-package-and-qualify-the-0-1-0-0-release.md) | EP-8 | None | Complete |

Row numbers equal the plan file numbers, so EP-5 is plan 5.


## Dependency Graph


EP-5 and EP-6 have no prerequisites and touch disjoint modules, so they can run in parallel. Do EP-5 first when working alone, because it contains the confirmed bug.

EP-7 has no hard prerequisite. It benefits from EP-5 and EP-6 being complete because its dead-letter milestone adds trace context to every rejection write, including the retry-exhaustion writes those two plans introduce. If EP-7 runs first, EP-5 and EP-6 must pass trace context at their new call sites; the Integration Points section states how.

EP-8 requires EP-5, EP-6 and EP-7 to be Complete. It moves definitions between modules, renames a configuration field and documents every export; doing that while behavior plans are still editing the same definitions would produce conflicts and stale documentation.

EP-9 requires EP-8 to be Complete. Its `cabal check`, source-archive build, documentation refresh and two-server test matrix are evidence about the final surface.

The recommended order for one contributor is EP-5, EP-6, EP-7, EP-8, EP-9.


## Integration Points


**The dead-letter record (`rejectionRecord` in `src/Shibuya/Adapter/Redis/DeadLetter.hs`).** EP-5 and EP-6 add new callers that write a `MaxRetriesExceeded` record. EP-7 owns the record's format and adds a trace-context argument. Whichever plan lands second adapts to the first: if EP-7 is complete, the new callers pass the current trace headers from the finalizer; if not, EP-7 updates every caller it finds, including the retry-exhaustion ones. The hash field (the idempotency key) never changes, so a record written before and after EP-7 for the same entry still collapses to one. EP-7 amends ADR 1.

**Retry-exhaustion semantics.** EP-5 and EP-6 must agree: with a dead-letter destination configured, exhaustion writes one record with reason `MaxRetriesExceeded`, emits the `DeadLettered` event and advances; without a destination, the processor stops and leaves progress before the entry (`GroupRetryLimit` or `ReaderRetryLimit`). The limit keeps its current meaning in each mode.

**Configuration records (`ReaderConfig` in `Reader.hs`, `GroupConfig` in `Group.hs`, `RuntimeOptions` in `Connection.hs`).** EP-7 adds a `telemetry` field and a trace-context hook to both mode records and removes `adapterTelemetry` from `RuntimeOptions`. EP-8 owns final field names and converts the default constructors to named fields. EP-5 and EP-6 add no configuration fields.

**The Lua script and Redis permissions.** EP-5 owns `src/Shibuya/Adapter/Redis/Group/Scripts.hs` and introduces `SCRIPT LOAD` and `EVALSHA`. It updates the worker row of the permissions table in `docs/compatibility.md` and the restricted-credential test in `test/ReleaseSpec.hs` in the same change. EP-9 re-verifies the table against the final code.

**The exposed module (`src/Shibuya/Adapter/Redis.hs`) and the Cabal file.** EP-5, EP-6 and EP-7 may add exports and dependencies they need. EP-8 owns the final export list and module layout. EP-9 owns metadata, bounds, flags and the final dependency ranges; EP-7 adds its OpenTelemetry dependencies with provisional ranges that EP-9 confirms against Hackage.

**User documentation (`README.md`, `docs/compatibility.md`).** Each behavior plan corrects the sentences its change makes false. EP-9 owns the final read-through and the qualification tables.

Decisions that should become or amend ADRs: retry eligibility no longer requires the idle interval (EP-5, amend ADR 4); retry exhaustion dead-letters when a destination exists (EP-5 and EP-6, amend ADR 3 and ADR 4); trace context in rejection records and default correlation per mode (EP-7, amend ADR 1 and add a telemetry ADR); the public/internal module boundary (EP-8, new ADR).


## Progress


All five child plans are complete. The release candidate passed all five suites on Redis 7.2.17 and 8.10.1. Cabal check, source-archive build, Haddock, Nix checks and the global Jaeger trace check are complete.


## Surprises & Discoveries


The review reproduced finding 1 directly against Redis 8.10.1 by running the script by hand: after `register` and `retry` for consumer `a`, a `claim` by `a` returned `skip` on every attempt while a `claim` by consumer `b` returned `claimed`. The existing `retry and rejection` test in `test/GroupSpec.hs` did not catch it because it restarts the worker under a different consumer name.


## Decision Log


- Decision: Keep the per-row claim script and remove only the calls for the worker's own in-flight rows; do not add a Redis-side index of retry due times.
  Rationale: The scan is already bounded by `groupScanLimit` per pass. An index adds durable state that must stay consistent with the pending list, which is a larger risk than its cost saving justifies in a first release. Recorded as a deliberate exclusion.
  Date: 2026-11-25

- Decision: Reshape the public API after the behavior fixes, not before.
  Rationale: The confirmed delivery bug should not wait behind a rename of every module, and documentation is written once against stable behavior.
  Date: 2026-11-25

- Decision: Exclude the Hackage upload and release tag from this initiative.
  Rationale: Publishing is outward-facing and irreversible; the owner triggers it through the release skill once EP-9's evidence is in.
  Date: 2026-11-25


## Outcomes & Retrospective


The adapter now redelivers a worker’s own retries and records exhausted retries before advancing when configured to dead-letter. The reader recovers its rejection write after a dropped command connection, while worker finalizers remain fenced across takeover. Both modes propagate producer context and record the consumer context on traced rejections. The facade has complete Haddock coverage and enforces one processor per runtime. Version 0.1.0.0 passed the [release matrix](../compatibility.md) and the [global Jaeger check](../plans/9-package-and-qualify-the-0-1-0-0-release.md#outcomes--retrospective).
