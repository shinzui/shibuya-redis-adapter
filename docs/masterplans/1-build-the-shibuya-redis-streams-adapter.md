---
id: 1
slug: build-the-shibuya-redis-streams-adapter
title: "Build the Shibuya Redis Streams adapter"
kind: master-plan
created_at: 2026-11-25T05:44:10Z
intention: intention_01m89djerpehcanj7cg6j7e431
provenance:
  created_by:
    model: "gpt-6.2-astra"
    harness: "codex-cli"
    at: 2026-11-25T05:44:10Z
  revisions:
    - model: "gpt-6.2-astra"
      harness: "codex-cli"
      at: 2026-11-25T05:54:57Z
      mode: "implement"
      note: "Coordinate bootstrap and runner acceptance."
    - model: "gpt-6.2-sol"
      harness: "codex-cli"
      at: 2026-11-25T13:15:07Z
      mode: "implement"
      note: "Implement and qualify the Redis delivery modes."
---

# Build the Shibuya Redis Streams adapter

This MasterPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.
If durable project context changes, update or create ADRs in docs/adr/ in the same change.


## Vision & Scope


Ship an independently usable Haskell library. A projection can resume its own ordered Redis Stream, a pool can divide jobs and reclaim abandoned assignments, and a live view can attach after a snapshot. Each uses ordinary Shibuya handlers. One instance consumes one stream on one primary. The release gate is every scenario and both consumer examples in [the initial specification](../initial-spec.md). Cluster routing, producer transactions, administration, deployment, cross-stream ordering and Kehai application services are outside this release.


## Decomposition Strategy


Start by proving a binary Redis entry reaches a real Shibuya handler in the generated development shell. Independent readers and group workers then have separate owners because progress is stored differently and a crash has different consequences. Qualify them together against the same Redis services, configuration limits and runner. A split by Redis command would scatter the correctness of a single decision across several plans.

Mori indexes the architecture guide but has no decision bundle for `mori://shinzui/shibuya`. Artifact-level decision URIs are pending. In that project, the relevant records are `docs/adr/0003-make-processor-termination-and-shutdown-outcomes-explicit.md`, `docs/adr/0010-pause-processors-at-the-adapter-source-and-report-pause-as-operator-intent.md`, `docs/adr/0012-keep-telemetry-ownership-and-disabled-context-contracts-explicit.md` and `docs/adr/0013-let-adapters-supersede-a-delivery-before-its-handler-runs.md`. They distinguish deliberate halt from failed finalization, gate intake before source pulls, preserve caller telemetry ownership and give the adapter a prompt, side-effect-free admission check. There are no local ADRs at creation.


## Exec-Plan Registry


| # | Title | Path | Hard deps | Soft deps | Status |
|---|---|---|---|---|---|
| 1 | Bootstrap a buildable adapter and Redis runner fixture | [docs/plans/1-bootstrap-a-buildable-adapter-and-redis-runner-fixture.md](../../docs/plans/1-bootstrap-a-buildable-adapter-and-redis-runner-fixture.md) | None | None | Complete |
| 2 | Deliver ordered independent readers with checkpoints | [docs/plans/2-deliver-ordered-independent-readers-with-checkpoints.md](../../docs/plans/2-deliver-ordered-independent-readers-with-checkpoints.md) | EP-1 | None | Complete |
| 3 | Distribute and recover fenced consumer group work | [docs/plans/3-distribute-and-recover-fenced-consumer-group-work.md](../../docs/plans/3-distribute-and-recover-fenced-consumer-group-work.md) | EP-1 | None | Complete |
| 4 | Qualify both delivery modes for release | [docs/plans/4-qualify-both-delivery-modes-for-release.md](../../docs/plans/4-qualify-both-delivery-modes-for-release.md) | EP-2, EP-3 | None | Complete |


## Dependency Graph


Complete EP-1 before either delivery mode starts. Both require its envelope representation, resource lifetime and executable runner fixture. EP-2 and EP-3 can then proceed in parallel. EP-4 consumes both finished public constructors and their mode suites. Its release matrix is the final integration gate.


## Integration Points


EP-1 owns the root Cabal file, Shibuya.Adapter.Redis facade, flake, test harness, binary identity codec, dead-letter storage primitive, telemetry event vocabulary and pool lifetime contract. Each mode contributes its own internal module, tests and facade exports through those extension points. EP-1 proves the full envelope/admission/finalizer path with a one-entry fixture before either mode expands. EP-2 owns checkpoints and history evidence. EP-3 owns pending-work keys, retry times and ownership tokens. Their durable data must use disjoint, versioned namespaces. Both consume the same logical rejection identity and reason encoding. EP-4 owns examples, release instructions and compatibility evidence, including testing both modes on one stream. Record the stable key format, checkpoint ownership, group fencing protocol and shutdown resource boundary as ADRs when verified.


## Progress


All four child plans are complete. All four integration suites passed on Redis 7.2.17 and 8.10.1 on Apple Silicon. The managed-service demos, Apple Silicon flake checks and unpacked Cabal build passed.


## Surprises & Discoveries


The read pool must close after polling stops, while the command pool must remain usable through the runner drain. The [reader checkpoint](../adr/0003-commit-reader-checkpoints-in-stream-order.md) and [group attempt state](../adr/0004-fence-group-finalizers-with-redis-attempt-state.md) own different progress mechanisms. Both modes share the [resource lifetime](../adr/0002-keep-command-connections-through-runner-drain.md) and [wire identity and rejection contracts](../adr/0001-stream-identity-and-rejection-records.md).





## Decision Log


2026-11-24 — Give each delivery mode its entire finalization path and share only the wire, resource and failure-record contracts. This lets each mode prove recovery under its own progress model before the combined release test.


## Outcomes & Retrospective

The adapter ships ordered readers with restartable checkpoints and consumer-group workers with fenced recovery. The combined suite verified that both modes can share a stream, that a projection resumes after Redis restarts, and that a halted worker leaves its pending job recoverable without interrupting the reader. The [examples](../../README.md#run-both-examples) and [compatibility guide](../compatibility.md) document the public setup and verified operating conditions.

Durable decisions are recorded in the local ADRs for wire identity, shutdown, checkpoint ownership and group fencing. No initiative-level friction remains open.
