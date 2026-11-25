---
id: 4
slug: qualify-both-delivery-modes-for-release
title: "Qualify both delivery modes for release"
kind: exec-plan
created_at: 2026-11-25T05:44:26Z
master_plan: "docs/masterplans/1-build-the-shibuya-redis-streams-adapter.md"
provenance:
  created_by:
    model: "gpt-6.2-astra"
    harness: "codex-cli"
    at: 2026-11-25T05:44:26Z
---

# Qualify both delivery modes for release

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.
If durable project context changes, update or create ADRs in docs/adr/ in the same change.
Record friction met while implementing in the friction-log bundle as it occurs (see FRICTION.md).


## Purpose / Big Picture


An application author can build a projection and a competing worker, select Redis permissions and versions using published evidence, and understand what happens on disconnect, crash, trim or shutdown.


## Progress


- [ ] Both examples run through the public constructors and real Shibuya processor runner.
- [ ] A reproducible server matrix passes every initial-spec scenario, including lifecycle and telemetry checks.


## Surprises & Discoveries





## Decision Log





## Outcomes & Retrospective





## Context and Orientation


Complete [docs/plans/2-deliver-ordered-independent-readers-with-checkpoints.md](../../docs/plans/2-deliver-ordered-independent-readers-with-checkpoints.md) and [docs/plans/3-distribute-and-recover-fenced-consumer-group-work.md](../../docs/plans/3-distribute-and-recover-fenced-consumer-group-work.md) first. They supply the two modes, per-mode recovery tests, owned/borrowed pool bracket and the socket-based real-server fixture. [The acceptance table](../initial-spec.md#acceptance-scenarios) defines eight observable scenarios. Use the released contracts behind `mori://shinzui/shibuya/packages/shibuya-core`, `mori://informatikr/hedis/packages/hedis` and `mori://shinzui/shibuya/docs/architecture`. The runner owns handler scheduling, admission and repeated finalization on transient exceptions. The adapter supplies a Streamly stream in Eff, an AckHandle per delivery, shutdown and AdapterTelemetry. A superseded item can skip both handler and finalizer. Release resources for it even when neither runs.

Mori indexes the architecture guide but has no decision bundle for `mori://shinzui/shibuya`. Artifact-level decision URIs are pending. In that project, the relevant records are `docs/adr/0003-make-processor-termination-and-shutdown-outcomes-explicit.md`, `docs/adr/0010-pause-processors-at-the-adapter-source-and-report-pause-as-operator-intent.md`, `docs/adr/0012-keep-telemetry-ownership-and-disabled-context-contracts-explicit.md` and `docs/adr/0013-let-adapters-supersede-a-delivery-before-its-handler-runs.md`. They distinguish deliberate halt from failed finalization, gate intake before source pulls, preserve caller telemetry ownership and give the adapter a prompt, side-effect-free admission check. There are no local ADRs at creation.


## Plan of Work


Add examples/Reader.hs and examples/Worker.hs as independently buildable Cabal executables. Both import Shibuya.Adapter.Redis and run ordinary processors. The reader resumes from a checkpoint and the worker uses explicit group creation/attachment and a per-instance consumer ID. Give the examples a bounded demo invocation that prints observed source IDs and exits after a requested count. Show an application-owned idempotency key. Create README.md with build, local Redis, mode selection, startup and stop instructions.

Add test/ReleaseSpec.hs and a release-integration target for interactions beyond the mode suites. Run a projection and two competing workers on the same stream. Test the snapshot handshake from `mori://shinzui/kehai/docs/initial-spec` with two After-cursor readers, a producer epoch/revision and a change inserted between snapshot and attach. Assert catch-up or explicit resnapshot. Hold a blocking read, stall a handler, disconnect and restore the fixture with known persistence. Exercise graceful and forced stop, repeat shutdown and inspect pending state and saved checkpoints from a fresh connection. Confirm sibling progress under an independent supervision strategy and keep caller pools usable. Inject bad ordering/concurrency/limits and require rejection before intake. Verify finite reconnect and shutdown budgets, bounded prefetch, recovery and retry counts. Timeouts signal a test failure; barriers establish the order of events.

In docs/compatibility.md record tested server builds, compiler, dependency versions, capabilities and command/key ACLs per mode including scripts, recovery, telemetry sampling, optional creation and rejection storage. Run the tests on the oldest and newest versions claimed plus any capability boundary that changes the implementation. Drive the same suite against an explicitly selected redis-server executable per run; make all fixtures including mode suites honor that selector. Prefer local Apple containers for Linux when needed. Exercise dedicated restricted credentials for each permission set. Publish retention, persistence, eviction, restore, key recreation, checkpoint ownership and deduplication window assumptions. Explain the difference between a quiet poll, recoverable disconnect, detected loss, stale finalizer and terminal rejection. State when replay is application-owned.


## Concrete Steps


From the repository root, expose the fixture server selector REDIS_SERVER in the harness (default redis-server from the shell). Run the following once per server selected for the matrix. Record actual server version, revision and command outcomes.

```bash
nix develop -c cabal build all
nix develop -c cabal test all --test-show-details=direct
nix develop -c cabal run reader-example -- --help
nix develop -c cabal run worker-example -- --help
nix flake check
```

Use the README demo commands as executable acceptance steps against a unique stream and the generated service. Cross-reference each scenario in docs/compatibility.md to its named test, server version and observed result.


## Validation and Acceptance


Require all eight spec scenarios to pass with independent connections and explicit delivery, claim and finalization barriers. Verify bounded labels for delivered, acknowledged, retried, reclaimed, superseded, dead-lettered, checkpoint failure, gap, pending depth and age where available. Inspect both enabled and disabled telemetry paths for caller resource lifetime and leakage of keys, payloads or credentials. Execute each example, append known entries, and verify IDs in output and durable progress in Redis or the checkpoint file. Validate the published source package includes scripts and examples by building an unpacked Cabal source distribution.


## Idempotence and Recovery


Every test run owns a private server or isolated namespace. Restart only fixture-owned processes and retain fixture data for persistence scenarios. Repeat the combined suite after restart and compare durable IDs and pending owners. Limit compatibility claims to passing runs and document gaps if a proposed server cannot meet a required capability.


## Interfaces and Dependencies


The examples use only the public API and Shibuya runner. The release suite reuses the mode tests and adds cross-mode and lifecycle assertions. Return protocol fixes to the mode owner and rerun all affected versions before recording acceptance.
