---
id: 2
slug: deliver-ordered-independent-readers-with-checkpoints
title: "Deliver ordered independent readers with checkpoints"
kind: exec-plan
created_at: 2026-11-25T05:44:26Z
master_plan: "docs/masterplans/1-build-the-shibuya-redis-streams-adapter.md"
provenance:
  created_by:
    model: "gpt-6.2-astra"
    harness: "codex-cli"
    at: 2026-11-25T05:44:26Z
---

# Deliver ordered independent readers with checkpoints

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.
If durable project context changes, update or create ADRs in docs/adr/ in the same change.
Record friction met while implementing in the friction-log bundle as it occurs (see FRICTION.md).


## Purpose / Big Picture


Two independent projections can consume every retained entry and resume at different checkpoints. A live subscriber can attach to a concrete snapshot cursor without losing a change appended during handover.


## Progress


- [ ] Independent starts and checkpoint restart yield the expected ordered IDs on two connections.
- [ ] Retry, permanent rejection, history loss and crash boundaries preserve contiguous completed progress.


## Surprises & Discoveries





## Decision Log





## Outcomes & Retrospective





## Context and Orientation


Complete [docs/plans/1-bootstrap-a-buildable-adapter-and-redis-runner-fixture.md](../../docs/plans/1-bootstrap-a-buildable-adapter-and-redis-runner-fixture.md) first. It supplies the Cabal package, binary identity and failure record codecs, bounded connection bracket and real Redis/runApp fixture. The [initial specification](../initial-spec.md) defines three starts. A checkpoint is the last entry whose completion the reader has saved. An exclusive stream read returns entries strictly after its input ID. Use the released contracts behind `mori://shinzui/shibuya/packages/shibuya-core`, `mori://informatikr/hedis/packages/hedis` and `mori://shinzui/shibuya/docs/architecture`. The runner owns handler scheduling, admission and repeated finalization on transient exceptions. The adapter supplies a Streamly stream in Eff, an AckHandle per delivery, shutdown and AdapterTelemetry. A superseded item can skip both handler and finalizer. Release resources for it even when neither runs.

Mori indexes the architecture guide but has no decision bundle for `mori://shinzui/shibuya`. Artifact-level decision URIs are pending. In that project, the relevant records are `docs/adr/0003-make-processor-termination-and-shutdown-outcomes-explicit.md`, `docs/adr/0010-pause-processors-at-the-adapter-source-and-report-pause-as-operator-intent.md`, `docs/adr/0012-keep-telemetry-ownership-and-disabled-context-contracts-explicit.md` and `docs/adr/0013-let-adapters-supersede-a-delivery-before-its-handler-runs.md`. They distinguish deliberate halt from failed finalization, gate intake before source pulls, preserve caller telemetry ownership and give the adapter a prompt, side-effect-free admission check. There are no local ADRs at creation.


## Plan of Work


Add src/Shibuya/Adapter/Redis/Reader.hs and export ReaderConfig, ReaderStart, checkpoint callbacks and a reader constructor from Shibuya.Adapter.Redis. Scope load/save by logical reader and source identity including optional generation. A loaded checkpoint wins over the configured fallback start. Earliest begins at 0-0, After begins at the supplied ID, and Live resolves the stream's last-generated ID once (0-0 for a missing stream). Persist that initial boundary when checkpoints are configured, before intake. Reconnect from the known position. Track the fetch position privately, cap the prefetch buffer and expose the completed position. Save or conditionally advance it only after all earlier work succeeded. Support serial readers by admitting only one unresolved entry at a time, even when several have already been fetched. Enforce that invariant inside the source. Reject runner configurations requiring batches of multiple unresolved items through the supported setup API.

Build decision handling around the head of the buffer. AckOk retries the same save until success or bounded finalizer failure. AckRetry schedules that same entry behind a cancellable timer, increments a known local attempt, and holds later entries. AckHalt closes intake at the prior cursor. For a permanent decode failure or AckDeadLetter, write the scoped failure record before saving, or stop intake with the cursor intact when there is no destination. Ensure the runner reports an observable terminal outcome, and retries after uncertain writes reuse the same identity. Expose an explicit gap callback/outcome with source and evidence: compare producer generation/predecessor or revision supplied by the caller against the checkpoint, and surface observable rollback or contradictory stream metadata. Allow stop/rebuild, resnapshot or explicit best effort. ID distance alone cannot establish missing history. Document the need to save continuity evidence with the cursor and who owns checkpoint exclusivity.


## Concrete Steps


Add test/ReaderSpec.hs to a Cabal reader-integration suite using the prerequisite fixture. Run from the repository root.

```bash
nix develop -c cabal test reader-integration --test-show-details=direct
```

Synchronize handler admission, application commit and checkpoint save with explicit barriers. Restart with a fresh adapter and a durable test checkpoint.


## Validation and Acceptance


Append A/B/C. Two named readers see A/B/C in order. Pause one, append D, resume from its saved checkpoint and verify both end at D. Start Live once on an empty stream, append between polls, disconnect and reconnect and verify intake. Fetch B while A is running, request retry on A and see A,A,B. Crash after A's idempotent effect but before saving: the same ID returns with one logical effect. Fail a save after dead-letter storage, replay, and inspect one record. Exercise decode rejection, no destination and halt, leaving the cursor before the entry. Use producer revision evidence to detect a trimmed change and generation replacement; separately run two readers After a common snapshot across a concurrent append. Assert the handler never sees a known gap as a normal successor unless best effort was selected.


## Idempotence and Recovery


On a transient read error retain buffered entries and fetch position together, or discard both and reconstruct from the completed boundary. A failed save must keep the head available. Conditional stores should accept an already saved identical checkpoint after reply loss. Separate processes sharing one checkpoint need external exclusion or compare-and-set. State the replay horizon required of the dead-letter destination.


## Interfaces and Dependencies


Use hedis xreadOpts with bounded COUNT and BLOCK on the reserved read connection. The checkpoint interface is application-owned and can be absent for ephemeral readers. Pass the configured AdapterTelemetry through and emit delivery, ack, retry, rejection, save failure and detected gap events through the common observer.
