---
id: 3
slug: distribute-and-recover-fenced-consumer-group-work
title: "Distribute and recover fenced consumer group work"
kind: exec-plan
created_at: 2026-11-25T05:44:26Z
master_plan: "docs/masterplans/1-build-the-shibuya-redis-streams-adapter.md"
intention: intention_01m89djerpehcanj7cg6j7e431
provenance:
  created_by:
    model: "gpt-6.2-astra"
    harness: "codex-cli"
    at: 2026-11-25T05:44:26Z
  revisions:
    - model: "gpt-6.2-sol"
      harness: "codex-cli"
      at: 2026-11-25T13:30:30Z
      mode: "implement"
      note: "Implement and verify consumer group assignment, fencing and recovery."
---

# Distribute and recover fenced consumer group work

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.
If durable project context changes, update or create ADRs in docs/adr/ in the same change.
Record friction met while implementing in the friction-log bundle as it occurs (see FRICTION.md).


## Purpose / Big Picture


Competing workers divide new jobs in one group, recover abandoned jobs and preserve pending work when an obsolete handler finishes. A second group can process the full stream independently.


## Progress


- [x] 2026-11-25. Two workers divided assignments, a separate group saw every ID, and a one-entry scan advanced past ineligible rows while admitting new jobs.
- [x] 2026-11-25. Ownership fences survived A→B→A, renewal, retry across worker restart and rejection replay. A restricted Redis credential proved replay after the rejection write succeeded and XACK failed.


## Surprises & Discoveries





## Decision Log





## Outcomes & Retrospective


The group-integration and release-integration suites passed with Redis 8.10.1. Pending recovery, retry deadlines and the namespace contract are recorded in [ADR 0004](../adr/0004-fence-group-finalizers-with-redis-attempt-state.md).





## Context and Orientation


Complete [docs/plans/1-bootstrap-a-buildable-adapter-and-redis-runner-fixture.md](../../docs/plans/1-bootstrap-a-buildable-adapter-and-redis-runner-fixture.md) first. It supplies the Cabal package, binary identity and failure record codecs, bounded connection bracket and real Redis/runApp fixture. The [initial specification](../initial-spec.md) requires a single-primary protocol. Redis stores assigned, unacknowledged IDs in the group's pending entries list (PEL). XACK addresses an ID in a group, so checking ownership and later sending XACK on the client leaves a race. Use the released contracts behind `mori://shinzui/shibuya/packages/shibuya-core`, `mori://informatikr/hedis/packages/hedis` and `mori://shinzui/shibuya/docs/architecture`. The runner owns handler scheduling, admission and repeated finalization on transient exceptions. The adapter supplies a Streamly stream in Eff, an AckHandle per delivery, shutdown and AdapterTelemetry. A superseded item can skip both handler and finalizer. Release resources for it even when neither runs.

Mori indexes the architecture guide but has no decision bundle for `mori://shinzui/shibuya`. Artifact-level decision URIs are pending. In that project, the relevant records are `docs/adr/0003-make-processor-termination-and-shutdown-outcomes-explicit.md`, `docs/adr/0010-pause-processors-at-the-adapter-source-and-report-pause-as-operator-intent.md`, `docs/adr/0012-keep-telemetry-ownership-and-disabled-context-contracts-explicit.md` and `docs/adr/0013-let-adapters-supersede-a-delivery-before-its-handler-runs.md`. They distinguish deliberate halt from failed finalization, gate intake before source pulls, preserve caller telemetry ownership and give the adapter a prompt, side-effect-free admission check. There are no local ADRs at creation.


## Plan of Work


Add src/Shibuya/Adapter/Redis/Group.hs and internal Group/Scripts.hs. Export GroupConfig and a group constructor from Shibuya.Adapter.Redis. Require group, unique live worker ID, finite batch/in-flight/scan limits, minimum idle time and explicit attachment policy. Only create on request with the configured starting ID and MKSTREAM when appropriate. Treat BUSYGROUP as a race won by an existing creator and attach without changing position. Missing group during processing is a distinct failure. Use XREADGROUP with > and acknowledgements enabled. Carry a bounded rotating XPENDING scan cursor with wraparound and alternate new work with eligible pending candidates, including when capacity is one. Bound blocking time so due retries get a turn. Deletion of a pending payload produces a diagnostic with the original ID and explicit disposition before cleanup.

Bind each attempt to an unguessable token stored under group, stream generation and ID. For new reads, conditionally install the token only if PEL owner/delivery generation still matches the initial assignment. For recovery, use a bounded server script to check PEL, server time, due time, payload and eligibility, then XCLAIM and rotate the token in the same operation. Ensure a delayed new-read registration cannot overwrite a later token even on A→B→A; never reuse a consumer name across live processes or restart. Preserve server-side due time across crashes. Keep all adapter workers on the same protocol. A crash between read and registration leaves an ordinary reclaimable PEL entry. Snapshot local known-superseded state for Shibuya admission. Recheck Redis owner and token in the script for every finalizer and lease extension. A same-owner extension refreshes idle time without creating a new attempt. Report an attempt number only when the protocol can derive it reliably.

Implement AckOk as fenced XACK, AckRetry as a durable due-time transition that invalidates the current token while retaining pending work, and AckHalt as stop with pending work intact. On AckDeadLetter or decode failure, fence the failure record and ack in one script, or stop intake and leave pending if there is no destination. Validate key types before mutation and order writes so an interrupted operation can resume. Use the shared record-as-deduplication-key design. Distinguish repeat success from supersession where metrics require it. Clean completed metadata with a documented retention horizon. Emit counters only for effective transitions. Sample bounded backlog/age diagnostics away from the admission callback.


## Concrete Steps


Create test/GroupSpec.hs and the group-integration Cabal target. From the repository root run the suite against the private Redis fixture.

```bash
nix develop -c cabal test group-integration --test-show-details=direct
```

Use separate worker connections, runner barriers before handler return and a separate connection inspecting XPENDING. Arrange eligibility using controlled idle state and server timestamps. Bound waits with diagnostic deadlines.


## Validation and Acceptance


Race creators with different start positions and show no resets on reconnect. A and B divide jobs, C in a second group receives them all, entries remain readable. Block A's handler, let B claim and release A. Verify stale success, retry, lease extension and dead-letter calls change neither pending ownership, deadline nor destination. Repeat after B→A. Drive continuous new traffic and a backlog larger than the scan budget, including ineligible entries at the front. Both work classes must advance. Delay retry, restart the worker, prove no early delivery and then observe the same ID. Drop a successful rejection reply and replay the decision, verifying one logical record, bounded reason detail and empty PEL. Trim a pending payload and inspect the diagnostic. Test two groups rejecting the same ID independently, long-handler renewal, explicit halt and missing destination.


## Idempotence and Recovery


Handle a lost read reply by eventual recovery from the PEL. Handle a lost mutation reply by retrying the identical token and decision, never claiming merely to finish an old callback. Scripts execute without interleaving but a command error can follow a successful earlier write. Test wrong-type destinations and retries after partial completion. Preserve cancellation, bound transient retries, and document at-least-once application effects.


## Interfaces and Dependencies


Use hedis xreadGroupOpts, bounded XPENDING detail, XCLAIM and Redis scripting. Use the common connection and message identity contracts. Record persistence, eviction, source generation and exclusive metadata namespace assumptions. Give the implementation minimum command and key permissions per configured option for release qualification.
