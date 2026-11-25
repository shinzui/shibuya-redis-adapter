---
id: 6
slug: dead-letter-exhausted-reader-retries-and-retry-reader-rejection-writes
title: "Dead-letter exhausted reader retries and retry reader rejection writes"
kind: exec-plan
created_at: 2026-11-25T13:50:20Z
intention: "intention_01m89fh99nex0r6hchenpa2c1b"
master_plan: "docs/masterplans/2-harden-the-redis-adapter-for-a-bug-free-first-release.md"
provenance:
  created_by:
    model: "claude-fable-5-2"
    harness: "claude-code"
    at: 2026-11-25T13:50:20Z
---

# Dead-letter exhausted reader retries and retry reader rejection writes

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.
If durable project context changes, update or create ADRs in docs/adr/ in the same change.
Record friction met while implementing in the friction-log bundle as it occurs (see FRICTION.md).


## Purpose / Big Picture


An independent reader gives one consumer its own ordered copy of a Redis Stream and remembers how far it has got. After this change, an entry that keeps asking for retries is written to the reader's dead-letter hash and the reader moves on, and a brief Redis disconnection while recording an undecodable entry no longer stops the reader. You can see both in the `reader-integration` test suite.


## Progress


- [ ] M1. With a dead-letter hash configured, an entry that exhausts `readerRetryLimit` produces one `max_retries_exceeded` record, the checkpoint advances and the next entry is delivered; without one, the reader stops with its checkpoint before the entry.
- [ ] M2. A dropped command connection during the rejection write for an undecodable entry is retried and the reader continues.
- [ ] All suites pass and ADR 3 is amended.


## Surprises & Discoveries


(None yet.)


## Decision Log


- Decision: Without a dead-letter destination, retry exhaustion still stops the reader.
  Rationale: A reader promises stream order. Skipping an entry silently would break a projection; stopping leaves the checkpoint before the entry so the application can decide.
  Date: 2026-11-25


## Outcomes & Retrospective


(To be filled during and after implementation.)


## Context and Orientation


This repository is a Haskell library, `shibuya-redis-adapter`, that feeds entries from a Redis Stream to a Shibuya processor. Shibuya is a queue-processing framework: an adapter supplies a stream of deliveries, the framework runs a handler on each, and the handler returns `AckOk`, `AckRetry delay`, `AckDeadLetter reason` or `AckHalt reason`. The framework then calls the adapter's finalizer with that decision. If the finalizer throws, the framework calls it again with the same decision up to three more times and then fails the processor. If the stream itself throws, the processor fails at once.

`src/Shibuya/Adapter/Redis/Reader.hs` holds `newReader`. Its `pull` function reads a batch with `XREAD`, then hands out one entry at a time and waits for that entry's finalizer before continuing. A "checkpoint" is the ID of the last completed entry, kept in memory and optionally saved through the application's `CheckpointStore`. Inside `deliver`:

- `reject reason` writes a dead-letter record to the hash named by `readerDeadLetters` using `storeRejection` (from `src/Shibuya/Adapter/Redis/DeadLetter.hs`, an `HSETNX`, which writes only if the field is absent), then commits the checkpoint. With no destination it throws `ReaderRejectionWithoutDestination`.
- `finalize` handles each decision. For `AckRetry` it throws `ReaderRetryLimit` when the local attempt number equals `readerRetryLimit`; otherwise it asks `pull` to deliver the same entry again after the delay.
- When the configured decoder rejects an entry, `deliver` calls `reject` directly from the stream, outside any finalizer.

Two helpers in `src/Shibuya/Adapter/Redis/Connection.hs` matter: `runCommand` runs one Redis command with a timeout, and `retryConnection` repeats an action after a lost connection or timeout, up to `reconnectAttempts` times. Its comment says to use it only for operations that are safe to repeat.

The defects:

1. On retry exhaustion the finalizer throws even when a dead-letter hash is configured. Shibuya defines the reason `MaxRetriesExceeded` for exactly this case, and its PGMQ adapter (`mori://shinzui/shibuya-pgmq-adapter`) dead-letters with it.
2. `reject` calls `runCommand` without `retryConnection`. In the finalizer the framework's own retries cover a blip. In the decode-failure path nothing does, so one dropped connection fails the processor.

`test/ReaderSpec.hs` is the `reader-integration` suite; it starts a private Redis server through `test/Fixture.hs`.

[ADR 3](../adr/0003-commit-reader-checkpoints-in-stream-order.md) requires the dead-letter record to be written before the checkpoint moves, and notes that replaying a rejection reuses the same hash field. [ADR 1](../adr/0001-stream-identity-and-rejection-records.md) defines the record format, used here unchanged.

This plan is a child of [the hardening MasterPlan](../masterplans/2-harden-the-redis-adapter-for-a-bug-free-first-release.md). It has no prerequisites. The group worker's equivalent of M1 belongs to [plan 5](5-correct-consumer-group-retry-recovery-and-lease-fencing.md); the two must behave the same way.


## Plan of Work


**M1, retry exhaustion.** In `finalize`'s `AckRetry` case, when the attempt number has reached `readerRetryLimit` and `readerDeadLetters` is set, call `reject MaxRetriesExceeded`, release the gate with `Advance`, and let `reject` emit `DeadLettered` as it does today. When no destination is set, keep throwing `ReaderRetryLimit`. Add a scenario to `test/ReaderSpec.hs`: two entries, `readerRetryLimit = 1`, a destination, a handler that always retries the first entry with a short delay and accepts the second. Assert that the hash holds one field, that `completedCheckpoint` reaches the second entry's ID, and that the first entry's handler ran exactly twice. Add or keep a scenario without a destination that asserts the reader stops and the checkpoint is before the first entry.

**M2, rejection write.** Wrap the `storeRejection` command in `reject` with `retryConnection runtime`. `HSETNX` is safe to repeat: a second attempt after a lost reply finds the field present and changes nothing. Emit `DeadLettered` on success whether or not the repeated write reported an insert, so a lost reply does not hide the event. Add a scenario: a decoder that rejects the first entry, and a way to drop the adapter's command connections just before the rejection write. One deterministic way is to have the test's decoder run `CLIENT KILL TYPE normal SKIPME yes` on the admin connection (through `unsafePerformIO`, in test code only) before returning `Left`; any equivalent hook is fine. Assert that the record exists and the second entry is delivered. Run it against the unfixed code first and watch the processor fail.

Amend [ADR 3](../adr/0003-commit-reader-checkpoints-in-stream-order.md) with the exhaustion rule, and correct `docs/compatibility.md` where it says both modes "cap delayed retries" so that it states what happens at the cap.


## Concrete Steps


Run from the repository root:

```bash
nix develop -c cabal test reader-integration --test-show-details=direct
nix develop -c cabal test all --test-show-details=direct
```

Each suite prints one success line and exits zero; a failing assertion prints its label and exits non-zero.


## Validation and Acceptance


M1 is accepted when the new scenario shows one dead-letter record, the checkpoint at the second entry and the second entry handled, and the no-destination scenario shows the reader stopped with the checkpoint unmoved. M2 is accepted when the connection-drop scenario fails before the change and passes after. All four test suites must pass.


## Idempotence and Recovery


All steps are source edits and can be repeated. Test servers run in temporary directories. `CLIENT KILL` in the M2 scenario affects only that scenario's private server.


## Interfaces and Dependencies


No new packages or configuration fields. `ReaderError` keeps `ReaderRetryLimit`, now thrown only when no dead-letter destination is configured. `storeRejection` and `rejectionRecord` are called with their current signatures; if [plan 7](7-propagate-trace-context-and-redis-span-attributes.md) has already added a trace-context argument, pass the current trace headers from the finalizer and none from the decode-failure path, as that plan specifies.
