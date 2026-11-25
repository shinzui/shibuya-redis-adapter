---
id: 5
slug: correct-consumer-group-retry-recovery-and-lease-fencing
title: "Correct consumer-group retry, recovery and lease fencing"
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

# Correct consumer-group retry, recovery and lease fencing

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.
If durable project context changes, update or create ADRs in docs/adr/ in the same change.
Record friction met while implementing in the friction-log bundle as it occurs (see FRICTION.md).


## Purpose / Big Picture


A consumer-group worker shares jobs from one Redis Stream with other workers. After this change, a worker that asks for a retry gets the job back itself after the delay it asked for, a job that keeps failing is moved to the dead-letter hash instead of stopping every worker, a handler that has lost its lease is told so, and the worker stops sending needless script calls. You can see it by running the `group-integration` test suite, which gains a scenario for each behavior.


## Progress


- [ ] M1. A single worker receives its own retried entry: new scenario in `test/GroupSpec.hs` fails before the script fix and passes after.
- [ ] M2. A retry runs after its requested delay even when `minimumIdleMillis` is much longer; a handler exception is redelivered promptly.
- [ ] M3. With a dead-letter hash configured, retry exhaustion writes one `max_retries_exceeded` record and the worker keeps processing; without one, the worker stops and the entry stays pending.
- [ ] M4. After a stale `leaseExtend`, the delivery's `deliveryStatus` reports `DeliverySuperseded`.
- [ ] M5. The worker calls the script by its hash, recovers from `SCRIPT FLUSH`, and makes no script calls for its own in-flight rows; the restricted-credential test and permissions table are updated.
- [ ] All suites pass and ADR 4 is amended.


## Surprises & Discoveries


(None yet.)


## Decision Log


- Decision: A retried entry becomes eligible when its due time passes, without also waiting for the idle interval.
  Rationale: The idle interval protects a handler that may still be running. A retry invalidates the attempt token, so no handler holds the entry.
  Date: 2026-11-25

- Decision: Keep one script call per pending row, and skip locally only rows this worker owns and is still processing.
  Rationale: A Redis-side retry index would remove more calls but adds durable state to keep consistent. The parent MasterPlan records this as a deliberate exclusion.
  Date: 2026-11-25

- Decision: A stale lease renewal marks the delivery superseded but does not throw.
  Rationale: `Lease.leaseExtend` returns unit in Shibuya's contract and sibling adapters do not throw from it. The finalizer is already fenced in Redis.
  Date: 2026-11-25


## Outcomes & Retrospective


(To be filled during and after implementation.)


## Context and Orientation


This repository is a Haskell library, `shibuya-redis-adapter`, that feeds entries from a Redis Stream to a Shibuya processor. Shibuya is a queue-processing framework: an adapter supplies a stream of `Ingested` values, the framework runs a handler on each, and the handler returns an `AckDecision` (`AckOk`, `AckRetry delay`, `AckDeadLetter reason` or `AckHalt reason`). The framework then calls the adapter's finalizer with that decision. If a handler throws, the framework finalizes with `AckRetry (RetryDelay 0)`. If a finalizer throws, the framework calls it again with the same decision up to three more times and then fails the processor.

A Redis consumer group tracks, for each delivered entry, a row in its pending entries list (PEL): the entry ID, the owning consumer name, the idle time since the last delivery, and a delivery count. `XACK` removes the row. `XCLAIM` moves it to another consumer.

The files that matter:

- `src/Shibuya/Adapter/Redis/Group.hs` holds `newGroupWorker`. Its `pull` function alternates between reading new entries (`XREADGROUP`) and a recovery scan that lists a page of the PEL (`XPENDING`) and tries to claim each row. `deliver` builds the `Ingested` value, its finalizer (`finalize`), its lease (`renew`) and its `deliveryStatus`. `attempt` sends the script. The `active` map holds this worker's outstanding deliveries keyed by attempt token.
- `src/Shibuya/Adapter/Redis/Group/Scripts.hs` holds `attemptScript`, one Lua script that performs every fenced operation. Its arguments are: operation, group, consumer, entry ID, attempt token, then operation arguments. It keeps a small hash per pending entry (the "attempt state") with fields `token`, `generation`, `until` (a due time in Redis server milliseconds) and `retried`.
- `src/Shibuya/Adapter/Redis/DeadLetter.hs` holds `rejectionRecord`, which builds the hash field and value for a dead-letter record, and `RejectionOwner`.
- `test/GroupSpec.hs` is the `group-integration` suite. It starts a private Redis server through `test/Fixture.hs` and uses plain assertions (`assert label condition`) inside `within label action`, which fails after twelve seconds.
- `test/ReleaseSpec.hs` contains a restricted-credential scenario that grants Redis commands individually.
- `docs/compatibility.md` has the permissions table and a "Runtime and recovery" section describing retry behavior.

The defects, as the code stands:

1. In the script's claim branch, the line `if owner and current and current == ARGV[7] then return {'skip'} end` is meant to stop a worker from reclaiming an entry it is still processing. `ARGV[7]` is the worker's local token for that entry, or the empty string when it has none. The `retry` operation sets `token` to the empty string. So after a retry, the owner's own scan compares `''` with `''` and skips forever. Only a different consumer can claim the entry.
2. The line above it, `if row[3] < tonumber(ARGV[6]) or eligibleAt > now() then return {'skip'} end`, requires the idle time to reach `minimumIdleMillis` (default 30 seconds) even for a retried entry, so `AckRetry (RetryDelay 1)` waits 30 seconds.
3. In `Group.hs`, `transition` turns the script reply `retry-limit` into `throwIO (GroupRetryLimit ...)`. The processor fails, the entry stays pending, another worker claims it and fails the same way.
4. `renew` emits the `Superseded` event on a stale reply but leaves the delivery's `valid` flag true, so `deliveryStatus` still says `DeliveryCurrent`.
5. `recover` calls the script for every row in the page, including rows this worker owns and is processing, and `attempt` sends the whole script text with `EVAL` each time.

[ADR 4](../adr/0004-fence-group-finalizers-with-redis-attempt-state.md) records the fencing protocol: a callback must match owner, token and delivery count. Nothing here weakens that. [ADR 1](../adr/0001-stream-identity-and-rejection-records.md) records the dead-letter record format, which this plan uses unchanged. In the Shibuya core project (`mori://shinzui/shibuya`, artifact URI pending), `docs/adr/0013-let-adapters-supersede-a-delivery-before-its-handler-runs.md` requires `deliveryStatus` to return promptly without side effects; reading a `TVar` satisfies it.

This plan is a child of [the hardening MasterPlan](../masterplans/2-harden-the-redis-adapter-for-a-bug-free-first-release.md). It has no prerequisites.


## Plan of Work


**M1, same-consumer retry.** First add a scenario to `test/GroupSpec.hs`: one worker named `solo` with a small `minimumIdleMillis`, one entry, a handler that returns `AckRetry (RetryDelay 0.2)` on the first call and `AckOk` on the second. Assert that the same worker's handler is called twice, that the second envelope has `attempt == Just 1` and the same `messageId`, and that the PEL ends empty. Run it and watch it time out. Then change the guard in `Scripts.hs` so it applies only when the stored token is non-empty:

```lua
if owner and current and current ~= '' and current == ARGV[7] then return {'skip'} end
```

The scenario now passes.

**M2, retry delay.** In the same branch, read the `retried` field. When it is set, require only that the due time has passed; when it is not, keep both conditions as they are. Add a scenario with `minimumIdleMillis = 5000` and `AckRetry (RetryDelay 0.3)`: the second call must arrive within about two seconds and not before 300 milliseconds of Redis server time (the existing `retryAndReject` scenario shows how to read `TIME`). Add a second scenario whose handler throws on the first call and assert prompt redelivery. Correct the sentence in `docs/compatibility.md` that says recovery "requires both the minimum idle interval and any stored lease or retry deadline to elapse": that remains true for abandoned and lease-extended entries, and a retried entry needs only its retry time.

**M3, retry exhaustion.** In `Group.hs`, when `transition "retry"` receives `retry-limit` and `cfg.groupDeadLetters` is set, run the `reject` operation with the record `rejectionRecord (ConsumerGroup cfg.groupName) entry MaxRetriesExceeded` and report `DeadLettered`. The `retry-limit` reply changes no state, so the attempt token is still valid for the `reject` call. When no destination is set, keep throwing `GroupRetryLimit`. A repeated finalizer after a lost reply sees the row absent and does nothing, as today. Add a scenario with `groupRetryLimit = 1`, a destination, a handler that always retries the first entry and accepts a second entry: assert one record in the hash, an empty PEL, and that the second entry was processed by the same worker. Keep or add the no-destination assertion: the processor ends and the row is still owned by the worker.

**M4, lease loss.** In `renew`, on a stale reply also write `False` to the delivery's `valid` variable. Do not remove the delivery from `active`; its finalizer still runs and releases the slot. In the existing `takeover` scenario, after the stale `extend item`, assert that `item.deliveryStatus` returns `DeliverySuperseded`.

**M5, script calls.** In `recover`, when a row's owner is this consumer and `active` holds a delivery for that entry, move to the next row without calling the script; the script would have answered `skip`. In `newGroupWorker`, after `attachGroup`, send `SCRIPT LOAD` with `attemptScript` and keep the returned hash. Make `attempt` send `EVALSHA` with that hash; when the reply is an error starting with `NOSCRIPT`, send the same request once with `EVAL` and the script text, which also restores the cache. Add a scenario that runs `SCRIPT FLUSH` from the admin connection between two entries and asserts the second is processed. Add a scenario that holds one handler open for about a second and compares the `calls` counters for `cmdstat_evalsha` and `cmdstat_eval` in `INFO commandstats` before and after: the count must not grow with the number of polls. Add `SCRIPT LOAD` and `EVALSHA` to the grants in the restricted-credential scenario in `test/ReleaseSpec.hs` and to the worker row of the table in `docs/compatibility.md`.

Finally amend [ADR 4](../adr/0004-fence-group-finalizers-with-redis-attempt-state.md) with the retry-eligibility rule, the exhaustion behavior and the cached script.


## Concrete Steps


All commands run from the repository root. The development shell supplies GHC, Cabal and `redis-server`.

```bash
nix develop -c cabal build all
nix develop -c cabal test group-integration --test-show-details=direct
nix develop -c cabal test all --test-show-details=direct
```

Before the M1 fix the new scenario ends with a line such as:

```text
group-integration: user error (same-consumer retry timed out)
```

After each milestone the suite ends with its success line (extend the text to name the new scenarios):

```text
Group creation, distribution, takeover, fencing, retry, rejection, pending loss and scan fairness passed
```

To inspect the script by hand, start a private server on a short relative socket path (Unix socket paths are limited to about 100 characters), then call the script with `redis-cli -s <socket> EVAL "$(cat script.lua)" 2 <stream> <state-key> <operation> <group> <consumer> <id> <token> ...`.


## Validation and Acceptance


Each milestone is accepted when its new scenario fails on the code before the change and passes after, and `cabal test all` passes. The observable behaviors are: one worker handles its own retry (M1); a 300 millisecond retry arrives in under two seconds with a five-second idle interval (M2); a poison entry produces exactly one dead-letter record and the worker goes on to the next entry (M3); a superseded delivery reports `DeliverySuperseded` (M4); processing continues after `SCRIPT FLUSH`, and script-call counters stay flat while a handler runs (M5). The existing takeover scenarios must still pass unchanged, because they prove that a stale callback cannot touch a newer attempt.


## Idempotence and Recovery


Every step is an ordinary source edit and can be repeated. The test suites start their own Redis server in a temporary directory and leave nothing behind. If a script change breaks an existing scenario, revert `Scripts.hs` alone and reapply one condition at a time; each milestone's script edit is one line or one small branch.


## Interfaces and Dependencies


No new packages. Public types keep their shapes: `GroupError` keeps `GroupRetryLimit`, now thrown only when no dead-letter destination is configured. `attemptScript :: ByteString` keeps its argument layout; the claim branch reads one more field of the attempt hash. `rejectionRecord` is called with its current signature; if [plan 7](7-propagate-trace-context-and-redis-span-attributes.md) has already added a trace-context argument to it, pass the current trace headers at the new call site as that plan's other callers do. Workers now need permission for `SCRIPT LOAD` and `EVALSHA` in addition to `EVAL`.
