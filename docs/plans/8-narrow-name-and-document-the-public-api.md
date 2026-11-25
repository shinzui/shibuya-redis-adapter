---
id: 8
slug: narrow-name-and-document-the-public-api
title: "Narrow, name and document the public API"
kind: exec-plan
created_at: 2026-11-25T13:50:20Z
intention: "intention_01m89fh99nex0r6hchenpa2c1b"
master_plan: "docs/masterplans/2-harden-the-redis-adapter-for-a-bug-free-first-release.md"
provenance:
  created_by:
    model: "claude-fable-5-2"
    harness: "claude-code"
    at: 2026-11-25T13:50:20Z
  revisions:
    - model: "gpt-6.2-sol"
      harness: "codex-cli"
      at: 2026-11-25T14:16:26Z
      mode: "implement"
      note: "Define and document the public API and enforce one processor per runtime"
---

# Narrow, name and document the public API

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.
If durable project context changes, update or create ADRs in docs/adr/ in the same change.
Record friction met while implementing in the friction-log bundle as it occurs (see FRICTION.md).


## Purpose / Big Picture


Once a library is published, every name it exports is a promise. Today the single exposed module, `Shibuya.Adapter.Redis`, exports connection plumbing and wire-format helpers alongside the constructors an application needs, almost nothing is documented, and two configuration fields name the same thing differently. After this change an application author reads one documented module with a small export list, internals live in a module that makes no compatibility promise, attaching two processors to one runtime fails immediately instead of misbehaving, and adding a configuration field later cannot silently shift a default. You can see it in the generated Haddock pages and in a new test for the attach check.


## Progress


- [x] M1. Internals are exported from `Shibuya.Adapter.Redis.Internal`; the facade has a sectioned export list; every module has an explicit export list and builds under the stricter warning set. Verified 2026-11-25. The build emitted no GHC warnings, and a scratch import of runCommand failed from the facade and compiled from Internal.
- [x] M2. `readerName` is renamed `readerAdapterName`; the default configurations are built with named fields; the facade re-exports Shibuya's telemetry types. Verified 2026-11-25. The named defaults and telemetry override are covered by runner-fixture. Both examples delivered and resumed in a private Redis run.
- [x] M3. Attaching a second reader or worker to a runtime throws `RuntimeAlreadyAttached`, proven by a test. Verified 2026-11-25. The fixture verifies failed initialization releases the reservation, and a successful reader rejects a second reader and a worker.
- [x] M4. Every export of the facade has Haddock documentation and the module header explains lifecycle, decisions and retry semantics; a new ADR records the public/internal boundary. Verified 2026-11-25. Haddock reports 100% (47/47) for the facade and all five suites pass.


## Surprises & Discoveries


(None yet.)


## Decision Log


- Decision: Expose internals through one `Internal` module instead of hiding them completely.
  Rationale: The test suites and advanced users need them, and Shibuya core uses the same convention with the same disclaimer.
  Date: 2026-11-25

- Decision: Keep `rejectionRecord` and `RejectionOwner` public.
  Rationale: Applications own replay from the dead-letter hash and need the documented field and value encoding. A decoder is out of scope for this release.
  Date: 2026-11-25


## Outcomes & Retrospective


The facade now documents its lifecycle, policies, configuration and public exports. The import probe confirmed that command plumbing is available through Internal. The attachment regression verified both the one-processor rule and recovery after a failed constructor. Haddock generated the public pages and reported 47 of 47 items documented. It reported unresolved links to some derived Generic representations and missing external documentation for two transitive dependencies.


## Context and Orientation


This repository is a Haskell library, `shibuya-redis-adapter`, that feeds entries from a Redis Stream to a Shibuya processor. Its Cabal file, `shibuya-redis-adapter.cabal`, exposes one module, `src/Shibuya/Adapter/Redis.hs`, which re-exports seven hidden modules under `src/Shibuya/Adapter/Redis/`: `Adapter`, `Connection`, `DeadLetter`, `Group`, `Group.Scripts`, `Reader` and `Types`. Most of those modules have no export list, so everything in them is visible to the facade.

A "runtime" (`RedisRuntime`, created by `withRedisRuntime` in `Connection.hs`) owns one dedicated read connection, a command pool and one stop flag. `newReader` and `newGroupWorker` each attach one processor to a runtime. Because the stop flag and read connection are shared, two processors on one runtime would stop together and contend for the read connection. The README says to use separate runtimes; nothing enforces it.

What an application needs: `withRedisRuntime`, `ConnectionSource`, `RuntimeOptions`, `defaultRuntimeOptions`, `RedisRuntime` (abstract), `stopIntake`; `SourceIdentity`, `EntryId`, `renderEntryId`, `parseEntryId`, `RedisEntry`; the reader types and `rawReaderConfig`, `newReader`, `readerProcessor`; the group types and `rawGroupConfig`, `newGroupWorker`, `groupProcessor`; `AdapterEvent`; the error types `AdapterError`, `ReaderError`, `GroupError`; `RejectionOwner` and `rejectionRecord`.

What it does not need, and is exported today: `runCommand`, `pollStream`, `readBatchOptions`, `mkRedisAdapter`, `validateDeliveryPolicy`, `DeliveryMode`, `storeRejection`, `decodeEnvelope`, `DecodeFailure`, `entryEnvelope`.

Other problems in the current surface: `ReaderConfig` calls the adapter's display name `readerName` while `GroupConfig` calls it `groupAdapterName`, and `readerName` sits beside the unrelated `readerIdentity` (the logical name used to scope checkpoints and dead-letter records). `rawReaderConfig`, `rawGroupConfig` and `defaultRuntimeOptions` construct their records positionally, so reordering or inserting a field changes which default lands where without a compile error when types coincide. The facade does not re-export `AdapterTelemetry`, `defaultAdapterTelemetry` or `TraceCorrelation`, which users need to set the `telemetry` field.

Conventions to follow from sibling projects: Shibuya core (`mori://shinzui/shibuya`) heads internal modules with "__Internal module.__ Exposed for the test suite and benchmarks only. No PVP guarantees: anything here may change or disappear in any release." The Kafka adapter (`mori://shinzui/shibuya-kafka-adapter`, project-relative `shibuya-kafka-adapter/src/Shibuya/Adapter/Kafka.hs`, artifact URI pending) shows the expected module header: a usage example followed by sections on message lifecycle and each decision's effect. Its Cabal file shows the warning set: `-Wall -Wcompat -Widentities -Wincomplete-uni-patterns -Wincomplete-record-updates -Wredundant-constraints -Wmissing-export-lists -Wpartial-fields -Wmissing-deriving-strategies`.

Local decisions: [ADR 2](../adr/0002-keep-command-connections-through-runner-drain.md) defines the runtime's connection lifetime, which the attach check must not alter. No existing ADR covers the public/internal boundary; this plan adds one.

This plan is a child of [the hardening MasterPlan](../masterplans/2-harden-the-redis-adapter-for-a-bug-free-first-release.md). It must start only after plans [5](5-correct-consumer-group-retry-recovery-and-lease-fencing.md), [6](6-dead-letter-exhausted-reader-retries-and-retry-reader-rejection-writes.md) and [7](7-propagate-trace-context-and-redis-span-attributes.md) are Complete, because it relocates and documents definitions those plans change. Plan 7 adds the `telemetry`, `readerTraceContext` and `groupTraceContext` fields and a trace-context argument to `rejectionRecord`; document them as they then stand.


## Plan of Work


**M1, module boundary.** Give every module under `src/Shibuya/Adapter/Redis/` an explicit export list. Create `src/Shibuya/Adapter/Redis/Internal.hs`, add it to `exposed-modules`, give it the Shibuya internal-module disclaimer, and re-export from it everything the facade drops plus `frame`, `entryIdentity`, `groupAttemptKey`, `groupStatePrefix`, `attemptScript`, `retryConnection`, `streamEntries` and `streamRecord`. Rewrite the facade's export list in sections (runtime, source identity and entries, reader, group worker, events and errors, dead-letter records, telemetry) with only the application-facing names listed above. Export `RedisRuntime` without its fields. Point the test suites and examples at `Internal` where they use dropped names. Add the Kafka adapter's warning flags to the `shared` stanza and fix what they report.

**M2, names and defaults.** Rename `readerName` to `readerAdapterName` everywhere. Rewrite `rawReaderConfig`, `rawGroupConfig` and `defaultRuntimeOptions` with record syntax so each default is named. Re-export `AdapterTelemetry (traceCorrelation)`, `defaultAdapterTelemetry` and `TraceCorrelation (..)` from the facade. Use `unMessageId` in place of the local `messageIdText` helper.

**M3, attach check.** Add an `attached` flag to `RedisRuntime` and a constructor `RuntimeAlreadyAttached` to `AdapterError`. At the start of `newReader` and `newGroupWorker`, after option validation, set the flag atomically and throw if it was already set. Add a case to `test/Main.hs`: a second `newReader` on the same runtime throws `RuntimeAlreadyAttached`, and so does `newGroupWorker` after `newReader`.

**M4, documentation.** Write a module header for `Shibuya.Adapter.Redis` with a short complete example for each mode and sections that state, for each mode: where progress is kept; what each of `AckOk`, `AckRetry`, `AckDeadLetter` and `AckHalt` does; what happens at the retry limit with and without a dead-letter hash; what a handler exception does; when to call `leaseExtend`; the one-runtime-per-processor rule; and the tracing defaults. Document every exported type, constructor, field and function, including units for every numeric option and the default value of every field. Add `docs/adr/` record for the public/internal boundary and the rule that `Internal` carries no compatibility promise. Update the README where it names moved or renamed items.


## Concrete Steps


From the repository root:

```bash
nix develop -c cabal build all
nix develop -c cabal test all --test-show-details=direct
nix develop -c cabal haddock shibuya-redis-adapter
```

The Haddock run prints a coverage line per module. The target is:

```text
 100% ( NN / NN) in 'Shibuya.Adapter.Redis'
```

To confirm the facade no longer leaks internals, this must fail to compile in a scratch module, and succeed when the import is `Shibuya.Adapter.Redis.Internal`:

```haskell
import Shibuya.Adapter.Redis (runCommand)
```


## Validation and Acceptance


Accepted when: the build is warning-free under the new flags; all suites pass; Haddock reports full coverage for the facade; importing a dropped name from the facade fails; the attach test passes; and both examples still build and run as the README describes. Behavior of delivery, retry and tracing must be unchanged, so no existing scenario's assertions change except for names.


## Idempotence and Recovery


Everything here is a source edit. Do M1 as moves and export lists only, with no behavior change, and commit it before M2, so a mistake in a rename is easy to isolate.


## Interfaces and Dependencies


No new packages. The exposed modules become `Shibuya.Adapter.Redis` and `Shibuya.Adapter.Redis.Internal`. `ReaderConfig.readerName` becomes `readerAdapterName`. `AdapterError` gains `RuntimeAlreadyAttached`. `RedisRuntime` becomes abstract in the facade. [Plan 9](9-package-and-qualify-the-0-1-0-0-release.md) depends on this surface being final.
