---
id: 7
slug: propagate-trace-context-and-redis-span-attributes
title: "Propagate trace context and Redis span attributes"
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
      at: 2026-11-25T14:14:16Z
      mode: "implement"
      note: "Propagate Redis tracing and verify span export through global Jaeger"
---

# Propagate trace context and Redis span attributes

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.
If durable project context changes, update or create ADRs in docs/adr/ in the same change.
Record friction met while implementing in the friction-log bundle as it occurs (see FRICTION.md).


## Purpose / Big Picture


Shibuya opens one OpenTelemetry span for each message a handler processes. For that span to join the trace of whoever produced the message, the adapter must hand Shibuya the producer's W3C trace headers (`traceparent` and optionally `tracestate`). This adapter never does, so traces stop at the stream. After this change, a producer that writes `traceparent` as a field of the stream entry gets a connected trace: the consumer span links to the producer span, and for group workers is also its child. Spans say `messaging.system = "redis"`, and a dead-letter record carries the failing consumer's trace context. You can see it in a new test suite that runs both modes with a real tracer and inspects the exported spans.


## Progress


- [x] M1. Envelopes carry `traceContext` from the entry's fields and Redis span attributes; an application hook can override the extraction. Verified 2026-11-25. The fixture covers both headers, tracestate without a parent, and an extractor override.
- [x] M2. `ReaderConfig` and `GroupConfig` each have a `telemetry` field (reader default `LinkOnly`, group default `ParentAndLink`); `RuntimeOptions` no longer has `adapterTelemetry`. Verified 2026-11-25. The fixture checks both defaults and a setting that reaches the reader adapter.
- [x] M3. A `telemetry-integration` suite with an in-memory span exporter proves link-only correlation for a reader, parent-and-link for a worker, and the attributes. Verified 2026-11-25. All five suites passed on Redis 8.10.1. The exporter checks the reader root and link, the group parent and link, no-context input, and span tags.
- [x] M4. A dead-letter record written from a finalizer includes the consumer span's trace headers; ADR 1 is amended and a telemetry ADR is added. Verified 2026-11-25. The suite checks the consumer traceparent for a reader rejection and group retry exhaustion, plus a tracing-disabled rejection.


## Surprises & Discoveries


Shibuya sets the processor name as the destination and ordinarily uses the full envelope identity as its span message ID. The latter encodes the stream key. The adapter overrides that span tag with the native Redis ID while preserving the envelope ID. See [ADR 5](../adr/0005-correlate-redis-deliveries-with-producer-traces.md).


## Decision Log


- Decision: By default, read `traceparent` and `tracestate` from the entry's own fields.
  Rationale: Redis Streams have no headers separate from fields, and the W3C names are what producers already write. A hook covers other conventions.
  Date: 2026-11-25

- Decision: Readers default to `LinkOnly`; group workers default to `ParentAndLink`.
  Rationale: A reader replays history and fans out to many readers, the cases Shibuya's guidance gives for `LinkOnly`. A work queue is consumed once, soon after production.
  Date: 2026-11-25

- Decision: Do not put the stream key in span attributes.
  Rationale: `docs/initial-spec.md` requires application-specific stream keys to stay out of default telemetry.
  Date: 2026-11-25


## Outcomes & Retrospective


Both processor modes exported correlated spans through OTLP to the global Jaeger instance on 2026-11-25. Jaeger returned the worker in the producer’s trace and the reader in a distinct trace linked to the producer. The in-memory test directly verifies the worker parent and both links. The live check can be repeated as follows.

```bash
nix develop -c env OTEL_SDK_DISABLED=false OTEL_TRACES_EXPORTER=otlp \
  OTEL_SERVICE_NAME=shibuya-redis-adapter-qualification \
  OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://127.0.0.1:10428/insert/opentelemetry \
  OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=http/protobuf \
  cabal test telemetry-integration --test-show-details=direct --test-options=--jaeger
```

Query the printed producer trace ID at `http://127.0.0.1:16686/api/traces/<trace-id>`. Search the same service for `process jaeger-reader` and match its producer reference to the printed trace and span IDs. Allow time for the collector to index new spans. [FRIC-2](../friction-log/2-haskell-otlp-exporter-appends-the-signal-path-to-the-trace-endpoint.md) is accepted because the released Haskell exporter appends the signal path and works with this collector prefix.


## Context and Orientation


This repository is a Haskell library, `shibuya-redis-adapter`, that feeds entries from a Redis Stream to a Shibuya processor in two modes: an independent reader (`newReader` in `src/Shibuya/Adapter/Redis/Reader.hs`) and a consumer-group worker (`newGroupWorker` in `src/Shibuya/Adapter/Redis/Group.hs`).

Shibuya wraps each message in an `Envelope`. Two envelope fields drive tracing. `traceContext :: Maybe TraceHeaders` holds the producer's `traceparent`/`tracestate` pairs; Shibuya uses it to parent or link the consumer span. `attributes :: HashMap Text Attribute` holds extra span attributes; a key the adapter sets overrides Shibuya's default of the same name, which is how an adapter replaces `messaging.system = "shibuya"`. Each `Adapter` value also carries `telemetry :: AdapterTelemetry`, whose `traceCorrelation` is `ParentAndLink` (consumer span is a child of the producer span and links to it) or `LinkOnly` (consumer span starts a new trace and links to the producer span). `AdapterTelemetry` has no exported constructor; build it as `defaultAdapterTelemetry {traceCorrelation = LinkOnly}`.

The authoritative guidance is in the Shibuya core project, `mori://shinzui/shibuya` (artifact URIs pending): `docs/user/opentelemetry.md`, section "Adapter authors: populating the two fields", and the decisions `docs/adr/0012-keep-telemetry-ownership-and-disabled-context-contracts-explicit.md` and `docs/adr/0014-define-consumer-correlation-and-bounded-batch-link-accounting.md`. Find the project on disk with `mori registry show shinzui/shibuya --full`. The convention there is that an adapter's configuration record has a field named `telemetry`, defaulted from `defaultAdapterTelemetry`, copied unchanged into the `Adapter`.

How this repository stands:

- `src/Shibuya/Adapter/Redis/Types.hs`: `entryEnvelope` builds the envelope from a `RedisEntry` (source identity, entry ID, ordered `fields`). It sets `messageId` and `cursor` only. `decodeEnvelope` applies the configured decoder and sets `headers` from a configured selector. Neither sets `traceContext` or `attributes`.
- `src/Shibuya/Adapter/Redis/Connection.hs`: `RuntimeOptions` has an `adapterTelemetry` field shared by whatever is attached to the runtime.
- `src/Shibuya/Adapter/Redis/Adapter.hs`: `mkRedisAdapter` copies `runtime.options.adapterTelemetry` into the `Adapter`.
- `src/Shibuya/Adapter/Redis/DeadLetter.hs`: `rejectionRecord owner entry reason` returns the hash field and value for a dead-letter record. The value is a sequence of length-prefixed components (the `frame` function in `Types.hs`): a version tag, the reason code, a detail marker and detail, then the entry's fields.
- Both modes build their finalizer as `AckHandle (liftIO . finalize)`, plain `IO` with no access to the active span.
- Every test and example runs under `runTracingNoop`. `test/Main.hs` (the `runner-fixture` suite) sets `adapterTelemetry` and checks it reaches the `Adapter`.

[ADR 1](../adr/0001-stream-identity-and-rejection-records.md) fixes the dead-letter record format; this plan extends the value and must amend it. The hash field, which is the idempotency key, does not change.

The Kafka adapter is the model to copy for tests: in `mori://shinzui/shibuya-kafka-adapter`, project-relative `shibuya-kafka-adapter/test/Shibuya/Adapter/Kafka/CorrelationTest.hs` (artifact URI pending) builds a tracer provider with an in-memory exporter, runs a processor and asserts on parent IDs and links. Its `Convert.hs` shows attribute construction.

This plan is a child of [the hardening MasterPlan](../masterplans/2-harden-the-redis-adapter-for-a-bug-free-first-release.md). It has no hard prerequisites. Plans [5](5-correct-consumer-group-retry-recovery-and-lease-fencing.md) and [6](6-dead-letter-exhausted-reader-retries-and-retry-reader-rejection-writes.md) add callers of `rejectionRecord` for retry exhaustion; if they are complete, update those callers in M4 too.


## Plan of Work


**M1, envelope fields.** In `Types.hs`, give `decodeEnvelope` a trace-context extractor argument and set `traceContext` from it. Provide the default extractor: look up `traceparent` in the entry's fields; if absent return `Nothing`; otherwise return the `traceparent` pair followed by the `tracestate` pair when present. Add the hook to both configuration records (`readerTraceContext`, `groupTraceContext`, each `RedisEntry -> Maybe TraceHeaders`) with the default extractor in `rawReaderConfig` and `rawGroupConfig`. Set `attributes` in both modes to `messaging.system = "redis"` and `shibuya.redis.entry_id` with the rendered entry ID as text. For group workers add `messaging.consumer.group.name` when the group name is valid UTF-8. Do not add the stream key. Extend `test/Main.hs` to assert these fields on a raw entry with and without a `traceparent` field.

**M2, configuration.** Add `telemetry :: AdapterTelemetry` to `ReaderConfig` and `GroupConfig`. `rawReaderConfig` defaults it to `defaultAdapterTelemetry {traceCorrelation = LinkOnly}`; `rawGroupConfig` to `defaultAdapterTelemetry`. Pass the value to `mkRedisAdapter` as an argument and remove `adapterTelemetry` from `RuntimeOptions`. Update `test/Main.hs` to set the field on the mode configuration.

**M3, real tracer.** Add a test suite `telemetry-integration` with `test/TelemetrySpec.hs`. Build a tracer provider with the in-memory exporter, run under `runTracing tracer`, append an entry whose fields include a known `traceparent` such as `00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01`, and process it once with a reader and once with a group worker. Assert: the reader's span has a new trace ID and one link to span `b7ad6b7169203331`; the worker's span has trace ID `0af7651916cd43dd8448eb211c80319c`, parent `b7ad6b7169203331` and the same link; both have `messaging.system = "redis"`; an entry without `traceparent` yields a span with no link. This suite is the representative check that the adapter's envelope and Shibuya's runner agree, so write it before M4.

**M4, dead-letter trace context.** Extend `rejectionRecord` and `storeRejection` with a `Maybe TraceHeaders` argument. In the value, after the detail components and before the entry's fields, add a marker component `absent`, or `present` followed by the `traceparent` bytes and the `tracestate` bytes (empty when there is none). Build each finalizer in `Eff` so it can call `currentTraceHeaders` from `Shibuya.Telemetry.Propagation` before entering `IO`; confirm its exact type in the shibuya-core source and add the `Tracing :> es` constraint to `newReader` and `newGroupWorker` if it requires one (every Shibuya application already has that effect, because `runApp` requires it). Pass `Nothing` from the decode-failure paths, which run in the stream outside any message span. In `TelemetrySpec.hs`, dead-letter an entry and assert the stored value contains the consumer span's `traceparent`; under `runTracingNoop` the marker is `absent`. Amend [ADR 1](../adr/0001-stream-identity-and-rejection-records.md) with the new components, and add `docs/adr/0005-...` recording the default correlation per mode, the attribute set and the stream-key exclusion.

Update the README's integration section with a short paragraph on tracing: which fields producers write, the per-mode defaults and how to change them.


## Concrete Steps


Locate dependency sources and confirm released versions before choosing ranges:

```bash
mori registry show shinzui/shibuya --full
mori registry show iand675/hs-opentelemetry --full
```

Check the current Hackage releases of `hs-opentelemetry-api`, `hs-opentelemetry-sdk`, `hs-opentelemetry-exporter-in-memory` and `unordered-containers`; the Kafka adapter currently uses `hs-opentelemetry-api ^>=1.0`, `hs-opentelemetry-exporter-in-memory ^>=1.0` and `unordered-containers ^>=0.2`. Add the API and `unordered-containers` to the library and the SDK and exporter to the new test suite in `shibuya-redis-adapter.cabal`. If the Nix package set lacks one, add it in `flake.module.nix` the way `shibuya-core` and `hedis` are added there; `docs/friction-log/1-default-nix-package-set-selects-an-incompatible-shibuya.md` describes the earlier occurrence.

Then, from the repository root:

```bash
nix develop -c cabal build all
nix develop -c cabal test runner-fixture telemetry-integration --test-show-details=direct
nix develop -c cabal test all --test-show-details=direct
```


## Validation and Acceptance


Accepted when `telemetry-integration` passes with the span assertions listed under M3 and the dead-letter assertion under M4, `runner-fixture` asserts the envelope fields and per-mode telemetry defaults, and all other suites still pass. A human check is also available: run a processor with an OTLP exporter against a local Jaeger, append an entry with a `traceparent` field, and see the consumer span attached to that trace for a group worker.


## Idempotence and Recovery


All steps are additive source edits. The dead-letter value format changes before any release, so no stored data needs migration; state this in the ADR amendment.


## Interfaces and Dependencies


New library dependencies: `hs-opentelemetry-api` and `unordered-containers`. New test dependencies: `hs-opentelemetry-sdk` and `hs-opentelemetry-exporter-in-memory`. Ranges are provisional until [plan 9](9-package-and-qualify-the-0-1-0-0-release.md) confirms them.

At the end of this plan these exist:

```haskell
-- Shibuya.Adapter.Redis.Types
defaultTraceContext :: RedisEntry -> Maybe TraceHeaders

-- ReaderConfig a
telemetry :: AdapterTelemetry
readerTraceContext :: RedisEntry -> Maybe TraceHeaders

-- GroupConfig a
telemetry :: AdapterTelemetry
groupTraceContext :: RedisEntry -> Maybe TraceHeaders

-- Shibuya.Adapter.Redis.DeadLetter
rejectionRecord :: RejectionOwner -> RedisEntry -> DeadLetterReason -> Maybe TraceHeaders -> (ByteString, ByteString)
```

`RuntimeOptions` loses `adapterTelemetry`. [Plan 8](8-narrow-name-and-document-the-public-api.md) owns the final names and export placement of everything above.
