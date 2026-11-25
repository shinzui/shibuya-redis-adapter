---
type: Friction Entry
title: "Haskell OTLP exporter appends the signal path to the trace endpoint"
description: "A full per-signal OTLP URL produced no traces in Jaeger until the URL was changed to a base prefix."
generated:
  by: "codex-cli/gpt-6.2-sol"
  at: "2026-11-25T14:13:21Z"
frictionId: FRIC-2
status: "accepted"
impact: detour
kind: interface
origin: "mori://shinzui/shibuya-redis-adapter"
affects: "mori://iand675/hs-opentelemetry/packages/hs-opentelemetry-exporter-otlp"
affectedVersion: "1.0.0.0"
plan: "docs/plans/7-propagate-trace-context-and-redis-span-attributes.md"
workaround: "Configure the collector prefix without /v1/traces and query the global Jaeger API for the exported spans."
resolution: "The Jaeger check uses the collector prefix for the released exporter and waits for the trace query to return the spans."
amended:
  - by: "codex-cli/gpt-6.2-sol"
    at: "2026-11-25T14:13:35Z"
    change: "Document the working endpoint and validate exported traces through the global Jaeger API."
---
# Haskell OTLP exporter appends the signal path to the trace endpoint

## Attempted

Set OTEL_EXPORTER_OTLP_TRACES_ENDPOINT to the collector URL ending in /v1/traces and run the Jaeger smoke test.

## Encountered

The 1.0.0.0 HTTP exporter appended /v1/traces to the configured URL. A successful processor flush alone did not establish that the endpoint stored the spans.

```text
Jaeger trace lookup returned HTTP 404 for the original run. The exporter builds its request with parseRequest (httpHost conf <> "/v1/traces").
```

## Workaround

Configure the collector prefix without /v1/traces and query the global Jaeger API for the exported spans.
