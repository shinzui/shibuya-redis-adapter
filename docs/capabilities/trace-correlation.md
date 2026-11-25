---
type: Capability
title: "Producer and consumer trace correlation"
description: "Correlate Shibuya consumer spans with W3C context carried by Redis stream fields."
capabilityId: CAP-6
provider: mori://shinzui/shibuya-redis-adapter
status: shipped
stability: experimental
since: unreleased
packages: [shibuya-redis-adapter]
interface: ["defaultTraceContext", "traceCorrelation", "readerTraceContext", "groupTraceContext"]
generated:
  by: codex-cli/0.172.0
  at: "2026-11-25T14:35:53Z"
evidence:
  - kind: test
    resource: test/TelemetrySpec.hs
    proves: "Inspects exported consumer spans, producer parent and links, Redis attributes and context in rejection records."
---

# Producer and consumer trace correlation

Readers link a new trace to the producer by default. Workers make the producer span their parent and also link to it. Both support a custom extractor and correlation policy. Redis span attributes include the native entry ID. A traced finalizer can carry its consumer context into the rejection record.

## Limits

Producer correlation needs valid incoming context. Applications configure their tracing interpreter, provider and export destination.
