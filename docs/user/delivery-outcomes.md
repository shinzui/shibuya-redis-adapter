---
type: Reference
title: "Delivery outcomes and trace context"
description: "Look up progress, dead-letter and tracing behavior for a reader or worker delivery."
generated:
  by: codex-cli/0.172.0
  at: "2026-11-25T14:35:06Z"
docId: DOC-4
tags: ["acknowledgement", "dead-letter", "opentelemetry"]
---

# Delivery outcomes and trace context

| Handler outcome | Independent reader | Group worker |
|---|---|---|
| `AckOk` | Saves the completed cursor. | Acknowledges the current attempt. |
| `AckRetry` | Holds the current entry until retry. | Leaves it pending until the retry deadline. |
| `AckDeadLetter` | Writes the rejection before advancing. | Writes the rejection before acknowledging the current attempt. |
| `AckHalt` | Stops with the checkpoint before the entry. | Stops with the entry pending. |

Set `readerDeadLetters` or `groupDeadLetters` to a Redis hash key to enable rejection storage. A permanent decode failure also takes the rejection path. Exhaustion of `readerRetryLimit` or `groupRetryLimit` uses reason `MaxRetriesExceeded`. Without a destination, rejection or exhaustion stops the processor at the current progress boundary.

`rejectionRecord` describes the binary field and value. The field scopes an entry to its reader or group. Redis keeps the first record on a repeated write. The value carries the reason, original ordered field pairs and any consumer trace context available at finalization. Applications own retention and replay.

Put a W3C `traceparent` and optional `tracestate` in the producer's stream fields. With tracing enabled, readers default to `LinkOnly` and start a new trace linked to the producer. Group workers default to `ParentAndLink` and join the producer trace. Override `telemetry.traceCorrelation` on the configuration or supply `readerTraceContext` / `groupTraceContext` for another field convention. Consumer spans use the native Redis entry ID for `messaging.message.id`. The envelope's idempotency key also incorporates source identity.
