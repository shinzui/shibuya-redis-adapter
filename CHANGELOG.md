# Changelog

## 0.1.0.0

- Add independent Redis Streams readers with ordered checkpoints and consumer-group workers with fenced recovery.
- Support delayed retries and idempotent dead-letter records, including exhausted retries and consumer trace context.
- Propagate W3C trace context to Shibuya’s OpenTelemetry consumer spans, with configurable reader and worker correlation policies.
- Provide optional reader and worker examples. The compatibility matrix covers Redis 7.2.17 and 8.10.1 on GHC 9.12.4.
