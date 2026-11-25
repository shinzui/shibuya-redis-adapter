# Correlate Redis deliveries with producer traces

Accepted 2026-11-25.

Redis Streams have no separate protocol headers. Both adapter modes extract `traceparent` and optional `tracestate` from stream fields by default, without removing them from the raw entry. Applications can replace the extractor if they use a different field convention. Shibuya validates W3C context and owns the consumer span.

An independent reader may replay old events or coexist with other readers. Its default `LinkOnly` telemetry starts a new trace with a link to the producer. A group worker defaults to `ParentAndLink` for queue processing. Its consumer span joins the producer trace and also links to the producer span. Applications can override either default on the mode configuration.

Consumer spans set `messaging.system` to `redis` and include the canonical ID in `shibuya.redis.entry_id`. Group worker spans also include `messaging.consumer.group.name` when the name is valid UTF-8. The span uses the native Redis entry ID for `messaging.message.id` to keep stream keys out of the attributes. The envelope still uses the full length-framed identity for application deduplication. A dead-letter record written during finalization stores the consumer trace context as specified by [ADR 1](0001-stream-identity-and-rejection-records.md).
