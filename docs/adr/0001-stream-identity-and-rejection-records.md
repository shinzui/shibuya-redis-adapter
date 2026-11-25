# Preserve binary stream identity across delivery attempts

Accepted 2026-11-24.

A Shibuya message ID is text. Redis stream keys and field names can contain arbitrary bytes, and two different streams can have the same numeric entry ID. A logical source consists of an application namespace, an optional generation and a stream key. Producers that recreate a key must change the generation when deduplication spans its lifetimes.

Use lowercase hexadecimal text for the message ID. Its underlying bytes are a sequence of components, each prefixed by its unsigned 64-bit big-endian byte length. The fields are the ASCII version tag redis-stream-v1, namespace, generation marker (absent or present), generation bytes when present, stream key, and canonical decimal milliseconds-sequence ID. The original entry keeps field/value pairs in order, including duplicate names. Envelopes leave enqueue time and attempt absent until their values are known.

A dead-letter destination is a Redis hash. Its field consists of framed components for redis-rejection-v1, owner kind (reader or group), logical owner name and the binary entry identity. Its value frames redis-rejection-v1, structured reason code, absent or present detail marker, optional UTF-8 detail bounded to 512 characters, then the trace marker and the raw fields and values in order. The trace marker is `absent` or `present` followed by the consumer span’s W3C `traceparent` and `tracestate` (empty when unavailable). Decode-time rejections have no consumer span. This value-format change precedes the first release and requires no migration. HSETNX preserves the first rejection for that owner and input. The group worker must include this operation within its ownership check and acknowledgement. The reader must finish it before saving the checkpoint.

The record itself provides deduplication for as long as the hash field exists. Retain it through the maximum supported replay interval. Removing it permits another rejection to recreate it. Ordinary handlers must deduplicate their own external effects.
