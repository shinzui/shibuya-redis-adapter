---
type: Capability
title: "Binary entries and stable message identity"
description: "Decode ordered binary field pairs while retaining a source-scoped message ID across redelivery."
capabilityId: CAP-4
provider: mori://shinzui/shibuya-redis-adapter
status: shipped
stability: experimental
since: unreleased
packages: [shibuya-redis-adapter]
interface: ["SourceIdentity", "RedisEntry", "readerDecode", "groupDecode"]
generated:
  by: codex-cli/0.172.0
  at: "2026-11-25T14:35:53Z"
evidence:
  - kind: test
    resource: test/Main.hs
    proves: "Checks binary delivery, identity framing, absent versus empty generation, and decoder failure identity."
  - kind: module
    resource: src/Shibuya/Adapter/Redis/Types.hs
    proves: "Defines the envelope cursor, message identity and field-preserving conversion."
---

# Binary entries and stable message identity

Both delivery modes expose the native entry ID and original field/value sequence, including duplicate field names. A typed decoder changes the payload while preserving the envelope identity. The message ID incorporates namespace, optional generation, stream key and native entry ID.

## Limits

Recreating a stream can reuse native IDs. Supply a distinct generation when old and new entries must remain distinguishable. The application owns durable deduplication of external writes.
