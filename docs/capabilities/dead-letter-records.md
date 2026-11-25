---
type: Capability
title: "Idempotent dead-letter records"
description: "Record rejected or retry-exhausted entries in a Redis hash before committing progress."
capabilityId: CAP-5
provider: mori://shinzui/shibuya-redis-adapter
status: shipped
stability: experimental
since: unreleased
packages: [shibuya-redis-adapter]
interface: ["readerDeadLetters", "groupDeadLetters", "rejectionRecord"]
generated:
  by: codex-cli/0.172.0
  at: "2026-11-25T14:35:53Z"
evidence:
  - kind: test
    resource: test/ReaderSpec.hs
    proves: "Checks ordered rejection, reconnect and retry exhaustion."
  - kind: test
    resource: test/ReleaseSpec.hs
    proves: "Exercises replay after the rejection write succeeds and XACK fails."
---

# Idempotent dead-letter records

Configure a rejection hash for either delivery mode. A permanent rejection stores the reason and original field pairs under a key scoped to the reader or group and message identity. Retry exhaustion uses MaxRetriesExceeded. Repeating the write keeps the first record. A reader then commits its checkpoint and a worker acknowledges its pending entry.

## Limits

Without a destination, rejection stops processing with the entry outstanding. Retain the hash through the replay horizon. Redrive is an application operation.
