---
type: Capability
title: "Snapshot handover and gap detection"
description: "Resume after a snapshot and inspect evidence of a stream rollback or producer discontinuity."
capabilityId: CAP-2
provider: mori://shinzui/shibuya-redis-adapter
status: shipped
stability: experimental
since: unreleased
packages: [shibuya-redis-adapter]
interface: ["After", "checkContinuity", "onReaderGap"]
generated:
  by: codex-cli/0.172.0
  at: "2026-11-25T14:35:53Z"
requires: [CAP-1]
evidence:
  - kind: test
    resource: test/ReaderSpec.hs
    proves: "The handover scenario exercises snapshot readers, predecessor and generation checks, and gap disposition."
---

# Snapshot handover and gap detection

Start an [ordered reader](ordered-readers.md) after the snapshot checkpoint. Persist opaque continuity evidence with each completed ID. The reader checks for Redis tail rollback and passes the candidate entry to the application continuity check. The application can stop, resnapshot or explicitly accept a discontinuity.

## Limits

Redis IDs alone cannot reveal all trimmed history. The producer must supply evidence such as a predecessor when the application needs to detect missing entries. Snapshot creation and rebuilding belong to the application.
