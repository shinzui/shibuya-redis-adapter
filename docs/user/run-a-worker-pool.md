---
type: Guide
title: "Run a recoverable worker pool"
description: "Configure group position, concurrency and lease renewal for competing workers."
generated:
  by: codex-cli/0.172.0
  at: "2026-11-25T14:35:06Z"
docId: DOC-3
tags: ["consumer-group", "lease", "recovery"]
---

# Run a recoverable worker pool

Build a worker with `newGroupWorker rt (rawGroupConfig name source group consumer start)`, then pass `groupProcessor worker handler` to the runner. Use the same source identity, group and dead-letter destination for all instances. Generate a unique consumer name on each launch as in `examples/Worker.hs`.

| Start policy | When to use it |
|---|---|
| `CreateIfAbsent (EntryId 0 0)` | Create a missing group to process retained history. |
| `CreateAtTailIfAbsent` | Create a missing group for future entries. |
| `AttachExisting` | Require an already provisioned group. |

Creation preserves an existing group's position. Set `groupInFlight` to the maximum outstanding deliveries per instance (default 1). Workers can complete different entries out of order. A separate group has its own position and pending list.

For a long-running handler, extend the message lease before the minimum idle interval expires (default 30 seconds). Abandoned work becomes reclaimable after the idle interval and any extended deadline. An explicit retry releases the local slot and uses its own Redis-time deadline. Any worker, including the original worker, can pick it up. Delivery and claim attempts count toward the retry budget.

A callback from an older attempt cannot acknowledge, retry or reject a newer attempt, even if ownership has returned to the original consumer. Deduplicate external effects independently because an old handler can still finish. Retain pending payloads through the recovery window and install `onMissingPending` to record a missing payload before the pending reference is acknowledged.
