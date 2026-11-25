---
type: Guide
title: "Resume an ordered projection"
description: "Persist a reader checkpoint and decide how to handle gaps after a restart or snapshot."
generated:
  by: codex-cli/0.172.0
  at: "2026-11-25T14:35:06Z"
docId: DOC-2
tags: ["reader", "checkpoint", "snapshot"]
---

# Resume an ordered projection

Start with `rawReaderConfig`. Set `checkpointStore = Just store` for a projection that survives process restarts. A stored checkpoint wins over every start policy. With an empty store, choose `Earliest` for retained history, `Live` for future entries, or `After checkpoint` for the suffix following a snapshot.

1. Key the store by the logical reader name and full source identity. Persist the completed entry ID and its continuity evidence together.
2. Give the store a single writer, or condition each save on the expected previous checkpoint. Accept an identical already-saved value so a lost write reply can be retried.
3. Return `AckOk` after the projection effect commits. A crash between that commit and the checkpoint save can replay the entry. Record the message ID with the effect in the same transaction when duplicates matter.

`readerProcessor` finalizes entries in order. A delayed retry holds later entries even when they have already been fetched. Use a handler that can complete one entry at a time.

For snapshot handover, have the producer supply a predecessor, revision or generation that the projection can check. Return the next evidence and any gap description from `checkContinuity`. Redis IDs may legitimately skip values. The adapter also checks for a stream tail behind the checkpoint. `onReaderGap` defaults to `StopAndRebuild`. Return `Resnapshot` to stop for application-managed snapshot replacement, or `BestEffort` when the projection can accept the gap.

The single-owner file store in `examples/Reader.hs` demonstrates cursor persistence. It stores no producer continuity evidence.
