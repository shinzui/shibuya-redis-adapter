---
okf_version: "0.2"
---

# Files

- [profile.dhall](profile.dhall)

# Capability

- [Binary entries and stable message identity](binary-entries.md) - Decode ordered binary field pairs while retaining a source-scoped message ID across redelivery.
- [Idempotent dead-letter records](dead-letter-records.md) - Record rejected or retry-exhausted entries in a Redis hash before committing progress.
- [Independent ordered readers](ordered-readers.md) - Run multiple projections over a stream with separate identities and ordered checkpoint commits.
- [Recoverable consumer-group workers](recoverable-worker-pools.md) - Share work across consumers and reclaim pending entries with attempt-fenced finalization.
- [Snapshot handover and gap detection](snapshot-continuity.md) - Resume after a snapshot and inspect evidence of a stream rollback or producer discontinuity.
- [Producer and consumer trace correlation](trace-correlation.md) - Correlate Shibuya consumer spans with W3C context carried by Redis stream fields.
