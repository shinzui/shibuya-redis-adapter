---
type: Capability
title: "Independent ordered readers"
description: "Run multiple projections over a stream with separate identities and ordered checkpoint commits."
capabilityId: CAP-1
provider: mori://shinzui/shibuya-redis-adapter
status: shipped
stability: experimental
since: unreleased
packages: [shibuya-redis-adapter]
interface: ["newReader", "readerProcessor", "CheckpointStore"]
generated:
  by: codex-cli/0.172.0
  at: "2026-11-25T14:35:53Z"
evidence:
  - kind: test
    resource: test/ReaderSpec.hs
    proves: "Exercises independent readers, restart, retry ordering and replay after a crash."
  - kind: example
    resource: examples/Reader.hs
    proves: "Runs a reader with a single-owner file checkpoint."
---

# Independent ordered readers

Each reader uses XREAD and tracks its own completed cursor. A saved checkpoint takes precedence over the configured earliest, live or snapshot start. Fetching ahead does not advance progress past an unfinished entry.

## Limits

The default checkpoint is in memory. A durable projection supplies its own store and handles replay between committing an effect and saving its cursor. A handler that waits for multiple unfinished entries can block this serial processor.
