---
type: Capability
title: "Recoverable consumer-group workers"
description: "Share work across consumers and reclaim pending entries with attempt-fenced finalization."
capabilityId: CAP-3
provider: mori://shinzui/shibuya-redis-adapter
status: shipped
stability: experimental
since: unreleased
packages: [shibuya-redis-adapter]
interface: ["newGroupWorker", "groupProcessor", "minimumIdleMillis", "groupInFlight"]
generated:
  by: codex-cli/0.172.0
  at: "2026-11-25T14:35:53Z"
evidence:
  - kind: test
    resource: test/GroupSpec.hs
    proves: "Exercises competing consumers, takeover back to an earlier owner, stale callbacks, lease renewal, delayed retry and bounded pending scans."
  - kind: example
    resource: examples/Worker.hs
    proves: "Creates or attaches to a group with a fresh consumer name."
---

# Recoverable consumer-group workers

Workers sharing one group divide entries among themselves. The adapter alternates new work with bounded scans of pending work. An explicit retry releases capacity until its deadline. Abandoned deliveries become eligible after the idle interval and any lease deadline. The worker checks attempt ownership before extending, retrying, rejecting or acknowledging.

## Limits

Workers may finish out of order. A stale handler can still perform external effects. Retain pending payloads until completion and use the missing-payload callback to record losses when retention removes an entry.
