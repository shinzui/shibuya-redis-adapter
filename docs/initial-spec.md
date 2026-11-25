---
title: "Shibuya Redis Streams Adapter — Initial Specification"
type: library-spec
status: proposed
canonical: mori://shinzui/shibuya-redis-adapter/docs/initial-spec
---

# Shibuya Redis Streams Adapter

## Purpose

A Haskell application should be able to attach a Shibuya processor to a Redis Stream and choose who receives each entry. A search indexer and a reporting projection may each need every event. A pool of thumbnail workers should share jobs and recover work when a worker disappears. A live view may need only the changes after a snapshot. The adapter must support all three uses with ordinary Shibuya handlers.

The proposed package is `shibuya-redis-adapter`, released independently of Shibuya. Its first-release contract covers independent readers and consumer groups against a single Redis primary. One adapter instance reads one stream. Applications can compose instances through Shibuya. Redis Cluster, cross-stream ordering, producer transactions and queue administration are outside the initial release. Package bounds and the supported Redis version range will be established against released dependencies and tested Redis servers.

## Delivery modes

| Contract | Independent reader | Consumer-group worker |
|---|---|---|
| Who receives an entry? | Every interested reader whose starting position includes it. | One member receives the initial assignment in each group. Another member may take over pending work. |
| Typical use | Rebuild a projection, tail changes, feed subscribers on multiple replicas. | Distribute jobs across processes or run several competing workers for one projection. |
| Where is progress kept? | A reader-local completed cursor, optionally persisted through an application checkpoint. | Redis holds the group's delivery position and pending entries. Successful finalization acknowledges each entry. |
| What order is promised? | Serial processing preserves stream order, including retries. | Redis assigns new entries in stream order. Concurrent execution, retries and claims can change completion order. |
| What survives a crash? | A replacement reader resumes after the last saved checkpoint, while entries are retained. | A replacement worker recovers unacknowledged, retained entries through pending-entry recovery. |

### Independent readers

Use `XREAD` with an exclusive position. Expose three explicit starts: earliest retained, after a supplied stream ID, and live tail. Resolve live tail to a concrete position once at startup, including for an empty stream. Reuse the concrete position on later polls and reconnects so an append between polls remains visible. Reading or finalizing on one reader leaves every other reader's position and the source entry intact. [Redis XREAD](https://redis.io/docs/latest/commands/xread/)

Expose the **completed cursor** separately from the last fetched ID. Prefetch may read B while A is running. A crash at that point must allow A to be delivered again. For restartable projections, provide a way to load and save a checkpoint under a caller-supplied reader identity and stream identity. A failed save leaves progress retryable. The application must arrange exclusive ownership or conditional writes if two processes can update the same durable checkpoint. An ephemeral subscriber can keep its cursor in memory.

A Redis ID orders entries within a stream. It cannot establish that every intervening event still exists. Preserve stream identity, entry ID and raw fields so consumers can check a producer revision, predecessor or generation. Expose detected loss of history or stream replacement as a distinct outcome. Document what is unknowable after deletion, trimming or recreation without producer evidence. A strict projection can stop and rebuild. A live view can resnapshot. A best-effort tailer can explicitly elect to continue.

### Consumer groups

Use `XREADGROUP` with acknowledgements enabled. Require an explicit group and a consumer identity unique to the running worker instance. Support attaching to an existing group and an opt-in create-if-absent policy with an explicit initial position. Concurrent creation must preserve the existing group's position. Acquisition and reconnect must never reset an existing group. [Redis XGROUP CREATE](https://redis.io/docs/latest/commands/xgroup-create/) and [XREADGROUP](https://redis.io/docs/latest/commands/xreadgroup/)

A delivered entry stays in the group's pending entries list until finalization or an explicit recovery action. Bound recovery work per poll and share capacity between new and abandoned work so both make progress. Reclaim entries after a configurable minimum idle interval. Long-running handlers need an ownership-checked extension of that interval or a documented handling-time limit. If retention removes a pending payload, surface a loss diagnostic with its identity and the chosen recovery action. A missing payload cannot yield a successful handler result. [Redis XPENDING](https://redis.io/docs/latest/commands/xpending/) and [XAUTOCLAIM](https://redis.io/docs/latest/commands/xautoclaim/)

Fence late callbacks after a claim. For example, worker A stalls, B claims the entry, then A returns success. Redis `XACK` identifies the stream, group and entry, so an unguarded call from A could clear B's pending work. Verify ownership and delivery generation atomically with acknowledgement, retry scheduling, lease extension and dead-letter finalization. This must hold even if ownership moves away and back. A stale callback leaves the current attempt untouched. Application effects may still occur twice across a crash or claim. Handlers that send mail, charge an account or write another database need their own idempotency key. [Redis XACK](https://redis.io/docs/latest/commands/xack/)

## Shibuya interface and message identity

Provide public constructors in `Shibuya.Adapter.Redis` with distinct configuration types for each mode. Each constructs a standard `Adapter es msg` with an effectful source, shutdown action, adapter name and configurable Shibuya telemetry. Use hedis for Redis access. A caller may supply a connection pool. Document which connections the adapter acquires and releases. The interface and envelope contract belong to `mori://shinzui/shibuya/packages/shibuya-core`. The Redis client is `mori://informatikr/hedis/packages/hedis`.

The default payload preserves the stream key, Redis entry ID and ordered binary field/value pairs, including duplicate field names. Consumers can opt into a typed decoder or header convention. Decoding errors must retain the source identity and follow the configured permanent-failure policy. Build a stable Shibuya message ID from the stream identity and entry ID with unambiguous encoding. A reclaimed or retried entry keeps its ID. Applications that recreate stream keys must include a source generation if deduplication spans those lifetimes. Expose a zero-based attempt count only where the backend or adapter can support it. Keep producer event time distinct from Redis entry ID time.

Shibuya owns scheduling, supervision and the call to finalize each decision. The adapter owns Redis progress, reader-local retry and recovery of pending group entries. Admission can suppress a known superseded delivery. Because ownership can change while the handler runs, the finalizer still needs its own fence. The relevant architecture is `mori://shinzui/shibuya/docs/architecture`.

## Handler decisions

| Shibuya decision | Independent reader | Consumer-group worker |
|---|---|---|
| `AckOk` | Advance the completed cursor through the contiguous successful prefix and persist it when configured. | Acknowledge the current owned attempt. Leave the source entry available to other groups and readers. |
| `AckRetry delay` | Retry the same entry after the delay. In serial mode hold later entries behind it. | Keep the work recoverable and honor the retry due time across workers and process restarts. Other jobs can proceed. |
| `AckDeadLetter reason` | With a configured destination, record source identity and reason, then advance. Without one, halt at the rejected entry. | With a configured destination, record the failure and acknowledge the owned attempt as one recoverable operation. Without one, halt and leave the entry pending. |
| `AckHalt reason` | Stop this processor with its completed cursor before the halted entry. | Stop this processor and leave the entry available for pending recovery. Sibling processors continue under their configured supervision. |

A repeated finalizer after a lost Redis reply must yield the same logical result. Preserve Shibuya's structured reason code and bounded detail in a dead-letter record. Use an idempotency identity scoped to the logical reader or group and source entry, so two groups can independently reject the same input. A checkpoint failure after a successful dead-letter write must allow a retry without creating a second logical record. Document the recovery of each partially completed operation and the retention window of any deduplication metadata. Applications own replay from a dead-letter destination.

## Operations and compatibility

Bound blocking-read duration, prefetch, in-flight work, retries and pending-recovery scans. Reserve connection capacity for acknowledgement, recovery and shutdown while reads block. Reject unsupported ordering or concurrency combinations before admitting work. Document which failures are transient, which stop a processor, and how to distinguish an idle stream from a lost Redis connection. Shutdown stops new polls, permits admitted finalizers to drain within the Shibuya deadline, and releases adapter-owned resources. Repeated shutdown is safe.

Use Shibuya's telemetry options. Report delivered, acknowledged, retried, reclaimed, superseded and dead-lettered counts, checkpoint failures and detected retention gaps. Expose pending backlog and age where available. Keep labels bounded and payloads, credentials and application-specific stream keys out of default telemetry.

The project follows `mori://shinzui/haskell-jitsurei/docs/core-standards` and uses `mori://shinzui/seihou-modules/seihou/modules/nix-haskell-flake` for development and real-Redis tests. Publish the tested Redis capabilities, server versions, and the minimum commands or ACL permissions for each mode. State the persistence, eviction and retention assumptions behind recovery guarantees. A Redis restore that rolls back group state or a stream that trims outstanding jobs can invalidate those guarantees.

## Acceptance scenarios

Run these cases with a real Redis server and the Shibuya processor runner. Use independent connections for competing actors and synchronize at delivery, claim and finalization boundaries so timing alone does not determine success.

| Scenario | Observable result |
|---|---|
| Independent projections | An indexer and a reporting projection read the same three IDs. Pause one, append another entry and restart it from its saved cursor. Both finish with the same ordered IDs and independent checkpoints. |
| Work pool and separate group | Two workers in one group divide jobs under normal operation. A worker in another group can process the entire input. Successful acknowledgements in either group leave the other group's progress intact. |
| Worker takeover | Stop A after delivery and let B claim its pending job. Release A's delayed finalizer. B still owns the job and completes it. Repeat with ownership moving back to A under a new attempt. |
| Retry and permanent failure | Request a delayed retry, restart a worker, then permanently reject the retried entry. It cannot run early. Drop the finalizer reply and retry: one logical dead-letter record exists with the original source identity. |
| Projection crash | Commit an application effect, then crash before saving its checkpoint. The same message ID is redelivered and an idempotent handler produces one logical effect. |
| Snapshot handover | Capture a roster and cursor, then append a change before attaching two independent Kehai readers. Both receive the change or the application detects lost continuity and resnapshots. Kehai owns lease updates, viewer authorization and SSE. Its contract is `mori://shinzui/kehai/docs/initial-spec`. |
| Retention and wire fidelity | Trim an unread change and a pending job. Observe the defined gap evidence or missing-payload outcome. A separate binary entry with repeated field names round-trips unchanged. |
| Bounded lifecycle | Hold a read open on an idle stream, stall a handler and disconnect Redis. Verify bounded intake and shutdown, intact completed progress, recoverable pending work, and continued service for an independent reader after reconnection. |

Initial release evidence includes a buildable consumer example for each delivery mode and the compatibility matrix used to run these scenarios. Redis deployment automation, domain-specific stream schemas, SSE, recurring job scheduling and a general producer API belong to their respective applications or follow-up specifications.
