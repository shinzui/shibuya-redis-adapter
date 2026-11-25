# Compatibility and operations

The adapter uses one Redis primary and one stream per instance. The public runtime supports both an owned command pool and a caller-borrowed command pool. Reserve capacity for finalization, recovery and shutdown independently of the blocking read connection.

## Qualified builds

The qualification target is GHC 9.12.4 on Apple Silicon with hedis 0.16.4, shibuya-core 0.13.0.1, streamly-core 0.3.3, effectful-core 2.7.1.2, entropy 0.4.1.11 and OpenTelemetry 1.0.0.0 (API, SDK and exporters). The release candidate passed all five suites on both Redis builds on 2026-11-25.

| Redis | Source revision | Build ID | Suites passed |
|---|---|---|---|
| 7.2.17 | `83937c51afb53e1c69661c8e1533e0e19b896fe1` | `68e86d123dada851` | 5 of 5 |
| 8.10.1 | `3399357e7c17b668289386b8a15a3037bc4527b1` | `63b009e75218a69d` | 5 of 5 |

Redis 7.2.17 was built from the upstream release tarball on Apple Silicon. The tarball SHA-256 was `07c679c013fd82513299733bddf91999bbbc830351ec9ea985a533b09a595d76`. Redis 8.10.1 came from the pinned Nix development shell. The version commands report `sha=00000000:0` for both source snapshots. The table lists the upstream tag revisions.

```sh
curl -fLO https://download.redis.io/releases/redis-7.2.17.tar.gz
tar -xzf redis-7.2.17.tar.gz
make -C redis-7.2.17 -j6 MALLOC=libc redis-server
redis-7.2.17/src/redis-server --version
nix develop -c redis-server --version
```

The group protocol uses exclusive `XPENDING` ranges, `XCLAIM` with `JUSTID`, Redis server time and server-side scripting. The qualified builds support these primitives without requiring server modules. Test another server executable by setting an absolute path in `REDIS_SERVER`.

```sh
nix develop -c cabal build all
nix develop -c cabal test all --test-show-details=direct
nix develop -c env REDIS_SERVER=/path/to/redis-server cabal test all --test-show-details=direct
nix flake check
```

On the same host, `cabal check` reported no errors or warnings. The unpacked source archive built with the default library-only configuration and with `-fexamples`. Haddock generated the public facade with 47 of 47 items documented. Both examples processed two entries across restarts. `nix flake check` passed, and the default Nix package built both examples and ran all five suites. An OTLP smoke test also retrieved both consumer spans from Jaeger.

## Scenario evidence

The suites launch private Redis servers through the selected executable and run the public processors through the Shibuya runner. Handler and finalization barriers coordinate competing connections. The release suite restarts its own AOF-enabled server and inspects progress over a fresh connection.

| Initial-spec scenario | Automated evidence |
|---|---|
| Independent projections | `reader-integration`, independent readers and checkpoint restart |
| Work pool and separate group | `group-integration`, concurrent creators, competing workers and another group |
| Worker takeover | `group-integration`, A→B→A ownership with stale success, retry, lease and rejection callbacks |
| Retry and permanent failure | `group-integration` covers same-worker retry, Redis-time deadlines, exception retry and exhausted retries. `reader-integration` covers exhaustion, rejection reconnect and ordered checkpointing. `release-integration` denies XACK after the failure record is written and verifies replay. |
| Script cache and lease loss | `group-integration` covers script cache flush recovery, skipping locally active rows, and stale lease status. |
| OpenTelemetry | `telemetry-integration` checks reader links, worker parent and link, Redis tags, and consumer context in dead letters. |
| Projection crash | `reader-integration`, forced stop after the effect and before checkpoint save |
| Snapshot handover | `reader-integration`, two readers after a shared cursor, predecessor and generation checks, and resnapshot |
| Retention and wire fidelity | `reader-integration` and `group-integration`, missing history and pending payloads. `runner-fixture` verifies ordered duplicate binary fields. |
| Bounded lifecycle | `release-integration`, blocked reads, stalled handler, Redis restart with AOF, borrow and drain, sibling halt, and finite reconnect exhaustion. The mode suites verify bounded pending scans, prefetch ordering and recovery. |

## Redis permissions

The connection requires `PING`. Configure authentication and database selection as needed. Grant key permissions only for the relevant stream, optional rejection hash and adapter state. Typical Redis ACL key patterns are `~stream-key`, `~failure-hash` and, for workers, `~shibuya:group:v1:*`.

| Capability | Commands |
|---|---|
| Reader | `XREAD`, `XINFO STREAM` |
| Optional reader rejection | `HSETNX` |
| Worker | `XREADGROUP`, `XPENDING`, `SCRIPT LOAD`, `EVALSHA`, `EVAL`, `XRANGE`, `XCLAIM`, `TIME`, `EXISTS`, `HGET`, `HSET`, `HDEL`, `XACK`, `DEL` |
| Optional worker group creation | `XGROUP CREATE` |
| Optional worker rejection | `HSETNX` executed in the finalizer script |
| Optional worker pending depth and age | `XPENDING`, including the summary form on scan wrap |

The restricted-credential test grants stream subcommands individually and exercises the Lua command permissions. Every worker in a group must use the same source identity, metadata protocol and rejection destination. Reserve the group state namespace for the adapter.

## Runtime and recovery

An empty blocking poll keeps the processor live. Socket loss, I/O failure or timeout consumes a finite reconnect budget. Exhausted polling retries, Redis command rejection and a missing group are processor failures. A reader detects cursor rollback from Redis metadata and checks application-provided producer evidence before admission. Redis ID distance cannot prove that an event was trimmed. Supply a revision, predecessor or generation when the application needs continuity across trim or stream recreation.

Reader retry holds subsequent entries behind the current one. Group retry stores a deadline on Redis and releases the local processing slot. Any worker, including the one that scheduled the retry, can reclaim the entry after that deadline. The group counts deliveries and claims toward the retry limit. On exhaustion either mode writes a `MaxRetriesExceeded` dead-letter record when a destination is configured. The reader commits afterward and the group acknowledges the entry. Without a destination, the processor stops with the checkpoint before the entry or with the entry still pending in the group. Recovery alternates new and pending work and caps pending detail pages. Abandoned work must meet the minimum idle interval and any lease deadline, while an explicit retry needs only its retry deadline. Optional pending telemetry reports total depth and the highest idle time in the sampled page. Redis summary work grows with the number of consumers, so enable it selectively for large groups.

A stale callback leaves current work untouched. Permanent failure writes an idempotent hash record before committing progress. With no destination, permanent failure stops the processor with the cursor or pending entry intact. An explicit halt also preserves that progress boundary. Missing pending payloads call the application loss observer before acknowledging the unrecoverable row. The application owns any replay from the failure destination.

Use persistence that can retain stream, group, attempt and rejection state together. If Redis rolls back acknowledged or pending state during restore or failover, applications may observe redelivery or lost work. Prevent eviction of all those keys and retain unread and pending payloads until they can be processed. Keep a failure record through its replay horizon and give a recreated stream a new source generation when identities span lifetimes. Maintain group positions and live consumer ownership through the adapter protocol. When retention removes a payload, preserve the pending reference so the adapter can report the loss. Operations that delete pending references can leave attempt hashes for operator cleanup. Unused Redis consumer records can be removed independently when an operator has confirmed that they own no pending entries.

Checkpoint callbacks own durable write consistency. Use one writer per checkpoint or a conditional write that accepts an already-committed identical value after reply loss. Shibuya owns runner scheduling and graceful shutdown deadlines. Adapter counters and pending samples carry no stream keys, values or credentials. The application owns its telemetry callbacks and tracing resources and should keep callbacks prompt and successful.

OpenTelemetry uses `traceparent` and optional `tracestate` stream fields by default. Set `telemetry` on the mode configuration to override the reader’s `LinkOnly` or the group worker’s `ParentAndLink` policy. Consumer spans identify Redis as the messaging system and use the native entry ID in the message ID tag. Rejection values written by a traced finalizer retain that consumer’s W3C context.
