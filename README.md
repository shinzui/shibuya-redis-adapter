# Shibuya Redis Streams adapter

This Haskell library connects Shibuya processors to one Redis Stream per adapter on a single primary.

| Use case | Constructor | Progress |
|---|---|---|
| A projection or live subscriber needs its own ordered copy | `newReader` and `readerProcessor` | Completed cursor with optional application-managed checkpoint |
| Workers share jobs and recover abandoned assignments | `newGroupWorker` and `groupProcessor` | Redis group position, pending entries and fenced attempt state |

Both constructors accept a raw binary entry or a typed decoder. They preserve field order and duplicate names. The envelope's stable message ID is an application idempotency key. Commit external effects with that key in durable application storage when they must survive replay.

## Run both examples

Start the project-local Redis service in one terminal. Stop it with Ctrl-C after the demo.

```sh
nix develop
process-compose -U up -t=false
```

In another terminal, start from the repository root. Use a fresh stream and checkpoint file.

```sh
nix develop
stream="shibuya-demo-$(date +%s)"
checkpoint=".dev/${stream}.cursor"
redis-cli -s "$REDIS_SOCKET" XADD "$stream" 1-0 item first
cabal run reader-example -- "$REDIS_SOCKET" "$stream" "$checkpoint" 1
cabal run worker-example -- "$REDIS_SOCKET" "$stream" thumbnails earliest 1

redis-cli -s "$REDIS_SOCKET" XADD "$stream" 2-0 item second
cabal run reader-example -- "$REDIS_SOCKET" "$stream" "$checkpoint" 1
cabal run worker-example -- "$REDIS_SOCKET" "$stream" thumbnails attach 1
```

Both examples print entry `1-0` on their first invocation and `2-0` on their second, followed by the stable message ID. Each exits after processing the requested number of entries. The reader replaces its checkpoint file atomically and expects a single owner. The worker generates a new consumer name each time it starts. Use `tail` to create a missing group at the current end of its stream.

## Integrate with an application

Use `withRedisRuntime` around processor construction, `runApp`, and the complete shutdown. Each runtime reserves one dedicated read connection alongside an owned or caller-borrowed command pool. Keep the borrowed pool open until finalizers drain.

Readers can start at `Earliest`, `After checkpoint`, or `Live`. A saved checkpoint takes precedence. Store the completed ID and any producer continuity evidence together under the reader's logical name and source identity. When attaching after a snapshot, pass the snapshot cursor and evidence to `After`, then check the next entry's generation or predecessor with `checkContinuity`. Use `onReaderGap` to stop, request a resnapshot, or accept the discontinuity explicitly.

Workers use explicit group creation or attachment and unique per-instance consumer names. Call the optional message lease's `leaseExtend` before a long-running handler becomes eligible for recovery. A stale finalizer cannot acknowledge or modify a newer attempt. A stale handler may still finish an external effect, so transactionally deduplicate effects that must apply once.

The processor helpers set compatible Shibuya policies. Readers use strict serial processing, and workers use unordered processing up to the configured group capacity. A reader cannot supply a batch that waits for several unfinalized entries. Use separate runtimes for separate processors.

See [compatibility and operations](docs/compatibility.md) for tested versions, Redis permissions and recovery behavior.
