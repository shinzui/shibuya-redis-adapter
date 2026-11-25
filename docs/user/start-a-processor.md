---
type: Guide
title: "Connect a Redis stream to a processor"
description: "Choose a delivery mode and run its processor for the lifetime of a Redis runtime."
generated:
  by: codex-cli/0.172.0
  at: "2026-11-25T14:35:06Z"
docId: DOC-1
tags: ["redis-streams", "integration", "shutdown"]
---

# Connect a Redis stream to a processor

Choose a reader when every projection needs every retained entry. Give each projection its own checkpoint identity. Choose a consumer group when several instances should share the jobs in a stream. Give those instances the same group name and a distinct consumer name on each start.

Import `Shibuya.Adapter.Redis` for the adapter and `Shibuya` for the runner. Set `SourceIdentity` to an application namespace, an optional generation, and the binary stream key. Change the generation if a rebuilt stream can reuse old IDs.

For example, this reader processes every retained entry and acknowledges it. It uses the no-op tracing interpreter.

```haskell
{-# LANGUAGE OverloadedStrings #-}

import Effectful (liftIO, runEff)
import Shibuya
import Shibuya.Adapter.Redis

runProjection connection =
  withRedisRuntime (OwnPools connection) defaultRuntimeOptions $ \rt -> do
    let source = SourceIdentity "inventory" Nothing "stock-events"
    reader <- newReader rt (rawReaderConfig "stock" "projection" source Earliest)
    runEff $ runTracingNoop $ do
      result <- runApp defaultAppConfig
        [(ProcessorId "stock", readerProcessor reader (const (pure AckOk)))]
      either (liftIO . fail . show) waitApp result
```

To process application data, replace the handler and set `readerDecode` or `groupDecode` on the raw configuration. The decoder receives field/value pairs in Redis order, including duplicate names and non-text bytes. Use the envelope's stable message ID as the idempotency key when recording an effect that must survive a crash.

Allocate one runtime per processor. `BorrowCommands` lets the application supply a command pool for the same primary and database. Both connection modes reserve a separate read connection. Run graceful shutdown inside the runtime bracket so acknowledgements and checkpoint saves can finish.

The repository README walks through running both executables against the local Redis socket. To configure durable progress, follow [Resume an ordered projection](resume-a-projection.md) or [Run a recoverable worker pool](run-a-worker-pool.md).
