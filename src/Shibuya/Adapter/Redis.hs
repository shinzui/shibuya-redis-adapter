-- | Redis Streams readers and consumer-group workers for Shibuya.
--
-- Keep 'withRedisRuntime' around construction, processing and graceful
-- shutdown. Attach one reader or worker per runtime. Each runtime uses a
-- dedicated Redis read connection and an owned or borrowed command pool.
--
-- @
-- {-# LANGUAGE OverloadedStrings #-}
--
-- import Effectful (liftIO, runEff)
-- import Shibuya
-- import Shibuya.Adapter.Redis
--
-- runReader connection = withRedisRuntime (OwnPools connection) defaultRuntimeOptions $ \rt -> do
--   let source = SourceIdentity "inventory" Nothing "inventory-events"
--   reader <- newReader rt (rawReaderConfig "projection" "search-index" source Earliest)
--   run (readerProcessor reader (const (pure AckOk)))
--
-- runWorker connection consumer = withRedisRuntime (OwnPools connection) defaultRuntimeOptions $ \rt -> do
--   let source = SourceIdentity "images" Nothing "image-jobs"
--   worker <- newGroupWorker rt (rawGroupConfig "worker" source "thumbnails" consumer (CreateIfAbsent (EntryId 0 0)))
--   run (groupProcessor worker (const (pure AckOk)))
--
-- run processor = runEff $ runTracingNoop $ do
--   result <- runApp defaultAppConfig [(ProcessorId "redis", processor)]
--   either (liftIO . fail . show) waitApp result
-- @
--
-- The reader resumes after its completed checkpoint and commits in stream
-- order. A worker uses Redis group state and its pending entries list. Give
-- each running worker a unique consumer name. Extend its lease before a
-- long-running handler reaches the recovery deadline.
--
-- * 'Shibuya.AckOk' commits the reader checkpoint or acknowledges the pending worker entry.
-- * 'Shibuya.AckRetry' delays the current reader entry or makes a pending worker entry eligible at the retry deadline. A thrown handler exception requests an immediate retry.
-- * 'Shibuya.AckDeadLetter' writes the failure to the configured hash before committing or acknowledging.
-- * 'Shibuya.AckHalt' stops intake and leaves the current entry at its existing progress boundary.
--
-- On retry exhaustion, a configured hash receives 'Shibuya.MaxRetriesExceeded'
-- and the processor continues. Without a hash, exhaustion or rejection stops
-- the processor with the checkpoint before the entry or the entry still pending.
--
-- Producers can write W3C @traceparent@ and optional @tracestate@ fields.
-- Readers use 'LinkOnly' and group workers use 'ParentAndLink' by default.
-- Set the mode configuration’s @telemetry@ field to override correlation.
module Shibuya.Adapter.Redis
  ( -- * Runtime
    withRedisRuntime,
    ConnectionSource (..),
    RuntimeOptions (..),
    defaultRuntimeOptions,
    RedisRuntime,
    stopIntake,

    -- * Sources and entries
    SourceIdentity (..),
    EntryId (..),
    renderEntryId,
    parseEntryId,
    RedisEntry (..),

    -- * Independent readers
    ReaderConfig (..),
    ReaderStart (..),
    Checkpoint (..),
    CheckpointStore (..),
    GapEvidence (..),
    ReaderGap (..),
    GapAction (..),
    Reader (..),
    rawReaderConfig,
    newReader,
    readerProcessor,

    -- * Consumer groups
    GroupConfig (..),
    GroupStart (..),
    Group (..),
    MissingPending (..),
    rawGroupConfig,
    newGroupWorker,
    groupProcessor,

    -- * Events and errors
    AdapterEvent (..),
    AdapterError (..),
    ReaderError (..),
    GroupError (..),

    -- * Dead-letter records
    RejectionOwner (..),
    rejectionRecord,

    -- * Telemetry
    AdapterTelemetry (traceCorrelation),
    defaultAdapterTelemetry,
    TraceCorrelation (..),
    defaultTraceContext,
  )
where

import Shibuya (AdapterTelemetry (traceCorrelation), TraceCorrelation (..), defaultAdapterTelemetry)
import Shibuya.Adapter.Redis.Connection
import Shibuya.Adapter.Redis.DeadLetter
import Shibuya.Adapter.Redis.Group
import Shibuya.Adapter.Redis.Reader
import Shibuya.Adapter.Redis.Types
