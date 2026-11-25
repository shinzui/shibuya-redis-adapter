-- | Redis Streams readers and consumer-group workers for Shibuya.
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
    defaultTraceContext,
  )
where

import Shibuya.Adapter.Redis.Connection
import Shibuya.Adapter.Redis.DeadLetter
import Shibuya.Adapter.Redis.Group
import Shibuya.Adapter.Redis.Reader
import Shibuya.Adapter.Redis.Types
