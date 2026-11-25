module Shibuya.Adapter.Redis
  ( EntryId (..),
    renderEntryId,
    parseEntryId,
    SourceIdentity (..),
    RedisEntry (..),
    entryEnvelope,
    DecodeFailure (..),
    decodeEnvelope,
    AdapterEvent (..),
    RejectionOwner (..),
    rejectionRecord,
    storeRejection,
    mkRedisAdapter,
    RedisRuntime,
    RuntimeOptions (..),
    defaultRuntimeOptions,
    ConnectionSource (..),
    DeliveryMode (..),
    validateDeliveryPolicy,
    readBatchOptions,
    AdapterError (..),
    withRedisRuntime,
    runCommand,
    pollStream,
    stopIntake,
  )
where

import Data.Text (Text)
import Effectful (Eff, IOE, liftIO, (:>))
import Shibuya (Adapter (..), Ingested)
import Shibuya.Adapter.Redis.Connection
import Shibuya.Adapter.Redis.DeadLetter
import Shibuya.Adapter.Redis.Types
import Streamly.Data.Stream (Stream)

-- | Attach a mode's delivery stream to the runner. Keep the runtime bracket
-- around the application until all finalizers have drained.
mkRedisAdapter :: (IOE :> es) => RedisRuntime -> Text -> Stream (Eff es) (Ingested es a) -> Adapter es a
mkRedisAdapter runtime name deliveries =
  Adapter
    { adapterName = name,
      source = deliveries,
      shutdown = liftIO (stopIntake runtime),
      telemetry = runtime.options.adapterTelemetry
    }
