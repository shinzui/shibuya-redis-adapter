module Shibuya.Adapter.Redis.Adapter (mkRedisAdapter) where

import Data.Text (Text)
import Effectful (Eff, IOE, liftIO, (:>))
import Shibuya (Adapter (..), AdapterTelemetry, Ingested)
import Shibuya.Adapter.Redis.Connection
import Streamly.Data.Stream (Stream)

-- | Attach a mode's delivery stream to the runner. Keep the runtime bracket
-- around the application until all finalizers have drained.
mkRedisAdapter :: (IOE :> es) => RedisRuntime -> Text -> AdapterTelemetry -> Stream (Eff es) (Ingested es a) -> Adapter es a
mkRedisAdapter runtime name settings deliveries =
  Adapter
    { adapterName = name,
      source = deliveries,
      shutdown = liftIO (stopIntake runtime),
      telemetry = settings
    }
