module Shibuya.Adapter.Redis.Connection
  ( ConnectionSource (..),
    RuntimeOptions (..),
    defaultRuntimeOptions,
    RedisRuntime (options, stopped),
    AdapterError (..),
    withRedisRuntime,
    withRuntimeAttachment,
    stopIntake,
    runCommand,
    DeliveryMode (..),
    validateDeliveryPolicy,
    readBatchOptions,
    pollStream,
    retryConnection,
    streamEntries,
    streamRecord,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (race)
import Control.Concurrent.STM (TVar, atomically, check, newTVarIO, readTVar, readTVarIO, writeTVar)
import Control.Exception (Exception, Handler (..), IOException, bracket, catches, evaluate, mask, onException, throwIO)
import Control.Monad (unless)
import Database.Redis qualified as Redis
import GHC.Generics (Generic)
import Shibuya (Concurrency, OrderingPolicy (..), PolicyError (..), validatePolicy)
import Shibuya.Adapter.Redis.Types
import System.Timeout (timeout)

-- | Create a dedicated read pool in both cases. A borrowed command pool must
-- address the same primary/database and stay live until the bracket returns.
data ConnectionSource
  = -- | Open and close both pools using the supplied connection details.
    OwnPools Redis.ConnectInfo
  | -- | Open the read pool and borrow the supplied command pool.
    BorrowCommands Redis.ConnectInfo Redis.Connection

-- | Bounds for a runtime and a callback for adapter events.
data RuntimeOptions = RuntimeOptions
  { -- | Timeout per Redis command in microseconds. Defaults to 2,000,000.
    commandTimeoutMicros :: !Int,
    -- | Blocking read time in milliseconds. Defaults to 250 and must be below the command timeout.
    pollMillis :: !Int,
    -- | Maximum entries per new read. Defaults to 32.
    prefetchLimit :: !Int,
    -- | Connection retries after the initial attempt. Defaults to 5.
    reconnectAttempts :: !Int,
    -- | Delay between connection attempts in microseconds. Defaults to 100,000.
    reconnectDelayMicros :: !Int,
    -- | Handle an adapter event promptly. Defaults to discarding events.
    observe :: AdapterEvent -> IO ()
  }
  deriving stock (Generic)

-- | Standard connection limits and a no-op observer.
defaultRuntimeOptions :: RuntimeOptions
defaultRuntimeOptions =
  RuntimeOptions
    { commandTimeoutMicros = 2000000,
      pollMillis = 250,
      prefetchLimit = 32,
      reconnectAttempts = 5,
      reconnectDelayMicros = 100000,
      observe = const (pure ())
    }

-- | Connection pools and intake state for one reader or worker.
data RedisRuntime = RedisRuntime
  { readPool :: !Redis.Connection,
    commandPool :: !Redis.Connection,
    stopped :: !(TVar Bool),
    attached :: !(TVar Bool),
    options :: !RuntimeOptions
  }

-- | Configuration, attachment, and Redis command errors.
data AdapterError
  = -- | Configuration fails the adapter's option checks.
    InvalidOptions
  | -- | Redis returned a reply the adapter cannot interpret.
    InvalidStreamReply
  | -- | The command exceeded its configured timeout.
    CommandTimedOut
  | -- | Redis rejected the command. Inspect the raw reply programmatically.
    CommandRejected Redis.Reply
  | -- | A reader or group worker already reserved this runtime.
    RuntimeAlreadyAttached

-- Redis errors can quote keys or requests. Keep the raw reply available for
-- programmatic inspection without including it in runner failure telemetry.
instance Show AdapterError where
  show RuntimeAlreadyAttached = "RuntimeAlreadyAttached"
  show InvalidOptions = "InvalidOptions"
  show InvalidStreamReply = "InvalidStreamReply"
  show CommandTimedOut = "CommandTimedOut"
  show (CommandRejected _) = "CommandRejected"

instance Exception AdapterError

-- | Bracket connection ownership around construction, processing, and finalizer drain.
-- The runtime holds one dedicated blocking read connection.
withRedisRuntime :: ConnectionSource -> RuntimeOptions -> (RedisRuntime -> IO a) -> IO a
withRedisRuntime source opts use = do
  let info = case source of
        OwnPools c -> c
        BorrowCommands c _ -> c
  unless
    ( opts.commandTimeoutMicros > 0
        && opts.pollMillis > 0
        && toInteger opts.pollMillis * 1000 < toInteger opts.commandTimeoutMicros
        && opts.prefetchLimit > 0
        && opts.reconnectAttempts >= 0
        && opts.reconnectDelayMicros > 0
        && Redis.connectMaxConnections info >= 1
    )
    (throwIO InvalidOptions)
  let owned config = bracket (Redis.connect config) Redis.disconnect
      withCommands = case source of
        OwnPools _ -> owned info
        BorrowCommands _ pool -> (\f -> f pool)
  owned (info {Redis.connectMaxConnections = 1, Redis.connectNumStripes = Just 1}) $ \readerPool ->
    withCommands $ \commands -> do
      closing <- newTVarIO False
      attachment <- newTVarIO False
      let runtime = RedisRuntime readerPool commands closing attachment opts
      _ <- boundedRequest opts readerPool Redis.ping
      _ <- runCommand runtime Redis.ping
      use runtime

-- Reserve a runtime before any constructor side effects. Failed initialization
-- releases the reservation, including when the constructing thread is canceled.
withRuntimeAttachment :: RedisRuntime -> IO a -> IO a
withRuntimeAttachment runtime initialize = mask $ \restore -> do
  acquired <- atomically $ do
    alreadyAttached <- readTVar runtime.attached
    if alreadyAttached
      then pure False
      else writeTVar runtime.attached True >> pure True
  unless acquired (throwIO RuntimeAlreadyAttached)
  restore initialize `onException` atomically (writeTVar runtime.attached False)

-- | Stop accepting deliveries and wake a blocked read. Run graceful shutdown
-- before exiting the runtime bracket so outstanding finalizers can finish.
stopIntake :: RedisRuntime -> IO ()
stopIntake runtime = atomically $ writeTVar runtime.stopped True

boundedRequest :: RuntimeOptions -> Redis.Connection -> Redis.Redis (Either Redis.Reply a) -> IO a
boundedRequest opts pool action = do
  result <-
    timeout opts.commandTimeoutMicros $
      Redis.runRedis pool action >>= either (throwIO . CommandRejected) evaluate
  maybe (throwIO CommandTimedOut) pure result

runCommand :: RedisRuntime -> Redis.Redis (Either Redis.Reply a) -> IO a
runCommand runtime = boundedRequest runtime.options runtime.commandPool

-- | Independent readers admit one outstanding entry. Groups share work across
-- processes, so the runner cannot impose source order on their side effects.
data DeliveryMode = IndependentReader | GroupWorker deriving stock (Eq, Show)

validateDeliveryPolicy :: DeliveryMode -> OrderingPolicy -> Concurrency -> Either PolicyError ()
validateDeliveryPolicy IndependentReader StrictInOrder concurrency = validatePolicy StrictInOrder concurrency
validateDeliveryPolicy GroupWorker Unordered concurrency = validatePolicy Unordered concurrency
validateDeliveryPolicy _ _ _ = Left (InvalidPolicyCombo "readers require strict order and groups require unordered execution")

readBatchOptions :: RedisRuntime -> Redis.XReadOpts
readBatchOptions runtime = Redis.XReadOpts (Just (toInteger runtime.options.pollMillis)) (Just (toInteger runtime.options.prefetchLimit))

-- | Nothing denotes shutdown. Redis blocking-read timeouts remain a successful
-- empty reply inside Just. A disconnect or command timeout propagates.
pollStream :: forall a. RedisRuntime -> Redis.Redis (Either Redis.Reply a) -> IO (Maybe a)
pollStream runtime action = do
  closed <- readTVarIO runtime.stopped
  if closed
    then pure Nothing
    else do
      outcome <-
        race
          (atomically (readTVar runtime.stopped >>= check))
          (retryConnection runtime (boundedRequest runtime.options runtime.readPool action))
      pure (either (const Nothing) Just outcome)

-- Only use this for operations that can be repeated after a lost reply.
retryConnection :: forall a. RedisRuntime -> IO a -> IO a
retryConnection runtime action = attempt runtime.options.reconnectAttempts
  where
    attempt remaining =
      action
        `catches` [ Handler (retry @Redis.ConnectionLostException remaining),
                    Handler (retry @Redis.ConnectTimeout remaining),
                    Handler (retry @IOException remaining),
                    Handler
                      ( \err -> case err of
                          CommandTimedOut -> retry remaining err
                          _ -> throwIO err
                      )
                  ]
    retry :: forall e. (Exception e) => Int -> e -> IO a
    retry remaining err
      | remaining == 0 = throwIO err
      | otherwise = threadDelay runtime.options.reconnectDelayMicros >> attempt (remaining - 1)

streamEntries :: SourceIdentity -> Maybe [Redis.XReadResponse] -> IO [RedisEntry]
streamEntries src rows = concat <$> traverse parseStream (maybe [] id rows)
  where
    parseStream (Redis.XReadResponse key records)
      | key == src.streamKey = traverse (streamRecord src) records
      | otherwise = throwIO InvalidStreamReply

streamRecord :: SourceIdentity -> Redis.StreamsRecord -> IO RedisEntry
streamRecord src (Redis.StreamsRecord rawId fields) =
  maybe (throwIO InvalidStreamReply) (\position -> pure (RedisEntry src position fields)) (parseEntryId rawId)
