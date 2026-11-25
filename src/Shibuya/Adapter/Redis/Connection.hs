module Shibuya.Adapter.Redis.Connection where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (race)
import Control.Concurrent.STM (TVar, atomically, check, newTVarIO, readTVar, readTVarIO, writeTVar)
import Control.Exception (Exception, Handler (..), IOException, bracket, catches, evaluate, throwIO)
import Control.Monad (unless)
import Database.Redis qualified as Redis
import GHC.Generics (Generic)
import Shibuya (Concurrency, OrderingPolicy (..), PolicyError (..), validatePolicy)
import Shibuya.Adapter.Redis.Types
import System.Timeout (timeout)

-- | Create a dedicated read pool in both cases. A borrowed command pool must
-- address the same primary/database and stay live until the bracket returns.
data ConnectionSource = OwnPools Redis.ConnectInfo | BorrowCommands Redis.ConnectInfo Redis.Connection

data RuntimeOptions = RuntimeOptions
  { commandTimeoutMicros :: !Int,
    pollMillis :: !Int,
    prefetchLimit :: !Int,
    reconnectAttempts :: !Int,
    reconnectDelayMicros :: !Int,
    observe :: AdapterEvent -> IO ()
  }
  deriving stock (Generic)

defaultRuntimeOptions :: RuntimeOptions
defaultRuntimeOptions = RuntimeOptions 2000000 250 32 5 100000 (const (pure ()))

data RedisRuntime = RedisRuntime
  { readPool :: !Redis.Connection,
    commandPool :: !Redis.Connection,
    stopped :: !(TVar Bool),
    options :: !RuntimeOptions
  }

data AdapterError = InvalidOptions | InvalidStreamReply | CommandTimedOut | CommandRejected Redis.Reply

-- Redis errors can quote keys or requests. Keep the raw reply available for
-- programmatic inspection without including it in runner failure telemetry.
instance Show AdapterError where
  show InvalidOptions = "InvalidOptions"
  show InvalidStreamReply = "InvalidStreamReply"
  show CommandTimedOut = "CommandTimedOut"
  show (CommandRejected _) = "CommandRejected"

instance Exception AdapterError

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
      let runtime = RedisRuntime readerPool commands closing opts
      _ <- boundedRequest opts readerPool Redis.ping
      _ <- runCommand runtime Redis.ping
      use runtime

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
