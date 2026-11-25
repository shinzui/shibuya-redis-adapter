module Example (Effects, connection, source, runExample, parseCount, bytes) where

import Control.Concurrent (newEmptyMVar, putMVar, readMVar)
import Control.Monad (unless, void, when)
import Data.ByteString (ByteString)
import Data.IORef
import Data.Set qualified as Set
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Database.Redis qualified as Redis
import Effectful (IOE, liftIO, runEff)
import Effectful.Exception qualified as E
import Shibuya
import Shibuya.Adapter.Redis
import System.Timeout (timeout)
import Text.Read (readMaybe)

type Effects = '[Tracing, IOE]

bytes :: String -> ByteString
bytes = Text.encodeUtf8 . Text.pack

connection :: FilePath -> ConnectionSource
connection socket = OwnPools (Redis.defaultConnectInfo {Redis.connectAddr = Redis.ConnectAddrUnixSocket socket})

source :: String -> SourceIdentity
source stream = SourceIdentity "shibuya-example" Nothing (bytes stream)

parseCount :: String -> IO Int
parseCount input = case readMaybe input of
  Just value | value > 0 -> pure value
  _ -> fail "COUNT must be a positive integer"

runExample :: Int -> IO () -> (Handler Effects RedisEntry -> QueueProcessor Effects) -> IO ()
runExample count stop processor = do
  finished <- newEmptyMVar
  seen <- newIORef Set.empty
  let handle message = liftIO $ do
        let envelope = message.envelope
            -- The stable ID is an application idempotency key. Use durable
            -- storage for an external effect that must survive restarts.
            MessageId effectKey = envelope.messageId
        effectCount <- atomicModifyIORef' seen $ \keys ->
          if Set.member effectKey keys
            then (keys, Nothing)
            else let updated = Set.insert effectKey keys in (updated, Just (Set.size updated))
        case effectCount of
          Nothing -> pure ()
          Just completed -> do
            putStrLn (showEntry envelope.payload.entryId <> " " <> Text.unpack effectKey)
            when (completed == count) (stop >> putMVar finished ())
        pure AckOk
  runEff $ runTracingNoop $ do
    result <- runApp defaultAppConfig [(ProcessorId "example", processor handle)]
    app <- either (liftIO . fail . show) pure result
    flip E.finally (void (stopAppGracefully (ShutdownConfig 1 2) app)) $ do
      ready <- liftIO (timeout 30000000 (readMVar finished))
      unless (ready == Just ()) (liftIO (fail "timed out waiting for entries"))
      stopAppGracefully (ShutdownConfig 2 4) app >>= \drained ->
        unless drained (liftIO (fail "processor did not drain"))
  where
    showEntry = Text.unpack . Text.decodeUtf8 . renderEntryId
