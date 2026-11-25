module Main where

import Control.Concurrent
import Control.Concurrent.Async (wait, withAsync)
import Control.Exception (bracket, try)
import Control.Monad (unless, void)
import Data.ByteString qualified as BS
import Data.IORef
import Data.Text qualified as T
import Database.Redis qualified as R
import Effectful (liftIO, runEff)
import Effectful.Exception qualified as E
import Fixture
import Shibuya
import Shibuya.Adapter.Redis
import Streamly.Data.Stream qualified as Stream
import System.Timeout (timeout)

check :: String -> Bool -> IO ()
check label ok = unless ok (fail label)

main :: IO ()
main = do
  completed <- timeout 15000000 $ withRedis $ \info ->
    bracket (R.connect info) R.disconnect $ \borrowed -> do
      let pools = BorrowCommands info borrowed
      bad <- try @AdapterError (withRedisRuntime pools (defaultRuntimeOptions {pollMillis = 0}) (const (pure ())))
      check "invalid poll limit" (case bad of Left InvalidOptions -> True; _ -> False)
      -- Fail the second PING after acquiring the dedicated read pool.
      let countClients =
            R.runRedis borrowed (R.sendRequest ["CLIENT", "LIST"])
              >>= either (fail . show) (pure . length . BS.split 10)
      before <- countClients
      bracket (R.connect (info {R.connectDatabase = -1})) R.disconnect $ \unusable -> do
        failed <- try @R.ConnectError (withRedisRuntime (BorrowCommands info unusable) defaultRuntimeOptions (const (pure ())))
        check "second pool failed to acquire" (either (const True) (const False) failed)
      after <- countClients
      check "partial acquisition released read connection" (before == after)
      check "reader rejects concurrent handlers" (either (const True) (const False) (validateDeliveryPolicy IndependentReader StrictInOrder (Async 2)))
      withRedisRuntime pools (defaultRuntimeOptions {adapterTelemetry = defaultAdapterTelemetry {traceCorrelation = LinkOnly}}) roundTrip
      withRedisRuntime pools defaultRuntimeOptions $ \runtime ->
        withAsync (pollStream runtime (R.xreadOpts [("idle", "0-0")] (R.XReadOpts (Just 1200) (Just 1)))) $ \reader -> do
          -- Wait for Redis to confirm that the dedicated connection is blocked.
          let awaitRead = do
                clients <- runCommand runtime (R.sendRequest ["INFO", "clients"])
                unless ("blocked_clients:1" `BS.isInfixOf` clients) (threadDelay 1000 >> awaitRead)
          awaitRead
          stopIntake runtime
          stopIntake runtime
          wait reader >>= check "poll cancelled on shutdown" . (== Nothing)
      R.runRedis borrowed R.ping >>= check "borrowed pool still usable" . (== Right R.Pong)
  check "runner fixture deadline" (completed == Just ())
  putStrLn "PONG; binary delivery, rejection replay, cancelled poll and finalizer drain passed"

roundTrip :: RedisRuntime -> IO ()
roundTrip runtime = do
  let key = "orders"
      identity = SourceIdentity "demo" Nothing key
      pairs = [("item", "a"), ("item", BS.pack [0, 255])]
      expected = RedisEntry identity (EntryId 7 2) pairs
      expectedId = (entryEnvelope expected).messageId
      alternate = RedisEntry (SourceIdentity "dem" Nothing "oorders") (EntryId 7 2) pairs
      reason = InvalidPayload (T.replicate 600 "x")
      owner = LogicalReader "index"
  check "length-framed message ID" (expectedId /= (entryEnvelope alternate).messageId)
  check "missing generation distinct from empty" (expectedId /= (entryEnvelope (expected {sourceIdentity = identity {generation = Just ""}})).messageId)
  check "reject invalid stream ID" (parseEntryId "18446744073709551616-1" == Nothing)
  check "decoder failure retains identity" (decodeEnvelope (const (Left reason)) (const Nothing) expected == Left @DecodeFailure @(Envelope ()) (DecodeFailure expected reason))
  void $ runCommand runtime (R.xadd key "7-2" pairs)
  started <- newEmptyMVar
  intakeClosed <- newEmptyMVar
  finalized <- newEmptyMVar
  firstReply <- newIORef True
  let sourceAction = do
        rows <- liftIO $ pollStream runtime (R.xreadOpts [(key, "0-0")] (readBatchOptions runtime))
        entry <- liftIO $ case rows of
          Just (Just [R.XReadResponse k [R.StreamsRecord rawId fs]])
            | k == key,
              Just i <- parseEntryId rawId ->
                pure (RedisEntry identity i fs)
          _ -> fail "expected one stream record"
        let finish decision = liftIO $ do
              check "runner decision" (decision == AckDeadLetter reason)
              readMVar intakeClosed
              inserted <- runCommand runtime (storeRejection "failures" owner entry reason)
              loseReply <- atomicModifyIORef' firstReply (\first -> (False, first))
              if loseReply && inserted then fail "fixture hid successful Redis reply" else pure ()
              count <- runCommand runtime (R.hlen "failures")
              check "one logical rejection" (not loseReply && not inserted && count == 1)
              putMVar finalized ()
        pure $ mkIngested (entryEnvelope entry) (AckHandle finish)
      baseAdapter = mkRedisAdapter runtime "redis-fixture" (Stream.fromEffect sourceAction)
      adapter = baseAdapter {shutdown = baseAdapter.shutdown >> liftIO (void (tryPutMVar intakeClosed ()))}
      handler message = liftIO $ do
        check "wire fidelity and stable ID" (message.envelope.payload == expected && message.envelope.messageId == expectedId)
        check "producer time and attempt absent" (message.envelope.enqueuedAt == Nothing && message.envelope.attempt == Nothing)
        putMVar started ()
        pure (AckDeadLetter reason)
  check "configured telemetry" (adapter.telemetry.traceCorrelation == LinkOnly)
  runEff $ runTracingNoop $ do
    result <- runApp defaultAppConfig [(ProcessorId "fixture", mkProcessor adapter handler)]
    app <- either (liftIO . fail . show) pure result
    flip E.finally (stopApp app) $ do
      liftIO $ takeMVar started
      drained <- stopAppGracefully (ShutdownConfig 2 4) app
      liftIO $ check "graceful app drain" drained
  takeMVar finalized
