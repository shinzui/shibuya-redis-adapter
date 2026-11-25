module Main (main) where

import Control.Concurrent
import Control.Concurrent.Async (withAsync)
import Control.Exception (bracket, displayException, try)
import Control.Monad (unless, void)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as C8
import Data.IORef
import Database.Redis qualified as R
import Effectful (Eff, IOE, Limit (..), Persistence (..), UnliftStrategy (..), liftIO, runEff, withEffToIO)
import Effectful.Exception qualified as E
import Fixture
import Shibuya
import Shibuya.Adapter.Redis
import Shibuya.Adapter.Redis.Internal
import Streamly.Data.Stream qualified as Stream
import System.Timeout (timeout)

type Effects = '[Tracing, IOE]

main :: IO ()
main = do
  within "combined modes and persistent restart" lifecycle
  within "restricted Redis command and key permissions" $ withRedis permissions
  within "bounded reconnect exhaustion" $ withRedisRestarts reconnectLimit
  putStrLn "Both modes passed persistent restart, independent supervision, ACLs and finite reconnect checks"

within :: String -> IO a -> IO a
within label action = timeout 15000000 action >>= maybe (fail (label <> " timed out")) pure

assert :: String -> Bool -> IO ()
assert label ok = unless ok (fail label)

send :: (R.RedisResult a) => R.Connection -> [ByteString] -> IO a
send conn args = R.runRedis conn (R.sendRequest args) >>= either (fail . show) pure

append :: R.Connection -> ByteString -> ByteString -> IO ()
append conn key identifier = void $ send @ByteString conn ["XADD", key, identifier, "field", "private-payload"]

withConnection :: R.ConnectInfo -> (R.Connection -> IO a) -> IO a
withConnection info = bracket (R.connect info) R.disconnect

runProcessors :: [(ProcessorId, QueueProcessor Effects)] -> (AppHandle Effects -> Eff Effects b) -> IO b
runProcessors processors use = runEff $ runTracingNoop $ do
  result <- runApp defaultAppConfig processors
  app <- either (liftIO . fail . show) pure result
  use app `E.finally` void (stopAppGracefully (ShutdownConfig 0.1 1) app)

lifecycle :: IO ()
lifecycle = withRedisRestarts $ \start ->
  start $ \info stop -> withConnection info $ \admin -> do
    let src = SourceIdentity "release" Nothing "private-source"
        key = src.streamKey
        worker consumer = rawGroupConfig "worker" src "jobs" consumer (CreateIfAbsent (EntryId 0 0))
        options = defaultRuntimeOptions {reconnectAttempts = 30, prefetchLimit = 2}
    append admin key "1-0"
    checkpoint <- newIORef Nothing
    entered <- newEmptyMVar
    parked <- newEmptyMVar
    requestStop <- newEmptyMVar
    aStopped <- newEmptyMVar
    projected <- newChan
    consumed <- newChan
    events <- newIORef []
    let store = CheckpointStore (\_ _ -> readIORef checkpoint) (\_ _ _ new -> writeIORef checkpoint (Just new))
        projection = (rawReaderConfig "projection" "reports" src Earliest) {checkpointStore = Just store}
        observeEvent event = atomicModifyIORef' events (\seen -> (event : seen, ()))
        track channel message = liftIO $ do
          writeChan channel message.envelope.payload.entryId
          pure AckOk
        runA = withRedisRuntime (OwnPools info) options $ \runtime -> do
          a <- newGroupWorker runtime (worker "a")
          let blocked message = liftIO $ do
                putMVar entered message.envelope.payload.entryId
                () <- readMVar parked
                pure AckOk
          runProcessors [(ProcessorId "a", groupProcessor a blocked)] $ \app -> do
            liftIO (readMVar requestStop)
            graceful <- stopAppGracefully (ShutdownConfig 0.1 1) app
            repeated <- stopAppGracefully (ShutdownConfig 0.1 1) app
            liftIO $ do
              assert "forced shutdown result is repeatable" (not graceful && not repeated)
              putMVar aStopped ()
    withAsync runA $ \_ -> do
      readMVar entered >>= assert "A receives first pending job" . (== EntryId 1 0)
      withConnection info $ \borrowed ->
        withRedisRuntime (BorrowCommands info borrowed) (options {observe = observeEvent}) $ \readerRuntime ->
          withRedisRuntime (OwnPools info) (options {observe = observeEvent}) $ \groupRuntime -> do
            reader <- newReader readerRuntime projection
            b <- newGroupWorker groupRuntime (worker "b")
            runProcessors
              [ (ProcessorId "reader", readerProcessor reader (track projected)),
                ( ProcessorId "b",
                  groupProcessor
                    b
                    ( \message -> do
                        _ <- track consumed message
                        pure $
                          if message.envelope.payload.entryId == EntryId 3 0
                            then AckHalt (HaltFatal "planned worker halt")
                            else AckOk
                    )
                )
              ]
              $ \app -> do
                liftIO $ do
                  readChan projected >>= assert "projection sees same input as A" . (== EntryId 1 0)
                  await $ (== Just (Checkpoint (EntryId 1 0) Nothing)) <$> readIORef checkpoint
                  await $ C8.isInfixOf "blocked_clients:2" <$> send @ByteString admin ["INFO", "clients"]
                  stop
                  putMVar requestStop ()
                  readMVar aStopped
                  readIORef checkpoint >>= assert "forced stop across outage does not move cursor" . (== Just (Checkpoint (EntryId 1 0) Nothing))
                startOnSameServer start key app projected consumed borrowed
            stopped <- readIORef events
            assert
              "bounded adapter event vocabulary"
              ( all (`elem` stopped) [Delivered, Acknowledged, Reclaimed]
                  && not ("private" `C8.isInfixOf` C8.pack (show stopped))
              )
  where
    startOnSameServer start key app projected consumed borrowed = do
      -- Run adapter callbacks in the surrounding Eff scope and hold the
      -- restarted child open with a barrier until the shutdown completes.
      ready <- liftIO newEmptyMVar
      done <- liftIO newEmptyMVar
      withEffToIO (ConcUnlift Persistent Unlimited) $ \unlift ->
        withAsync (start (\info _ -> withConnection info (\fresh -> putMVar ready fresh >> readMVar done))) $ \_ ->
          unlift $ flip E.finally (liftIO (putMVar done ())) $ do
            fresh <- liftIO (readMVar ready)
            liftIO $ do
              rows <- send @[[R.Reply]] fresh ["XPENDING", key, "jobs", "-", "+", "2"]
              assert "Redis restart retained A's pending row" $ case rows of
                [[R.Bulk (Just "1-0"), R.Bulk (Just "a"), _, _]] -> True
                _ -> False
              void $ send @[ByteString] fresh ["XCLAIM", key, "jobs", "a", "0", "1-0", "IDLE", "60000", "JUSTID"]
              append fresh key "2-0"
              readChan projected >>= assert "independent reader continues after reconnection" . (== EntryId 2 0)
              jobs <- sequence [readChan consumed, readChan consumed]
              assert "replacement worker resumes new and abandoned work" (EntryId 1 0 `elem` jobs && EntryId 2 0 `elem` jobs)
              await (null <$> send @[[R.Reply]] fresh ["XPENDING", key, "jobs", "-", "+", "2"])
              append fresh key "3-0"
              readChan consumed >>= assert "worker receives its halt job" . (== EntryId 3 0)
              append fresh key "4-0"
              sequence [readChan projected, readChan projected] >>= assert "reader progresses after sibling halt" . (== [EntryId 3 0, EntryId 4 0])
            drained <- stopAppGracefully (ShutdownConfig 1 2) app
            liftIO $ do
              assert "surviving processors drain" drained
              jobs <- send @[[R.Reply]] fresh ["XPENDING", key, "jobs", "-", "+", "2"]
              assert "only the deliberately halted job remains pending" $ case jobs of
                [[R.Bulk (Just "3-0"), R.Bulk (Just "b"), _, _]] -> True
                _ -> False
              R.runRedis borrowed R.ping >>= assert "borrowed commands remain live through drain" . (== Right R.Pong)

await :: IO Bool -> IO ()
await predicate = do
  result <- predicate
  unless result (threadDelay 10000 >> await predicate)

permissions :: R.ConnectInfo -> IO ()
permissions info = withConnection info $ \admin -> do
  let identifier = SourceIdentity "acl" Nothing "acl-input"
      key = identifier.streamKey
      passwords = [">test-password", "on", "~acl-input", "~acl-rejections", "+ping"]
      user role privileges = send @R.Status admin (["ACL", "SETUSER", role, "reset"] <> passwords <> privileges)
      login role = info {R.connectUsername = Just role, R.connectAuth = Just "test-password"}
      readerCommands = ["+xread", "+xinfo|stream", "+hsetnx"]
      workerCommands =
        [ "~shibuya:group:v1:*",
          "+xreadgroup",
          "+xgroup|create",
          "+xpending",
          "+script|load",
          "+evalsha",
          "+eval",
          "+xrange",
          "+xclaim",
          "+time",
          "+exists",
          "+hget",
          "+hset",
          "+hdel",
          "+del",
          "+hsetnx"
        ]
      reason = InvalidPayload "bad job"
  let gap = ReaderGap identifier (Checkpoint (EntryId 0 0) Nothing) Nothing (ProducerDiscontinuity "private-payload")
      failures = [displayException (ReaderHistoryLost gap Resnapshot), displayException (CommandRejected (R.Error "acl-input"))]
  assert
    "runner failure text redacts keys and producer details"
    ( all
        ( \failure ->
            not ("acl-input" `C8.isInfixOf` C8.pack failure)
              && not ("private-payload" `C8.isInfixOf` C8.pack failure)
        )
        failures
    )
  _ <- user "projection" readerCommands
  -- Omit XACK for the first finalizer. The script writes the rejection before
  -- encountering the ACL error, and the exact callback can finish after grant.
  _ <- user "worker" workerCommands
  append admin key "1-0"
  let readerCfg = (rawReaderConfig "projection" "acl" identifier Earliest) {readerDeadLetters = Just "acl-rejections"}
  withRedisRuntime (OwnPools (login "projection")) defaultRuntimeOptions $ \runtime -> do
    reader <- newReader runtime readerCfg
    processed <- newEmptyMVar
    runProcessors [(ProcessorId "projection", readerProcessor reader (\_ -> liftIO (putMVar processed ()) >> pure (AckDeadLetter reason)))] $ \app -> do
      liftIO (readMVar processed)
      stopAppGracefully (ShutdownConfig 1 2) app >>= liftIO . assert "restricted reader drains"
    denied <- try @AdapterError (runCommand runtime (R.get "alien-key"))
    assert "reader credentials deny unrelated commands and keys" (either (const True) (const False) denied)
  withRedisRuntime (OwnPools (login "worker")) defaultRuntimeOptions $ \runtime -> do
    let cfg =
          (rawGroupConfig "worker" identifier "acl-group" "worker-one" (CreateIfAbsent (EntryId 0 0)))
            { groupDeadLetters = Just "acl-rejections"
            }
    invalid <- try @AdapterError (newGroupWorker @Effects runtime cfg {groupInFlight = 0})
    assert "invalid group capacity rejected before creating a group" (case invalid of Left InvalidOptions -> True; _ -> False)
    send @[R.Reply] admin ["XINFO", "GROUPS", key] >>= assert "invalid worker left no group" . null
    worker <- newGroupWorker runtime cfg
    runEff $ runTracingNoop $ do
      (item, _) <- Stream.uncons worker.groupAdapter.source >>= maybe (liftIO (fail "restricted delivery missing")) pure
      let AckHandle finish = item.ack
      failed <- E.try @AdapterError (finish (AckDeadLetter reason))
      liftIO $ do
        assert "partial Redis script failed as intended" (either (const True) (const False) failed)
        send @Integer admin ["HLEN", "acl-rejections"] >>= assert "both failure records exist before group ack" . (== 2)
        send @[[R.Reply]] admin ["XPENDING", key, "acl-group", "-", "+", "2"] >>= assert "partial write leaves the PEL row" . ((== 1) . length)
        void $ send @R.Status admin ["ACL", "SETUSER", "worker", "+xack"]
      finish (AckDeadLetter reason)
      finish (AckDeadLetter reason)
  send @Integer admin ["HLEN", "acl-rejections"] >>= assert "same failure record after partial replay" . (== 2)
  send @[[R.Reply]] admin ["XPENDING", key, "acl-group", "-", "+", "2"] >>= assert "proper worker ACL lets replay acknowledge" . null

reconnectLimit :: (forall b. (R.ConnectInfo -> IO () -> IO b) -> IO b) -> IO ()
reconnectLimit start = start $ \info stop -> do
  let options = defaultRuntimeOptions {reconnectAttempts = 1, reconnectDelayMicros = 20000, pollMillis = 20, commandTimeoutMicros = 300000}
  withRedisRuntime (OwnPools info) options $ \runtime -> do
    stop
    exhausted <- try @E.SomeException (timeout 2000000 (pollStream runtime (R.xreadOpts [("idle", "0-0")] (readBatchOptions runtime))))
    assert "connection failure differs from a quiet poll and exhausts finite budget" $ case exhausted of
      Left _ -> True
      _ -> False
