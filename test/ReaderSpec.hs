module Main where

import Control.Concurrent
import Control.Concurrent.Async (wait, withAsync)
import Control.Exception (bracket)
import Control.Monad (unless, void)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as C8
import Data.IORef
import Data.List.NonEmpty (NonEmpty (..))
import Database.Redis qualified as R
import Effectful (Eff, IOE, liftIO, runEff, (:>))
import Effectful.Exception qualified as E
import Fixture
import Shibuya
import Shibuya.Adapter.Redis
import System.IO.Unsafe (unsafePerformIO)
import System.Timeout (timeout)

main :: IO ()
main = do
  result <- timeout 20000000 $ withRedis $ \info ->
    bracket (R.connect info) R.disconnect $ \producer -> do
      let pools = OwnPools info
          append key i fields = R.runRedis producer (R.xadd key i fields) >>= either (fail . show) pure
      putStrLn "Reader scenario independent"
      independent pools append
      putStrLn "Reader scenario retries"
      retries pools append
      rejectionReconnect info producer append
      retryExhaustion pools producer append
      putStrLn "Reader scenario rejection"
      rejection pools producer append
      putStrLn "Reader scenario handover"
      handover pools producer append
  assert "reader integration timeout" (result == Just ())
  putStrLn "Reader order, durable restart, retry, crash, rejection and continuity passed"

type Append = ByteString -> ByteString -> [(ByteString, ByteString)] -> IO ByteString

assert :: String -> Bool -> IO ()
assert message ok = unless ok (fail message)

storeIn :: IORef (Maybe Checkpoint) -> CheckpointStore
storeIn cell = CheckpointStore (\_ _ -> readIORef cell) $ \_ _ old new -> do
  updated <- atomicModifyIORef' cell $ \current ->
    if current == old || current == Just new then (Just new, True) else (current, False)
  assert "checkpoint compare-and-set" updated

withReader :: (IOE :> es) => ConnectionSource -> ReaderConfig a -> (Reader es a -> IO b) -> IO b
withReader pools config use = withRedisRuntime pools defaultRuntimeOptions $ \runtime ->
  newReader runtime config >>= use

runReader :: Reader '[Tracing, IOE] a -> Handler '[Tracing, IOE] a -> (AppHandle '[Tracing, IOE] -> Eff '[Tracing, IOE] b) -> IO b
runReader reader handle use = runEff $ runTracingNoop $ do
  result <- runApp defaultAppConfig [(ProcessorId "reader", readerProcessor reader handle)]
  app <- either (liftIO . fail . show) pure result
  use app `E.finally` void (stopAppGracefully (ShutdownConfig 0.1 1) app)

recording :: (IOE :> es) => Chan EntryId -> Handler es RedisEntry
recording channel message = liftIO $ do
  writeChan channel message.envelope.payload.entryId
  pure AckOk

expectIds :: Chan EntryId -> [EntryId] -> IO ()
expectIds channel = mapM_ $ \expected ->
  readChan channel >>= assert ("expected reader entry " <> show expected) . (== expected)

independent :: ConnectionSource -> Append -> IO ()
independent pools append = do
  let src = SourceIdentity "test" Nothing "projections"
      config name cell = (rawReaderConfig "projection" name src Earliest) {checkpointStore = Just (storeIn cell)}
      appendAt i = void (append src.streamKey (renderEntryId (EntryId i 0)) [("value", "x")])
      first = map (`EntryId` 0) [1, 2, 3]
  mapM_ appendAt [1, 2, 3]
  aStore <- newIORef Nothing
  bStore <- newIORef Nothing
  a <- newChan
  b <- newChan
  bReady <- newEmptyMVar
  finishB <- newEmptyMVar
  withAsync
    ( withReader pools (config "reporting" bStore) $ \reader ->
        runReader reader (recording b) $ \app -> do
          liftIO (putMVar bReady () >> takeMVar finishB)
          stopAppGracefully (ShutdownConfig 2 4) app >>= liftIO . assert "projection B drains"
    )
    $ \worker -> do
      takeMVar bReady
      withReader pools (config "index" aStore) $ \reader ->
        runReader reader (recording a) $ \app -> do
          liftIO (expectIds a first >> expectIds b first)
          stopAppGracefully (ShutdownConfig 2 4) app >>= liftIO . assert "projection A drains"
      appendAt 4
      withReader pools ((config "index" aStore) {readerStart = Live}) $ \reader ->
        runReader reader (recording a) $ \app -> do
          liftIO (expectIds a [EntryId 4 0] >> expectIds b [EntryId 4 0])
          stopAppGracefully (ShutdownConfig 2 4) app >>= liftIO . assert "projection A resumes"
      putMVar finishB ()
      wait worker
  forStores aStore bStore
  where
    forStores a b = mapM_ (\store -> readIORef store >>= assert "independent saved cursor" . (== Just (Checkpoint (EntryId 4 0) Nothing))) [a, b]

retries :: ConnectionSource -> Append -> IO ()
retries pools append = do
  let src = SourceIdentity "test" Nothing "retry"
      config = rawReaderConfig "retry" "index" src Earliest
  void (append src.streamKey "1-0" [("value", "A")])
  void (append src.streamKey "2-0" [("value", "B")])
  deliveries <- newChan
  withReader pools config $ \reader -> do
    let handle message = liftIO $ do
          let envelope = message.envelope
          writeChan deliveries (envelope.payload.entryId, envelope.attempt)
          pure $ if envelope.payload.entryId == EntryId 1 0 && envelope.attempt == Just 0 then AckRetry (RetryDelay 0) else AckOk
    runReader reader handle $ \app -> do
      liftIO $
        mapM_
          (\expected -> readChan deliveries >>= assert "retry holds the next prefetched entry" . (== expected))
          [(EntryId 1 0, Just 0), (EntryId 1 0, Just 1), (EntryId 2 0, Just 0)]
      stopAppGracefully (ShutdownConfig 2 4) app >>= liftIO . assert "retry processor drains"
  let crashSource = src {streamKey = "crash"}
  void (append crashSource.streamKey "1-0" [("value", "effect")])
  store <- newIORef Nothing
  effects <- newIORef []
  committed <- newEmptyMVar
  parked <- newEmptyMVar
  let crashConfig = config {readerSource = crashSource, checkpointStore = Just (storeIn store)}
      applyEffect identifier = atomicModifyIORef' effects $ \seen ->
        (if identifier `elem` seen then seen else identifier : seen, ())
  withReader pools crashConfig $ \reader ->
    runReader
      reader
      ( \message -> liftIO $ do
          applyEffect message.envelope.messageId
          putMVar committed message.envelope.messageId
          () <- takeMVar parked
          pure AckOk
      )
      $ \_ -> liftIO (readMVar committed >> pure ())
  readIORef store >>= assert "forced stop preserves initial checkpoint" . (== Just (Checkpoint (EntryId 0 0) Nothing))
  replayed <- newEmptyMVar
  withReader pools crashConfig $ \reader ->
    runReader
      reader
      ( \message -> liftIO $ do
          applyEffect message.envelope.messageId
          putMVar replayed message.envelope.messageId
          pure AckOk
      )
      $ \app -> do
        liftIO $ do
          first <- readMVar committed
          readMVar replayed >>= assert "same ID after forced stop" . (== first)
        stopAppGracefully (ShutdownConfig 2 4) app >>= liftIO . assert "replayed reader drains"
  readIORef effects >>= assert "one logical external effect" . (== 1) . length

retryExhaustion :: ConnectionSource -> R.Connection -> Append -> IO ()
retryExhaustion pools producer append = do
  let src = SourceIdentity "test" Nothing "retry-exhaustion"
      dest = "exhausted-reader"
      cfg = (rawReaderConfig "retry-exhaustion" "index" src Earliest) {readerRetryLimit = 1}
      retryEntry = AckRetry (RetryDelay 0)
  void (append src.streamKey "1-0" [("value", "poison")])
  withReader pools cfg $ \reader -> do
    runReader reader (const (pure retryEntry)) waitApp
    reader.completedCheckpoint >>= assert "exhaustion without a destination preserves the checkpoint" . (== Checkpoint (EntryId 0 0) Nothing)
  void (append src.streamKey "2-0" [("value", "healthy")])
  seen <- newChan
  withReader pools (cfg {readerDeadLetters = Just dest}) $ \reader -> do
    let handle message = liftIO $ do
          let envelope = message.envelope
          writeChan seen (envelope.payload.entryId, envelope.attempt)
          pure $ if envelope.payload.entryId == EntryId 1 0 then retryEntry else AckOk
    runReader reader handle $ \app -> do
      liftIO $
        mapM_
          (\expected -> readChan seen >>= assert "exhausted retries release the next entry in order" . (== expected))
          [(EntryId 1 0, Just 0), (EntryId 1 0, Just 1), (EntryId 2 0, Just 0)]
      stopAppGracefully (ShutdownConfig 2 4) app >>= liftIO . assert "exhausted retry processor drains"
    reader.completedCheckpoint >>= assert "checkpoint reaches the next accepted entry" . (== Checkpoint (EntryId 2 0) Nothing)
  records <- R.runRedis producer (R.sendRequest ["HVALS", dest]) >>= either (fail . show) pure
  assert
    "one exhaustion record"
    ( case records of
        [value] -> "max_retries_exceeded" `C8.isInfixOf` value
        _ -> False
    )

rejectionReconnect :: R.ConnectInfo -> R.Connection -> Append -> IO ()
rejectionReconnect info admin append = do
  let src = SourceIdentity "test" Nothing "reject-disconnect"
      dest = "reconnect-failures"
      send :: (R.RedisResult a) => R.Connection -> [ByteString] -> IO a
      send connection request = R.runRedis connection (R.sendRequest request) >>= either (fail . show) pure
      commandInfo = info {R.connectMaxConnections = 1, R.connectNumStripes = Just 1}
  void (append src.streamKey "1-0" [("value", "bad")])
  void (append src.streamKey "2-0" [("value", "good")])
  bracket (R.connect commandInfo) R.disconnect $ \commands -> do
    client <- send @Integer commands ["CLIENT", "ID"]
    let disconnect =
          send @Integer admin ["CLIENT", "KILL", "ID", C8.pack (show client)]
            >>= assert "fixture disconnects the borrowed command socket" . (== 1)
        cfg =
          (rawReaderConfig "reject-disconnect" "index" src Earliest)
            { readerDecode = disconnectingDecoder disconnect,
              readerDeadLetters = Just dest
            }
    withReader (BorrowCommands info commands) cfg $ \reader -> do
      seen <- newChan
      runReader reader (recording seen) $ \app -> do
        liftIO (timeout 3000000 (expectIds seen [EntryId 2 0]) >>= assert "decode rejection recovers the connection" . (== Just ()))
        stopAppGracefully (ShutdownConfig 2 4) app >>= liftIO . assert "rejection processor drains"
      reader.completedCheckpoint >>= assert "checkpoint advances after reconnect" . (== Checkpoint (EntryId 2 0) Nothing)
    send @Integer admin ["HLEN", dest] >>= assert "rejection written once after reconnect" . (== 1)

-- Test-only fault injection at the boundary immediately before the rejection write.
{-# NOINLINE disconnectingDecoder #-}
disconnectingDecoder :: IO () -> RedisEntry -> Either DeadLetterReason RedisEntry
disconnectingDecoder disconnect entry
  | entry.entryId == EntryId 1 0 = unsafePerformIO (disconnect >> pure (Left (InvalidPayload "malformed")))
  | otherwise = Right entry

rejection :: ConnectionSource -> R.Connection -> Append -> IO ()
rejection pools producer append = do
  let src = SourceIdentity "test" Nothing "reject"
      reason = InvalidPayload "malformed"
      raw = rawReaderConfig "reject" "index" src Earliest
  void (append src.streamKey "1-0" [("value", "invalid")])
  saved <- newIORef Nothing
  let store = storeIn saved
      rejecting = raw {readerDecode = const (Left reason), checkpointStore = Just store, readerDeadLetters = Just "failures"}
      failing =
        store
          { saveCheckpoint = \name identity old new ->
              if new.position == EntryId 1 0 then fail "checkpoint store unavailable" else store.saveCheckpoint name identity old new
          }
      noHandler _ = liftIO (fail "decode failure reached the handler")
      failureCount = R.runRedis producer (R.hlen "failures")
  withReader pools (rejecting {checkpointStore = Just failing}) $ \reader ->
    runReader reader noHandler waitApp
  failureCount >>= assert "reject written before failed checkpoint" . (== Right 1)
  readIORef saved >>= assert "failed checkpoint remains at boundary" . (== Just (Checkpoint (EntryId 0 0) Nothing))
  withReader pools rejecting $ \reader ->
    runReader reader noHandler $ \app -> do
      let awaitSave = do
            cursor <- reader.completedCheckpoint
            unless (cursor.position == EntryId 1 0) (threadDelay 1000 >> awaitSave)
      liftIO awaitSave
      void (stopAppGracefully (ShutdownConfig 2 4) app)
  failureCount >>= assert "replayed rejection stored once" . (== Right 1)
  let assertOrigin reader = reader.completedCheckpoint >>= assert "terminal decision leaves cursor intact" . (== Checkpoint (EntryId 0 0) Nothing)
  withReader pools (rejecting {checkpointStore = Nothing, readerDeadLetters = Nothing}) $ \reader -> do
    runReader reader noHandler waitApp
    assertOrigin reader
  withReader pools raw $ \reader -> do
    runReader reader (const (pure (AckHalt (HaltFatal "planned halt")))) waitApp
    assertOrigin reader

handover :: ConnectionSource -> R.Connection -> Append -> IO ()
handover pools producer append = do
  let src = SourceIdentity "test" (Just "generation-1") "handover"
      previous = Checkpoint (EntryId 1 0) (Just "A")
      liveSource = src {streamKey = "empty-live"}
  withReader pools (rawReaderConfig "live" "viewer" liveSource Live) $ \reader -> do
    reader.completedCheckpoint >>= assert "missing stream live boundary" . (== Checkpoint (EntryId 0 0) Nothing)
    void (append liveSource.streamKey "1-0" [("value", "new")])
    seen <- newChan
    runReader reader (recording seen) $ \app -> do
      liftIO $ do
        expectIds seen [EntryId 1 0]
        let blockedRead = do
              clients <- R.runRedis producer (R.sendRequest ["CLIENT", "LIST"]) >>= either (fail . show) pure
              case [ identifier
                   | row <- C8.lines clients,
                     "cmd=xread" `elem` C8.words row,
                     field <- C8.words row,
                     Just identifier <- [C8.stripPrefix "id=" field]
                   ] of
                [identifier] -> pure identifier
                _ -> threadDelay 10000 >> blockedRead
        client <- blockedRead
        killed <- R.runRedis producer (R.sendRequest ["CLIENT", "KILL", "ID", client]) >>= either (fail . show) pure
        assert "disconnect the one dedicated reader socket" (killed == (1 :: Integer))
        void (append liveSource.streamKey "2-0" [("value", "after disconnect")])
        expectIds seen [EntryId 2 0]
      void (stopAppGracefully (ShutdownConfig 2 4) app)
  void (append src.streamKey "1-0" [("revision", "A")])
  void (append src.streamKey "2-0" [("revision", "B"), ("previous", "A"), ("generation", "generation-1")])
  let configs = rawReaderConfig "snapshot" "viewer" src (After previous)
      observeContinuity checkpoint entry =
        let revision = lookup "revision" entry.fields
            valid = lookup "previous" entry.fields == checkpoint.continuity && lookup "generation" entry.fields == src.generation
         in (revision, if valid then Nothing else Just "producer generation or predecessor changed")
      checked = configs {checkContinuity = observeContinuity}
      attach = withReader pools checked $ \reader -> do
        seen <- newChan
        runReader reader (recording seen) $ \app -> do
          liftIO (expectIds seen [EntryId 2 0])
          void (stopAppGracefully (ShutdownConfig 2 4) app)
  withAsync attach $ \other -> attach >> wait other
  void $ R.runRedis producer (R.xdel src.streamKey ("2-0" :| []))
  void (append src.streamKey "3-0" [("revision", "C"), ("previous", "B"), ("generation", "generation-1")])
  gap <- newEmptyMVar
  let strict = checked {onReaderGap = \evidence -> putMVar gap evidence >> pure Resnapshot}
      testGap expected = withReader pools strict $ \reader ->
        runReader reader (const (liftIO (fail "known gap reached the handler"))) $ \app -> do
          liftIO $ takeMVar gap >>= assert "detected continuity evidence" . (== expected) . (.gapEvidence)
          waitApp app
  testGap (ProducerDiscontinuity "producer generation or predecessor changed")
  withReader pools (checked {onReaderGap = const (pure BestEffort)}) $ \reader -> do
    seen <- newChan
    runReader reader (recording seen) $ \app -> do
      liftIO (expectIds seen [EntryId 3 0])
      void (stopAppGracefully (ShutdownConfig 2 4) app)
  void $ R.runRedis producer (R.del (src.streamKey :| []))
  testGap (StreamRolledBack Nothing)

  void (append src.streamKey "4-0" [("revision", "B"), ("previous", "A"), ("generation", "generation-2")])
  testGap (ProducerDiscontinuity "producer generation or predecessor changed")
