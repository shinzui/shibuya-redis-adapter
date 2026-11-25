module Main (main) where

import Control.Concurrent
import Control.Concurrent.Async (async, concurrently, wait, withAsync)
import Control.Exception (bracket)
import Control.Monad (forM_, unless, void)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as C8
import Data.IORef
import Data.List (sort)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Text qualified as T
import Database.Redis qualified as R
import Effectful (Eff, IOE, liftIO, runEff)
import Effectful.Exception qualified as E
import Fixture
import Shibuya
import Shibuya.Adapter.Redis
import Streamly.Data.Stream qualified as Stream
import System.Timeout (timeout)

type Effects = '[Tracing, IOE]

assert :: String -> Bool -> IO ()
assert label condition = unless condition (fail label)

within :: String -> IO a -> IO a
within label action = timeout 12000000 action >>= maybe (fail (label <> " timed out")) pure

main :: IO ()
main = withRedis $ \info ->
  bracket (R.connect info) R.disconnect $ \admin -> do
    within "same-consumer retry" $ sameConsumerRetry info admin "solo-retry" 1 (Just (RetryDelay 0.2))
    within "creation and distribution" $ distribution info admin
    forM_ [AckOk, AckRetry (RetryDelay 40), AckDeadLetter rejection] $ \decision ->
      within ("takeover and late " <> show decision) $ takeover info admin decision
    within "long-handler renewal" $ leaseRenewal info admin
    within "retry deadline" $ sameConsumerRetry info admin "retry-deadline" 5000 (Just (RetryDelay 0.3))
    within "handler exception retry" $ sameConsumerRetry info admin "retry-exception" 5000 Nothing
    within "retry exhaustion" $ retryExhaustion info admin
    within "script cache and active recovery" $ scriptCache info admin
    within "retry and rejection" $ retryAndReject info admin
    within "pending loss and failed destination" $ payloadLoss info admin
    within "rotating pending scan" $ boundedScan info admin
    putStrLn "Group creation, distribution, takeover, fencing, solo retry, retry exhaustion, script caching, rejection, pending loss and scan fairness passed"

command :: (R.RedisResult a) => R.Connection -> [ByteString] -> IO a
command connection request = R.runRedis connection (R.sendRequest request) >>= either (fail . show) pure

append :: R.Connection -> ByteString -> ByteString -> IO ()
append connection key identifier =
  void $ R.runRedis connection (R.xadd key identifier [("data", "job")]) >>= either (fail . show) pure

pending :: R.Connection -> ByteString -> ByteString -> IO [[R.Reply]]
pending admin key group = command admin ["XPENDING", key, group, "-", "+", "100"]

ownerIs :: ByteString -> [[R.Reply]] -> Bool
ownerIs worker [[_, R.Bulk (Just who), _, _]] = who == worker
ownerIs _ _ = False

age :: R.Connection -> ByteString -> ByteString -> ByteString -> ByteString -> IO ()
age admin key group owner identifier =
  void $ command @([ByteString]) admin ["XCLAIM", key, group, owner, "0", identifier, "IDLE", "10000", "JUSTID"]

await :: IO Bool -> IO ()
await action = do
  satisfied <- action
  unless satisfied (threadDelay 10000 >> await action)

withGroup :: R.ConnectInfo -> GroupConfig a -> (Group Effects a -> IO b) -> IO b
withGroup info config use =
  withRedisRuntime (OwnPools info) defaultRuntimeOptions $ \runtime ->
    newGroupWorker runtime config >>= use

runWorker :: Group Effects a -> Handler Effects a -> (AppHandle Effects -> Eff Effects b) -> IO b
runWorker group handler use = runEff $ runTracingNoop $ do
  result <- runApp defaultAppConfig [(ProcessorId "worker", groupProcessor group handler)]
  app <- either (liftIO . fail . show) pure result
  use app `E.finally` void (stopAppGracefully (ShutdownConfig 0.1 1) app)

drain :: AppHandle Effects -> Eff Effects ()
drain app = stopAppGracefully (ShutdownConfig 1 2) app >>= liftIO . assert "group drain"

configuration :: ByteString -> ByteString -> ByteString -> GroupStart -> GroupConfig RedisEntry
configuration key group worker start =
  (rawGroupConfig "worker" (SourceIdentity "tests" Nothing key) group worker start)
    { minimumIdleMillis = 5000
    }

record :: Chan (ByteString, EntryId) -> ByteString -> Handler Effects RedisEntry
record channel name message = liftIO $ do
  writeChan channel (name, message.envelope.payload.entryId)
  pure AckOk

distribution :: R.ConnectInfo -> R.Connection -> IO ()
distribution info admin = do
  let key = "distribution"
      cfg = configuration key "jobs"
      use owner start = withGroup info (cfg owner start)
  append admin key "1-0"
  void $
    concurrently
      (use "creator-a" (CreateIfAbsent (EntryId 0 0)) (const (pure ())))
      (use "creator-b" CreateAtTailIfAbsent (const (pure ())))
  -- Read the actual winning position before deliberately requesting other starts.
  details <- command @[R.Reply] admin ["XINFO", "GROUPS", key]
  use "creator-c" CreateAtTailIfAbsent (const (pure ()))
  use "creator-d" (CreateIfAbsent (EntryId 0 0)) (const (pure ()))
  command @[R.Reply] admin ["XINFO", "GROUPS", key] >>= assert "concurrent creation preserves winning position" . (== details)
  append admin key "2-0"
  append admin key "3-0"
  claimedA <- newEmptyMVar
  releaseA <- newEmptyMVar
  seen <- newChan
  allSeen <- newChan
  withAsync
    ( use "a" AttachExisting $ \worker ->
        runWorker
          worker
          ( \message -> liftIO $ do
              putMVar claimedA message.envelope.payload.entryId
              readMVar releaseA
              pure AckOk
          )
          $ \app -> liftIO (readMVar releaseA) >> drain app
    )
    $ \a -> do
      first <- readMVar claimedA
      use "b" AttachExisting $ \worker ->
        runWorker worker (record seen "b") $ \app -> do
          (_, second) <- liftIO (readChan seen)
          liftIO $ assert "other worker receives a distinct job" (first /= second)
          liftIO (putMVar releaseA ())
          drain app
      wait a
  withGroup info (configuration key "separate" "c" (CreateIfAbsent (EntryId 0 0))) $ \worker ->
    runWorker worker (record allSeen "c") $ \app -> do
      ids <- liftIO (mapM (const (snd <$> readChan allSeen)) [1 :: Int, 2, 3])
      liftIO (assert "separate group sees all original jobs" (sort ids == map (`EntryId` 0) [1, 2, 3]))
      drain app
  command @Integer admin ["XLEN", key] >>= assert "group ack leaves source intact" . (== 3)

takeover :: R.ConnectInfo -> R.Connection -> AckDecision -> IO ()
takeover info admin oldDecision = do
  let key = C8.pack (show oldDecision)
      config worker = (configuration key "jobs" worker (CreateIfAbsent (EntryId 0 0))) {groupDeadLetters = Just (key <> ":rejects")}
      gid = "jobs"
      release m = liftIO (readMVar m)
  append admin key "1-0"
  second <- newEmptyMVar
  third <- newEmptyMVar
  unpause <- newEmptyMVar
  allowB <- newEmptyMVar
  done <- newEmptyMVar
  bReturned <- newEmptyMVar
  -- Keep the first callback outside the runner to replay every stale operation
  -- after A→B→A. A and B's subsequent deliveries use the public processors.
  withGroup info ((config "a") {groupInFlight = 2}) $ \a ->
    runEff $ runTracingNoop $ do
      item <- Stream.uncons a.groupAdapter.source >>= maybe (liftIO (fail "first delivery missing")) (pure . fst)
      liftIO $ assert "initial attempt" (item.envelope.attempt == Just 0)
      run <- liftIO $ runAsyncWorker info (config "b") second allowB bReturned
      E.finally
        ( do
            liftIO $ age admin key gid "a" "1-0"
            liftIO $ readMVar second >>= assert "B claims first generation" . (== Just 1)
            liftIO $ pending admin key gid >>= assert "B owns pending work" . ownerIs "b"
            extend item
            status <- item.deliveryStatus
            case status of
              DeliverySuperseded -> pure ()
              _ -> liftIO (fail "stale lease did not supersede the delivery")
            acknowledge item oldDecision
            liftIO $ pending admin key gid >>= assert "stale callback cannot touch B" . ownerIs "b"
            output <-
              runApp
                defaultAppConfig
                [ ( ProcessorId "worker",
                    groupProcessor
                      a
                      ( \message -> do
                          liftIO $ putMVar third message.envelope.attempt
                          release unpause
                          pure AckOk
                      )
                  )
                ]
            app <- either (liftIO . fail . show) pure output
            flip E.finally (drain app) $ do
              liftIO $ do
                age admin key gid "b" "1-0"
                readMVar third >>= assert "A claims with new generation" . (== Just 2)
              extend item
              acknowledge item oldDecision
              liftIO $ do
                pending admin key gid >>= assert "stale callback cannot touch new A" . ownerIs "a"
                putMVar allowB ()
                readMVar bReturned
                putMVar unpause ()
              drain app
              liftIO $ putMVar done ()
        )
        (liftIO run)
  readMVar done
  pending admin key gid >>= assert "new A acknowledged" . null
  command @Integer admin ["HLEN", key <> ":rejects"] >>= assert "stale rejection writes nothing" . (== 0)
  where
    acknowledge item decision = let AckHandle finish = item.ack in finish decision
    extend item = mapM_ (\lease -> lease.leaseExtend 60) item.lease

runAsyncWorker :: R.ConnectInfo -> GroupConfig RedisEntry -> MVar (Maybe Attempt) -> MVar () -> MVar () -> IO (IO ())
runAsyncWorker info config started proceed returned = do
  let use = withGroup info config $ \worker ->
        runWorker
          worker
          ( \message -> liftIO $ do
              putMVar started message.envelope.attempt
              readMVar proceed
              pure AckOk
          )
          $ \app -> do
            liftIO (readMVar proceed)
            drain app
            liftIO (putMVar returned ())
  -- Return a join action. The caller must release proceed before joining.
  job <- async use
  pure (void (tryPutMVar proceed ()) >> wait job)

rejection :: DeadLetterReason
rejection = InvalidPayload (T.replicate 600 "x")

retryAndReject :: R.ConnectInfo -> R.Connection -> IO ()
retryAndReject info admin = do
  let key = "retry-group"
      cfg owner =
        (configuration key "jobs" owner (CreateIfAbsent (EntryId 0 0)))
          { minimumIdleMillis = 1,
            groupDeadLetters = Just "retry-rejections"
          }
  append admin key "1-0"
  initial <- newEmptyMVar
  withGroup info (cfg "a") $ \worker ->
    runWorker
      worker
      ( \message -> liftIO $ do
          now <- redisMillis admin
          putMVar initial (message.envelope.messageId, now)
          pure (AckRetry (RetryDelay 1))
      )
      $ \app -> do
        _ <- liftIO (readMVar initial)
        drain app
  (original, issued) <- readMVar initial
  calls <- newIORef (0 :: Int)
  finalized <- newEmptyMVar
  withGroup info (cfg "b") $ \worker -> do
    let base = worker.groupAdapter
        hideReply delivery =
          let AckHandle finish = delivery.ack
           in delivery
                { ack = AckHandle $ \decision -> do
                    finish decision
                    first <- liftIO $ atomicModifyIORef' calls (\n -> (n + 1, n == 0))
                    liftIO $ if first then fail "fixture hid the committed rejection reply" else putMVar finalized ()
                }
        instrumented = worker {groupAdapter = base {source = Stream.mapM (pure . hideReply) base.source}}
        handle message = liftIO $ do
          now <- redisMillis admin
          assert "retry did not run before due time" (now >= issued + 1000)
          assert "retry preserves message identity" (message.envelope.messageId == original && message.envelope.attempt == Just 1)
          pure (AckDeadLetter rejection)
    runWorker instrumented handle $ \app -> liftIO (readMVar finalized) >> drain app
  readIORef calls >>= assert "runner repeated the finalizer after an uncertain reply" . (== 2)
  pending admin key "jobs" >>= assert "rejected ID acknowledged" . null
  command @Integer admin ["HLEN", "retry-rejections"] >>= assert "one logical rejection after replay" . (== 1)
  let other = (cfg "c") {groupName = "another-group"}
  withGroup info other $ \worker ->
    runEff $ runTracingNoop $ do
      (delivery, _) <- Stream.uncons worker.groupAdapter.source >>= maybe (liftIO (fail "second group's input is missing")) pure
      let AckHandle finish = delivery.ack
      finish (AckDeadLetter rejection)
  command @Integer admin ["HLEN", "retry-rejections"] >>= assert "rejection scoped to group" . (== 2)

sameConsumerRetry :: R.ConnectInfo -> R.Connection -> ByteString -> Int -> Maybe RetryDelay -> IO ()
sameConsumerRetry info admin key idle delay = do
  let cfg = (configuration key "jobs" "solo" (CreateIfAbsent (EntryId 0 0))) {minimumIdleMillis = idle}
  seen <- newChan
  let handler message = liftIO $ do
        now <- redisMillis admin
        let envelope = message.envelope
        writeChan seen (envelope.messageId, envelope.attempt, now)
        case envelope.attempt of
          Just 0 -> maybe (fail "handler failed on the first attempt") (pure . AckRetry) delay
          _ -> pure AckOk
  append admin key "1-0"
  withGroup info cfg $ \worker ->
    runWorker worker handler $ \app -> do
      liftIO $ do
        (original, first, issued) <- readChan seen
        later <- timeout 2000000 (readChan seen)
        (retried, next, delivered) <- maybe (fail "solo worker did not retry within two seconds") pure later
        assert "retry preserves the identity and increments the attempt" (original == retried && first == Just 0 && next == Just 1)
        forM_ delay $ \(RetryDelay duration) ->
          assert "retry respects Redis server deadline" (delivered >= issued + floor (duration * 1000))
      drain app
  pending admin key "jobs" >>= assert "solo retry was acknowledged" . null

redisMillis :: R.Connection -> IO Integer
redisMillis admin = do
  serverTime <- command @[ByteString] admin ["TIME"]
  case serverTime of
    [seconds, micros] -> pure (read (C8.unpack seconds) * 1000 + read (C8.unpack micros) `div` 1000)
    _ -> fail "invalid server TIME reply"

retryExhaustion :: R.ConnectInfo -> R.Connection -> IO ()
retryExhaustion info admin = do
  let cfg key = (configuration key "jobs" "solo" (CreateIfAbsent (EntryId 0 0))) {groupRetryLimit = 1}
      retry = const (pure (AckRetry (RetryDelay 0)))
  append admin "limited-pending" "1-0"
  withGroup info (cfg "limited-pending") $ \worker -> runWorker worker retry waitApp
  pending admin "limited-pending" "jobs" >>= assert "exhaustion preserves pending work without a destination" . ownerIs "solo"
  seen <- newChan
  let key = "limited-group"
      dest = "limited-rejections"
      handler message
        | message.envelope.payload.entryId == EntryId 1 0 = retry message
        | otherwise = record seen "solo" message
  append admin key "1-0"
  withGroup info ((cfg key) {groupDeadLetters = Just dest}) $ \worker ->
    runWorker worker handler $ \app -> do
      liftIO $ do
        await ((== 1) <$> command @Integer admin ["HLEN", dest])
        command @[ByteString] admin ["HVALS", dest] >>= \records ->
          assert
            "one rejection with the exhaustion reason"
            ( case records of
                [value] -> "max_retries_exceeded" `C8.isInfixOf` value
                _ -> False
            )
        append admin key "2-0"
        readChan seen >>= assert "same worker processes the next job" . (== ("solo", EntryId 2 0))
      drain app
  pending admin key "jobs" >>= assert "no pending work after dead-lettering" . null

scriptCache :: R.ConnectInfo -> R.Connection -> IO ()
scriptCache info admin = do
  let key = "script-cache"
      cfg = (configuration key "jobs" "solo" (CreateIfAbsent (EntryId 0 0))) {groupInFlight = 2}
      stats = command @ByteString admin ["INFO", "commandstats"]
      count prefix = map (C8.takeWhile (/= ',')) . filter (prefix `C8.isPrefixOf`) . C8.lines
  started <- newEmptyMVar
  release <- newEmptyMVar
  seen <- newChan
  let handler message
        | message.envelope.payload.entryId == EntryId 1 0 = liftIO $ do
            putMVar started ()
            readMVar release
            pure AckOk
        | otherwise = record seen "solo" message
  append admin key "1-0"
  withGroup info cfg $ \worker ->
    runWorker worker handler $ \app -> do
      liftIO $ do
        readMVar started
        before <- stats
        threadDelay 1100000
        after <- stats
        assert "active row is scanned without a Lua call" $
          count "cmdstat_xpending:" after /= count "cmdstat_xpending:" before
            && count "cmdstat_evalsha:" after == count "cmdstat_evalsha:" before
            && count "cmdstat_eval:" after == count "cmdstat_eval:" before
        putMVar release ()
        await (null <$> pending admin key "jobs")
        void $ command @R.Status admin ["SCRIPT", "FLUSH"]
        append admin key "2-0"
        readChan seen >>= assert "worker reloads the flushed script" . (== ("solo", EntryId 2 0))
      drain app
  pending admin key "jobs" >>= assert "flushed script recovered through finalization" . null

payloadLoss :: R.ConnectInfo -> R.Connection -> IO ()
payloadLoss info admin = do
  let key = "loss"
      cfg worker = (configuration key "jobs" worker (CreateIfAbsent (EntryId 0 0))) {groupDeadLetters = Just "wrong-type"}
  append admin key "1-0"
  void $ command @R.Status admin ["SET", "wrong-type", "string"]
  withGroup info (cfg "a") $ \worker ->
    runWorker worker (const (pure (AckDeadLetter rejection))) waitApp
  pending admin key "jobs" >>= assert "invalid hash preserved pending entry" . ownerIs "a"
  age admin key "jobs" "a" "1-0"
  void $ R.runRedis admin (R.xdel key ("1-0" :| [])) >>= either (fail . show) pure
  observed <- newEmptyMVar
  withGroup info ((cfg "b") {onMissingPending = putMVar observed}) $ \worker ->
    runWorker worker (const (liftIO (fail "missing payload reached handler"))) $ \app -> do
      liftIO $ do
        loss <- readMVar observed
        assert "diagnostic carries source and ID" (loss.missingEntry == EntryId 1 0 && loss.missingSource.streamKey == key)
        await (null <$> pending admin key "jobs")
      drain app
  append admin key "2-0"
  withGroup info ((cfg "c") {groupDeadLetters = Nothing}) $ \worker ->
    runWorker worker (const (pure (AckDeadLetter rejection))) waitApp
  pending admin key "jobs" >>= assert "rejection without destination preserves work" . ownerIs "c"
  age admin key "jobs" "c" "2-0"
  withGroup info ((cfg "d") {groupDeadLetters = Nothing}) $ \worker ->
    runWorker worker (const (pure (AckHalt (HaltFatal "planned halt")))) waitApp
  pending admin key "jobs" >>= assert "halt preserves work" . ownerIs "d"

boundedScan :: R.ConnectInfo -> R.Connection -> IO ()
boundedScan info admin = do
  let key = "scan"
      cfg =
        (configuration key "jobs" "scan-worker" AttachExisting)
          { groupInFlight = 1,
            groupScanLimit = 1,
            samplePending = True
          }
  mapM_ (append admin key) ["1-0", "2-0", "3-0", "4-0", "5-0"]
  void $ command @R.Status admin ["XGROUP", "CREATE", key, "jobs", "0-0"]
  void $ command @[R.Reply] admin ["XREADGROUP", "GROUP", "jobs", "parked", "COUNT", "3", "STREAMS", key, ">"]
  age admin key "jobs" "parked" "3-0"
  channel <- newChan
  events <- newIORef []
  let options = defaultRuntimeOptions {observe = \event -> atomicModifyIORef' events (\seen -> (event : seen, ()))}
  withRedisRuntime (OwnPools info) options $ \runtime -> do
    worker <- newGroupWorker runtime cfg
    runWorker worker (record channel "scan-worker") $ \app -> do
      observed <- liftIO $ mapM (const (snd <$> readChan channel)) [1 :: Int, 2, 3]
      liftIO $ assert "later pending page and new work both progress" (sort observed == map (`EntryId` 0) [3, 4, 5])
      drain app
  readIORef events >>= assert "group emits counts and bounded backlog sample" . (\seen -> all (`elem` seen) [Delivered, Acknowledged, Reclaimed] && any isSample seen)
  where
    isSample (PendingSample depth (Just ageMillis)) = depth > 0 && ageMillis >= 0
    isSample _ = False

leaseRenewal :: R.ConnectInfo -> R.Connection -> IO ()
leaseRenewal info admin = do
  let key = "lease"
      cfg worker = configuration key "jobs" worker (CreateIfAbsent (EntryId 0 0))
  append admin key "1-0"
  append admin key "2-0"
  withGroup info (cfg "holder") $ \a ->
    runEff $ runTracingNoop $ do
      delivery <- Stream.uncons a.groupAdapter.source >>= maybe (liftIO (fail "no lease to renew")) (pure . fst)
      case delivery.lease of
        Nothing -> liftIO (fail "group has no lease")
        Just lease -> lease.leaseExtend 60
      liftIO $ do
        age admin key "jobs" "holder" "1-0"
        withGroup info (cfg "peer") $ \b -> do
          seen <- newChan
          runWorker b (record seen "peer") $ \app -> do
            (_, identifier) <- liftIO (readChan seen)
            liftIO (assert "renewed lease withstands reclamation while new work proceeds" (identifier == EntryId 2 0))
            drain app
        pending admin key "jobs" >>= assert "renewal retained original owner" . ownerIs "holder"
      let AckHandle finish = delivery.ack
      finish AckOk
  pending admin key "jobs" >>= assert "renewal did not rotate the finalizer fence" . null
