module Shibuya.Adapter.Redis.Reader where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (race)
import Control.Concurrent.STM
import Control.Exception (Exception, mask, onException, throwIO, tryJust)
import Control.Monad (unless, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.IORef
import Data.Text (Text)
import Database.Redis qualified as Redis
import Effectful (IOE, liftIO, (:>))
import Shibuya
import Shibuya.Adapter.Redis.Adapter
import Shibuya.Adapter.Redis.Connection
import Shibuya.Adapter.Redis.DeadLetter
import Shibuya.Adapter.Redis.Types
import Shibuya.Telemetry.Propagation (currentTraceHeaders)
import Streamly.Data.Stream qualified as Stream

data Checkpoint = Checkpoint
  { position :: !EntryId,
    continuity :: !(Maybe ByteString)
  }
  deriving stock (Eq, Show)

-- | A conditional save must accept an identical already-committed update.
-- Load and save receive the same logical reader and source identity.
data CheckpointStore = CheckpointStore
  { loadCheckpoint :: ByteString -> SourceIdentity -> IO (Maybe Checkpoint),
    saveCheckpoint :: ByteString -> SourceIdentity -> Maybe Checkpoint -> Checkpoint -> IO ()
  }

data ReaderStart = Earliest | After !Checkpoint | Live
  deriving stock (Eq, Show)

data GapEvidence = StreamRolledBack !(Maybe EntryId) | ProducerDiscontinuity !Text
  deriving stock (Eq, Show)

data ReaderGap = ReaderGap
  { gapSource :: !SourceIdentity,
    lastCheckpoint :: !Checkpoint,
    nextEntry :: !(Maybe EntryId),
    gapEvidence :: !GapEvidence
  }
  deriving stock (Eq, Show)

-- | Resnapshot stops this reader so the application can construct a new one.
data GapAction = StopAndRebuild | Resnapshot | BestEffort
  deriving stock (Eq, Show)

data ReaderError
  = ReaderHistoryLost !ReaderGap !GapAction
  | ReaderRejectionWithoutDestination !EntryId
  | ReaderRetryLimit !EntryId
  deriving anyclass (Exception)

instance Show ReaderError where
  show (ReaderHistoryLost _ disposition) = "ReaderHistoryLost " <> show disposition
  show (ReaderRejectionWithoutDestination position) = "ReaderRejectionWithoutDestination " <> show position
  show (ReaderRetryLimit position) = "ReaderRetryLimit " <> show position

data ReaderConfig a = ReaderConfig
  { readerName :: !Text,
    readerIdentity :: !ByteString,
    readerSource :: !SourceIdentity,
    readerStart :: !ReaderStart,
    checkpointStore :: !(Maybe CheckpointStore),
    readerDecode :: RedisEntry -> Either DeadLetterReason a,
    readerHeaders :: RedisEntry -> Maybe Headers,
    readerTraceContext :: RedisEntry -> Maybe TraceHeaders,
    telemetry :: !AdapterTelemetry,
    -- Return the evidence to save with this entry and report a discontinuity
    -- separately. BestEffort accepts the returned evidence even on a gap.
    checkContinuity :: Checkpoint -> RedisEntry -> (Maybe ByteString, Maybe Text),
    onReaderGap :: ReaderGap -> IO GapAction,
    readerDeadLetters :: !(Maybe ByteString),
    readerRetryLimit :: !Int
  }

rawReaderConfig :: Text -> ByteString -> SourceIdentity -> ReaderStart -> ReaderConfig RedisEntry
rawReaderConfig name identity src start =
  ReaderConfig
    name
    identity
    src
    start
    Nothing
    Right
    (const Nothing)
    defaultTraceContext
    (defaultAdapterTelemetry {traceCorrelation = LinkOnly})
    (\_ _ -> (Nothing, Nothing))
    (const (pure StopAndRebuild))
    Nothing
    10

data Reader es a = Reader
  { readerAdapter :: !(Adapter es a),
    completedCheckpoint :: IO Checkpoint
  }

-- | Use the standard one-at-a-time Shibuya processor. A batch that waits for
-- multiple entries is incompatible with the adapter's single outstanding entry.
readerProcessor :: Reader es a -> Handler es a -> QueueProcessor es
readerProcessor reader handler = QueueProcessor reader.readerAdapter handler StrictInOrder Serial

data HeadResult = Advance | RepeatAfter !Int

data ReaderPull
  = Ready ![RedisEntry]
  | Await !(TMVar HeadResult) !RedisEntry !Checkpoint !Int ![RedisEntry]

-- | Create one reader per runtime. Keep the runtime open through runner drain.
newReader :: (IOE :> es, Tracing :> es) => RedisRuntime -> ReaderConfig a -> IO (Reader es a)
newReader runtime cfg = do
  when
    ( cfg.readerRetryLimit < 0
        || Just src.streamKey == cfg.readerDeadLetters
        || groupStatePrefix `BS.isPrefixOf` src.streamKey
        || any (groupStatePrefix `BS.isPrefixOf`) cfg.readerDeadLetters
    )
    (throwIO InvalidOptions)
  loaded <- maybe (pure Nothing) (\store -> store.loadCheckpoint cfg.readerIdentity src) cfg.checkpointStore
  initial <- case loaded of
    Just saved -> pure saved
    Nothing -> do
      boundary <- case cfg.readerStart of
        Earliest -> pure origin
        After checkpoint -> pure checkpoint
        Live -> do
          tailId <- streamTail runtime src
          pure (Checkpoint (maybe origin.position id tailId) Nothing)
      save Nothing boundary
      pure boundary
  completed <- newIORef initial
  let commit previous next = do
        save (Just previous) next
        writeIORef completed next

      reportGap previous entry evidence = do
        let gap = ReaderGap src previous entry evidence
        emit HistoryGap
        disposition <- cfg.onReaderGap gap
        unless (disposition == BestEffort) (throwIO (ReaderHistoryLost gap disposition))

      pull state = do
        stopped <- readTVarIO runtime.stopped
        if stopped
          then pure Nothing
          else case state of
            Ready [] -> do
              previous <- readIORef completed
              tailId <- streamTail runtime src
              boundary <-
                if maybe origin.position id tailId < previous.position
                  then do
                    reportGap previous Nothing (StreamRolledBack tailId)
                    commit previous origin
                    pure origin
                  else pure previous
              batch <- pollStream runtime (Redis.xreadOpts [(src.streamKey, renderEntryId boundary.position)] (readBatchOptions runtime))
              case batch of
                Nothing -> pure Nothing
                Just rows -> streamEntries src rows >>= pull . Ready
            Ready (entry : rest) -> do
              previous <- readIORef completed
              let (evidence, problem) = cfg.checkContinuity previous entry
                  next = Checkpoint entry.entryId evidence
              mapM_ (reportGap previous (Just entry.entryId) . ProducerDiscontinuity) problem
              deliver previous next entry 0 rest
            Await result entry next attempt rest -> do
              outcome <- atomically $ (Just <$> readTMVar result) `orElse` (readTVar runtime.stopped >>= check >> pure Nothing)
              case outcome of
                Nothing -> pure Nothing
                Just Advance -> pull (Ready rest)
                Just (RepeatAfter micros) -> do
                  waited <- race (atomically (readTVar runtime.stopped >>= check)) (threadDelay micros)
                  case waited of
                    Left () -> pure Nothing
                    Right () -> do
                      previous <- readIORef completed
                      deliver previous next entry (attempt + 1) rest

      deliver previous next entry attempt rest = do
        gate <- newEmptyTMVarIO
        let complete = commit previous next
            reject reason headers = case cfg.readerDeadLetters of
              Nothing -> throwIO (ReaderRejectionWithoutDestination entry.entryId)
              Just key -> do
                _ <- retryConnection runtime $ runCommand runtime $ storeRejection key (LogicalReader cfg.readerIdentity) entry reason headers
                emit DeadLettered
                complete
            finalize headers decision = mask $ \restore -> do
              unfinalized <- atomically (isEmptyTMVar gate)
              when unfinalized $ case decision of
                AckOk -> do
                  restore complete
                  atomically (putTMVar gate Advance)
                  emit Acknowledged
                AckDeadLetter reason -> do
                  restore (reject reason headers)
                  atomically (putTMVar gate Advance)
                AckRetry (RetryDelay delay)
                  | attempt == cfg.readerRetryLimit -> case cfg.readerDeadLetters of
                      Nothing -> throwIO (ReaderRetryLimit entry.entryId)
                      Just _ -> do
                        restore (reject MaxRetriesExceeded headers)
                        atomically (putTMVar gate Advance)
                  | otherwise -> do
                      let micros = max 0 (min (toInteger (maxBound :: Int)) (ceiling (delay * 1000000)))
                      atomically (putTMVar gate (RepeatAfter (fromInteger micros)))
                      emit Retried
                AckHalt _ -> stopIntake runtime
        case decodeEnvelope cfg.readerDecode cfg.readerHeaders cfg.readerTraceContext entry of
          Left (DecodeFailure _ reason) -> reject reason Nothing >> pull (Ready rest)
          Right envelope -> do
            emit Delivered
            let finish decision = do
                  headers <- case decision of
                    AckDeadLetter _ -> currentTraceHeaders
                    AckRetry _ | attempt == cfg.readerRetryLimit -> currentTraceHeaders
                    _ -> pure Nothing
                  liftIO (finalize headers decision)
                delivery = mkIngested (envelope {attempt = Just (Attempt (fromIntegral attempt))}) (AckHandle finish)
            pure (Just (delivery, Await gate entry next attempt rest))

  let deliveries = Stream.unfoldrM (liftIO . pull) (Ready [])
  pure (Reader (mkRedisAdapter runtime cfg.readerName cfg.telemetry deliveries) (readIORef completed))
  where
    src = cfg.readerSource
    origin = Checkpoint (EntryId 0 0) Nothing
    emit = runtime.options.observe
    save previous next = mapM_ persist cfg.checkpointStore
      where
        persist store =
          store.saveCheckpoint cfg.readerIdentity src previous next
            `onException` emit CheckpointFailed

streamTail :: RedisRuntime -> SourceIdentity -> IO (Maybe EntryId)
streamTail runtime src = do
  result <- tryJust missing $ retryConnection runtime $ runCommand runtime $ Redis.sendRequest ["XINFO", "STREAM", src.streamKey]
  case result of
    Left () -> pure Nothing
    Right (Redis.Error "ERR no such key") -> pure Nothing
    Right err@(Redis.Error _) -> throwIO (CommandRejected err)
    Right (Redis.MultiBulk (Just properties)) ->
      case lookup "last-generated-id" (pairs properties) >>= parseEntryId of
        Just position -> pure (Just position)
        Nothing -> throwIO InvalidStreamReply
    Right _ -> throwIO InvalidStreamReply
  where
    missing (CommandRejected (Redis.Error "ERR no such key")) = Just ()
    missing _ = Nothing
    pairs (Redis.Bulk (Just key) : Redis.Bulk (Just value) : rest) = (key, value) : pairs rest
    pairs (_ : _ : rest) = pairs rest
    pairs _ = []
