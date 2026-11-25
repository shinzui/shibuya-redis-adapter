module Shibuya.Adapter.Redis.Reader
  ( Checkpoint (..),
    CheckpointStore (..),
    ReaderStart (..),
    GapEvidence (..),
    ReaderGap (..),
    GapAction (..),
    ReaderError (..),
    ReaderConfig (..),
    rawReaderConfig,
    Reader (..),
    readerProcessor,
    newReader,
  )
where

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

-- | Last completed entry and optional producer-supplied continuity evidence.
data Checkpoint = Checkpoint
  { -- | Cursor used to resume strictly after this entry.
    position :: !EntryId,
    -- | Opaque producer evidence persisted with the cursor.
    continuity :: !(Maybe ByteString)
  }
  deriving stock (Eq, Show)

-- | Application-owned checkpoint persistence. A conditional save must accept
-- an identical already-committed update. Both callbacks receive the same
-- logical reader name and source identity.
data CheckpointStore = CheckpointStore
  { -- | Load the reader’s durable completed cursor, if present.
    loadCheckpoint :: ByteString -> SourceIdentity -> IO (Maybe Checkpoint),
    -- | Save the next completed cursor given the expected previous value.
    -- The previous value is absent when writing the start boundary.
    saveCheckpoint :: ByteString -> SourceIdentity -> Maybe Checkpoint -> Checkpoint -> IO ()
  }

-- | Initial position when the checkpoint store has no saved cursor.
data ReaderStart
  = -- | Deliver every retained entry starting at the first.
    Earliest
  | -- | Deliver entries strictly after the supplied checkpoint.
    After !Checkpoint
  | -- | Start after the stream’s current tail.
    Live
  deriving stock (Eq, Show)

-- | Evidence that previously completed history may no longer match the stream.
data GapEvidence
  = -- | Redis’s tail moved behind the checkpoint. A missing tail denotes an absent stream.
    StreamRolledBack !(Maybe EntryId)
  | -- | The application’s continuity callback reported a discontinuity.
    ProducerDiscontinuity !Text
  deriving stock (Eq, Show)

-- | Information passed to the application when the reader detects a gap.
data ReaderGap = ReaderGap
  { -- | Logical source whose history changed.
    gapSource :: !SourceIdentity,
    -- | Completed checkpoint before the gap.
    lastCheckpoint :: !Checkpoint,
    -- | Candidate next entry when the gap was detected during delivery.
    nextEntry :: !(Maybe EntryId),
    -- | Evidence that triggered the gap callback.
    gapEvidence :: !GapEvidence
  }
  deriving stock (Eq, Show)

-- | Choose how a reader responds to a detected history gap.
data GapAction
  = -- | Stop and leave reconstruction to the application.
    StopAndRebuild
  | -- | Stop so the application can resnapshot and construct a fresh reader.
    Resnapshot
  | -- | Accept the discontinuity and keep reading.
    BestEffort
  deriving stock (Eq, Show)

-- | Terminal failures specific to an independent reader.
data ReaderError
  = -- | A detected gap and the application's terminal disposition.
    ReaderHistoryLost !ReaderGap !GapAction
  | -- | The entry requires rejection but no destination is configured.
    ReaderRejectionWithoutDestination !EntryId
  | -- | The entry exhausted its retries without a dead-letter destination.
    ReaderRetryLimit !EntryId
  deriving anyclass (Exception)

instance Show ReaderError where
  show (ReaderHistoryLost _ disposition) = "ReaderHistoryLost " <> show disposition
  show (ReaderRejectionWithoutDestination position) = "ReaderRejectionWithoutDestination " <> show position
  show (ReaderRetryLimit position) = "ReaderRetryLimit " <> show position

-- | Configuration of one ordered independent reader. 'rawReaderConfig'
-- supplies the defaults for a raw payload.
data ReaderConfig a = ReaderConfig
  { -- | Display name passed to Shibuya. Supplied by the caller.
    readerAdapterName :: !Text,
    -- | Logical name for checkpoints and dead-letter records. Supplied by the caller.
    readerIdentity :: !ByteString,
    -- | Namespace, generation and Redis key. Supplied by the caller.
    readerSource :: !SourceIdentity,
    -- | Position when no checkpoint is saved. Supplied by the caller.
    readerStart :: !ReaderStart,
    -- | Optional durable checkpoint storage. Defaults to an in-memory checkpoint.
    checkpointStore :: !(Maybe CheckpointStore),
    -- | Decode the entry or return a permanent failure. Defaults to the raw entry.
    readerDecode :: RedisEntry -> Either DeadLetterReason a,
    -- | Extract application headers. Defaults to no application headers.
    readerHeaders :: RedisEntry -> Maybe Headers,
    -- | Extract incoming W3C context. Defaults to 'defaultTraceContext'.
    readerTraceContext :: RedisEntry -> Maybe TraceHeaders,
    -- | Shibuya tracing settings. Defaults to 'LinkOnly' correlation.
    telemetry :: !AdapterTelemetry,
    -- | Evidence to save with this entry and an optional discontinuity message.
    -- Defaults to neither. 'BestEffort' accepts the evidence even on a gap.
    checkContinuity :: Checkpoint -> RedisEntry -> (Maybe ByteString, Maybe Text),
    -- | Respond to a history gap. Defaults to 'StopAndRebuild'.
    onReaderGap :: ReaderGap -> IO GapAction,
    -- | Redis hash for rejected entries. Defaults to no destination.
    readerDeadLetters :: !(Maybe ByteString),
    -- | Maximum retries after the first delivery. Defaults to 10.
    readerRetryLimit :: !Int
  }

-- | Configure a reader that delivers raw entries. Arguments are the adapter
-- name, logical reader name, source identity, and start position.
rawReaderConfig :: Text -> ByteString -> SourceIdentity -> ReaderStart -> ReaderConfig RedisEntry
rawReaderConfig name identity src start =
  ReaderConfig
    { readerAdapterName = name,
      readerIdentity = identity,
      readerSource = src,
      readerStart = start,
      checkpointStore = Nothing,
      readerDecode = Right,
      readerHeaders = const Nothing,
      readerTraceContext = defaultTraceContext,
      telemetry = defaultAdapterTelemetry {traceCorrelation = LinkOnly},
      checkContinuity = \_ _ -> (Nothing, Nothing),
      onReaderGap = const (pure StopAndRebuild),
      readerDeadLetters = Nothing,
      readerRetryLimit = 10
    }

-- | An attached reader and access to its progress.
data Reader es a = Reader
  { -- | Shibuya adapter for advanced runner integration.
    readerAdapter :: !(Adapter es a),
    -- | Read the latest completed checkpoint.
    completedCheckpoint :: IO Checkpoint
  }

-- | Run one delivery at a time in stream order. Batches that wait for several
-- outstanding entries cannot use the single-entry reader.
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
  withRuntimeAttachment runtime $ do
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
    pure (Reader (mkRedisAdapter runtime cfg.readerAdapterName cfg.telemetry deliveries) (readIORef completed))
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
