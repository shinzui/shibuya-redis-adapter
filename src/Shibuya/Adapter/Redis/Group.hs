module Shibuya.Adapter.Redis.Group
  ( GroupStart (..),
    MissingPending (..),
    GroupError (..),
    GroupConfig (..),
    rawGroupConfig,
    Group (..),
    groupProcessor,
    groupAttemptKey,
    newGroupWorker,
  )
where

import Control.Concurrent.STM
import Control.Exception (Exception, catch, mask, throwIO)
import Control.Monad (unless, void, when)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as C8
import Data.HashMap.Strict qualified as HashMap
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text.Encoding (decodeUtf8')
import Database.Redis qualified as Redis
import Effectful (IOE, liftIO, (:>))
import OpenTelemetry.Attributes (toAttribute)
import Shibuya
import Shibuya.Adapter.Redis.Adapter
import Shibuya.Adapter.Redis.Connection
import Shibuya.Adapter.Redis.DeadLetter
import Shibuya.Adapter.Redis.Group.Scripts
import Shibuya.Adapter.Redis.Types
import Shibuya.Telemetry.Propagation (currentTraceHeaders)
import Streamly.Data.Stream qualified as Stream
import System.Entropy (getEntropy)

-- | How to attach a worker to a Redis consumer group.
data GroupStart
  = -- | Require a group that already exists.
    AttachExisting
  | -- | Create a missing group at the supplied position.
    CreateIfAbsent !EntryId
  | -- | Create a missing group at the stream’s current tail.
    CreateAtTailIfAbsent
  deriving stock (Eq, Show)

-- | A pending ID whose source payload no longer exists.
data MissingPending = MissingPending
  { -- | Source associated with the missing payload.
    missingSource :: !SourceIdentity,
    -- | Group with the lost pending payload.
    missingGroup :: !ByteString,
    -- | ID of the unavailable entry.
    missingEntry :: !EntryId
  }
  deriving stock (Eq, Show)

-- | Terminal failures specific to consumer-group workers.
data GroupError
  = -- | The Redis consumer group is missing.
    GroupUnavailable
  | -- | The entry requires rejection but no destination is configured.
    GroupRejectionWithoutDestination !EntryId
  | -- | The entry exhausted its retries without a dead-letter destination.
    GroupRetryLimit !EntryId
  deriving stock (Show)
  deriving anyclass (Exception)

-- | Configuration of a consumer-group worker. 'rawGroupConfig' supplies
-- defaults for a raw payload.
data GroupConfig a = GroupConfig
  { -- | Display name passed to Shibuya. Supplied by the caller.
    groupAdapterName :: !Text,
    -- | Namespace, generation and Redis key. Supplied by the caller.
    groupSource :: !SourceIdentity,
    -- | Redis consumer group name. Supplied by the caller.
    groupName :: !ByteString,
    -- | Consumer name unique to the running instance, including across restarts.
    -- Supplied by the caller.
    consumerName :: !ByteString,
    -- | Attach or create the group. Supplied by the caller.
    groupStart :: !GroupStart,
    -- | Maximum outstanding deliveries per worker. Defaults to 1.
    groupInFlight :: !Int,
    -- | Maximum pending rows per recovery page. Defaults to 32.
    groupScanLimit :: !Int,
    -- | Idle time in milliseconds before abandoned work is eligible for recovery.
    -- Defaults to 30,000. Explicit retries use their own deadline.
    minimumIdleMillis :: !Int,
    -- | Maximum retries, counting prior deliveries and claims. Defaults to 10.
    groupRetryLimit :: !Int,
    -- | Redis hash for rejected entries. Defaults to no destination.
    groupDeadLetters :: !(Maybe ByteString),
    -- | Decode the entry or return a permanent failure. Defaults to the raw entry.
    groupDecode :: RedisEntry -> Either DeadLetterReason a,
    -- | Extract application headers. Defaults to no application headers.
    groupHeaders :: RedisEntry -> Maybe Headers,
    -- | Extract incoming W3C context. Defaults to 'defaultTraceContext'.
    groupTraceContext :: RedisEntry -> Maybe TraceHeaders,
    -- | Shibuya tracing settings. Defaults to 'ParentAndLink' correlation.
    telemetry :: !AdapterTelemetry,
    -- | Called before acknowledging a pending ID whose payload has disappeared.
    -- Defaults to a no-op.
    onMissingPending :: MissingPending -> IO (),
    -- | Sample total pending depth on scan wrap and the highest idle time per page.
    -- Defaults to disabled.
    samplePending :: !Bool
  }

-- | Configure a worker that delivers raw entries. Arguments are the adapter
-- name, source identity, group name, consumer name, and start policy.
rawGroupConfig :: Text -> SourceIdentity -> ByteString -> ByteString -> GroupStart -> GroupConfig RedisEntry
rawGroupConfig name src group worker start =
  GroupConfig
    { groupAdapterName = name,
      groupSource = src,
      groupName = group,
      consumerName = worker,
      groupStart = start,
      groupInFlight = 1,
      groupScanLimit = 32,
      minimumIdleMillis = 30000,
      groupRetryLimit = 10,
      groupDeadLetters = Nothing,
      groupDecode = Right,
      groupHeaders = const Nothing,
      groupTraceContext = defaultTraceContext,
      telemetry = defaultAdapterTelemetry,
      onMissingPending = const (pure ()),
      samplePending = False
    }

-- | An attached consumer-group worker.
data Group es a = Group
  { -- | Shibuya adapter for advanced runner integration.
    groupAdapter :: !(Adapter es a),
    -- | Shibuya concurrency selected from the configured outstanding limit.
    configuredConcurrency :: !Concurrency
  }

-- | Process deliveries without an ordering guarantee, using the worker’s
-- configured concurrency.
groupProcessor :: Group es a -> Handler es a -> QueueProcessor es
groupProcessor group handler = QueueProcessor group.groupAdapter handler Unordered group.configuredConcurrency

data Pending = Pending !EntryId !ByteString !Integer

data GroupPull
  = Buffered ![RedisEntry] !EntryId !Bool
  | Poll !EntryId !Bool

data Active = Active !EntryId !(TVar Bool)

-- | Versioned, length-framed state for one pending ID. Keep this namespace
-- reserved for the adapter and consistent across every worker in the group.
groupAttemptKey :: SourceIdentity -> ByteString -> EntryId -> ByteString
groupAttemptKey src group position =
  groupStatePrefix <> frame [group, entryIdentity (RedisEntry src position [])]

-- | Attach one consumer-group worker to a runtime and load the fencing script.
-- Keep the runtime open until all finalizers have drained.
newGroupWorker :: (IOE :> es, Tracing :> es) => RedisRuntime -> GroupConfig a -> IO (Group es a)
newGroupWorker runtime cfg = do
  let concurrency = if cfg.groupInFlight == 1 then Serial else Async cfg.groupInFlight
  unless
    ( cfg.groupScanLimit > 0
        && cfg.minimumIdleMillis > 0
        && cfg.groupRetryLimit >= 0
        && not (C8.null cfg.groupName || C8.null cfg.consumerName)
        && Just src.streamKey /= cfg.groupDeadLetters
        && not (groupStatePrefix `C8.isPrefixOf` src.streamKey)
        && not (any (groupStatePrefix `C8.isPrefixOf`) cfg.groupDeadLetters)
        && either (const False) (const True) (validateDeliveryPolicy GroupWorker Unordered concurrency)
    )
    (throwIO InvalidOptions)
  withRuntimeAttachment runtime $ do
    attachGroup runtime cfg
    scriptHash <- groupCommand runtime (Redis.scriptLoad attemptScript)
    active <- newTVarIO Map.empty
    let freeCapacity =
          atomically $
            ( do
                closed <- readTVar runtime.stopped
                check closed
                pure 0
            )
              `orElse` ( do
                           count <- Map.size <$> readTVar active
                           let available = cfg.groupInFlight - count
                           check (available > 0)
                           pure available
                       )

        pull state = do
          capacity <- freeCapacity
          if capacity == 0
            then pure Nothing
            else case state of
              Buffered [] cursor recoverNext -> pull (Poll cursor recoverNext)
              Buffered (entry : rest) cursor recoverNext -> do
                token <- getEntropy 24
                result <- attempt "register" entry.entryId token []
                case result of
                  [Redis.Bulk (Just "ok")] -> deliver entry (Just 0) token (Buffered rest cursor recoverNext)
                  _ | superseded result -> emit Superseded >> pull (Buffered rest cursor recoverNext)
                  _ -> throwIO InvalidStreamReply
              Poll cursor True -> do
                let from = if cursor == origin then "-" else "(" <> renderEntryId cursor
                reply <- groupCommand runtime $ Redis.sendRequest ["XPENDING", src.streamKey, cfg.groupName, from, "+", number cfg.groupScanLimit]
                pending <- traverse pendingRow reply
                when (cfg.samplePending && cursor == origin) (sample pending)
                recover origin pending
              Poll cursor False -> do
                let opts =
                      Redis.XReadGroupOpts
                        (Just (toInteger runtime.options.pollMillis))
                        (Just (toInteger (min capacity runtime.options.prefetchLimit)))
                        False
                    readNew = Redis.xreadGroupOpts cfg.groupName cfg.consumerName [(src.streamKey, ">")] opts
                fetched <- groupFailure (pollStream runtime readNew)
                case fetched of
                  Nothing -> pure Nothing
                  Just rows -> streamEntries src rows >>= \entries -> pull (Buffered entries cursor True)

        recover nextCursor [] = pull (Poll nextCursor False)
        recover _ (Pending entryId owner _ : remaining) = do
          held <-
            if owner == cfg.consumerName
              then any (\(Active position _) -> position == entryId) <$> readTVarIO active
              else pure False
          if held then recover entryId remaining else claim entryId remaining

        claim entryId remaining = do
          let arguments = [number cfg.minimumIdleMillis, ""]
          token <- getEntropy 24
          result <- attempt "claim" entryId token arguments
          case result of
            [Redis.Bulk (Just "claimed"), raw, Redis.Integer count] -> do
              record <- either (const (throwIO InvalidStreamReply)) pure (Redis.decode raw)
              entry <- streamRecord src record
              emit Reclaimed
              let countKnown = count > 0 && count < 9007199254740992 && count - 1 <= toInteger (maxBound :: Word)
              deliver entry (if countKnown then Just (Attempt (fromInteger (count - 1))) else Nothing) token (Poll entryId False)
            [Redis.Bulk (Just "missing")] -> do
              cfg.onMissingPending (MissingPending src cfg.groupName entryId)
              emit PendingPayloadMissing
              void (attempt "drop-missing" entryId token arguments)
              recover entryId remaining
            [Redis.Bulk (Just "skip")] -> recover entryId remaining
            _ | superseded result -> recover entryId remaining
            _ -> throwIO InvalidStreamReply

        deliver entry count token next = do
          valid <- newTVarIO True
          atomically $ do
            deliveries <- readTVar active
            let updated = Map.filter (\(Active position _) -> position /= entry.entryId) deliveries
            mapM_ (\(Active position status) -> when (position == entry.entryId) (writeTVar status False)) deliveries
            writeTVar active (Map.insert token (Active entry.entryId valid) updated)
          let retire = atomically $ do
                writeTVar valid False
                modifyTVar' active (Map.delete token)
              transition operation args event = do
                result <- attempt operation entry.entryId token args
                transitionEvent result event
              transitionEvent result event =
                case result of
                  [Redis.Bulk (Just "ok")] -> pure (Just event)
                  [Redis.Bulk (Just "repeat")] -> pure Nothing
                  [Redis.Bulk (Just "absent")] -> pure Nothing
                  [Redis.Bulk (Just "stale")] -> pure (Just Superseded)
                  _ -> throwIO InvalidStreamReply
              reject reason headers = do
                let (field, value) = rejectionRecord (ConsumerGroup cfg.groupName) entry reason headers
                transition "reject" [field, value] DeadLettered
              scheduleRetry delay headers = do
                let args = [milliseconds delay, number cfg.groupRetryLimit]
                result <- attempt "retry" entry.entryId token args
                case result of
                  [Redis.Bulk (Just "retry-limit")] -> case cfg.groupDeadLetters of
                    Nothing -> throwIO (GroupRetryLimit entry.entryId)
                    Just _ -> reject MaxRetriesExceeded headers
                  _ -> transitionEvent result Retried
              finalize headers decision = mask $ \restore -> do
                event <- restore $ case decision of
                  AckOk -> transition "ack" [] Acknowledged
                  AckRetry (RetryDelay delay) -> scheduleRetry delay headers
                  AckDeadLetter reason -> case cfg.groupDeadLetters of
                    Nothing -> throwIO (GroupRejectionWithoutDestination entry.entryId)
                    Just _ -> reject reason headers
                  AckHalt _ -> stopIntake runtime >> pure Nothing
                retire
                mapM_ emit event
              renew duration = do
                result <- attempt "renew" entry.entryId token [milliseconds duration]
                if superseded result
                  then atomically (writeTVar valid False) >> emit Superseded
                  else unless (result == [Redis.Bulk (Just "ok")]) (throwIO InvalidStreamReply)
          case decodeEnvelope cfg.groupDecode cfg.groupHeaders cfg.groupTraceContext entry of
            Left (DecodeFailure _ reason) -> finalize Nothing (AckDeadLetter reason) >> pull next
            Right envelope -> do
              emit Delivered
              let redisAttributes = case decodeUtf8' cfg.groupName of
                    Left _ -> envelope.attributes
                    Right group -> HashMap.insert "messaging.consumer.group.name" (toAttribute group) envelope.attributes
                  finish decision = do
                    headers <- case decision of
                      AckDeadLetter _ -> currentTraceHeaders
                      AckRetry _ | maybe True (>= Attempt (fromIntegral cfg.groupRetryLimit)) count -> currentTraceHeaders
                      _ -> pure Nothing
                    liftIO (finalize headers decision)
                  delivery = mkIngested (envelope {attempt = count, attributes = redisAttributes}) (AckHandle finish)
              pure $
                Just
                  ( delivery
                      { lease = Just (Lease envelope.messageId.unMessageId (liftIO . renew)),
                        deliveryStatus = liftIO $ do
                          current <- readTVarIO valid
                          pure (if current then DeliveryCurrent else DeliverySuperseded)
                      },
                    next
                  )

        attempt operation position token args = do
          let keys = [src.streamKey, groupAttemptKey src cfg.groupName position] <> maybe [] pure cfg.groupDeadLetters
              arguments = [operation, cfg.groupName, cfg.consumerName, renderEntryId position, token] <> args
          groupCommand runtime (Redis.evalsha scriptHash keys arguments)
            `catch` \err -> case err of
              CommandRejected (Redis.Error message)
                | "NOSCRIPT " `C8.isPrefixOf` message ->
                    groupCommand runtime (Redis.eval attemptScript keys arguments)
              _ -> throwIO err

        sample pending = do
          reply <- groupCommand runtime $ Redis.sendRequest ["XPENDING", src.streamKey, cfg.groupName]
          case reply of
            Redis.Integer depth : _ ->
              let age = case [idle | Pending _ _ idle <- pending] of
                    [] -> Nothing
                    ages -> Just (maximum ages)
               in emit (PendingSample depth age)
            _ -> throwIO InvalidStreamReply

    let source = Stream.unfoldrM (liftIO . pull) (Poll origin True)
    pure (Group (mkRedisAdapter runtime cfg.groupAdapterName cfg.telemetry source) concurrency)
  where
    src = cfg.groupSource
    emit = runtime.options.observe
    origin = EntryId 0 0

attachGroup :: RedisRuntime -> GroupConfig a -> IO ()
attachGroup runtime cfg =
  case cfg.groupStart of
    AttachExisting -> void $ groupCommand runtime pendingProbe
    CreateIfAbsent position -> create (renderEntryId position)
    CreateAtTailIfAbsent -> create "$"
  where
    key = cfg.groupSource.streamKey
    pendingProbe :: Redis.Redis (Either Redis.Reply [Redis.Reply])
    pendingProbe = Redis.sendRequest ["XPENDING", key, cfg.groupName, "-", "+", "1"]
    create position = do
      result <- retryConnection runtime $ runCommand runtime $ Redis.sendRequest ["XGROUP", "CREATE", key, cfg.groupName, position, "MKSTREAM"]
      case result of
        Redis.SingleLine "OK" -> pure ()
        Redis.Error message | "BUSYGROUP " `C8.isPrefixOf` message -> pure ()
        Redis.Error message -> throwIO (CommandRejected (Redis.Error message))
        _ -> throwIO InvalidStreamReply

groupCommand :: RedisRuntime -> Redis.Redis (Either Redis.Reply a) -> IO a
groupCommand runtime = groupFailure . retryConnection runtime . runCommand runtime

groupFailure :: IO a -> IO a
groupFailure action =
  action `catch` \err ->
    case err of
      CommandRejected (Redis.Error message) | "NOGROUP " `C8.isPrefixOf` message -> throwIO GroupUnavailable
      _ -> throwIO err

pendingRow :: [Redis.Reply] -> IO Pending
pendingRow [Redis.Bulk (Just rawId), Redis.Bulk (Just owner), Redis.Integer idle, Redis.Integer _]
  | Just position <- parseEntryId rawId = pure (Pending position owner idle)
pendingRow _ = throwIO InvalidStreamReply

superseded :: [Redis.Reply] -> Bool
superseded result = result == [Redis.Bulk (Just "stale")] || result == [Redis.Bulk (Just "absent")]

number :: (Show a) => a -> ByteString
number = C8.pack . show

-- Keep durations and absolute server time within Lua's exact integer range.
milliseconds :: (RealFrac a) => a -> ByteString
milliseconds duration = number (max 0 (min 315360000000 (ceiling (duration * 1000))) :: Integer)
