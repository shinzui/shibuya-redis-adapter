module Shibuya.Adapter.Redis.Group where

import Control.Concurrent.STM
import Control.Exception (Exception, catch, mask, throwIO)
import Control.Monad (unless, void, when)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as C8
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Database.Redis qualified as Redis
import Effectful (IOE, liftIO, (:>))
import Shibuya
import Shibuya.Adapter.Redis.Adapter
import Shibuya.Adapter.Redis.Connection
import Shibuya.Adapter.Redis.DeadLetter
import Shibuya.Adapter.Redis.Group.Scripts
import Shibuya.Adapter.Redis.Types
import Streamly.Data.Stream qualified as Stream
import System.Entropy (getEntropy)

data GroupStart
  = AttachExisting
  | CreateIfAbsent !EntryId
  | CreateAtTailIfAbsent
  deriving stock (Eq, Show)

data MissingPending = MissingPending
  { missingSource :: !SourceIdentity,
    missingGroup :: !ByteString,
    missingEntry :: !EntryId
  }
  deriving stock (Eq, Show)

data GroupError
  = GroupUnavailable
  | GroupRejectionWithoutDestination !EntryId
  | GroupRetryLimit !EntryId
  deriving stock (Show)
  deriving anyclass (Exception)

data GroupConfig a = GroupConfig
  { groupAdapterName :: !Text,
    groupSource :: !SourceIdentity,
    groupName :: !ByteString,
    -- | Identify this running instance uniquely, including across restarts.
    consumerName :: !ByteString,
    groupStart :: !GroupStart,
    groupInFlight :: !Int,
    groupScanLimit :: !Int,
    minimumIdleMillis :: !Int,
    groupRetryLimit :: !Int,
    groupDeadLetters :: !(Maybe ByteString),
    groupDecode :: RedisEntry -> Either DeadLetterReason a,
    groupHeaders :: RedisEntry -> Maybe Headers,
    -- | Called before removing a pending ID whose payload has disappeared.
    onMissingPending :: MissingPending -> IO (),
    -- | Sample total PEL depth on scan wrap and highest idle time in the page.
    samplePending :: !Bool
  }

rawGroupConfig :: Text -> SourceIdentity -> ByteString -> ByteString -> GroupStart -> GroupConfig RedisEntry
rawGroupConfig name src group worker start =
  GroupConfig name src group worker start 1 32 30000 10 Nothing Right (const Nothing) (const (pure ())) False

data Group es a = Group
  { groupAdapter :: !(Adapter es a),
    configuredConcurrency :: !Concurrency
  }

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

newGroupWorker :: (IOE :> es) => RedisRuntime -> GroupConfig a -> IO (Group es a)
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
  attachGroup runtime cfg
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
        -- Suppress self-claim while our local delivery is still outstanding.
        -- A different worker is free to take over at the configured deadline.
        held <- Map.toList <$> readTVarIO active
        let localToken = case [token | (token, Active position _) <- held, position == entryId] of
              token : _ | owner == cfg.consumerName -> token
              _ -> ""
            arguments = [number cfg.minimumIdleMillis, localToken]
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
              case result of
                [Redis.Bulk (Just "retry-limit")] -> throwIO (GroupRetryLimit entry.entryId)
                [Redis.Bulk (Just "ok")] -> pure (Just event)
                [Redis.Bulk (Just "repeat")] -> pure Nothing
                [Redis.Bulk (Just "absent")] -> pure Nothing
                [Redis.Bulk (Just "stale")] -> pure (Just Superseded)
                _ -> throwIO InvalidStreamReply
            finalize decision = mask $ \restore -> do
              event <- restore $ case decision of
                AckOk -> transition "ack" [] Acknowledged
                AckRetry (RetryDelay delay) -> transition "retry" [milliseconds delay, number cfg.groupRetryLimit] Retried
                AckDeadLetter reason -> case cfg.groupDeadLetters of
                  Nothing -> throwIO (GroupRejectionWithoutDestination entry.entryId)
                  Just _ -> do
                    let (field, value) = rejectionRecord (ConsumerGroup cfg.groupName) entry reason
                    transition "reject" [field, value] DeadLettered
                AckHalt _ -> stopIntake runtime >> pure Nothing
              retire
              mapM_ emit event
            renew duration = do
              result <- attempt "renew" entry.entryId token [milliseconds duration]
              if superseded result
                then emit Superseded
                else unless (result == [Redis.Bulk (Just "ok")]) (throwIO InvalidStreamReply)
        case decodeEnvelope cfg.groupDecode cfg.groupHeaders entry of
          Left (DecodeFailure _ reason) -> finalize (AckDeadLetter reason) >> pull next
          Right envelope -> do
            emit Delivered
            let delivery = mkIngested (envelope {attempt = count}) (AckHandle (liftIO . finalize))
            pure $
              Just
                ( delivery
                    { lease = Just (Lease (messageIdText envelope.messageId) (liftIO . renew)),
                      deliveryStatus = liftIO $ do
                        current <- readTVarIO valid
                        pure (if current then DeliveryCurrent else DeliverySuperseded)
                    },
                  next
                )

      attempt operation position token args = do
        let keys = [src.streamKey, groupAttemptKey src cfg.groupName position] <> maybe [] pure cfg.groupDeadLetters
            arguments = [operation, cfg.groupName, cfg.consumerName, renderEntryId position, token] <> args
        groupCommand runtime $ Redis.sendRequest (["EVAL", attemptScript, number (length keys)] <> keys <> arguments)

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
  pure (Group (mkRedisAdapter runtime cfg.groupAdapterName source) concurrency)
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

messageIdText :: MessageId -> Text
messageIdText (MessageId value) = value

number :: (Show a) => a -> ByteString
number = C8.pack . show

-- Keep durations and absolute server time within Lua's exact integer range.
milliseconds :: (RealFrac a) => a -> ByteString
milliseconds duration = number (max 0 (min 315360000000 (ceiling (duration * 1000))) :: Integer)
