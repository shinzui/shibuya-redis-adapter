module Shibuya.Adapter.Redis.DeadLetter
  ( RejectionOwner (..),
    rejectionRecord,
    storeRejection,
  )
where

import Data.ByteString (ByteString)
import Data.Maybe (fromMaybe)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Database.Redis qualified as Redis
import Shibuya (DeadLetterReason, TraceHeaders, deadLetterCodeText, deadLetterReasonCode, deadLetterReasonDetail)
import Shibuya.Adapter.Redis.Types

-- | Namespace for the dead-letter hash field.
data RejectionOwner
  = -- | The reader’s logical checkpoint identity.
    LogicalReader !ByteString
  | -- | The Redis consumer group name shared by its workers.
    ConsumerGroup !ByteString
  deriving stock (Eq, Show)

-- | Encode the field and value for an idempotent Redis @HSETNX@.
--
-- Components have unsigned 64-bit big-endian byte lengths. The field
-- contains @redis-rejection-v1@, owner kind and name, then the framed message
-- identity. The value contains @redis-rejection-v1@, reason code, a detail
-- marker and optional UTF-8 detail of at most 512 characters, a trace marker
-- and optional W3C traceparent/tracestate, then alternating entry field names
-- and values. Pass no trace headers for rejections outside a consumer span.
rejectionRecord :: RejectionOwner -> RedisEntry -> DeadLetterReason -> Maybe TraceHeaders -> (ByteString, ByteString)
rejectionRecord owner entry reason headers = (key, value)
  where
    scope = case owner of
      LogicalReader name -> ["reader", name]
      ConsumerGroup name -> ["group", name]
    key = frame (["redis-rejection-v1"] <> scope <> [entryIdentity entry])
    code = Text.encodeUtf8 (deadLetterCodeText (deadLetterReasonCode reason))
    detail = maybe ["absent"] (\s -> ["present", Text.encodeUtf8 (Text.take 512 s)]) (deadLetterReasonDetail reason)
    trace = case headers >>= lookup "traceparent" of
      Nothing -> ["absent"]
      Just parent -> ["present", parent, fromMaybe "" (headers >>= lookup "tracestate")]
    value = frame (["redis-rejection-v1", code] <> detail <> trace <> concatMap (\(k, v) -> [k, v]) entry.fields)

storeRejection :: ByteString -> RejectionOwner -> RedisEntry -> DeadLetterReason -> Maybe TraceHeaders -> Redis.Redis (Either Redis.Reply Bool)
storeRejection destination owner entry reason headers =
  let (key, value) = rejectionRecord owner entry reason headers
   in Redis.hsetnx destination key value
