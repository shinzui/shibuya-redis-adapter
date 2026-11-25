module Shibuya.Adapter.Redis.DeadLetter where

import Data.ByteString (ByteString)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Database.Redis qualified as Redis
import Shibuya (DeadLetterReason, deadLetterCodeText, deadLetterReasonCode, deadLetterReasonDetail)
import Shibuya.Adapter.Redis.Types

data RejectionOwner = LogicalReader !ByteString | ConsumerGroup !ByteString deriving stock (Eq, Show)

-- | Hash field and value. The same bytes can be passed to HSETNX in a fenced script.
rejectionRecord :: RejectionOwner -> RedisEntry -> DeadLetterReason -> (ByteString, ByteString)
rejectionRecord owner entry reason = (key, value)
  where
    scope = case owner of
      LogicalReader name -> ["reader", name]
      ConsumerGroup name -> ["group", name]
    key = frame (["redis-rejection-v1"] <> scope <> [entryIdentity entry])
    code = Text.encodeUtf8 (deadLetterCodeText (deadLetterReasonCode reason))
    detail = maybe ["absent"] (\s -> ["present", Text.encodeUtf8 (Text.take 512 s)]) (deadLetterReasonDetail reason)
    value = frame (["redis-rejection-v1", code] <> detail <> concatMap (\(k, v) -> [k, v]) entry.fields)

storeRejection :: ByteString -> RejectionOwner -> RedisEntry -> DeadLetterReason -> Redis.Redis (Either Redis.Reply Bool)
storeRejection destination owner entry reason =
  let (key, value) = rejectionRecord owner entry reason
   in Redis.hsetnx destination key value
