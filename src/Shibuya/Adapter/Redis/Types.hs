-- | Binary stream entries and their Shibuya envelope representation.
module Shibuya.Adapter.Redis.Types where

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Builder qualified as Builder
import Data.ByteString.Char8 qualified as C8
import Data.ByteString.Lazy qualified as LBS
import Data.Text.Encoding qualified as Text
import Data.Word (Word64)
import GHC.Generics (Generic)
import Shibuya (Cursor (..), DeadLetterReason, Envelope (..), Headers, MessageId (..), mkEnvelope)

data EntryId = EntryId !Word64 !Word64 deriving stock (Eq, Ord, Show, Generic)

renderEntryId :: EntryId -> ByteString
renderEntryId (EntryId ms seqNo) = C8.pack (show ms <> "-" <> show seqNo)

parseEntryId :: ByteString -> Maybe EntryId
parseEntryId input = case C8.split '-' input of
  [ms, seqNo] -> EntryId <$> component ms <*> component seqNo
  _ -> Nothing
  where
    component s = do
      (n, rest) <- C8.readInteger s
      if BS.null rest && n >= 0 && n <= toInteger (maxBound :: Word64) && C8.pack (show n) == s
        then Just (fromInteger n)
        else Nothing

data SourceIdentity = SourceIdentity
  { namespace :: !ByteString,
    generation :: !(Maybe ByteString),
    streamKey :: !ByteString
  }
  deriving stock (Eq, Show, Generic)

data RedisEntry = RedisEntry
  { sourceIdentity :: !SourceIdentity,
    entryId :: !EntryId,
    fields :: ![(ByteString, ByteString)]
  }
  deriving stock (Eq, Show, Generic)

-- | Components have unsigned 64-bit big-endian byte lengths.
frame :: [ByteString] -> ByteString
frame = LBS.toStrict . Builder.toLazyByteString . foldMap component
  where
    component bytes = Builder.word64BE (fromIntegral (BS.length bytes)) <> Builder.byteString bytes

entryIdentity :: RedisEntry -> ByteString
entryIdentity entry =
  frame $
    ["redis-stream-v1", src.namespace]
      <> maybe ["absent"] (\g -> ["present", g]) src.generation
      <> [src.streamKey, renderEntryId entry.entryId]
  where
    src = entry.sourceIdentity

entryEnvelope :: RedisEntry -> Envelope RedisEntry
entryEnvelope entry =
  (mkEnvelope (MessageId (Text.decodeUtf8 hexId)) entry)
    { cursor = Just (CursorText (Text.decodeUtf8 (renderEntryId entry.entryId)))
    }
  where
    hexId = LBS.toStrict (Builder.toLazyByteString (Builder.byteStringHex (entryIdentity entry)))

data DecodeFailure = DecodeFailure !RedisEntry !DeadLetterReason deriving stock (Eq, Show)

decodeEnvelope :: (RedisEntry -> Either DeadLetterReason a) -> (RedisEntry -> Maybe Headers) -> RedisEntry -> Either DecodeFailure (Envelope a)
decodeEnvelope decode selectHeaders entry = case decode entry of
  Left reason -> Left (DecodeFailure entry reason)
  Right value -> Right ((value <$ entryEnvelope entry) {headers = selectHeaders entry})

data AdapterEvent
  = Delivered
  | Acknowledged
  | Retried
  | Reclaimed
  | Superseded
  | DeadLettered
  | CheckpointFailed
  | HistoryGap
  | PendingPayloadMissing
  | PendingSample !Integer !(Maybe Integer)
  deriving stock (Eq, Show)

groupStatePrefix :: ByteString
groupStatePrefix = "shibuya:group:v1:"
