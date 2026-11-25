-- | __Internal module.__ This module exposes implementation details for tests
-- and advanced use. Its exports may change or disappear in any release.
module Shibuya.Adapter.Redis.Internal
  ( runCommand,
    pollStream,
    readBatchOptions,
    retryConnection,
    streamEntries,
    streamRecord,
    mkRedisAdapter,
    DeliveryMode (..),
    validateDeliveryPolicy,
    storeRejection,
    entryEnvelope,
    decodeEnvelope,
    DecodeFailure (..),
    frame,
    entryIdentity,
    groupAttemptKey,
    groupStatePrefix,
    attemptScript,
  )
where

import Shibuya.Adapter.Redis.Adapter
import Shibuya.Adapter.Redis.Connection
import Shibuya.Adapter.Redis.DeadLetter
import Shibuya.Adapter.Redis.Group
import Shibuya.Adapter.Redis.Group.Scripts
import Shibuya.Adapter.Redis.Types
