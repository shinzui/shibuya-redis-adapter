module Main where

import Data.ByteString.Builder qualified as Builder
import Data.ByteString.Lazy qualified as LBS
import Example
import Shibuya.Adapter.Redis
import System.Entropy (getEntropy)
import System.Environment (getArgs)

usage :: String
usage = "worker-example SOCKET STREAM GROUP (attach|earliest|tail) COUNT"

main :: IO ()
main =
  getArgs >>= \case
    ["--help"] -> putStrLn usage
    [socket, stream, group, positioning, countArg] -> do
      count <- parseCount countArg
      start <- case positioning of
        "attach" -> pure AttachExisting
        "earliest" -> pure (CreateIfAbsent (EntryId 0 0))
        "tail" -> pure CreateAtTailIfAbsent
        _ -> fail usage
      instanceId <- LBS.toStrict . Builder.toLazyByteString . Builder.byteStringHex <$> getEntropy 16
      let config = rawGroupConfig "worker-example" (Example.source stream) (bytes group) instanceId start
      withRedisRuntime (connection socket) defaultRuntimeOptions {prefetchLimit = 1} $ \runtime -> do
        worker <- newGroupWorker runtime config
        runExample count (stopIntake runtime) (groupProcessor worker)
    _ -> fail usage
