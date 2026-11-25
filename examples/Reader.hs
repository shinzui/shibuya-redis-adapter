module Main (main) where

import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as C8
import Example
import Shibuya.Adapter.Redis
import System.Directory (doesFileExist, renameFile)
import System.Environment (getArgs)
import System.FilePath (takeDirectory)
import System.IO (hClose)
import System.IO.Temp (withTempFile)

usage :: String
usage = "reader-example SOCKET STREAM CHECKPOINT_FILE COUNT"

main :: IO ()
main =
  getArgs >>= \case
    ["--help"] -> putStrLn usage
    [socket, stream, file, countArg] -> do
      count <- parseCount countArg
      let src = Example.source stream
          config =
            (rawReaderConfig "reader-example" "projection" src Earliest)
              { checkpointStore = Just (fileStore file)
              }
      withRedisRuntime (connection socket) defaultRuntimeOptions {prefetchLimit = 1} $ \runtime -> do
        reader <- newReader runtime config
        runExample count (stopIntake runtime) (readerProcessor reader)
    _ -> fail usage

-- One process owns this file. Atomically replacing it keeps the cursor usable
-- after an interrupted write. The demo uses no producer continuity evidence.
fileStore :: FilePath -> CheckpointStore
fileStore file = CheckpointStore load save
  where
    scope name identity = C8.pack (show (name, identity))
    load name identity = do
      exists <- doesFileExist file
      if exists
        then do
          content <- C8.lines <$> BS.readFile file
          case content of
            [version, savedScope, rawId]
              | version == "shibuya-example-v1",
                savedScope == scope name identity,
                Just position <- parseEntryId rawId ->
                  pure (Just (Checkpoint position Nothing))
            _ -> fail "checkpoint file is malformed or belongs to a different reader"
        else pure Nothing
    save name identity _ checkpoint =
      withTempFile (takeDirectory file) ".checkpoint" $ \temporary handle -> do
        BS.hPut handle (C8.unlines ["shibuya-example-v1", scope name identity, renderEntryId checkpoint.position])
        hClose handle
        renameFile temporary file
