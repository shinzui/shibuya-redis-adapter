module Fixture (withRedis, withRedisRestarts) where

import Control.Concurrent (threadDelay)
import Control.Exception (bracket)
import Control.Monad (unless, void, when)
import Database.Redis qualified as R
import System.Directory (doesPathExist)
import System.Environment (lookupEnv)
import System.FilePath ((</>))
import System.IO.Temp (withTempDirectory)
import System.Process

withRedis :: (R.ConnectInfo -> IO a) -> IO a
withRedis use = withRedisDirectory [] (\start -> start (\info _ -> use info))

-- | Start another server on the same socket and data after stopping the
-- preceding child. The caller can keep adapter connections across restarts.
withRedisRestarts :: ((forall b. (R.ConnectInfo -> IO () -> IO b) -> IO b) -> IO a) -> IO a
withRedisRestarts = withRedisDirectory ["--appendonly", "yes", "--appendfsync", "always"]

withRedisDirectory :: [String] -> ((forall b. (R.ConnectInfo -> IO () -> IO b) -> IO b) -> IO a) -> IO a
withRedisDirectory persistence use = withTempDirectory "/tmp" "shibuya-redis" $ \dir -> do
  executable <- maybe "redis-server" id <$> lookupEnv "REDIS_SERVER"
  let socket = dir </> "redis.sock"
      info = R.defaultConnectInfo {R.connectAddr = R.ConnectAddrUnixSocket socket}
      args = ["--port", "0", "--unixsocket", socket, "--dir", dir, "--save", "", "--logfile", dir </> "redis.log"] <> persistence
      launch = do
        (_, _, _, child) <- createProcess (proc executable args)
        pure child
      ready child = do
        getProcessExitCode child >>= mapM_ (fail . ("redis-server exited: " <>) . show)
        exists <- doesPathExist socket
        unless exists (threadDelay 10000 >> ready child)
      stop child = do
        status <- getProcessExitCode child
        when (status == Nothing) (terminateProcess child >> void (waitForProcess child))
      start :: (R.ConnectInfo -> IO () -> IO b) -> IO b
      start action = bracket launch stop $ \child -> do
        ready child
        action info (stop child)
  use start
