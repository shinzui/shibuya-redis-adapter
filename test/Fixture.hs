module Fixture (withRedis) where

import Control.Concurrent (threadDelay)
import Control.Exception (bracket)
import Control.Monad (unless, void)
import Database.Redis qualified as R
import System.Directory (doesPathExist)
import System.Environment (lookupEnv)
import System.FilePath ((</>))
import System.IO.Temp (withTempDirectory)
import System.Process

-- | The short socket path also works on Darwin. Only stop this fixture's child.
withRedis :: (R.ConnectInfo -> IO a) -> IO a
withRedis use = withTempDirectory "/tmp" "shibuya-redis" $ \dir -> do
  executable <- maybe "redis-server" id <$> lookupEnv "REDIS_SERVER"
  let socket = dir </> "redis.sock"
      info = R.defaultConnectInfo {R.connectAddr = R.ConnectAddrUnixSocket socket}
      args = ["--port", "0", "--unixsocket", socket, "--dir", dir, "--save", "", "--logfile", dir </> "redis.log"]
      launch = do
        (_, _, _, child) <- createProcess (proc executable args)
        pure child
      ready child = do
        getProcessExitCode child >>= mapM_ (fail . ("redis-server exited: " <>) . show)
        exists <- doesPathExist socket
        unless exists (threadDelay 10000 >> ready child)
  bracket launch (\child -> terminateProcess child >> void (waitForProcess child)) $ \child -> do
    ready child
    use info
