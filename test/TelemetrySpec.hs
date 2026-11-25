module Main (main) where

import Control.Concurrent (newEmptyMVar, putMVar, readMVar)
import Control.Exception (bracket)
import Control.Monad (unless, void)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as C8
import Data.Foldable (toList)
import Data.IORef (readIORef)
import Data.Maybe (isNothing)
import Data.Text (Text)
import Data.Text.Encoding (decodeUtf8)
import Data.Text.IO qualified as Text
import Database.Redis qualified as Redis
import Effectful (IOE, liftIO, runEff)
import Effectful.Exception qualified as Exception
import Fixture (withRedis)
import OpenTelemetry.Attributes (emptyAttributes, lookupAttribute, toAttribute)
import OpenTelemetry.Exporter.InMemory (inMemoryListExporter)
import OpenTelemetry.Trace qualified as SDK
import OpenTelemetry.Trace.Core qualified as OTel
import OpenTelemetry.Trace.Id (Base (Base16), spanIdBaseEncodedText, traceIdBaseEncodedText)
import OpenTelemetry.Trace.Id.Generator (IdGenerator (DefaultIdGenerator))
import OpenTelemetry.Util (appendOnlyBoundedCollectionValues)
import Shibuya
import Shibuya.Adapter.Redis
import Shibuya.Telemetry.Propagation (injectTraceContext)
import Shibuya.Telemetry.Semantic (processSpanName)
import System.Environment (getArgs)
import System.Timeout (timeout)

type Effects = '[Tracing, IOE]

assert :: String -> Bool -> IO ()
assert label condition = unless condition (fail label)

main :: IO ()
main = do
  arguments <- getArgs
  completed <- timeout 15000000 $ withRedis $ \info ->
    bracket (Redis.connect info) Redis.disconnect $ \admin ->
      case arguments of
        [] -> verifyInMemory info admin
        ["--jaeger"] -> exportToJaeger info admin
        _ -> fail "expected no arguments or --jaeger"
  assert "telemetry integration timed out" (completed == Just ())

verifyInMemory :: Redis.ConnectInfo -> Redis.Connection -> IO ()
verifyInMemory info admin = do
  let fields = [("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"), ("tracestate", "acme=test")]
  (processor, spansRef) <- inMemoryListExporter
  bracket
    (OTel.createTracerProvider [processor] OTel.emptyTracerProviderOptions {OTel.tracerProviderOptionsIdGenerator = DefaultIdGenerator})
    shutdown
    $ \provider -> do
      let tracer = makeTracer provider
      processEntry info admin (Just tracer) "reader" fields reader (AckDeadLetter MaxRetriesExceeded)
      processEntry info admin (Just tracer) "worker" fields worker (AckRetry (RetryDelay 0))
      processEntry info admin (Just tracer) "untraced" [] reader AckOk
      processEntry info admin Nothing "noop" fields reader (AckDeadLetter MaxRetriesExceeded)
  spans <- readIORef spansRef
  let producer = ("0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331")
      inspect name = do
        named <- traverse (\sp -> (sp,) <$> readIORef (OTel.spanHot sp)) [sp | sp <- spans, OTel.spanKind sp == OTel.Consumer]
        case [(sp, hot) | (sp, hot) <- named, OTel.hotName hot == processSpanName name] of
          [(sp, hot)] -> pure (sp, hot)
          _ -> fail ("expected one consumer span for " <> show name)
      links hot = map (contextIds . OTel.frozenLinkContext) (toList (appendOnlyBoundedCollectionValues (OTel.hotLinks hot)))
      attr key hot = lookupAttribute (OTel.hotAttributes hot) key
      redisAttributes processorName hot = do
        assert "span names Redis as the messaging system" (attr "messaging.system" hot == Just (toAttribute ("redis" :: Text)))
        assert "messaging ID uses the native Redis ID without the stream key" (attr "messaging.message.id" hot == Just (toAttribute ("1-0" :: Text)))
        assert "span carries the Redis entry ID" (attr "shibuya.redis.entry_id" hot == Just (toAttribute ("1-0" :: Text)))
        assert "span destination stays the processor name" (attr "messaging.destination.name" hot == Just (toAttribute (processorName :: Text)))
  (readerSpan, readerHot) <- inspect "reader"
  assert "reader starts a linked root" $
    fst (contextIds (OTel.spanContext readerSpan)) /= fst producer
      && isNothing (OTel.spanParent readerSpan)
      && links readerHot == [producer]
  redisAttributes "reader" readerHot
  assertRejectionTrace admin "reader" readerSpan
  (workerSpan, workerHot) <- inspect "worker"
  parent <- traverse OTel.getSpanContext (OTel.spanParent workerSpan)
  assert "worker uses the producer as parent and link" $
    fst (contextIds (OTel.spanContext workerSpan)) == fst producer
      && fmap contextIds parent == Just producer
      && links workerHot == [producer]
  redisAttributes "worker" workerHot
  assertRejectionTrace admin "worker" workerSpan
  assert "worker span identifies the UTF-8 group" (attr "messaging.consumer.group.name" workerHot == Just (toAttribute ("jobs" :: Text)))
  (_, untracedHot) <- inspect "untraced"
  assert "untraced entries have no link" (null (links untracedHot))
  noopValue <- rejectionValue admin "noop"
  let noopEntry = RedisEntry (SourceIdentity "telemetry" Nothing (streamKeyFor "noop")) (EntryId 1 0) (("value", "test") : fields)
  assert
    "disabled tracing stores the rejection without a consumer context"
    (noopValue == snd (rejectionRecord (LogicalReader "projection") noopEntry MaxRetriesExceeded Nothing))
  putStrLn "Redis telemetry correlation, span attributes and dead-letter context passed"

rejectionValue :: Redis.Connection -> ByteString -> IO ByteString
rejectionValue admin name = do
  result <- Redis.runRedis admin (Redis.sendRequest ["HVALS", name <> "-rejections"])
  case result of
    Right [value] -> pure value
    _ -> fail ("expected one rejection for " <> show name)

assertRejectionTrace :: Redis.Connection -> ByteString -> OTel.ImmutableSpan -> IO ()
assertRejectionTrace admin name sp = do
  value <- rejectionValue admin name
  headers <- injectTraceContext (OTel.wrapSpanContext (OTel.spanContext sp))
  assert "dead letter carries the consumer traceparent" $
    maybe False (`C8.isInfixOf` value) (lookup "traceparent" headers)

exportToJaeger :: Redis.ConnectInfo -> Redis.Connection -> IO ()
exportToJaeger info admin = bracket SDK.initializeGlobalTracerProvider shutdown $ \provider -> do
  let tracer = makeTracer provider
  (headers, trace, parent) <- OTel.inSpan' tracer "redis qualification producer" OTel.defaultSpanArguments {OTel.kind = OTel.Producer} $ \producerSpan -> do
    headers <- injectTraceContext producerSpan
    (trace, parent) <- contextIds <$> OTel.getSpanContext producerSpan
    pure (headers, trace, parent)
  assert "OTLP smoke test generated a sampled producer context" (not (null headers))
  processEntry info admin (Just tracer) "jaeger-worker" headers worker AckOk
  processEntry info admin (Just tracer) "jaeger-reader" headers reader AckOk
  flushed <- OTel.forceFlushTracerProvider provider (Just 5_000_000)
  case flushed of
    OTel.FlushSuccess -> pure ()
    _ -> fail "OTLP trace flush failed"
  Text.putStrLn ("Jaeger producer trace " <> trace <> " span " <> parent)

shutdown :: OTel.TracerProvider -> IO ()
shutdown provider = void (OTel.shutdownTracerProvider provider (Just 5_000_000))

makeTracer :: OTel.TracerProvider -> OTel.Tracer
makeTracer provider = OTel.makeTracer provider (OTel.InstrumentationLibrary "shibuya-redis-qualification" "" "" emptyAttributes) OTel.tracerOptions

contextIds :: OTel.SpanContext -> (Text, Text)
contextIds ctx = (traceIdBaseEncodedText Base16 ctx.traceId, spanIdBaseEncodedText Base16 ctx.spanId)

reader :: ByteString -> RedisRuntime -> Handler Effects RedisEntry -> IO (QueueProcessor Effects)
reader key runtime handler = do
  source <- newReader runtime ((rawReaderConfig "reader" "projection" (SourceIdentity "telemetry" Nothing (streamKeyFor key)) Earliest) {readerDeadLetters = Just (key <> "-rejections")})
  pure (readerProcessor source handler)

worker :: ByteString -> RedisRuntime -> Handler Effects RedisEntry -> IO (QueueProcessor Effects)
worker key runtime handler = do
  group <- newGroupWorker runtime ((rawGroupConfig "worker" (SourceIdentity "telemetry" Nothing (streamKeyFor key)) "jobs" "solo" (CreateIfAbsent (EntryId 0 0))) {groupDeadLetters = Just (key <> "-rejections"), groupRetryLimit = 0})
  pure (groupProcessor group handler)

processEntry ::
  Redis.ConnectInfo ->
  Redis.Connection ->
  Maybe OTel.Tracer ->
  ByteString ->
  TraceHeaders ->
  (ByteString -> RedisRuntime -> Handler Effects RedisEntry -> IO (QueueProcessor Effects)) ->
  AckDecision ->
  IO ()
processEntry info admin tracer name headers create decision = do
  _ <- Redis.runRedis admin (Redis.xadd (streamKeyFor name) "1-0" (("value", "test") : headers)) >>= either (fail . show) pure
  handled <- newEmptyMVar
  withRedisRuntime (OwnPools info) defaultRuntimeOptions $ \runtime -> do
    processor <- create name runtime (const (liftIO (putMVar handled ()) >> pure decision))
    runEff $ maybe runTracingNoop runTracing tracer $ do
      app <- runApp defaultAppConfig [(ProcessorId (decodeUtf8 name), processor)] >>= either (liftIO . fail . show) pure
      flip Exception.finally (stopApp app) $ do
        liftIO (readMVar handled)
        stopAppGracefully (ShutdownConfig 1 2) app >>= liftIO . assert "telemetry processor drains"

streamKeyFor :: ByteString -> ByteString
streamKeyFor name = "telemetry-input:" <> name
