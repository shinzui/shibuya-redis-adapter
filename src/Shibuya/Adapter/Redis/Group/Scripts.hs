module Shibuya.Adapter.Redis.Group.Scripts (attemptScript) where

import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as C8

-- KEYS: stream, per-entry attempt state, optional rejection hash.
-- ARGV: operation, group, consumer, entry ID, token, operation arguments.
--
-- An attempt token is meaningful only while its consumer still owns the PEL
-- row. Pending metadata has no expiry. Completion removes it after XACK.
attemptScript :: ByteString
attemptScript =
  C8.unlines
    [ "local op, grp, consumer, id, token = unpack(ARGV)",
      "local stream, state, failures = unpack(KEYS)",
      "local row = redis.call('XPENDING', stream, grp, id, id, 1)[1]",
      "if not row then",
      "  if op == 'ack' or op == 'reject' or op == 'drop-missing' then",
      "    redis.call('DEL', state)",
      "  end",
      "  return {'absent'}",
      "end",
      "if row[4] >= 9007199254740991 then return redis.error_reply('ERR attempt count exhausted') end",
      "local generation = string.format('%.0f', row[4])",
      "local registered = redis.call('HGET', state, 'generation') == generation",
      "local current = redis.call('HGET', state, 'token')",
      "local owner = row[2] == consumer",
      "local function now()",
      "  local t = redis.call('TIME')",
      "  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)",
      "end",
      -- XREADGROUP and registration are separate. A later claim always consumes
      -- the first delivery generation, even if the consumer name returns.
      "if op == 'register' then",
      "  if owner and registered and current == token then return {'ok'} end",
      "  if not owner or row[4] ~= 1 or redis.call('EXISTS', state) ~= 0 then",
      "    return {'stale'}",
      "  end",
      "  redis.call('HSET', state, 'token', token, 'generation', generation)",
      "  return {'ok'}",
      "end",
      "if op == 'claim' or op == 'drop-missing' then",
      "  if op == 'claim' and owner and registered and current == token then",
      "    local record = redis.call('XRANGE', stream, id, id)[1]",
      "    if record then return {'claimed', record, row[4]} end",
      "  end",
      "  local eligibleAt = tonumber(redis.call('HGET', state, 'until') or '0')",
      "  if row[3] < tonumber(ARGV[6]) or eligibleAt > now() then return {'skip'} end",
      "  if owner and current and current == ARGV[7] then return {'skip'} end",
      "  local record = redis.call('XRANGE', stream, id, id)[1]",
      "  if not record then",
      "    if op == 'claim' then return {'missing'} end",
      "    redis.call('XACK', stream, grp, id)",
      "    redis.call('DEL', state)",
      "    return {'ok'}",
      "  end",
      "  if op == 'drop-missing' then return {'skip'} end",
      "  local claimed = redis.call('XCLAIM', stream, grp, consumer, 0, id)",
      "  if not claimed[1] then return {'skip'} end",
      "  redis.call('HSET', state, 'token', token, 'generation', string.format('%.0f', row[4] + 1))",
      "  redis.call('HDEL', state, 'until', 'retried')",
      "  return {'claimed', claimed[1], row[4] + 1}",
      "end",
      -- Repeating a committed retry must leave its original due time alone.
      "if op == 'retry' and owner and registered and redis.call('HGET', state, 'retried') == token then",
      "  return {'repeat'}",
      "end",
      "if not owner or not registered or current ~= token then return {'stale'} end",
      "if op == 'retry' then",
      "  if row[4] - 1 >= tonumber(ARGV[7]) then return {'retry-limit'} end",
      "  redis.call('HSET', state, 'until', now() + tonumber(ARGV[6]), 'retried', token, 'token', '')",
      "  return {'ok'}",
      "end",
      "if op == 'renew' then",
      "  if not redis.call('XRANGE', stream, id, id)[1] then return {'stale'} end",
      "  local duration = tonumber(ARGV[6])",
      "  local previous = tonumber(redis.call('HGET', state, 'until') or '0')",
      "  redis.call('XCLAIM', stream, grp, consumer, 0, id, 'JUSTID')",
      "  redis.call('HSET', state, 'until', math.max(previous, now() + duration))",
      "  return {'ok'}",
      "end",
      "if op == 'reject' then",
      -- HSETNX validates the destination type before progress changes. If it
      -- succeeds and XACK errors, the next call can repeat the same write.
      "  redis.call('HSETNX', failures, ARGV[6], ARGV[7])",
      "end",
      "redis.call('XACK', stream, grp, id)",
      "redis.call('DEL', state)",
      "return {'ok'}"
    ]
