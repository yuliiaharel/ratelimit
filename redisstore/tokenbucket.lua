-- Atomic token bucket check-and-consume.
--
-- KEYS[1] = bucket key
-- ARGV[1] = rate   (tokens added per second)
-- ARGV[2] = burst  (bucket capacity)
-- ARGV[3] = cost   (tokens this request consumes)
-- ARGV[4] = ttl    (seconds; how long to keep an idle key around)
-- ARGV[5] = floor  (minimum tokens this request's priority must leave
--                   behind; 0 means it may drain the bucket fully)
--
-- floor is how lower-priority traffic gets shed before higher-priority
-- traffic: a Critical request's floor is 0 (the whole bucket is its to
-- spend), while Default and Sheddable carry a positive floor, so once
-- the bucket drains to that point, only Critical (and whichever other
-- priority's floor is already below it) keeps getting through.
--
-- Returns { allowed (0/1), tokens_remaining, retry_after_seconds, burst }.
-- tokens_remaining and retry_after_seconds come back as strings:
-- returning a Lua number directly truncates it to an integer on the
-- way out (Redis's number reply is an integer), which would throw away
-- their fractional part.
--
-- Redis runs a Lua script to completion before serving any other
-- command, so this whole read-refill-check-write sequence is atomic:
-- two clients racing for the last token can't both win the way they
-- could with separate GET/SET calls.
--
-- Using Redis's own TIME instead of a timestamp the caller computed
-- means every app server agrees on "now", regardless of clock skew
-- between them.

local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local floor = tonumber(ARGV[5])

local time = redis.call('TIME')
local now = tonumber(time[1]) + tonumber(time[2]) / 1e6

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens
local last
if data[1] == false then
  -- Fresh key: start full, so it can burst immediately.
  tokens = burst
  last = now
else
  tokens = tonumber(data[1])
  last = tonumber(data[2])
end

local elapsed = math.max(0, now - last)
tokens = math.min(burst, tokens + elapsed * rate)

local allowed
local retry_after
if (tokens - cost) >= floor then
  tokens = tokens - cost
  allowed = 1
  retry_after = 0
else
  -- A denied request consumes nothing.
  allowed = 0
  local deficit = cost + floor - tokens
  if deficit < 0 then deficit = 0 end
  retry_after = deficit / rate
end

redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', key, ttl)

return { allowed, tostring(tokens), tostring(retry_after), burst }
