-- Atomic multi-key token bucket check-and-consume, each key with its
-- own policy: every key must have enough tokens (after accounting for
-- its own rate/burst and its request's priority floor), or none of
-- them are charged.
--
-- Unlike tokenbucketbatch.lua (one shared rate/burst/ttl), this is for
-- a batch that spans keys governed by different domains' policies --
-- so every parameter is per-key, not shared.
--
-- KEYS[1..N] = bucket keys
-- For each i in 1..N, five consecutive ARGV entries starting at 5*(i-1):
--   cost  for KEYS[i]
--   rate  for KEYS[i]
--   burst for KEYS[i]
--   floor for KEYS[i] (see tokenbucket.lua for what floor means)
--   ttl   for KEYS[i]
--
-- Returns a flat array of N groups of
-- { allowed (0/1), tokens_remaining, retry_after_seconds }
-- (see tokenbucket.lua for why the last two come back as strings).

local n = #KEYS

local time = redis.call('TIME')
local now = tonumber(time[1]) + tonumber(time[2]) / 1e6

local tokens = {}
local costs = {}
local rates = {}
local floors = {}
local ttls = {}
local all_ok = true

for i = 1, n do
  local base = 5 * (i - 1)
  local cost = tonumber(ARGV[base + 1])
  local rate = tonumber(ARGV[base + 2])
  local burst = tonumber(ARGV[base + 3])
  local floor = tonumber(ARGV[base + 4])
  local ttl = tonumber(ARGV[base + 5])
  costs[i] = cost
  rates[i] = rate
  floors[i] = floor
  ttls[i] = ttl

  local key = KEYS[i]
  local data = redis.call('HMGET', key, 'tokens', 'ts')
  local tok, last
  if data[1] == false then
    tok = burst
    last = now
  else
    tok = tonumber(data[1])
    last = tonumber(data[2])
  end
  tok = math.min(burst, tok + math.max(0, now - last) * rate)
  tokens[i] = tok

  if (tok - cost) < floor then
    all_ok = false
  end
end

local out = {}
for i = 1, n do
  local key = KEYS[i]
  local tok = tokens[i]
  local cost = costs[i]
  local floor = floors[i]
  local allowed = 0
  local retry_after = 0

  if all_ok then
    tok = tok - cost
    allowed = 1
  elseif (tok - cost) < floor then
    local deficit = cost + floor - tok
    if deficit < 0 then deficit = 0 end
    retry_after = deficit / rates[i]
  end
  -- else: this key alone had enough, but a sibling check failed the
  -- batch -- see tokenbucketbatch.lua for why allowed/retry_after stay
  -- 0 in that case.

  redis.call('HMSET', key, 'tokens', tok, 'ts', now)
  redis.call('EXPIRE', key, ttls[i])

  table.insert(out, allowed)
  table.insert(out, tostring(tok))
  table.insert(out, tostring(retry_after))
end

return out
