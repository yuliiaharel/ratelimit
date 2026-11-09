-- Atomic multi-key token bucket check-and-consume: every key must have
-- enough tokens, or none of them are charged.
--
-- KEYS[1..N]   = bucket keys
-- ARGV[1]      = rate  (tokens added per second, shared by every key)
-- ARGV[2]      = burst (bucket capacity, shared by every key)
-- ARGV[3]      = ttl   (seconds)
-- ARGV[4..N+3] = cost for KEYS[1..N], same order
--
-- Returns a flat array of N groups of
-- { allowed (0/1), tokens_remaining, retry_after_seconds }
-- (see tokenbucket.lua for why the last two come back as strings).
--
-- Refill always happens and is saved -- it reflects real elapsed time,
-- same as if each key had been checked alone. Only the deduction
-- (actually spending tokens) is conditional on every key in the batch
-- having enough, which is what "all pass or none do" means in practice.

local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local n = #KEYS

local time = redis.call('TIME')
local now = tonumber(time[1]) + tonumber(time[2]) / 1e6

local tokens = {}
local costs = {}
local all_ok = true

for i = 1, n do
  local key = KEYS[i]
  local cost = tonumber(ARGV[3 + i])
  costs[i] = cost

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

  if tok < cost then
    all_ok = false
  end
end

local out = {}
for i = 1, n do
  local key = KEYS[i]
  local tok = tokens[i]
  local cost = costs[i]
  local allowed = 0
  local retry_after = 0

  if all_ok then
    tok = tok - cost
    allowed = 1
  elseif tok < cost then
    retry_after = (cost - tok) / rate
  end
  -- else: this key alone had enough, but a sibling check failed the
  -- batch, so allowed stays 0 and retry_after stays 0 -- it's not
  -- *this* key's shortfall; the caller should look at the other
  -- results to see which one was.

  redis.call('HMSET', key, 'tokens', tok, 'ts', now)
  redis.call('EXPIRE', key, ttl)

  table.insert(out, allowed)
  table.insert(out, tostring(tok))
  table.insert(out, tostring(retry_after))
end

return out
