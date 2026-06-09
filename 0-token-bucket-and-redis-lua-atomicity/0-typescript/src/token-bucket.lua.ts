// The token bucket Lua script — the atomic core of the lesson.
// It is byte-for-byte identical across all four language implementations;
// only the host client that loads and invokes it differs.
//
// KEYS[1] = bucket key, e.g. "tb:user-42"
// ARGV[1] = capacity        (max tokens the bucket can hold)
// ARGV[2] = refillPerSec    (tokens added per second)
// ARGV[3] = nowMs           (caller's current time in milliseconds)
// ARGV[4] = want            (tokens this call wants to consume)
//
// Returns a 3-element array: { allowed (0|1), remaining (int), retryAfterMs (int) }.
export const TOKEN_BUCKET_LUA = `
local capacity       = tonumber(ARGV[1])
local refill_per_sec = tonumber(ARGV[2])
local now_ms         = tonumber(ARGV[3])
local want           = tonumber(ARGV[4])

-- Read current bucket state (tokens + last refill timestamp) in one HMGET.
local data    = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens  = tonumber(data[1])
local last_ts = tonumber(data[2])

-- First time we ever see this key: start full at capacity.
if tokens == nil then
    tokens  = capacity
    last_ts = now_ms
end

-- Refill: tokens accrue at refill_per_sec for the elapsed time, capped at capacity.
local delta_ms = math.max(0, now_ms - last_ts)
local refilled = (delta_ms / 1000.0) * refill_per_sec
tokens = math.min(capacity, tokens + refilled)

local allowed  = 0
local retry_ms = 0
if tokens >= want then
    -- Enough tokens: grant the request and deduct atomically.
    tokens  = tokens - want
    allowed = 1
else
    -- Not enough: compute how long until 'want' tokens accrue.
    local deficit = want - tokens
    retry_ms = math.ceil((deficit / refill_per_sec) * 1000)
end

-- Persist new state and refresh the 60s TTL so idle buckets evict.
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now_ms)
redis.call('PEXPIRE', KEYS[1], 60000)

-- Return remaining as an integer floor for a stable cross-language JSON shape.
return { allowed, math.floor(tokens), retry_ms }
`
