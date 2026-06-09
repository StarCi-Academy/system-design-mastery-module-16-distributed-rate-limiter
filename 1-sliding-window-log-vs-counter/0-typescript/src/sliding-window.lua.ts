// The two Lua scripts are byte-for-byte identical across all four language
// implementations. They execute inside Redis (atomic read-modify-write), so the
// HTTP contract and numeric outcomes are unchanged regardless of the host client.

// log.lua — exact sliding-window log over a ZSET.
// KEYS[1] = the ZSET key for this client.
// ARGV[1] = now (ms), ARGV[2] = windowMs, ARGV[3] = limit, ARGV[4] = unique request id.
export const LOG_LUA = `
local now      = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])
local limit    = tonumber(ARGV[3])
local member   = ARGV[4]

-- Evict every timestamp older than (now - windowMs): the window truly slides.
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - windowMs)

-- Count the requests still inside the window: an EXACT count, no approximation.
local count = redis.call('ZCARD', KEYS[1])

if count < limit then
  -- Room left: record this request (score = now) and allow it.
  redis.call('ZADD', KEYS[1], now, member)
  redis.call('PEXPIRE', KEYS[1], windowMs)
  return {1, count + 1}
end

-- At or over the limit: reject WITHOUT recording, so rejected traffic never grows RAM.
return {0, count}
`

// counter.lua — approximate sliding-window counter using two integer buckets.
// KEYS[1] = base rate-limit key.
// ARGV[1] = now (ms), ARGV[2] = windowMs, ARGV[3] = limit.
export const COUNTER_LUA = `
local now      = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])
local limit    = tonumber(ARGV[3])

-- Which fixed bucket are we in, and how far (0..1) through it are we?
local currBucket   = math.floor(now / windowMs)
local prevBucket   = currBucket - 1
local elapsedRatio = (now % windowMs) / windowMs

local currKey = KEYS[1] .. ':' .. currBucket
local prevKey = KEYS[1] .. ':' .. prevBucket

local prevCount = tonumber(redis.call('GET', prevKey) or '0')
local currCount = tonumber(redis.call('GET', currKey) or '0')

-- Weighted estimate: the previous bucket fades out as the current window fills up.
local estimate = prevCount * (1 - elapsedRatio) + currCount

if estimate < limit then
  redis.call('INCR', currKey)
  redis.call('PEXPIRE', currKey, windowMs * 2)
  return {1, math.floor(estimate) + 1}
end

return {0, math.floor(estimate)}
`
