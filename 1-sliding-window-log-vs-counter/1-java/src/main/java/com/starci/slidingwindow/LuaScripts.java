package com.starci.slidingwindow;

// The two Lua scripts are byte-for-byte identical across all four language
// implementations. They run inside Redis (atomic read-modify-write), so the HTTP
// contract and numeric outcomes are unchanged regardless of the host client.
public final class LuaScripts {
    private LuaScripts() {
    }

    // log.lua — exact sliding-window log over a ZSET.
    // KEYS[1] = ZSET key; ARGV: now (ms), windowMs, limit, unique member id.
    public static final String LOG = """
            local now      = tonumber(ARGV[1])
            local windowMs = tonumber(ARGV[2])
            local limit    = tonumber(ARGV[3])
            local member   = ARGV[4]

            redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - windowMs)
            local count = redis.call('ZCARD', KEYS[1])

            if count < limit then
              redis.call('ZADD', KEYS[1], now, member)
              redis.call('PEXPIRE', KEYS[1], windowMs)
              return {1, count + 1}
            end

            return {0, count}
            """;

    // counter.lua — approximate sliding-window counter using two integer buckets.
    // KEYS[1] = base key; ARGV: now (ms), windowMs, limit.
    public static final String COUNTER = """
            local now      = tonumber(ARGV[1])
            local windowMs = tonumber(ARGV[2])
            local limit    = tonumber(ARGV[3])

            local currBucket   = math.floor(now / windowMs)
            local prevBucket   = currBucket - 1
            local elapsedRatio = (now % windowMs) / windowMs

            local currKey = KEYS[1] .. ':' .. currBucket
            local prevKey = KEYS[1] .. ':' .. prevBucket

            local prevCount = tonumber(redis.call('GET', prevKey) or '0')
            local currCount = tonumber(redis.call('GET', currKey) or '0')

            local estimate = prevCount * (1 - elapsedRatio) + currCount

            if estimate < limit then
              redis.call('INCR', currKey)
              redis.call('PEXPIRE', currKey, windowMs * 2)
              return {1, math.floor(estimate) + 1}
            end

            return {0, math.floor(estimate)}
            """;
}
