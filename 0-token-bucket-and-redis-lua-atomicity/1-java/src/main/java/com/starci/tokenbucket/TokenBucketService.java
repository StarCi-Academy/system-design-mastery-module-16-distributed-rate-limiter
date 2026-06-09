package com.starci.tokenbucket;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Token bucket limiter backed by a single Lua script executed atomically on
 * Redis. The Lua source is byte-for-byte identical across all four language
 * bodies; only the host client (here Lettuce via Spring Data Redis) differs.
 */
@Service
public class TokenBucketService {

    static final String ALGORITHM = "Token Bucket (atomic via Redis Lua EVALSHA)";

    // Byte-for-byte identical to the TypeScript / Go / C# bodies.
    static final String LUA = """
            local capacity       = tonumber(ARGV[1])
            local refill_per_sec = tonumber(ARGV[2])
            local now_ms         = tonumber(ARGV[3])
            local want           = tonumber(ARGV[4])

            local data    = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
            local tokens  = tonumber(data[1])
            local last_ts = tonumber(data[2])

            if tokens == nil then
                tokens  = capacity
                last_ts = now_ms
            end

            local delta_ms = math.max(0, now_ms - last_ts)
            local refilled = (delta_ms / 1000.0) * refill_per_sec
            tokens = math.min(capacity, tokens + refilled)

            local allowed  = 0
            local retry_ms = 0
            if tokens >= want then
                tokens  = tokens - want
                allowed = 1
            else
                local deficit = want - tokens
                retry_ms = math.ceil((deficit / refill_per_sec) * 1000)
            end

            redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now_ms)
            redis.call('PEXPIRE', KEYS[1], 60000)

            return { allowed, math.floor(tokens), retry_ms }
            """;

    private final StringRedisTemplate redis;
    private volatile String scriptSha;

    public TokenBucketService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    private String sha() {
        // Lazily SCRIPT LOAD the source once and cache the SHA for EVALSHA.
        if (scriptSha == null) {
            synchronized (this) {
                if (scriptSha == null) {
                    scriptSha = redis.execute((RedisConnection conn) ->
                            conn.scriptingCommands().scriptLoad(LUA.getBytes(StandardCharsets.UTF_8)));
                }
            }
        }
        return scriptSha;
    }

    /** Run the atomic token-bucket decision via EVALSHA. */
    long[] eval(String fullKey, double capacity, double refillPerSec, long want) {
        byte[][] keysAndArgs = new byte[][]{
                fullKey.getBytes(StandardCharsets.UTF_8),
                String.valueOf(capacity).getBytes(StandardCharsets.UTF_8),
                String.valueOf(refillPerSec).getBytes(StandardCharsets.UTF_8),
                String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8),
                String.valueOf(want).getBytes(StandardCharsets.UTF_8),
        };
        @SuppressWarnings("unchecked")
        List<Long> res = redis.execute((RedisConnection conn) -> {
            try {
                return (List<Long>) conn.scriptingCommands()
                        .evalSha(sha(), ReturnType.MULTI, 1, keysAndArgs);
            } catch (Exception noscript) {
                // NOSCRIPT after a flush/restart: re-EVAL the source and retry.
                return (List<Long>) conn.scriptingCommands()
                        .eval(LUA.getBytes(StandardCharsets.UTF_8), ReturnType.MULTI, 1, keysAndArgs);
            }
        });
        return new long[]{res.get(0), res.get(1), res.get(2)};
    }

    public ConsumeResult consume(String key, double capacity, double refillPerSec, Long tokens) {
        long want = (tokens != null && tokens > 0) ? tokens : 1;
        long[] r = eval("tb:" + key, capacity, refillPerSec, want);
        boolean allowed = r[0] == 1;
        String note = allowed
                ? "Tokens available — request allowed."
                : "Out of tokens — wait " + r[2] + "ms for the next refill.";
        return new ConsumeResult(allowed, r[1], r[2], ALGORITHM, note);
    }

    public StateResult state(String key) {
        List<Object> data = redis.opsForHash().multiGet("tb:" + key, List.of("tokens", "ts"))
                .stream().map(o -> (Object) o).toList();
        boolean exists = data.get(0) != null;
        long tokens = exists ? (long) Math.floor(Double.parseDouble(data.get(0).toString())) : 0;
        long lastTs = exists ? Long.parseLong(data.get(1).toString()) : 0;
        return new StateResult(key, tokens, lastTs, exists);
    }

    public record ConsumeResult(boolean allowed, long remaining, long retryAfterMs,
                                String algorithm, String note) {}

    public record StateResult(String key, long tokens, long lastTs, boolean exists) {}
}
