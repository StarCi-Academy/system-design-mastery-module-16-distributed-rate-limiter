package com.starci.slidingwindow;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
public class SlidingWindowService {

    private final RedisCommands<String, String> redis;
    private String logSha = "";
    private String counterSha = "";

    public SlidingWindowService(StatefulRedisConnection<String, String> connection) {
        this.redis = connection.sync();
    }

    @PostConstruct
    void loadScripts() {
        // Load both Lua scripts once at startup; EVALSHA then ships only the digest.
        this.logSha = redis.scriptLoad(LuaScripts.LOG);
        this.counterSha = redis.scriptLoad(LuaScripts.COUNTER);
    }

    public LimitResult checkLog(String key, long limit, long windowMs) {
        long now = System.currentTimeMillis();
        String member = now + "-" + UUID.randomUUID();
        @SuppressWarnings("unchecked")
        List<Long> raw = (List<Long>) redis.evalsha(
                logSha, ScriptOutputType.MULTI, new String[] { "log:" + key },
                String.valueOf(now), String.valueOf(windowMs), String.valueOf(limit), member);
        return new LimitResult(raw.get(0) == 1L, raw.get(1));
    }

    public LimitResult checkCounter(String key, long limit, long windowMs) {
        long now = System.currentTimeMillis();
        @SuppressWarnings("unchecked")
        List<Long> raw = (List<Long>) redis.evalsha(
                counterSha, ScriptOutputType.MULTI, new String[] { "cnt:" + key },
                String.valueOf(now), String.valueOf(windowMs), String.valueOf(limit));
        return new LimitResult(raw.get(0) == 1L, raw.get(1));
    }

    public CompareResult compare(String key, long limit, long windowMs, int n) {
        resetKeys(key);
        long logAllowed = 0;
        long counterAllowed = 0;
        for (int i = 0; i < n; i++) {
            if (checkLog(key, limit, windowMs).allowed()) {
                logAllowed++;
            }
            if (checkCounter(key, limit, windowMs).allowed()) {
                counterAllowed++;
            }
        }
        double errorPct = logAllowed == 0
                ? 0
                : Math.round((Math.abs(logAllowed - counterAllowed) / (double) logAllowed) * 100 * 1000) / 1000.0;
        return new CompareResult(
                n, logAllowed, counterAllowed, errorPct,
                "O(N) — one ZSET member per request",
                "O(1) — two integer buckets");
    }

    private void resetKeys(String key) {
        redis.del("log:" + key);
        List<String> cntKeys = redis.keys("cnt:" + key + ":*");
        if (!cntKeys.isEmpty()) {
            redis.del(cntKeys.toArray(new String[0]));
        }
    }

    public record LimitResult(boolean allowed, long count) {
    }

    public record CompareResult(
            int n, long logCount, long counterCount, double errorPct,
            String logMemory, String counterMemory) {
    }
}
