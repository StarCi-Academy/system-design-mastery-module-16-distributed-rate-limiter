package com.starci.tokenbucket;

import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

@RestController
@RequestMapping("/api/token-bucket")
public class TokenBucketController {

    private final TokenBucketService service;
    private final org.springframework.data.redis.core.StringRedisTemplate redis;

    public TokenBucketController(TokenBucketService service,
                                 org.springframework.data.redis.core.StringRedisTemplate redis) {
        this.service = service;
        this.redis = redis;
    }

    @PostMapping("/consume")
    public TokenBucketService.ConsumeResult consume(@RequestBody Map<String, Object> body) {
        return service.consume(
                (String) body.get("key"),
                ((Number) body.get("capacity")).doubleValue(),
                ((Number) body.get("refillPerSec")).doubleValue(),
                body.get("tokens") != null ? ((Number) body.get("tokens")).longValue() : null);
    }

    @GetMapping("/state")
    public TokenBucketService.StateResult state(@RequestParam String key) {
        return service.state(key);
    }

    @GetMapping("/burst-demo")
    public Map<String, Object> burstDemo(@RequestParam String key,
                                         @RequestParam double capacity,
                                         @RequestParam double refill,
                                         @RequestParam int n) {
        String fullKey = "tb:burst:" + key;
        redis.delete(fullKey); // reset so the demo is repeatable
        List<Map<String, Object>> results = new ArrayList<>();
        int allowedCount = 0;
        for (int i = 0; i < n; i++) {
            long[] r = service.eval(fullKey, capacity, refill, 1);
            boolean allowed = r[0] == 1;
            if (allowed) allowedCount++;
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("i", i);
            row.put("allowed", allowed);
            row.put("remaining", r[1]);
            results.add(row);
        }
        int deniedCount = n - allowedCount;
        int expectedAllowed = Math.min(n, (int) capacity);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("scenario", "Fire " + n + " requests back-to-back into bucket(capacity=" + capacity + ", refill=" + refill + "/s)");
        out.put("allowedCount", allowedCount);
        out.put("deniedCount", deniedCount);
        out.put("burstObserved", allowedCount == expectedAllowed && deniedCount > 0);
        out.put("results", results);
        out.put("note", "The first burst drains all capacity tokens, then requests are denied until refill catches up.");
        return out;
    }

    @PostMapping("/race-demo")
    public Map<String, Object> raceDemo(@RequestBody Map<String, Object> body) {
        String key = (String) body.get("key");
        double capacity = ((Number) body.get("capacity")).doubleValue();
        double refillPerSec = ((Number) body.get("refillPerSec")).doubleValue();
        int concurrency = ((Number) body.get("concurrency")).intValue();
        String fullKey = "tb:race:" + key;
        redis.delete(fullKey); // reset bucket so the demo is repeatable

        AtomicInteger allowed = new AtomicInteger(0);
        // Fire `concurrency` requests at once; only Lua atomicity bounds grants.
        IntStream.range(0, concurrency).parallel().forEach(i -> {
            long[] r = service.eval(fullKey, capacity, refillPerSec, 1);
            if (r[0] == 1) allowed.incrementAndGet();
        });
        int allowedExpected = Math.min(concurrency, (int) capacity);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("scenario", "Fire " + concurrency + " requests AT ONCE into bucket(capacity=" + capacity + ")");
        out.put("allowedExpected", allowedExpected);
        out.put("allowedActual", allowed.get());
        out.put("raceFree", allowed.get() == allowedExpected);
        out.put("note", "Lua EVALSHA runs atomically inside Redis — no race even when N clients send concurrently.");
        return out;
    }
}
