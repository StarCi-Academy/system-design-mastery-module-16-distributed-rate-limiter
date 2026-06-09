package com.starci.slidingwindow;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/sliding-window")
public class SlidingWindowController {

    private final SlidingWindowService service;

    public SlidingWindowController(SlidingWindowService service) {
        this.service = service;
    }

    public record CheckRequest(String key, long limit, long windowMs) {
    }

    @PostMapping("/log/check")
    public Map<String, Object> logCheck(@RequestBody CheckRequest req) {
        SlidingWindowService.LimitResult r = service.checkLog(req.key(), req.limit(), req.windowMs());
        return body("log", r, req.limit());
    }

    @PostMapping("/counter/check")
    public Map<String, Object> counterCheck(@RequestBody CheckRequest req) {
        SlidingWindowService.LimitResult r = service.checkCounter(req.key(), req.limit(), req.windowMs());
        return body("counter", r, req.limit());
    }

    @GetMapping("/compare")
    public SlidingWindowService.CompareResult compare(
            @RequestParam(defaultValue = "demo") String key,
            @RequestParam(defaultValue = "5") long limit,
            @RequestParam(defaultValue = "2000") long windowMs,
            @RequestParam(defaultValue = "10") int n) {
        return service.compare(key, limit, windowMs, n);
    }

    // Shared contract: { algorithm, allowed, count, limit }.
    private Map<String, Object> body(String algorithm, SlidingWindowService.LimitResult r, long limit) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("algorithm", algorithm);
        out.put("allowed", r.allowed());
        out.put("count", r.count());
        out.put("limit", limit);
        return out;
    }
}
