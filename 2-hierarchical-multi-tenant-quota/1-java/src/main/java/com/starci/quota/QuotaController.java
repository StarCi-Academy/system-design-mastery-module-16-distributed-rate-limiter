package com.starci.quota;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/quota")
public class QuotaController {

    private final QuotaService quota;

    public QuotaController(QuotaService quota) {
        this.quota = quota;
    }

    // Allowed -> HTTP 200; rejected -> HTTP 429. Identical status/field/value
    // contract across all four language implementations.
    @PostMapping("/check")
    public ResponseEntity<QuotaResult> check(@RequestBody CheckRequest req) {
        QuotaResult r = quota.check(req.tenantId(), req.userId());
        return ResponseEntity
                .status(r.allowed() ? HttpStatus.OK : HttpStatus.TOO_MANY_REQUESTS)
                .body(r);
    }

    @PostMapping("/hot-tenant-demo")
    public Map<String, Object> hotTenantDemo(@RequestBody HotRequest req) {
        Map<String, Integer> counts =
                quota.hotTenantDemo(req.tenantId(), req.requests(), req.distinctUsers());
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("tenantId", req.tenantId());
        body.put("requests", req.requests());
        body.put("distinctUsers", req.distinctUsers());
        body.put("counts", counts);
        return body;
    }

    @GetMapping("/cluster-info")
    public Map<String, Object> clusterInfo(@RequestParam(defaultValue = "t1") String tenantId) {
        return quota.clusterInfo(tenantId);
    }

    public record CheckRequest(String tenantId, String userId) {}

    public record HotRequest(String tenantId, int requests, int distinctUsers) {}
}
