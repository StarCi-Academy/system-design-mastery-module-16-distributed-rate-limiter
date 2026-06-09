package com.starci.quota;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public record QuotaResult(
        boolean allowed,
        @JsonProperty("blockedAt") String blockedAt,
        @JsonProperty("userCount") long userCount,
        @JsonProperty("tenantCount") long tenantCount,
        @JsonProperty("globalCount") long globalCount,
        Map<String, Integer> limits,
        @JsonProperty("windowMs") int windowMs) {
}
