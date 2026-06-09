package com.starci.quota;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Implements the three-tier user → tenant → global quota cascade on Redis Cluster.
 * The cascade is run as a single atomic Lua script via EVALSHA so no interleaving
 * can occur between INCR and compensating DECR operations.
 */
@Service
public class QuotaService {

    // Byte-for-byte identical Lua across all four language implementations.
    private static final String CASCADE_LUA = """
            local user_key   = KEYS[1]
            local tenant_key = KEYS[2]
            local global_key = KEYS[3]
            local window_ms    = tonumber(ARGV[2])
            local user_limit   = tonumber(ARGV[3])
            local tenant_limit = tonumber(ARGV[4])
            local global_limit = tonumber(ARGV[5])

            local function bump(key, limit)
                local v = tonumber(redis.call('GET', key) or '0')
                if v >= limit then return 0, v end
                redis.call('INCR', key)
                redis.call('PEXPIRE', key, window_ms + 500)
                return 1, v + 1
            end

            local user_ok, user_v = bump(user_key, user_limit)
            if user_ok == 0 then return {0, 'USER', user_v, 0, 0} end

            local tenant_ok, tenant_v = bump(tenant_key, tenant_limit)
            if tenant_ok == 0 then
                redis.call('DECR', user_key)
                return {0, 'TENANT', user_v - 1, tenant_v, 0}
            end

            local global_ok, global_v = bump(global_key, global_limit)
            if global_ok == 0 then
                redis.call('DECR', user_key)
                redis.call('DECR', tenant_key)
                return {0, 'GLOBAL', user_v - 1, tenant_v - 1, global_v}
            end

            return {1, 'OK', user_v, tenant_v, global_v}
            """;

    private final StatefulRedisClusterConnection<String, String> conn;
    private final RedisClusterClient client;

    @Value("${USER_LIMIT:5}") private int userLimit;
    @Value("${TENANT_LIMIT:20}") private int tenantLimit;
    @Value("${GLOBAL_LIMIT:50}") private int globalLimit;
    @Value("${WINDOW_MS:1000}") private int windowMs;

    private String sha;

    public QuotaService(StatefulRedisClusterConnection<String, String> conn, RedisClusterClient client) {
        this.conn = conn;
        this.client = client;
    }

    /**
     * Pre-load the Lua script onto the cluster so EVALSHA can be used on every request.
     * Lettuce routes scriptLoad to a single node; the same SHA1 is returned on all nodes
     * because the SHA is deterministic (a hash of the body).
     */
    @PostConstruct
    public void init() {
        // SCRIPT LOAD propagates to every master via Lettuce; same SHA on all.
        this.sha = conn.sync().scriptLoad(CASCADE_LUA);
    }

    /**
     * Build the three Redis keys for a (tenant, user) pair.
     * The shared {@code {q:<tenantId>}} hash-tag forces all three keys onto ONE slot so
     * the multi-key Lua can execute atomically without a CROSSSLOT error.
     *
     * @param tenantId Tenant identifier used as the hash-tag anchor.
     * @param userId   User identifier appended after the hash-tag.
     * @return Array of [userKey, tenantKey, globalKey].
     */
    static String[] buildKeys(String tenantId, String userId) {
        String tag = "{q:" + tenantId + "}";
        return new String[]{tag + ":u:" + userId, tag + ":t", tag + ":g"};
    }

    /**
     * Check a single request against the user → tenant → global cascade.
     * Runs the pre-loaded Lua atomically via EVALSHA; falls back to EVAL on NOSCRIPT.
     *
     * @param tenantId Tenant that owns the request.
     * @param userId   User within that tenant issuing the request.
     * @return {@link QuotaResult} with the allow/deny decision and per-tier counters.
     */
    @SuppressWarnings("unchecked")
    public QuotaResult check(String tenantId, String userId) {
        String[] keys = buildKeys(tenantId, userId);
        String[] argv = {
                String.valueOf(System.currentTimeMillis()),
                String.valueOf(windowMs),
                String.valueOf(userLimit),
                String.valueOf(tenantLimit),
                String.valueOf(globalLimit),
        };
        RedisAdvancedClusterCommands<String, String> sync = conn.sync();
        List<Object> raw;
        try {
            raw = sync.evalsha(sha, ScriptOutputType.MULTI, keys, argv);
        } catch (Exception ex) {
            if (ex.getMessage() != null && ex.getMessage().contains("NOSCRIPT")) {
                raw = sync.eval(CASCADE_LUA, ScriptOutputType.MULTI, keys, argv);
            } else {
                throw ex;
            }
        }
        boolean allowed = ((Long) raw.get(0)) == 1L;
        String layer = (String) raw.get(1);
        return new QuotaResult(
                allowed,
                allowed ? null : layer,
                (Long) raw.get(2), (Long) raw.get(3), (Long) raw.get(4),
                Map.of("user", userLimit, "tenant", tenantLimit, "global", globalLimit),
                windowMs);
    }

    /**
     * Fire {@code requests} sequential checks cycling through {@code distinctUsers} user ids
     * to simulate a hot tenant and demonstrate noisy-neighbour isolation.
     *
     * @param tenantId      The hot tenant id to simulate.
     * @param requests      Total number of requests to fire.
     * @param distinctUsers Number of distinct user ids to cycle through.
     * @return Count of results per blocking layer (OK / USER / TENANT / GLOBAL).
     */
    public Map<String, Integer> hotTenantDemo(String tenantId, int requests, int distinctUsers) {
        Map<String, Integer> counts = new java.util.LinkedHashMap<>(
                Map.of("OK", 0, "USER", 0, "TENANT", 0, "GLOBAL", 0));
        for (int i = 0; i < requests; i++) {
            QuotaResult r = check(tenantId, "u" + (i % distinctUsers));
            String bucket = r.allowed() ? "OK" : r.blockedAt();
            counts.merge(bucket, 1, Integer::sum);
        }
        return counts;
    }

    /**
     * Return a snapshot of the cluster topology and prove that all three tier keys for
     * {@code tenantId} hash to the same slot, confirming hash-tag routing is correct.
     *
     * @param tenantId Tenant id whose keys are examined.
     * @return Map containing node count, slot count, sameSlot flag, slot number, and keys.
     */
    public Map<String, Object> clusterInfo(String tenantId) {
        String[] keys = buildKeys(tenantId, "u0");
        long nodes = client.getPartitions().stream()
                .filter(p -> p.is(RedisClusterNode.NodeFlag.UPSTREAM)).count();
        int[] slots = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            slots[i] = SlotHash.getSlot(keys[i]);
        }
        boolean sameSlot = slots[0] == slots[1] && slots[1] == slots[2];
        return Map.of(
                "nodes", nodes,
                "slotsTotal", 16384,
                "sampleTenant", tenantId,
                "sameSlot", sameSlot,
                "slot", slots[0],
                "keys", keys);
    }
}
