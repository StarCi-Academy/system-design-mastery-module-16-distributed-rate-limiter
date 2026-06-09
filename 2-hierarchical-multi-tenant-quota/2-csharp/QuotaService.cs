using StackExchange.Redis;

namespace HierarchicalQuotaService;

public sealed record QuotaConfig(int UserLimit, int TenantLimit, int GlobalLimit, int WindowMs);

public sealed record QuotaResult(
    bool Allowed,
    string? BlockedAt,
    long UserCount,
    long TenantCount,
    long GlobalCount,
    object Limits,
    int WindowMs);

/// <summary>
/// Implements the three-tier user → tenant → global quota cascade on Redis Cluster.
/// The cascade runs as a single atomic Lua script via EVALSHA so no interleaving can
/// occur between INCR and compensating DECR operations.
/// </summary>
public sealed class QuotaService
{
    // Byte-for-byte identical Lua across all four language implementations.
    private const string CascadeLua = @"
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
";

    private readonly IConnectionMultiplexer _mux;
    private readonly QuotaConfig _cfg;
    private byte[] _sha = Array.Empty<byte>();

    public QuotaService(IConnectionMultiplexer mux, QuotaConfig cfg)
    {
        _mux = mux;
        _cfg = cfg;
    }

    /// <summary>
    /// Pre-load the Lua cascade script onto every master node so EVALSHA succeeds for
    /// any slot without a NOSCRIPT miss.  Redis's script cache is per-node; loading on
    /// a single endpoint leaves other nodes without the compiled script.
    /// </summary>
    public async Task InitAsync()
    {
        // Load the script onto EVERY master so any slot's node can EVALSHA it.
        foreach (var ep in _mux.GetEndPoints())
        {
            var server = _mux.GetServer(ep);
            if (!server.IsReplica)
            {
                _sha = await server.ScriptLoadAsync(CascadeLua);
            }
        }
    }

    /// <summary>
    /// Build the three Redis keys for a (tenant, user) pair.
    /// All three carry the same <c>{q:&lt;tenantId&gt;}</c> hash-tag so they land on ONE slot —
    /// the precondition for a multi-key Lua to run atomically without a CROSSSLOT error.
    /// Layout: {q:t}:u:id / {q:t}:t / {q:t}:g.
    /// </summary>
    private static RedisKey[] BuildKeys(string tenantId, string userId) => new RedisKey[]
    {
        $"{{q:{tenantId}}}:u:{userId}",
        $"{{q:{tenantId}}}:t",
        $"{{q:{tenantId}}}:g",
    };

    /// <summary>
    /// Check a single request against the user → tenant → global cascade.
    /// Returns HTTP 200 (Allowed) or HTTP 429 (Rejected) with the blocking tier name.
    /// </summary>
    /// <param name="tenantId">Tenant that owns the request.</param>
    /// <param name="userId">User within that tenant issuing the request.</param>
    public async Task<QuotaResult> CheckAsync(string tenantId, string userId)
    {
        var db = _mux.GetDatabase();
        var keys = BuildKeys(tenantId, userId);
        var args = new RedisValue[]
        {
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            _cfg.WindowMs, _cfg.UserLimit, _cfg.TenantLimit, _cfg.GlobalLimit,
        };

        RedisResult raw;
        try
        {
            raw = await db.ScriptEvaluateAsync(_sha, keys, args);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("NOSCRIPT"))
        {
            raw = await db.ScriptEvaluateAsync(CascadeLua, keys, args);
        }

        var arr = (RedisResult[])raw!;
        var allowed = (long)arr[0] == 1;
        var layer = (string)arr[1]!;
        return new QuotaResult(
            allowed,
            allowed ? null : layer,
            (long)arr[2], (long)arr[3], (long)arr[4],
            new { user = _cfg.UserLimit, tenant = _cfg.TenantLimit, global = _cfg.GlobalLimit },
            _cfg.WindowMs);
    }

    /// <summary>
    /// Simulate a hot tenant firing many requests across distinct users to demonstrate
    /// noisy-neighbour isolation: the tenant tier (20/s) trips before the global tier (50/s).
    /// </summary>
    /// <param name="tenantId">The hot tenant id to simulate.</param>
    /// <param name="requests">Total number of requests to fire.</param>
    /// <param name="distinctUsers">Number of distinct user ids to cycle through.</param>
    public async Task<Dictionary<string, int>> HotTenantDemoAsync(
        string tenantId, int requests, int distinctUsers)
    {
        var counts = new Dictionary<string, int>
        {
            ["OK"] = 0, ["USER"] = 0, ["TENANT"] = 0, ["GLOBAL"] = 0,
        };
        for (var i = 0; i < requests; i++)
        {
            var r = await CheckAsync(tenantId, $"u{i % distinctUsers}");
            counts[r.Allowed ? "OK" : r.BlockedAt!]++;
        }
        return counts;
    }

    /// <summary>
    /// Return a snapshot of the cluster topology and prove that all three tier keys for
    /// <paramref name="tenantId"/> hash to the same slot, confirming hash-tag routing.
    /// </summary>
    /// <param name="tenantId">Tenant id whose keys are examined.</param>
    public async Task<object> ClusterInfoAsync(string tenantId)
    {
        var keys = BuildKeys(tenantId, "u0");
        var masters = _mux.GetEndPoints()
            .Select(ep => _mux.GetServer(ep))
            .Count(s => !s.IsReplica);
        var slots = keys.Select(k => HashSlot(k.ToString()!)).ToArray();
        var sameSlot = slots.All(s => s == slots[0]);
        return new
        {
            nodes = masters,
            slotsTotal = 16384,
            sampleTenant = tenantId,
            sameSlot,
            slot = slots[0],
            keys = keys.Select(k => k.ToString()).ToArray(),
        };
    }

    // CRC16 over the {hash-tag} substring, mod 16384 — the Redis Cluster slot rule.
    private static int HashSlot(string key)
    {
        var start = key.IndexOf('{');
        if (start >= 0)
        {
            var end = key.IndexOf('}', start + 1);
            if (end > start + 1)
            {
                key = key.Substring(start + 1, end - start - 1);
            }
        }
        return Crc16(key) % 16384;
    }

    private static int Crc16(string s)
    {
        const ushort poly = 0x1021;
        ushort crc = 0;
        foreach (var b in System.Text.Encoding.ASCII.GetBytes(s))
        {
            crc ^= (ushort)(b << 8);
            for (var i = 0; i < 8; i++)
            {
                crc = (crc & 0x8000) != 0 ? (ushort)((crc << 1) ^ poly) : (ushort)(crc << 1);
            }
        }
        return crc;
    }
}
