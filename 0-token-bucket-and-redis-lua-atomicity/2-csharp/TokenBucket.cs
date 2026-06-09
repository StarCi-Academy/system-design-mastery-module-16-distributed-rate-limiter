using StackExchange.Redis;

namespace TokenBucketService;

// Token bucket limiter backed by a single Lua script executed atomically on
// Redis. The Lua source is byte-for-byte identical across all four language
// bodies; only the host client (here StackExchange.Redis) differs.
public sealed class TokenBucket
{
    public const string Algorithm = "Token Bucket (atomic via Redis Lua EVALSHA)";

    // Byte-for-byte identical to the TypeScript / Java / Go bodies.
    private const string Lua = @"
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
";

    private readonly IDatabase _db;
    private readonly byte[] _sha;

    public TokenBucket(IConnectionMultiplexer mux)
    {
        _db = mux.GetDatabase();
        // SCRIPT LOAD once on construction; subsequent calls use EVALSHA by SHA.
        var server = mux.GetServer(mux.GetEndPoints()[0]);
        _sha = server.ScriptLoad(Lua);
    }

    public long[] Eval(string fullKey, double capacity, double refillPerSec, long want)
    {
        var keys = new RedisKey[] { fullKey };
        var args = new RedisValue[]
        {
            capacity, refillPerSec, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), want
        };
        RedisResult res;
        try
        {
            res = _db.ScriptEvaluate(_sha, keys, args);
        }
        catch (RedisServerException)
        {
            // NOSCRIPT after a flush/restart: re-EVAL the source and re-cache.
            res = _db.ScriptEvaluate(Lua, keys, args);
        }
        var arr = (RedisResult[])res!;
        return new[] { (long)arr[0], (long)arr[1], (long)arr[2] };
    }

    public object Consume(string key, double capacity, double refillPerSec, long? tokens)
    {
        var want = (tokens is > 0) ? tokens.Value : 1;
        var r = Eval("tb:" + key, capacity, refillPerSec, want);
        var allowed = r[0] == 1;
        var note = allowed
            ? "Tokens available — request allowed."
            : $"Out of tokens — wait {r[2]}ms for the next refill.";
        return new
        {
            allowed,
            remaining = r[1],
            retryAfterMs = r[2],
            algorithm = Algorithm,
            note
        };
    }

    public object State(string key)
    {
        var data = _db.HashGet("tb:" + key, new RedisValue[] { "tokens", "ts" });
        var exists = !data[0].IsNull;
        long tokens = exists ? (long)Math.Floor((double)data[0]) : 0;
        long lastTs = exists ? (long)data[1] : 0;
        return new { key, tokens, lastTs, exists };
    }
}
