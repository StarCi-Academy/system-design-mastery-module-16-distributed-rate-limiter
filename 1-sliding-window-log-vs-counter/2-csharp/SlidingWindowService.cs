using StackExchange.Redis;

namespace SlidingWindowService;

public sealed record LimitResult(bool Allowed, long Count);

public sealed record CompareResult(
    int N, long LogCount, long CounterCount, double ErrorPct,
    string LogMemory, string CounterMemory);

public sealed class SlidingWindowLimiter
{
    private readonly IDatabase _db;
    private readonly IServer _server;
    private byte[] _logSha = Array.Empty<byte>();
    private byte[] _counterSha = Array.Empty<byte>();

    public SlidingWindowLimiter(IConnectionMultiplexer mux)
    {
        _db = mux.GetDatabase();
        _server = mux.GetServer(mux.GetEndPoints()[0]);
    }

    // Load both Lua scripts once at startup; EVALSHA then ships only the digest.
    public async Task LoadScriptsAsync()
    {
        _logSha = await _server.ScriptLoadAsync(LuaScripts.Log);
        _counterSha = await _server.ScriptLoadAsync(LuaScripts.Counter);
    }

    public async Task<LimitResult> CheckLogAsync(string key, long limit, long windowMs)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var member = $"{now}-{Guid.NewGuid():N}";
        var raw = (RedisValue[])(await _db.ScriptEvaluateAsync(
            _logSha,
            new RedisKey[] { $"log:{key}" },
            new RedisValue[] { now, windowMs, limit, member }))!;
        return new LimitResult((long)raw[0] == 1, (long)raw[1]);
    }

    public async Task<LimitResult> CheckCounterAsync(string key, long limit, long windowMs)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var raw = (RedisValue[])(await _db.ScriptEvaluateAsync(
            _counterSha,
            new RedisKey[] { $"cnt:{key}" },
            new RedisValue[] { now, windowMs, limit }))!;
        return new LimitResult((long)raw[0] == 1, (long)raw[1]);
    }

    public async Task<CompareResult> CompareAsync(string key, long limit, long windowMs, int n)
    {
        await ResetKeysAsync(key);
        long logAllowed = 0;
        long counterAllowed = 0;
        for (var i = 0; i < n; i++)
        {
            if ((await CheckLogAsync(key, limit, windowMs)).Allowed)
            {
                logAllowed++;
            }
            if ((await CheckCounterAsync(key, limit, windowMs)).Allowed)
            {
                counterAllowed++;
            }
        }
        var errorPct = logAllowed == 0
            ? 0
            : Math.Round(Math.Abs(logAllowed - counterAllowed) / (double)logAllowed * 100 * 1000) / 1000.0;
        return new CompareResult(
            n, logAllowed, counterAllowed, errorPct,
            "O(N) — one ZSET member per request",
            "O(1) — two integer buckets");
    }

    private async Task ResetKeysAsync(string key)
    {
        await _db.KeyDeleteAsync($"log:{key}");
        foreach (var k in _server.Keys(pattern: $"cnt:{key}:*"))
        {
            await _db.KeyDeleteAsync(k);
        }
    }
}
