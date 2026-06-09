using StackExchange.Redis;
using SlidingWindowService;

var builder = WebApplication.CreateBuilder(args);

var redisHost = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost";
var redisPort = Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379";

builder.Services.AddSingleton<IConnectionMultiplexer>(
    ConnectionMultiplexer.Connect($"{redisHost}:{redisPort}"));
builder.Services.AddSingleton<SlidingWindowLimiter>();

var app = builder.Build();

// Load the Lua scripts once at startup.
await app.Services.GetRequiredService<SlidingWindowLimiter>().LoadScriptsAsync();

app.MapPost("/api/sliding-window/log/check", async (CheckRequest req, SlidingWindowLimiter limiter) =>
{
    var r = await limiter.CheckLogAsync(req.key, req.limit, req.windowMs);
    // Shared contract: { algorithm, allowed, count, limit }.
    return Results.Ok(new { algorithm = "log", allowed = r.Allowed, count = r.Count, limit = req.limit });
});

app.MapPost("/api/sliding-window/counter/check", async (CheckRequest req, SlidingWindowLimiter limiter) =>
{
    var r = await limiter.CheckCounterAsync(req.key, req.limit, req.windowMs);
    return Results.Ok(new { algorithm = "counter", allowed = r.Allowed, count = r.Count, limit = req.limit });
});

app.MapGet("/api/sliding-window/compare", async (
    string? key, long? limit, long? windowMs, int? n, SlidingWindowLimiter limiter) =>
{
    var result = await limiter.CompareAsync(key ?? "demo", limit ?? 5, windowMs ?? 2000, n ?? 10);
    return Results.Ok(new
    {
        n = result.N,
        logCount = result.LogCount,
        counterCount = result.CounterCount,
        errorPct = result.ErrorPct,
        logMemory = result.LogMemory,
        counterMemory = result.CounterMemory,
    });
});

app.Run("http://0.0.0.0:3000");

public record CheckRequest(string key, long limit, long windowMs);
