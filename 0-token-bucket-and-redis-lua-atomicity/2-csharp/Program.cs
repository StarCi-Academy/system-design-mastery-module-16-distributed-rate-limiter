using StackExchange.Redis;
using TokenBucketService;

var builder = WebApplication.CreateBuilder(args);

var redisHost = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost";
var redisPort = Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379";
var port = Environment.GetEnvironmentVariable("PORT") ?? "3000";

builder.WebHost.UseUrls($"http://0.0.0.0:{port}");
builder.Services.AddSingleton<IConnectionMultiplexer>(
    ConnectionMultiplexer.Connect($"{redisHost}:{redisPort}"));
builder.Services.AddSingleton<TokenBucket>();

var app = builder.Build();

app.MapPost("/api/token-bucket/consume", (ConsumeBody body, TokenBucket tb) =>
    Results.Ok(tb.Consume(body.key, body.capacity, body.refillPerSec, body.tokens)));

app.MapGet("/api/token-bucket/state", (string key, TokenBucket tb) =>
    Results.Ok(tb.State(key)));

app.MapGet("/api/token-bucket/burst-demo",
    (string key, double capacity, double refill, int n, TokenBucket tb, IConnectionMultiplexer mux) =>
{
    var fullKey = "tb:burst:" + key;
    mux.GetDatabase().KeyDelete(fullKey); // reset so the demo is repeatable
    var results = new List<object>();
    var allowedCount = 0;
    for (var i = 0; i < n; i++)
    {
        var r = tb.Eval(fullKey, capacity, refill, 1);
        var allowed = r[0] == 1;
        if (allowed) allowedCount++;
        results.Add(new { i, allowed, remaining = r[1] });
    }
    var deniedCount = n - allowedCount;
    var expectedAllowed = Math.Min(n, (int)capacity);
    return Results.Ok(new
    {
        scenario = $"Fire {n} requests back-to-back into bucket(capacity={capacity}, refill={refill}/s)",
        allowedCount,
        deniedCount,
        burstObserved = allowedCount == expectedAllowed && deniedCount > 0,
        results,
        note = "The first burst drains all capacity tokens, then requests are denied until refill catches up."
    });
});

app.MapPost("/api/token-bucket/race-demo", (RaceBody body, TokenBucket tb, IConnectionMultiplexer mux) =>
{
    var fullKey = "tb:race:" + body.key;
    mux.GetDatabase().KeyDelete(fullKey); // reset bucket so the demo is repeatable
    var allowed = 0;
    // Fire `concurrency` requests at once; only Lua atomicity bounds grants.
    Parallel.For(0, body.concurrency, _ =>
    {
        var r = tb.Eval(fullKey, body.capacity, body.refillPerSec, 1);
        if (r[0] == 1) Interlocked.Increment(ref allowed);
    });
    var allowedExpected = Math.Min(body.concurrency, (int)body.capacity);
    return Results.Ok(new
    {
        scenario = $"Fire {body.concurrency} requests AT ONCE into bucket(capacity={body.capacity})",
        allowedExpected,
        allowedActual = allowed,
        raceFree = allowed == allowedExpected,
        note = "Lua EVALSHA runs atomically inside Redis — no race even when N clients send concurrently."
    });
});

app.Run();

public record ConsumeBody(string key, double capacity, double refillPerSec, long? tokens);
public record RaceBody(string key, double capacity, double refillPerSec, int concurrency);
