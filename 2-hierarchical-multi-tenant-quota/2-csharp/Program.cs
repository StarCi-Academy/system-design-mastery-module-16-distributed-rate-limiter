using HierarchicalQuotaService;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// One shared cluster ConnectionMultiplexer for the whole app. StackExchange.Redis
// discovers the cluster topology and routes commands to the node owning the slot.
var seeds = Environment.GetEnvironmentVariable("REDIS_CLUSTER_NODES")
            ?? "redis-1:6379,redis-2:6379,redis-3:6379";
var options = new ConfigurationOptions { AbortOnConnectFail = false, ConnectTimeout = 10000 };
foreach (var node in seeds.Split(','))
{
    options.EndPoints.Add(node.Trim());
}
var mux = await ConnectionMultiplexer.ConnectAsync(options);

var cfg = new QuotaConfig(
    UserLimit: EnvInt("USER_LIMIT", 5),
    TenantLimit: EnvInt("TENANT_LIMIT", 20),
    GlobalLimit: EnvInt("GLOBAL_LIMIT", 50),
    WindowMs: EnvInt("WINDOW_MS", 1000));

var quota = new QuotaService(mux, cfg);
await quota.InitAsync();

var app = builder.Build();

app.MapPost("/api/quota/check", async (CheckRequest req) =>
{
    var r = await quota.CheckAsync(req.TenantId, req.UserId);
    return Results.Json(r, statusCode: r.Allowed ? 200 : 429);
});

app.MapPost("/api/quota/hot-tenant-demo", async (HotRequest req) =>
{
    var counts = await quota.HotTenantDemoAsync(req.TenantId, req.Requests, req.DistinctUsers);
    return Results.Json(new
    {
        tenantId = req.TenantId,
        requests = req.Requests,
        distinctUsers = req.DistinctUsers,
        counts
    });
});

app.MapGet("/api/quota/cluster-info", async (string? tenantId) =>
{
    var info = await quota.ClusterInfoAsync(tenantId ?? "t1");
    return Results.Json(info);
});

app.Run("http://0.0.0.0:3000");

static int EnvInt(string key, int def) =>
    int.TryParse(Environment.GetEnvironmentVariable(key), out var v) ? v : def;

record CheckRequest(string TenantId, string UserId);
record HotRequest(string TenantId, int Requests, int DistinctUsers);
