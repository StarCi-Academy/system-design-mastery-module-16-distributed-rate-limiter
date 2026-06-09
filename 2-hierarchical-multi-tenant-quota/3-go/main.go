package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

// cascadeLua is the three-tier quota cascade script, byte-for-byte identical across
// all four language implementations. Only the host client (ioredis / Lettuce /
// StackExchange.Redis / go-redis) differs — the Lua itself is language-agnostic.
//
// KEYS[1]=user key, KEYS[2]=tenant key, KEYS[3]=global key
// ARGV[1]=now_ms (placeholder), ARGV[2]=window_ms, ARGV[3..5]=user/tenant/global limit
// Returns {allowed(1/0), blockedAt("OK"|"USER"|"TENANT"|"GLOBAL"), userCount, tenantCount, globalCount}
const cascadeLua = `
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
`

type config struct {
	userLimit, tenantLimit, globalLimit, windowMs int
}

type handler struct {
	rdb *redis.ClusterClient
	sha string
	cfg config
}

// buildKeys returns the three Redis keys for a (tenant, user) pair.
// All three carry the same {q:<tenantID>} hash-tag so they land on ONE slot, which
// is the mandatory condition for a multi-key EVALSHA to run atomically on a cluster.
// Layout: {q:t}:u:id / {q:t}:t / {q:t}:g.
func buildKeys(tenantID, userID string) []string {
	return []string{
		"{q:" + tenantID + "}:u:" + userID,
		"{q:" + tenantID + "}:t",
		"{q:" + tenantID + "}:g",
	}
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// eval runs the cascade Lua via EVALSHA for the given (tenant, user) pair.
// Returns (allowed, blockedAt, userCount, tenantCount, globalCount, error).
func (h *handler) eval(ctx context.Context, tenantID, userID string) (bool, string, int64, int64, int64, error) {
	keys := buildKeys(tenantID, userID)
	res, err := h.rdb.EvalSha(ctx, h.sha, keys,
		time.Now().UnixMilli(), h.cfg.windowMs, h.cfg.userLimit, h.cfg.tenantLimit, h.cfg.globalLimit).Result()
	if err != nil {
		return false, "", 0, 0, 0, err
	}
	arr := res.([]interface{})
	allowed := arr[0].(int64) == 1
	layer := arr[1].(string)
	return allowed, layer, arr[2].(int64), arr[3].(int64), arr[4].(int64), nil
}

// check handles POST /api/quota/check {tenantId, userId}.
// Returns HTTP 200 when allowed, HTTP 429 when any tier rejects, with the same JSON
// body shape in both cases — identical contract across all four language implementations.
func (h *handler) check(c *gin.Context) {
	var req struct {
		TenantID string `json:"tenantId"`
		UserID   string `json:"userId"`
	}
	_ = c.ShouldBindJSON(&req)
	allowed, layer, uc, tc, gc, err := h.eval(c, req.TenantID, req.UserID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var blockedAt interface{}
	if !allowed {
		blockedAt = layer
	}
	body := gin.H{
		"allowed": allowed, "blockedAt": blockedAt,
		"userCount": uc, "tenantCount": tc, "globalCount": gc,
		"limits":   gin.H{"user": h.cfg.userLimit, "tenant": h.cfg.tenantLimit, "global": h.cfg.globalLimit},
		"windowMs": h.cfg.windowMs,
	}
	status := http.StatusOK
	if !allowed {
		status = http.StatusTooManyRequests
	}
	c.JSON(status, body)
}

// hotTenantDemo handles POST /api/quota/hot-tenant-demo.
// Fires requests sequential checks cycling through distinctUsers user ids within one
// window to demonstrate noisy-neighbour isolation (tenant tier trips before global tier).
func (h *handler) hotTenantDemo(c *gin.Context) {
	var req struct {
		TenantID      string `json:"tenantId"`
		Requests      int    `json:"requests"`
		DistinctUsers int    `json:"distinctUsers"`
	}
	_ = c.ShouldBindJSON(&req)
	counts := map[string]int{"OK": 0, "USER": 0, "TENANT": 0, "GLOBAL": 0}
	for i := 0; i < req.Requests; i++ {
		userID := "u" + strconv.Itoa(i%req.DistinctUsers)
		allowed, layer, _, _, _, err := h.eval(c, req.TenantID, userID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if allowed {
			counts["OK"]++
		} else {
			counts[layer]++
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"tenantId": req.TenantID, "requests": req.Requests,
		"distinctUsers": req.DistinctUsers, "counts": counts,
	})
}

// clusterInfo handles GET /api/quota/cluster-info?tenantId=<id>.
// Returns a snapshot of the cluster topology and proves that all three tier keys for
// the given tenantId hash to the same slot via the {q:<tenantId>} hash-tag.
func (h *handler) clusterInfo(c *gin.Context) {
	tenantID := c.DefaultQuery("tenantId", "t1")
	keys := buildKeys(tenantID, "u0")
	var nodes int
	_ = h.rdb.ForEachMaster(c, func(ctx context.Context, _ *redis.Client) error {
		nodes++
		return nil
	})
	slots := make([]int64, len(keys))
	for i, k := range keys {
		slots[i] = h.rdb.ClusterKeySlot(c, k).Val()
	}
	sameSlot := slots[0] == slots[1] && slots[1] == slots[2]
	c.JSON(http.StatusOK, gin.H{
		"nodes": nodes, "slotsTotal": 16384, "sampleTenant": tenantID,
		"sameSlot": sameSlot, "slot": slots[0], "keys": keys,
	})
}

func main() {
	seeds := os.Getenv("REDIS_CLUSTER_NODES")
	if seeds == "" {
		seeds = "redis-1:6379,redis-2:6379,redis-3:6379"
	}
	rdb := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(seeds, ",")})
	sha, err := rdb.ScriptLoad(context.Background(), cascadeLua).Result()
	if err != nil {
		log.Fatalf("script load failed: %v", err)
	}
	h := &handler{rdb: rdb, sha: sha, cfg: config{
		userLimit:   envInt("USER_LIMIT", 5),
		tenantLimit: envInt("TENANT_LIMIT", 20),
		globalLimit: envInt("GLOBAL_LIMIT", 50),
		windowMs:    envInt("WINDOW_MS", 1000),
	}}
	r := gin.New()
	r.POST("/api/quota/check", h.check)
	r.POST("/api/quota/hot-tenant-demo", h.hotTenantDemo)
	r.GET("/api/quota/cluster-info", h.clusterInfo)
	log.Println("hierarchical-quota-service listening on :3000")
	_ = r.Run(":3000")
}
