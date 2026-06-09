package main

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

const algorithm = "Token Bucket (atomic via Redis Lua EVALSHA)"

// luaTokenBucket is byte-for-byte identical across all four language bodies;
// only the host client that loads and invokes it differs.
const luaTokenBucket = `
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
`

// TokenBucket wraps the shared client + compiled Lua script.
type TokenBucket struct {
	rdb    *redis.Client
	script *redis.Script
}

func NewTokenBucket(rdb *redis.Client) *TokenBucket {
	// go-redis lazily SCRIPT LOADs on first Run and uses EVALSHA afterwards
	// (falling back to EVAL on NOSCRIPT).
	return &TokenBucket{rdb: rdb, script: redis.NewScript(luaTokenBucket)}
}

// consume runs the atomic token-bucket step. The whole read-modify-write
// happens inside Redis via one EVALSHA — no race even under concurrency.
func (t *TokenBucket) consume(ctx context.Context, fullKey string, capacity, refillPerSec float64, want int) (bool, int64, int64, error) {
	now := time.Now().UnixMilli()
	raw, err := t.script.Run(ctx, t.rdb, []string{fullKey},
		capacity, refillPerSec, now, want).Result()
	if err != nil {
		return false, 0, 0, err
	}
	arr := raw.([]interface{})
	allowed := arr[0].(int64) == 1
	remaining := arr[1].(int64)
	retryAfterMs := arr[2].(int64)
	return allowed, remaining, retryAfterMs, nil
}

func (t *TokenBucket) handleConsume(c *gin.Context) {
	var body struct {
		Key          string  `json:"key"`
		Capacity     float64 `json:"capacity"`
		RefillPerSec float64 `json:"refillPerSec"`
		Tokens       *int    `json:"tokens"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	want := 1
	if body.Tokens != nil && *body.Tokens > 0 {
		want = *body.Tokens
	}
	allowed, remaining, retryAfterMs, err := t.consume(c, "tb:"+body.Key, body.Capacity, body.RefillPerSec, want)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	note := "Tokens available — request allowed."
	if !allowed {
		note = fmt.Sprintf("Out of tokens — wait %dms for the next refill.", retryAfterMs)
	}
	c.JSON(http.StatusOK, gin.H{
		"allowed":      allowed,
		"remaining":    remaining,
		"retryAfterMs": retryAfterMs,
		"algorithm":    algorithm,
		"note":         note,
	})
}

func (t *TokenBucket) handleState(c *gin.Context) {
	key := c.Query("key")
	data, err := t.rdb.HMGet(c, "tb:"+key, "tokens", "ts").Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	exists := data[0] != nil
	var tokens, lastTs int64
	if exists {
		var f float64
		fmt.Sscanf(data[0].(string), "%g", &f)
		tokens = int64(math.Floor(f))
		fmt.Sscanf(data[1].(string), "%d", &lastTs)
	}
	c.JSON(http.StatusOK, gin.H{
		"key":    key,
		"tokens": tokens,
		"lastTs": lastTs,
		"exists": exists,
	})
}

func (t *TokenBucket) handleBurstDemo(c *gin.Context) {
	key := c.Query("key")
	var capacity, refill float64
	var n int
	fmt.Sscanf(c.Query("capacity"), "%g", &capacity)
	fmt.Sscanf(c.Query("refill"), "%g", &refill)
	fmt.Sscanf(c.Query("n"), "%d", &n)

	fullKey := "tb:burst:" + key
	t.rdb.Del(c, fullKey) // reset so the demo is repeatable

	results := make([]gin.H, 0, n)
	allowedCount := 0
	for i := 0; i < n; i++ {
		ok, remaining, _, _ := t.consume(c, fullKey, capacity, refill, 1)
		if ok {
			allowedCount++
		}
		results = append(results, gin.H{"i": i, "allowed": ok, "remaining": remaining})
	}
	deniedCount := n - allowedCount
	expectedAllowed := n
	if int(capacity) < expectedAllowed {
		expectedAllowed = int(capacity)
	}
	c.JSON(http.StatusOK, gin.H{
		"scenario":      fmt.Sprintf("Fire %d requests back-to-back into bucket(capacity=%v, refill=%v/s)", n, capacity, refill),
		"allowedCount":  allowedCount,
		"deniedCount":   deniedCount,
		"burstObserved": allowedCount == expectedAllowed && deniedCount > 0,
		"results":       results,
		"note":          "The first burst drains all capacity tokens, then requests are denied until refill catches up.",
	})
}

func (t *TokenBucket) handleRaceDemo(c *gin.Context) {
	var body struct {
		Key          string  `json:"key"`
		Capacity     float64 `json:"capacity"`
		RefillPerSec float64 `json:"refillPerSec"`
		Concurrency  int     `json:"concurrency"`
	}
	_ = c.ShouldBindJSON(&body)
	fullKey := "tb:race:" + body.Key
	t.rdb.Del(c, fullKey) // reset bucket so the demo is repeatable

	var wg sync.WaitGroup
	var mu sync.Mutex
	allowedActual := 0
	for i := 0; i < body.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each goroutine hits the SAME bucket concurrently; only the Lua
			// atomicity prevents over-granting beyond capacity.
			ok, _, _, _ := t.consume(context.Background(), fullKey, body.Capacity, body.RefillPerSec, 1)
			if ok {
				mu.Lock()
				allowedActual++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	allowedExpected := body.Concurrency
	if int(body.Capacity) < allowedExpected {
		allowedExpected = int(body.Capacity)
	}
	c.JSON(http.StatusOK, gin.H{
		"scenario":        fmt.Sprintf("Fire %d requests AT ONCE into bucket(capacity=%v)", body.Concurrency, body.Capacity),
		"allowedExpected": allowedExpected,
		"allowedActual":   allowedActual,
		"raceFree":        allowedActual == allowedExpected,
		"note":            "Lua EVALSHA runs atomically inside Redis — no race even when N clients send concurrently.",
	})
}

func main() {
	host := os.Getenv("REDIS_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("REDIS_PORT")
	if port == "" {
		port = "6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: host + ":" + port})
	tb := NewTokenBucket(rdb)

	r := gin.Default()
	g := r.Group("/api/token-bucket")
	g.POST("/consume", tb.handleConsume)
	g.GET("/state", tb.handleState)
	g.GET("/burst-demo", tb.handleBurstDemo)
	g.POST("/race-demo", tb.handleRaceDemo)

	appPort := os.Getenv("PORT")
	if appPort == "" {
		appPort = "3000"
	}
	_ = r.Run("0.0.0.0:" + appPort)
}
