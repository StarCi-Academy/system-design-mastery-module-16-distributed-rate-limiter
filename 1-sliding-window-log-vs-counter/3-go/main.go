package main

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// The two Lua scripts are byte-for-byte identical across all four language
// implementations. They run inside Redis (atomic read-modify-write), so the HTTP
// contract and numeric outcomes are unchanged regardless of the host client.
const logLua = `
local now      = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])
local limit    = tonumber(ARGV[3])
local member   = ARGV[4]

redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - windowMs)
local count = redis.call('ZCARD', KEYS[1])

if count < limit then
  redis.call('ZADD', KEYS[1], now, member)
  redis.call('PEXPIRE', KEYS[1], windowMs)
  return {1, count + 1}
end

return {0, count}
`

const counterLua = `
local now      = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])
local limit    = tonumber(ARGV[3])

local currBucket   = math.floor(now / windowMs)
local prevBucket   = currBucket - 1
local elapsedRatio = (now % windowMs) / windowMs

local currKey = KEYS[1] .. ':' .. currBucket
local prevKey = KEYS[1] .. ':' .. prevBucket

local prevCount = tonumber(redis.call('GET', prevKey) or '0')
local currCount = tonumber(redis.call('GET', currKey) or '0')

local estimate = prevCount * (1 - elapsedRatio) + currCount

if estimate < limit then
  redis.call('INCR', currKey)
  redis.call('PEXPIRE', currKey, windowMs * 2)
  return {1, math.floor(estimate) + 1}
end

return {0, math.floor(estimate)}
`

// Limiter holds one shared go-redis client and the cached SHA1 digests.
type Limiter struct {
	rdb        *redis.Client
	logSha     string
	counterSha string
}

type limitResult struct {
	allowed bool
	count   int64
}

func newLimiter(ctx context.Context, rdb *redis.Client) (*Limiter, error) {
	logSha, err := rdb.ScriptLoad(ctx, logLua).Result()
	if err != nil {
		return nil, err
	}
	counterSha, err := rdb.ScriptLoad(ctx, counterLua).Result()
	if err != nil {
		return nil, err
	}
	return &Limiter{rdb: rdb, logSha: logSha, counterSha: counterSha}, nil
}

func toResult(raw interface{}) limitResult {
	pair := raw.([]interface{})
	return limitResult{allowed: pair[0].(int64) == 1, count: pair[1].(int64)}
}

func (l *Limiter) checkLog(ctx context.Context, key string, limit, windowMs int64) (limitResult, error) {
	now := time.Now().UnixMilli()
	member := fmt.Sprintf("%d-%s", now, uuid.NewString())
	raw, err := l.rdb.EvalSha(ctx, l.logSha, []string{"log:" + key}, now, windowMs, limit, member).Result()
	if err != nil {
		return limitResult{}, err
	}
	return toResult(raw), nil
}

func (l *Limiter) checkCounter(ctx context.Context, key string, limit, windowMs int64) (limitResult, error) {
	now := time.Now().UnixMilli()
	raw, err := l.rdb.EvalSha(ctx, l.counterSha, []string{"cnt:" + key}, now, windowMs, limit).Result()
	if err != nil {
		return limitResult{}, err
	}
	return toResult(raw), nil
}

func (l *Limiter) resetKeys(ctx context.Context, key string) {
	l.rdb.Del(ctx, "log:"+key)
	keys, _ := l.rdb.Keys(ctx, "cnt:"+key+":*").Result()
	if len(keys) > 0 {
		l.rdb.Del(ctx, keys...)
	}
}

func main() {
	host := getEnv("REDIS_HOST", "localhost")
	port := getEnv("REDIS_PORT", "6379")
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{Addr: host + ":" + port})
	limiter, err := newLimiter(ctx, rdb)
	if err != nil {
		panic(err)
	}

	r := gin.Default()

	r.POST("/api/sliding-window/log/check", func(c *gin.Context) {
		var req struct {
			Key      string `json:"key"`
			Limit    int64  `json:"limit"`
			WindowMs int64  `json:"windowMs"`
		}
		_ = c.ShouldBindJSON(&req)
		res, err := limiter.checkLog(c, req.Key, req.Limit, req.WindowMs)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		// Shared contract: { algorithm, allowed, count, limit }.
		c.JSON(http.StatusOK, gin.H{"algorithm": "log", "allowed": res.allowed, "count": res.count, "limit": req.Limit})
	})

	r.POST("/api/sliding-window/counter/check", func(c *gin.Context) {
		var req struct {
			Key      string `json:"key"`
			Limit    int64  `json:"limit"`
			WindowMs int64  `json:"windowMs"`
		}
		_ = c.ShouldBindJSON(&req)
		res, err := limiter.checkCounter(c, req.Key, req.Limit, req.WindowMs)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"algorithm": "counter", "allowed": res.allowed, "count": res.count, "limit": req.Limit})
	})

	r.GET("/api/sliding-window/compare", func(c *gin.Context) {
		key := c.DefaultQuery("key", "demo")
		limit := parseInt(c.DefaultQuery("limit", "5"))
		windowMs := parseInt(c.DefaultQuery("windowMs", "2000"))
		n := int(parseInt(c.DefaultQuery("n", "10")))

		limiter.resetKeys(c, key)
		var logAllowed, counterAllowed int64
		for i := 0; i < n; i++ {
			if lr, _ := limiter.checkLog(c, key, limit, windowMs); lr.allowed {
				logAllowed++
			}
			if cr, _ := limiter.checkCounter(c, key, limit, windowMs); cr.allowed {
				counterAllowed++
			}
		}
		errorPct := 0.0
		if logAllowed != 0 {
			errorPct = math.Round(math.Abs(float64(logAllowed-counterAllowed))/float64(logAllowed)*100*1000) / 1000
		}
		c.JSON(http.StatusOK, gin.H{
			"n":             n,
			"logCount":      logAllowed,
			"counterCount":  counterAllowed,
			"errorPct":      errorPct,
			"logMemory":     "O(N) — one ZSET member per request",
			"counterMemory": "O(1) — two integer buckets",
		})
	})

	_ = r.Run(":3000")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseInt(s string) int64 {
	var n int64
	_, _ = fmt.Sscanf(s, "%d", &n)
	return n
}
