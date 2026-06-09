import { Inject, Injectable, OnModuleInit } from "@nestjs/common"
import Redis from "ioredis"
import { REDIS_CLIENT } from "./redis.provider"
import { TOKEN_BUCKET_LUA } from "./token-bucket.lua"

const ALGORITHM = "Token Bucket (atomic via Redis Lua EVALSHA)"

export interface ConsumeInput {
    key: string
    capacity: number
    refillPerSec: number
    tokens?: number
}

export interface ConsumeResult {
    allowed: boolean
    remaining: number
    retryAfterMs: number
    algorithm: string
    note: string
}

@Injectable()
export class TokenBucketService implements OnModuleInit {
    private scriptSha!: string

    constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

    async onModuleInit(): Promise<void> {
        // Load the Lua source into Redis ONCE at boot; Redis returns its SHA1
        // digest. From now on we send the 40-char sha (not the whole script)
        // on every consume, so the script body travels the network only once.
        this.scriptSha = (await this.redis.script("LOAD", TOKEN_BUCKET_LUA)) as string
    }

    private async evalScript(
        fullKey: string,
        capacity: number,
        refillPerSec: number,
        want: number,
    ): Promise<[number, number, number]> {
        // One atomic round-trip: the host passes only data, all logic lives in Lua.
        return (await this.redis.evalsha(
            this.scriptSha,
            1, // number of KEYS
            fullKey, // KEYS[1]
            capacity.toString(), // ARGV[1]
            refillPerSec.toString(), // ARGV[2]
            Date.now().toString(), // ARGV[3] nowMs
            want.toString(), // ARGV[4]
        )) as [number, number, number]
    }

    async consume(input: ConsumeInput): Promise<ConsumeResult> {
        const { key, capacity, refillPerSec } = input
        const want = input.tokens && input.tokens > 0 ? input.tokens : 1
        const [allowed, remaining, retryAfterMs] = await this.evalScript(
            `tb:${key}`,
            capacity,
            refillPerSec,
            want,
        )
        const ok = allowed === 1
        return {
            allowed: ok,
            remaining,
            retryAfterMs,
            algorithm: ALGORITHM,
            note: ok
                ? "Tokens available — request allowed."
                : `Out of tokens — wait ${retryAfterMs}ms for the next refill.`,
        }
    }

    // GET /api/token-bucket/state — inspect the bucket WITHOUT consuming a token.
    async state(key: string): Promise<{
        key: string
        tokens: number
        lastTs: number
        exists: boolean
    }> {
        const data = await this.redis.hmget(`tb:${key}`, "tokens", "ts")
        const exists = data[0] !== null
        return {
            key,
            tokens: exists ? Math.floor(Number(data[0])) : 0,
            lastTs: exists ? Number(data[1]) : 0,
            exists,
        }
    }

    // GET /api/token-bucket/burst-demo — fire n sequential consumes into a fresh bucket.
    async burstDemo(
        key: string,
        capacity: number,
        refillPerSec: number,
        n: number,
    ): Promise<{
        scenario: string
        allowedCount: number
        deniedCount: number
        burstObserved: boolean
        results: Array<{ i: number; allowed: boolean; remaining: number }>
        note: string
    }> {
        const fullKey = `burst:${key}`
        await this.redis.del(`tb:${fullKey}`) // reset so the demo is repeatable
        const results: Array<{ i: number; allowed: boolean; remaining: number }> = []
        for (let i = 0; i < n; i++) {
            const [allowed, remaining] = await this.evalScript(
                `tb:${fullKey}`,
                capacity,
                refillPerSec,
                1,
            )
            results.push({ i, allowed: allowed === 1, remaining })
        }
        const allowedCount = results.filter((r) => r.allowed).length
        const deniedCount = results.length - allowedCount
        return {
            scenario: `Fire ${n} requests back-to-back into bucket(capacity=${capacity}, refill=${refillPerSec}/s)`,
            allowedCount,
            deniedCount,
            burstObserved: allowedCount === Math.min(n, capacity) && deniedCount > 0,
            results,
            note: "The first burst drains all capacity tokens, then requests are denied until refill catches up.",
        }
    }

    // POST /api/token-bucket/race-demo — fire `concurrency` consumes at once.
    async raceDemo(input: {
        key: string
        capacity: number
        refillPerSec: number
        concurrency: number
    }): Promise<{
        scenario: string
        allowedExpected: number
        allowedActual: number
        raceFree: boolean
        note: string
    }> {
        const { key, capacity, refillPerSec, concurrency } = input
        const fullKey = `race:${key}`
        await this.redis.del(`tb:${fullKey}`) // reset bucket so the demo is repeatable
        // Fire N consume() calls truly concurrently. Without Lua atomicity a naive
        // GET/SET limiter would over-grant here; with EVALSHA exactly `capacity` win.
        const calls = Array.from({ length: concurrency }, () =>
            this.evalScript(`tb:${fullKey}`, capacity, refillPerSec, 1),
        )
        const results = await Promise.all(calls)
        const allowedActual = results.filter(([allowed]) => allowed === 1).length
        const allowedExpected = Math.min(concurrency, capacity)
        return {
            scenario: `Fire ${concurrency} requests AT ONCE into bucket(capacity=${capacity})`,
            allowedExpected,
            allowedActual,
            raceFree: allowedActual === allowedExpected,
            note: "Lua EVALSHA runs atomically inside Redis — no race even when N clients send concurrently.",
        }
    }
}
