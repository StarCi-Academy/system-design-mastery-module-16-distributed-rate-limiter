import { Inject, Injectable, OnModuleInit } from "@nestjs/common"
import { randomUUID } from "crypto"
import Redis from "ioredis"
import { REDIS_CLIENT } from "./redis.provider"
import { LOG_LUA, COUNTER_LUA } from "./sliding-window.lua"

export interface CheckResult {
    allowed: boolean
    count: number
}

export interface CompareResult {
    n: number
    logCount: number
    counterCount: number
    errorPct: number
    logMemory: string
    counterMemory: string
}

@Injectable()
export class SlidingWindowService implements OnModuleInit {
    public constructor(
        @Inject(REDIS_CLIENT) private readonly redis: Redis,
    ) {}

    public async onModuleInit(): Promise<void> {
        // Register both Lua scripts as first-class ioredis commands. ioredis runs
        // EVALSHA under the hood and re-uploads via EVAL on a NOSCRIPT miss, so the
        // atomic read-modify-write always executes server-side, race-free across replicas.
        this.redis.defineCommand("slidingLog", { numberOfKeys: 1, lua: LOG_LUA })
        this.redis.defineCommand("slidingCounter", { numberOfKeys: 1, lua: COUNTER_LUA })
    }

    public async checkLog(key: string, limit: number, windowMs: number): Promise<CheckResult> {
        const now: number = Date.now()
        // Unique member so two requests in the same millisecond are both recorded.
        const member = `${now}-${randomUUID()}`
        const [allowed, count] = (await (this.redis as unknown as {
            slidingLog: (k: string, ...args: (string | number)[]) => Promise<[number, number]>
        }).slidingLog(`log:${key}`, now, windowMs, limit, member))
        return { allowed: allowed === 1, count }
    }

    public async checkCounter(key: string, limit: number, windowMs: number): Promise<CheckResult> {
        const now: number = Date.now()
        const [allowed, count] = (await (this.redis as unknown as {
            slidingCounter: (k: string, ...args: (string | number)[]) => Promise<[number, number]>
        }).slidingCounter(`cnt:${key}`, now, windowMs, limit))
        return { allowed: allowed === 1, count }
    }

    public async compare(key: string, limit: number, windowMs: number, n: number): Promise<CompareResult> {
        // Reset state for both algorithms before the run so the comparison is clean.
        await this.resetKeys(key)

        let logAllowed = 0
        let counterAllowed = 0
        for (let i = 0; i < n; i += 1) {
            const log = await this.checkLog(key, limit, windowMs)
            if (log.allowed) logAllowed += 1
            const counter = await this.checkCounter(key, limit, windowMs)
            if (counter.allowed) counterAllowed += 1
        }

        const errorPct = logAllowed === 0
            ? 0
            : Math.round((Math.abs(logAllowed - counterAllowed) / logAllowed) * 100 * 1000) / 1000
        return {
            n,
            logCount: logAllowed,
            counterCount: counterAllowed,
            errorPct,
            logMemory: "O(N) — one ZSET member per request",
            counterMemory: "O(1) — two integer buckets",
        }
    }

    private async resetKeys(key: string): Promise<void> {
        const logKey = `log:${key}`
        const cntPattern = `cnt:${key}:*`
        const cntKeys = await this.redis.keys(cntPattern)
        const toDelete = [logKey, ...cntKeys]
        if (toDelete.length > 0) {
            await this.redis.del(...toDelete)
        }
    }
}
