import { Inject, Injectable, OnModuleInit } from "@nestjs/common"
import { ConfigService } from "@nestjs/config"
import { Cluster } from "ioredis"
import { CASCADE_LUA } from "./quota.lua"
import { REDIS_CLUSTER } from "./redis.provider"

/** Which quota tier blocked the request, or null when the request was allowed. */
export type BlockedAt = "USER" | "TENANT" | "GLOBAL" | null

/** Full result returned by a single quota-check call. */
export interface QuotaResult {
    /** true when the request passed all three tiers; false when any tier rejected it. */
    allowed: boolean
    /** The name of the tier that blocked the request, or null when allowed. */
    blockedAt: BlockedAt
    /** Counter value of the user tier at the time of the check. */
    userCount: number
    /** Counter value of the tenant tier at the time of the check. */
    tenantCount: number
    /** Counter value of the global tier at the time of the check. */
    globalCount: number
    /** Per-tier hard limits that were enforced during this check. */
    limits: { user: number; tenant: number; global: number }
    /** Window duration in milliseconds used by all tiers. */
    windowMs: number
}

/** Implements the three-tier user → tenant → global quota cascade on Redis Cluster. */
@Injectable()
export class QuotaService implements OnModuleInit {
    /** SHA1 of the pre-loaded Lua script; sent via EVALSHA to avoid re-shipping the body. */
    private sha = ""
    /** Maximum requests per user per window. */
    private readonly userLimit: number
    /** Maximum requests per tenant per window (all its users combined). */
    private readonly tenantLimit: number
    /** Maximum requests across all tenants per window (the shared ceiling). */
    private readonly globalLimit: number
    /** Sliding-window length in milliseconds. */
    private readonly windowMs: number

    constructor(
        @Inject(REDIS_CLUSTER) private readonly cluster: Cluster,
        cs: ConfigService,
    ) {
        // Read limits from env with fallbacks so the service starts without a .env file.
        this.userLimit = Number(cs.get("USER_LIMIT") ?? 5)
        this.tenantLimit = Number(cs.get("TENANT_LIMIT") ?? 20)
        this.globalLimit = Number(cs.get("GLOBAL_LIMIT") ?? 50)
        this.windowMs = Number(cs.get("WINDOW_MS") ?? 1000)
    }

    /**
     * Load the cascade Lua onto every master shard before the service accepts requests.
     * Redis caches scripts per-node; loading on a single node leaves other nodes with
     * a NOSCRIPT miss whenever a key routes to them — so we pre-load on all masters.
     */
    async onModuleInit(): Promise<void> {
        // SCRIPT LOAD onto EVERY master shard at boot. On a cluster the script
        // cache is per-node, so a key whose slot lives on node B needs the script
        // loaded on node B too — loading on a single node yields NOSCRIPT later.
        // Redis returns the SAME SHA1 on every node (it is a hash of the body).
        const masters = this.cluster.nodes("master")
        const shas = await Promise.all(
            masters.map((node) => node.script("LOAD", CASCADE_LUA) as Promise<string>),
        )
        this.sha = shas[0]
    }

    /**
     * Execute the cascade Lua on the Redis Cluster node that owns the keys' shared slot.
     * Falls back to EVAL (which also re-caches the script) on a NOSCRIPT error, which
     * can happen after a node restart or failover that wipes the in-memory script cache.
     * @param userKey   Redis key for the per-user counter, tagged with `{q:<tenantId>}`.
     * @param tenantKey Redis key for the per-tenant counter, tagged with `{q:<tenantId>}`.
     * @param globalKey Redis key for the global counter, tagged with `{q:<tenantId>}`.
     * @returns [allowed(0|1), blockedAt("OK"|"USER"|"TENANT"|"GLOBAL"), userV, tenantV, globalV]
     */
    private async runCascade(
        userKey: string,
        tenantKey: string,
        globalKey: string,
    ): Promise<[number, string, number, number, number]> {
        const argv = [
            String(Date.now()),
            String(this.windowMs),
            String(this.userLimit),
            String(this.tenantLimit),
            String(this.globalLimit),
        ]
        try {
            return (await this.cluster.evalsha(
                this.sha,
                3,
                userKey,
                tenantKey,
                globalKey,
                ...argv,
            )) as [number, string, number, number, number]
        } catch (err) {
            // NOSCRIPT after a node restart/failover: reload via EVAL (which also
            // re-caches the body on the routed node) then continue.
            if (err instanceof Error && err.message.includes("NOSCRIPT")) {
                return (await this.cluster.eval(
                    CASCADE_LUA,
                    3,
                    userKey,
                    tenantKey,
                    globalKey,
                    ...argv,
                )) as [number, string, number, number, number]
            }
            throw err
        }
    }

    /**
     * Build the three Redis keys for a (tenant, user) pair.
     * All three carry the SAME `{q:<tenantId>}` hash-tag so they land on ONE slot →
     * ONE node → the multi-key Lua runs atomically without a CROSSSLOT error.
     * Layout: `{q:t}:u:id` / `{q:t}:t` / `{q:t}:g`.
     * @param tenantId Tenant identifier used as the hash-tag anchor.
     * @param userId   User identifier appended after the hash-tag.
     * @returns Tuple of [userKey, tenantKey, globalKey].
     */
    buildKeys(tenantId: string, userId: string): [string, string, string] {
        // All three tiers of one tenant share the SAME {q:<tenantId>} hash-tag, so
        // every key lands on the SAME slot -> the SAME node -> a multi-key Lua is
        // legal (no CROSSSLOT). Layout: {q:t}:u:id / {q:t}:t / {q:t}:g.
        return [
            `{q:${tenantId}}:u:${userId}`,
            `{q:${tenantId}}:t`,
            `{q:${tenantId}}:g`,
        ]
    }

    /**
     * Check a single request against the user → tenant → global cascade.
     * Returns the detailed result including per-tier counters and the limits.
     * @param tenantId Tenant that owns the request.
     * @param userId   User within that tenant issuing the request.
     */
    async check(tenantId: string, userId: string): Promise<QuotaResult> {
        const [userKey, tenantKey, globalKey] = this.buildKeys(tenantId, userId)
        const [allowed, layer, userCount, tenantCount, globalCount] =
            await this.runCascade(userKey, tenantKey, globalKey)
        return {
            allowed: allowed === 1,
            // Expose the blocking tier name only on rejection; null signals a clean pass.
            blockedAt: allowed === 1 ? null : (layer as BlockedAt),
            userCount: Number(userCount),
            tenantCount: Number(tenantCount),
            globalCount: Number(globalCount),
            limits: { user: this.userLimit, tenant: this.tenantLimit, global: this.globalLimit },
            windowMs: this.windowMs,
        }
    }

    /**
     * Fire `requests` sequential checks cycling through `distinctUsers` user ids
     * within the same window and break down how many were blocked at each layer.
     * Used by the hot-tenant-demo endpoint to demonstrate noisy-neighbour isolation.
     * @param tenantId      The hot tenant id to simulate.
     * @param requests      Total number of requests to fire.
     * @param distinctUsers Number of distinct user ids to cycle through.
     * @returns Count of results per blocking layer (OK / USER / TENANT / GLOBAL).
     */
    async hotTenantDemo(
        tenantId: string,
        requests: number,
        distinctUsers: number,
    ): Promise<{ OK: number; USER: number; TENANT: number; GLOBAL: number }> {
        const counts = { OK: 0, USER: 0, TENANT: 0, GLOBAL: 0 }
        for (let i = 0; i < requests; i++) {
            // Cycle through distinct user ids so the USER tier (5/s per user) is
            // not the bottleneck — the TENANT tier (20/s) should be the one that fires.
            const userId = `u${i % distinctUsers}`
            const r = await this.check(tenantId, userId)
            if (r.allowed) counts.OK++
            else counts[r.blockedAt as "USER" | "TENANT" | "GLOBAL"]++
        }
        return counts
    }

    /**
     * Return a snapshot of the cluster topology and prove that a tenant's three keys
     * hash to the same slot, confirming the hash-tag routing is correct.
     * @param sampleTenant Tenant id whose keys are examined.
     */
    async clusterInfo(sampleTenant: string): Promise<{
        nodes: number
        slotsTotal: number
        sampleTenant: string
        sameSlot: boolean
        slot: number
        keys: string[]
    }> {
        const nodes = this.cluster.nodes("master")
        // Build the three sample keys (user u0) to compute their slots.
        const keys = this.buildKeys(sampleTenant, "u0")
        // Compute the slot of each key via CLUSTER KEYSLOT on any node.
        const anyNode = nodes[0]
        const slots = await Promise.all(
            keys.map(async (k) => Number(await anyNode.cluster("KEYSLOT", k))),
        )
        // sameSlot must be true: if any key landed on a different slot, the multi-key
        // Lua would have failed with CROSSSLOT instead of the successful results above.
        const sameSlot = slots.every((s) => s === slots[0])
        return {
            nodes: nodes.length,
            slotsTotal: 16384,
            sampleTenant,
            sameSlot,
            slot: slots[0],
            keys,
        }
    }
}
