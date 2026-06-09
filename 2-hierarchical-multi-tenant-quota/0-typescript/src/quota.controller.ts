import { Body, Controller, Get, HttpCode, Post, Query, Res } from "@nestjs/common"
import { Response } from "express"
import { QuotaService } from "./quota.service"

/** HTTP layer for the hierarchical quota service. */
@Controller("api/quota")
export class QuotaController {
    constructor(private readonly quota: QuotaService) {}

    /**
     * Check a single request against the three-tier cascade (user → tenant → global).
     * Returns HTTP 200 when allowed and HTTP 429 when any layer rejects, with the same
     * JSON body shape in both cases (including the blocking tier name on reject).
     * Identical status/field/value contract across all four language implementations.
     */
    @Post("check")
    async check(
        @Body() body: { tenantId: string; userId: string },
        @Res() res: Response,
    ): Promise<void> {
        const result = await this.quota.check(body.tenantId, body.userId)
        // Allowed -> HTTP 200; rejected -> HTTP 429. Same status/field/value contract
        // across all four language implementations.
        res.status(result.allowed ? 200 : 429).json(result)
    }

    /**
     * Simulate a hot tenant firing many requests across several distinct users in one
     * window.  Returns a breakdown of how many requests each tier blocked, proving
     * that a noisy tenant is isolated at its own tenant tier (not the shared global tier).
     */
    @Post("hot-tenant-demo")
    @HttpCode(200)
    async hotTenantDemo(
        @Body() body: { tenantId: string; requests: number; distinctUsers: number },
    ): Promise<unknown> {
        const counts = await this.quota.hotTenantDemo(
            body.tenantId,
            body.requests,
            body.distinctUsers,
        )
        return {
            tenantId: body.tenantId,
            requests: body.requests,
            distinctUsers: body.distinctUsers,
            counts,
        }
    }

    /**
     * Return cluster topology info and confirm that a tenant's three tier keys all hash
     * to the same slot, proving the `{q:<tenantId>}` hash-tag is routing them correctly.
     * @param tenantId Tenant to inspect (defaults to "t1").
     */
    @Get("cluster-info")
    @HttpCode(200)
    async clusterInfo(@Query("tenantId") tenantId = "t1"): Promise<unknown> {
        return this.quota.clusterInfo(tenantId)
    }
}
