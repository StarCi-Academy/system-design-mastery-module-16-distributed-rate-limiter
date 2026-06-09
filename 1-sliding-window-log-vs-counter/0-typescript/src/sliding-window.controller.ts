import { Body, Controller, Get, Post, Query } from "@nestjs/common"
import { SlidingWindowService } from "./sliding-window.service"

interface CheckBody {
    key: string
    limit: number
    windowMs: number
}

@Controller("api/sliding-window")
export class SlidingWindowController {
    public constructor(private readonly service: SlidingWindowService) {}

    @Post("log/check")
    public async logCheck(@Body() body: CheckBody): Promise<Record<string, unknown>> {
        const r = await this.service.checkLog(body.key, body.limit, body.windowMs)
        // Shared contract: { algorithm, allowed, count, limit }.
        return { algorithm: "log", allowed: r.allowed, count: r.count, limit: body.limit }
    }

    @Post("counter/check")
    public async counterCheck(@Body() body: CheckBody): Promise<Record<string, unknown>> {
        const r = await this.service.checkCounter(body.key, body.limit, body.windowMs)
        return { algorithm: "counter", allowed: r.allowed, count: r.count, limit: body.limit }
    }

    @Get("compare")
    public async compare(
        @Query("key") key = "demo",
        @Query("limit") limit = "5",
        @Query("windowMs") windowMs = "2000",
        @Query("n") n = "10",
    ): Promise<Record<string, unknown>> {
        const result = await this.service.compare(key, Number(limit), Number(windowMs), Number(n))
        return { ...result }
    }
}
