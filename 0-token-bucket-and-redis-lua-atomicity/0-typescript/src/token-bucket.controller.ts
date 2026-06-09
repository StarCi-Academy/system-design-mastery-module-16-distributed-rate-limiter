import { Body, Controller, Get, Post, Query } from "@nestjs/common"
import { ConsumeInput, TokenBucketService } from "./token-bucket.service"

@Controller("api/token-bucket")
export class TokenBucketController {
    constructor(private readonly service: TokenBucketService) {}

    @Post("consume")
    consume(@Body() body: ConsumeInput) {
        return this.service.consume(body)
    }

    @Get("state")
    state(@Query("key") key: string) {
        return this.service.state(key)
    }

    @Get("burst-demo")
    burstDemo(
        @Query("key") key: string,
        @Query("capacity") capacity: string,
        @Query("refill") refill: string,
        @Query("n") n: string,
    ) {
        return this.service.burstDemo(key, Number(capacity), Number(refill), Number(n))
    }

    @Post("race-demo")
    raceDemo(
        @Body()
        body: { key: string; capacity: number; refillPerSec: number; concurrency: number },
    ) {
        return this.service.raceDemo(body)
    }
}
