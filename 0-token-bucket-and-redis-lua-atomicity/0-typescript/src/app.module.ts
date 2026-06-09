import { Module } from "@nestjs/common"
import { ConfigModule } from "@nestjs/config"
import { redisProvider } from "./redis.provider"
import { TokenBucketController } from "./token-bucket.controller"
import { TokenBucketService } from "./token-bucket.service"

@Module({
    imports: [ConfigModule.forRoot({ isGlobal: true })],
    controllers: [TokenBucketController],
    providers: [redisProvider, TokenBucketService],
})
export class AppModule {}
