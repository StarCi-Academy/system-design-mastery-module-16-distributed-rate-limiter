import { Module } from "@nestjs/common"
import { ConfigModule } from "@nestjs/config"
import { QuotaController } from "./quota.controller"
import { QuotaService } from "./quota.service"
import { redisClusterProvider } from "./redis.provider"

@Module({
    imports: [ConfigModule.forRoot({ isGlobal: true })],
    controllers: [QuotaController],
    providers: [redisClusterProvider, QuotaService],
})
export class AppModule {}
