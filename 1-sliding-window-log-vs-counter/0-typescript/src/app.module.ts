import { Module } from "@nestjs/common"
import { ConfigModule } from "@nestjs/config"
import { SlidingWindowController } from "./sliding-window.controller"
import { SlidingWindowService } from "./sliding-window.service"
import { redisProvider } from "./redis.provider"

@Module({
    imports: [ConfigModule.forRoot({ isGlobal: true })],
    controllers: [SlidingWindowController],
    providers: [redisProvider, SlidingWindowService],
})
export class AppModule {}
