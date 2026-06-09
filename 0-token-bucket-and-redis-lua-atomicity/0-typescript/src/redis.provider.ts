import { Provider } from "@nestjs/common"
import { ConfigService } from "@nestjs/config"
import Redis from "ioredis"

export const REDIS_CLIENT = "REDIS_CLIENT"

export const redisProvider: Provider = {
    provide: REDIS_CLIENT,
    inject: [ConfigService],
    useFactory: (cs: ConfigService): Redis => {
        // Single shared ioredis connection for the whole app; the persistent
        // TCP socket lets EVALSHA round-trips stay cheap under load.
        return new Redis({
            host: cs.get<string>("REDIS_HOST") ?? "localhost",
            port: Number(cs.get<string>("REDIS_PORT") ?? 6379),
        })
    },
}
