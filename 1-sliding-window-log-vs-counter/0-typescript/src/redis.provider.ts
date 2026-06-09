import { Provider } from "@nestjs/common"
import { ConfigService } from "@nestjs/config"
import Redis from "ioredis"

export const REDIS_CLIENT = "REDIS_CLIENT"

// One shared ioredis client to a single Redis node. The two Lua scripts run
// server-side via EVALSHA, so the read-modify-write stays atomic across replicas.
export const redisProvider: Provider = {
    provide: REDIS_CLIENT,
    inject: [ConfigService],
    useFactory: (cs: ConfigService): Redis => {
        const host = cs.get<string>("REDIS_HOST") ?? "localhost"
        const port = Number(cs.get<string>("REDIS_PORT") ?? "6379")
        return new Redis({ host, port, connectTimeout: 10000 })
    },
}
