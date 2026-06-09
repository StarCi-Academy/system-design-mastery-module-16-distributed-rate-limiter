import { Provider } from "@nestjs/common"
import { ConfigService } from "@nestjs/config"
import { Cluster } from "ioredis"

/** Injection token for the shared ioredis Cluster client. */
export const REDIS_CLUSTER = "REDIS_CLUSTER"

/**
 * NestJS provider that creates the single shared ioredis Cluster client.
 * The client maintains a live connection to every master shard, learns the cluster
 * topology (slot → node mapping) at startup, and routes each command automatically.
 * The API container runs INSIDE the compose network, so it can reach the internal
 * container IPs that Redis announces during the CLUSTER MEET handshake.
 */
export const redisClusterProvider: Provider = {
    provide: REDIS_CLUSTER,
    inject: [ConfigService],
    useFactory: (cs: ConfigService): Cluster => {
        const seeds = (cs.get<string>("REDIS_CLUSTER_NODES") ?? "redis-1:6379,redis-2:6379,redis-3:6379")
            .split(",")
            .map((entry) => {
                const [host, port] = entry.trim().split(":")
                return { host, port: Number(port) }
            })
        return new Cluster(seeds, {
            redisOptions: { connectTimeout: 10000 },
            clusterRetryStrategy: (times) => Math.min(times * 200, 2000),
        })
    },
}
