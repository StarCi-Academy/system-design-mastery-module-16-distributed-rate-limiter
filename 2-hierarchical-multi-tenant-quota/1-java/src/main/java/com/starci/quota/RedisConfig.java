package com.starci.quota;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;

@Configuration
public class RedisConfig {

    @Value("${REDIS_CLUSTER_NODES:redis-1:6379,redis-2:6379,redis-3:6379}")
    private String clusterNodes;

    // One shared cluster client + connection. Lettuce keeps a persistent
    // connection per node and routes commands by slot.
    @Bean(destroyMethod = "shutdown")
    public RedisClusterClient redisClusterClient() {
        List<RedisURI> seeds = Arrays.stream(clusterNodes.split(","))
                .map(node -> RedisURI.create("redis://" + node.trim()))
                .toList();
        return RedisClusterClient.create(seeds);
    }

    @Bean(destroyMethod = "close")
    public StatefulRedisClusterConnection<String, String> clusterConnection(RedisClusterClient client) {
        return client.connect();
    }
}
