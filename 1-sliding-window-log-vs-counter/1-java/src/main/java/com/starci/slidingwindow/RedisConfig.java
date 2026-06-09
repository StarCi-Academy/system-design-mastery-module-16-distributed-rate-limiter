package com.starci.slidingwindow;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedisConfig {

    @Bean
    public StatefulRedisConnection<String, String> redisConnection(
            @Value("${redis.host}") String host,
            @Value("${redis.port}") int port) {
        RedisClient client = RedisClient.create(RedisURI.create(host, port));
        return client.connect();
    }
}
