package com.barabanov.dynamically.kafka.listener;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@ConfigurationProperties(prefix = "kafka.shard")
public record KafkaShardsProperties(
        Map<String, KafkaShardProperties> config) {


    public record KafkaShardProperties(
            Map<String, String> topics,
            ShardKafkaProperties properties
    ) {}


    @Data
    @EqualsAndHashCode(callSuper = true)
    @NoArgsConstructor
    public static class ShardKafkaProperties extends KafkaProperties {}
}
