package com.barabanov.dynamically.kafka.listener;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@ConfigurationProperties(prefix = "shards.kafka")
public record ShardsKafkaConfigs(
        Map<String, ShardKafkaConfig> config) {


    public record ShardKafkaConfig(
            Map<String, String> topics,
            ShardKafkaProperties properties
    ) {}


    @Data
    @EqualsAndHashCode(callSuper = true)
    @NoArgsConstructor
    public static class ShardKafkaProperties extends KafkaProperties {}
}
