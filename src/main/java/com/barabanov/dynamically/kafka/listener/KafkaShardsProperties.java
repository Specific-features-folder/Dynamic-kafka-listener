package com.barabanov.dynamically.kafka.listener;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@ConfigurationProperties(prefix = "kafka.shard")
public record KafkaShardsProperties(
        Map<String, KafkaShardProperties> config) {


    public record KafkaShardProperties(
            String kafkaBrokers,
            Map<String, TopicProperties> topicsProperties
    ) {}

    public record TopicProperties(
            String name,
            Integer concurrency,
            Long pollTimeout,
            Integer maxPollRecords
            ) {}
}
