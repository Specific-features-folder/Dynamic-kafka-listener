package com.barabanov.specific.features.kafka.shard;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;

import java.util.List;


@ConfigurationProperties(prefix = "shard")
public record ShardsConfig(
        List<ShardConfig> configs,
        KafkaProperties kafkaProperties) {

    public ShardsConfig {
        for (ShardConfig shardConfig : configs) {
            if (!StringUtils.hasText(shardConfig.shardName()))
                throw new IllegalArgumentException("В shard.configs не должно быть элемента с пустым shardName полем");
            ShardKafkaConfig shardKafkaConfig = shardConfig.kafkaConfig();
            if (shardKafkaConfig == null)
                throw new IllegalArgumentException("В shard.configs не должно быть элемента с пустым kafka-config");
            if (!StringUtils.hasText(shardKafkaConfig.requestTopicName()))
                throw new IllegalArgumentException("В shard.configs не должно быть элемента с пустым requestTopicName полем");
            if (!StringUtils.hasText(shardKafkaConfig.responseTopicName()))
                throw new IllegalArgumentException("В shard.configs не должно быть элемента с пустым responseTopicName полем");
            if (shardKafkaConfig.bootstrapServers() == null)
                throw new IllegalArgumentException("В shard.configs не должно быть элемента с пустым bootstrap-servers полем");
        }
        if (kafkaProperties == null)
            throw new IllegalArgumentException("shard.kafka-properties должны быть заполнены");
    }


    public record ShardConfig(
            String shardName,
            ShardKafkaConfig kafkaConfig
    ) {
    }

    public record ShardKafkaConfig(
            String requestTopicName,
            String responseTopicName,
            List<String> bootstrapServers
    ) {
    }

}
