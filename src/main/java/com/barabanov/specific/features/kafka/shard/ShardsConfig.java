package com.barabanov.specific.features.kafka.shard;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.List;

//TODO: Возможно, избавиться от лишнего класса-оболочки (если это возможно сделать безболезненно).
// Но можно и оставить для других properties шард на одном уровне с массивом конфигураций
@ConfigurationProperties(prefix = "shard")
public record ShardsConfig(
        List<ShardConfig> configs) {

    public ShardsConfig(List<ShardConfig> configs) {
        for (ShardConfig shardConfig : configs) {
            if (!StringUtils.hasText(shardConfig.shardName()))
                throw new IllegalArgumentException("В configs-arr не должно быть элемента с пустым shardName полем");
            ShardKafkaConfig shardKafkaConfig = shardConfig.kafkaConfig();
            if (shardKafkaConfig == null)
                throw new IllegalArgumentException("В configs-arr не должно быть элемента с пустым kafka-config");
            if (!StringUtils.hasText(shardKafkaConfig.requestTopicName()))
                throw new IllegalArgumentException("В configs-arr не должно быть элемента с пустым requestTopicName полем");
            if (!StringUtils.hasText(shardKafkaConfig.responseTopicName()))
                throw new IllegalArgumentException("В configs-arr не должно быть элемента с пустым responseTopicName полем");
            if (shardKafkaConfig.properties() == null)
                throw new IllegalArgumentException("В configs-arr не должно быть элемента с пустым properties полем для kafka");
            if (CollectionUtils.isEmpty(shardKafkaConfig.properties.getBootstrapServers()))
                throw new IllegalArgumentException("В configs-arr не должно быть элемента с пустыми brokers в properties для kafka");
        }
        this.configs = configs;
    }


    public record ShardConfig(
            String shardName,
            ShardKafkaConfig kafkaConfig
    ) {
    }
    
    public record ShardKafkaConfig(
            String requestTopicName,
            String responseTopicName,
            KafkaProperties properties
    ) {
    }

}
