package com.barabanov.dynamically.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.*;


@Configuration
public class ShardsKafkaConfiguration {

    private static final String RESPONSE_TOPIC_PROPERTY = "response-topic";


    @Bean
    public Map<String, List<KafkaListenerEndpoint>> shardsKafkaEndpoints(ShardsKafkaConfigs shardsKafkaConfigs,
                                                                         KafkaMessageHandler<Response> resposeKafkaMessageHandler) {

        Map<String, ShardsKafkaConfigs.ShardKafkaConfig> shardsKafkaConfigMap = shardsKafkaConfigs.config();
        if (CollectionUtils.isEmpty(shardsKafkaConfigMap))
            return Collections.emptyMap();

        Map<String, List<KafkaListenerEndpoint>> shardsKafkaEndpoints = new HashMap<>();
        for (Map.Entry<String, ShardsKafkaConfigs.ShardKafkaConfig> shardKafkaConfig : shardsKafkaConfigMap.entrySet()) {
            String shardName = shardKafkaConfig.getKey();
            List<KafkaListenerEndpoint> endPointsForShard = createKafkaEndPointsForShard(shardName, shardKafkaConfig.getValue(), resposeKafkaMessageHandler);
            shardsKafkaEndpoints.put(shardName, endPointsForShard);
        }

        return shardsKafkaEndpoints;
    }


    private List<KafkaListenerEndpoint> createKafkaEndPointsForShard(String shardName,
                                                                     ShardsKafkaConfigs.ShardKafkaConfig shardProperties,
                                                                     KafkaMessageHandler<Response> resposeKafkaMessageHandler) {

        MethodKafkaListenerEndpoint<String, Response> responseKafkaEndPoint = createDefaultMethodKafkaListenerEndpoint(
                shardName,
                shardProperties.topics().get(RESPONSE_TOPIC_PROPERTY),
                resposeKafkaMessageHandler,
                "rsp");

        return List.of(responseKafkaEndPoint);
    }


    private <K, V> MethodKafkaListenerEndpoint<K, V> createDefaultMethodKafkaListenerEndpoint(String shardName,
                                                                                              String topic,
                                                                                              KafkaMessageHandler<V> kafkaMessageHandler,
                                                                                              String topicAlias) {
        KafkaMessageListenerImpl<K, V> kafkaMessageListener = new KafkaMessageListenerImpl<>(shardName, kafkaMessageHandler);

        MethodKafkaListenerEndpoint<K, V> kafkaListenerEndpoint = new MethodKafkaListenerEndpoint<>();
        kafkaListenerEndpoint.setBean(kafkaMessageListener);

        kafkaListenerEndpoint.setId(generateListenerId(shardName, topicAlias));
        kafkaListenerEndpoint.setAutoStartup(true);
        kafkaListenerEndpoint.setTopics(topic);
        kafkaListenerEndpoint.setMessageHandlerMethodFactory(new DefaultMessageHandlerMethodFactory()); //TODO: думаю factory лучше бином

        Method onMessageMethod = ReflectionUtils.findMethod(KafkaMessageListenerImpl.class, "onMessage", ConsumerRecord.class);
        kafkaListenerEndpoint.setMethod(Objects.requireNonNull(onMessageMethod));

        return kafkaListenerEndpoint;
    }

    private String generateListenerId(String shardName, String topicAlias) {
        return shardName + "-" + topicAlias;
    }


    @Bean
    public Map<String, ConcurrentKafkaListenerContainerFactory<Object, Object>> shardsKafkaListenerContainerFactories(
            ShardsKafkaConfigs shardsKafkaConfigs) {

        //TODO: посмотреть как задаются concurrency и другие настройки
        Map<String, ShardsKafkaConfigs.ShardKafkaConfig> shardsKafkaConfigMap = shardsKafkaConfigs.config();
        if (CollectionUtils.isEmpty(shardsKafkaConfigMap))
            return Collections.emptyMap();

        Map<String, ConcurrentKafkaListenerContainerFactory<Object, Object>> shardsKafkaListenerContainerFactories = new HashMap<>();
        for (Map.Entry<String, ShardsKafkaConfigs.ShardKafkaConfig> shardKafkaConfig : shardsKafkaConfigMap.entrySet()) {
            DefaultKafkaConsumerFactory<Object, Object> kafkaConsumerFactory = new DefaultKafkaConsumerFactory<>(shardKafkaConfig.getValue().properties().buildConsumerProperties());
            ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<>();
            kafkaListenerContainerFactory.setConsumerFactory(kafkaConsumerFactory);

            shardsKafkaListenerContainerFactories.put(shardKafkaConfig.getKey(), kafkaListenerContainerFactory);
        }

        return shardsKafkaListenerContainerFactories;
    }


    @Bean
    public Map<String, KafkaTemplate<Object, Object>> shardsKafkaTemplates(ShardsKafkaConfigs shardsKafkaConfigs) {

        Map<String, ShardsKafkaConfigs.ShardKafkaConfig> shardsKafkaConfigMap = shardsKafkaConfigs.config();
        if (CollectionUtils.isEmpty(shardsKafkaConfigMap))
            return Collections.emptyMap();

        Map<String, KafkaTemplate<Object, Object>> shardsKafkaTemplates = new HashMap<>();
        for (Map.Entry<String, ShardsKafkaConfigs.ShardKafkaConfig> shardKafkaConfig : shardsKafkaConfigMap.entrySet()) {

            DefaultKafkaProducerFactory<Object, Object> kafkaProducerFactory = new DefaultKafkaProducerFactory<>(shardKafkaConfig.getValue().properties().buildProducerProperties());
            KafkaTemplate<Object, Object> kafkaTemplate = new KafkaTemplate<>(kafkaProducerFactory);
            shardsKafkaTemplates.put(shardKafkaConfig.getKey(), kafkaTemplate);
        }

        return shardsKafkaTemplates;
    }

}
