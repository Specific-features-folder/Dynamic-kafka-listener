package com.barabanov.dynamically.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


@Configuration
public class DynamicKafkaConsumersConfiguration {

    String kafkaListenerId = "kafkaListenerId-";
    static AtomicLong endpointIdIndex = new AtomicLong(1);


    @Bean
    public Map<String, KafkaListenerEndpoint> createKafkaListenerEndpoint(Map<String, KafkaMessageListenerImpl> kafkaListenerEndpoints,
                                                                           KafkaShardsProperties kafkaShardsProperties) {
        Map<String, KafkaShardsProperties.KafkaShardProperties> shardNameToProperties = kafkaShardsProperties.config();

        Map<String, KafkaListenerEndpoint> endpointMap = new HashMap<>();
        for (Map.Entry<String, KafkaMessageListenerImpl> entry : kafkaListenerEndpoints.entrySet()) {
            MethodKafkaListenerEndpoint<String, String> kafkaListenerEndpoint =
                    createDefaultMethodKafkaListenerEndpoint(shardNameToProperties.get(entry.getKey()).topics().get("response-topic"));
            kafkaListenerEndpoint.setBean(entry.getValue());
            try {
                kafkaListenerEndpoint.setMethod(KafkaMessageListenerImpl.class.getMethod("onMessage", ConsumerRecord.class));
                endpointMap.put(entry.getKey(), kafkaListenerEndpoint);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Attempt to call a non-existent method " + e);
            }
        }
        return endpointMap;
    }


    private MethodKafkaListenerEndpoint<String, String> createDefaultMethodKafkaListenerEndpoint(String topic) {
        MethodKafkaListenerEndpoint<String, String> kafkaListenerEndpoint = new MethodKafkaListenerEndpoint<>();
        kafkaListenerEndpoint.setId(generateListenerId());
        kafkaListenerEndpoint.setAutoStartup(true);
        kafkaListenerEndpoint.setTopics(topic);
        kafkaListenerEndpoint.setMessageHandlerMethodFactory(new DefaultMessageHandlerMethodFactory());
        return kafkaListenerEndpoint;
    }

    private String generateListenerId() {
        return kafkaListenerId + endpointIdIndex.getAndIncrement();
    }


    @Bean
    public Map<String, KafkaMessageListenerImpl> createKafkaMessageListeners(KafkaShardsProperties kafkaShardsProperties) {
        Map<String, KafkaShardsProperties.KafkaShardProperties> shardNameToProperties = kafkaShardsProperties.config();

        Map<String, KafkaMessageListenerImpl> listeners = new HashMap<>();
        for (Map.Entry<String, KafkaShardsProperties.KafkaShardProperties> entry : shardNameToProperties.entrySet()) {
            listeners.put(entry.getKey(), new KafkaMessageListenerImpl(entry.getKey()));
        }

        return listeners;
    }


    @Bean
    public Map<String, ConcurrentKafkaListenerContainerFactory<String, Object>> kafkaListenerContainerFactories(
            KafkaShardsProperties kafkaShardsProperties,
            Map<String, ConsumerFactory<String, Object>> kafkaConsumerFactoryMap) {
        //TODO: посмотреть как задаются concurrency и другие настройки
        Map<String, KafkaShardsProperties.KafkaShardProperties> shardNameToProperties = kafkaShardsProperties.config();

        Map<String, ConcurrentKafkaListenerContainerFactory<String, Object>> result = new HashMap<>();
        for (Map.Entry<String, ConsumerFactory<String, Object>> entry : kafkaConsumerFactoryMap.entrySet()) {
            ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(entry.getValue());

            result.put(entry.getKey(), factory);
        }

        return result;
    }


    @Bean
    public Map<String, ConsumerFactory<String, Object>> kafkaConsumerFactories(
            KafkaShardsProperties kafkaShardsProperties) {
        Map<String, KafkaShardsProperties.KafkaShardProperties> shardNameToProperties = kafkaShardsProperties.config();

        Map<String, ConsumerFactory<String, Object>> consumerFactoryMap = new HashMap<>();
        for (Map.Entry<String, KafkaShardsProperties.KafkaShardProperties> entry : shardNameToProperties.entrySet()) {
            consumerFactoryMap.put(entry.getKey(), new DefaultKafkaConsumerFactory<>(entry.getValue().properties().buildConsumerProperties()));
        }

        return consumerFactoryMap;
    }
}
