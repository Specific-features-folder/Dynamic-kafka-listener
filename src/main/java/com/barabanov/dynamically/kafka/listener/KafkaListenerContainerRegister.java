package com.barabanov.dynamically.kafka.listener;

import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;


@AllArgsConstructor
@Service
public class KafkaListenerContainerRegister {

    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Qualifier("shardsKafkaEndpoints")
    private final Map<String, List<KafkaListenerEndpoint>> shardsKafkaEndpoints;

    @Qualifier("shardsKafkaListenerContainerFactories")
    private final Map<String, ConcurrentKafkaListenerContainerFactory<Object, Object>> shardsKafkaListenerContainerFactories;


    @PostConstruct
    private void registerKafkaListenerEndpoints() {
        for (Map.Entry<String, List<KafkaListenerEndpoint>> shardKafkaEndpoints : shardsKafkaEndpoints.entrySet()) {
            String shardName = shardKafkaEndpoints.getKey();
            for (KafkaListenerEndpoint kafkaListenerEndpoint : shardKafkaEndpoints.getValue()) {
                //TODO: а нужно ли тут true
                kafkaListenerEndpointRegistry.registerListenerContainer(
                        kafkaListenerEndpoint,
                        shardsKafkaListenerContainerFactories.get(shardName),
                        true);
            }
        }
    }
}
