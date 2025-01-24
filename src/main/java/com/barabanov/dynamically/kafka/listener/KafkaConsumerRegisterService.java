package com.barabanov.dynamically.kafka.listener;

import lombok.AllArgsConstructor;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Map;

@AllArgsConstructor
@Service
public class KafkaConsumerRegisterService {

    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    private final Map<String, KafkaListenerEndpoint> kafkaListenerEndpoints;

    private final Map<String, ConcurrentKafkaListenerContainerFactory<String, Object>> kafkaListenerContainerFactories;


    @PostConstruct
    private void registerKafkaListenerEndpoints() {
        for (Map.Entry<String, KafkaListenerEndpoint> entry : kafkaListenerEndpoints.entrySet()) {

            //TODO: а нужно ли тут true
            kafkaListenerEndpointRegistry.registerListenerContainer(
                    entry.getValue(),
                    kafkaListenerContainerFactories.get(entry.getKey()),
                    true);
        }
    }
}
