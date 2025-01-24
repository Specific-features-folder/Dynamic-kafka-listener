package com.barabanov.dynamically.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;

import java.util.concurrent.atomic.AtomicLong;

public class KafkaListenerCreator {
    String kafkaGroupId = "kafkaGroupId";
    String kafkaListenerId = "kafkaListenerId-";
    static AtomicLong endpointIdIndex = new AtomicLong(1);

    private KafkaListenerEndpoint createKafkaListenerEndpoint(String topic, String kafkaName) {
        MethodKafkaListenerEndpoint<String, String> kafkaListenerEndpoint =
                createDefaultMethodKafkaListenerEndpoint(topic);
        kafkaListenerEndpoint.setBean(new KafkaMessageListenerImpl(kafkaName));
        try {
            kafkaListenerEndpoint.setMethod(KafkaMessageListenerImpl.class.getMethod("onMessage", ConsumerRecord.class));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Attempt to call a non-existent method " + e);
        }
        return kafkaListenerEndpoint;
    }

    private MethodKafkaListenerEndpoint<String, String> createDefaultMethodKafkaListenerEndpoint(String topic) {
        MethodKafkaListenerEndpoint<String, String> kafkaListenerEndpoint = new MethodKafkaListenerEndpoint<>();
        kafkaListenerEndpoint.setId(generateListenerId());
        kafkaListenerEndpoint.setGroupId(kafkaGroupId);
        kafkaListenerEndpoint.setAutoStartup(true);
        kafkaListenerEndpoint.setTopics(topic);
        kafkaListenerEndpoint.setConcurrency(5);
        kafkaListenerEndpoint.setMessageHandlerMethodFactory(new DefaultMessageHandlerMethodFactory());
        return kafkaListenerEndpoint;
    }

    private String generateListenerId() {
        return kafkaListenerId + endpointIdIndex.getAndIncrement();
    }
}
