package com.barabanov.dynamically.kafka.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;

@Slf4j
@RequiredArgsConstructor
public class KafkaMessageListenerImpl<K, V> implements MessageListener<K, V> {

    private final String kafkaName;
    private final KafkaMessageHandler<V> kafkaMessageHandler;


    @Override
    public void onMessage(ConsumerRecord<K, V> consumerRecord) {
        try {
            log.info("Получена запись из кафки {} из топика {}", kafkaName, consumerRecord.topic());

            kafkaMessageHandler.handle(consumerRecord.value());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }
}
