package com.barabanov.dynamically.kafka.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;

@Slf4j
@RequiredArgsConstructor
public class KafkaMessageListenerImpl implements MessageListener<String, Object> {

    private final String kafkaName;


    @Override
    public void onMessage(ConsumerRecord<String, Object> data) {
      log.info("Обрабатываем запись: {}, из кафки {}", data, kafkaName);
    }
}
