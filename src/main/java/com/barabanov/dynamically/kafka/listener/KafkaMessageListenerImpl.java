package com.barabanov.dynamically.kafka.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
@RequiredArgsConstructor
public class KafkaMessageListenerImpl<K, V> implements BatchMessageListener<K, V> {

    private final String kafkaName;
    private final BatchKafkaMessageHandler<V> batchKafkaMessageHandler;


    @Override
    public void onMessage(List<ConsumerRecord<K, V>> data) {
        try {
            if (CollectionUtils.isEmpty(data)) {
                log.warn("Получен пустой батч из кафки {}", kafkaName);
                return;
            }
            List<V> valueList = new ArrayList<>();
            Set<String> topics = new HashSet<>();
            for (ConsumerRecord<K, V> consumerRecord : data) {
                valueList.add(consumerRecord.value());
                topics.add(consumerRecord.topic());
            }
            log.info("Получен батч из кафки {} размером {} из топиков {}", kafkaName, data.size(), topics);

            batchKafkaMessageHandler.handle(valueList);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
