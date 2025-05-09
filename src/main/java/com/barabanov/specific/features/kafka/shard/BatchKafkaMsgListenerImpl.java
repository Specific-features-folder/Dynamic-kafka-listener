package com.barabanov.specific.features.kafka.shard;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Тело метода onMessage аналогично телу метода с @KafkaListener аннотацией.
 */
@Slf4j
@RequiredArgsConstructor
public class BatchKafkaMsgListenerImpl<K, V> implements BatchMessageListener<K, V> {

    private final String shardName;
    private final BatchShardKafkaMsgHandler<V> batchShardKafkaMsgHandler;


    @Override
    public void onMessage(List<ConsumerRecord<K, V>> data) {
        try {
            if (CollectionUtils.isEmpty(data)) {
                log.warn("Получен пустой батч из кафки шарды {}", shardName);
                return;
            }
            List<V> valueList = new ArrayList<>();
            Set<String> topics = new HashSet<>();
            for (ConsumerRecord<K, V> consumerRecord : data) {
                valueList.add(consumerRecord.value());
                topics.add(consumerRecord.topic());
            }
            log.info("Получен батч из кафки шарды {} размером {} из топиков {}", shardName, data.size(), topics);

            batchShardKafkaMsgHandler.handle(valueList, shardName);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
