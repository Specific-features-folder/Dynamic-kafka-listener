package com.barabanov.specific.features.kafka.shard;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;

/**
 * Тело метода onMessage аналогично телу метода с @KafkaListener аннотацией.
 */
@Slf4j
@RequiredArgsConstructor
public class KafkaMsgListenerImpl<K, V> implements MessageListener<K, V> {

    private final String shardName;
    private final ShardKafkaMsgHandler<V> kafkaMsgHandler;


    @Override
    public void onMessage(ConsumerRecord<K, V> data) {
        log.info("Получено сообщение из кафки шарды {} из топика {}", shardName, data.topic());
        kafkaMsgHandler.handle(data.value(), shardName);
    }
}
