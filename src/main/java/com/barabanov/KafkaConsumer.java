package com.barabanov;

import com.barabanov.dynamically.kafka.listener.ShardsKafkaConfiguration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumer {

    private final Instant initTime = Instant.now();
    private final DataSource dataSource;

    @Qualifier("shardsKafkaTemplates")
    private final Map<String, KafkaTemplate<Object, Object>> shardsKafkaTemplates;

    private final Map<String, Map<String, String>> shardsTopicsMap;


    @KafkaListener(topics = "${kafka-config.response-topic-name}",
            properties = "spring.json.value.default.type=com.barabanov.Car",
            id = "my-id",
            idIsGroup = false)
    public void listenMsg(Car car) {
        log.info("Получен ответ из подсистемы: {}", car);

        KafkaTemplate<Object, Object> shardKafkaTemplate = null;
        String topicName = null;
        if (car.model().contains("shard-a")) {
            shardKafkaTemplate = shardsKafkaTemplates.get("shard-a");
            topicName = shardsTopicsMap.get("shard-a").get(ShardsKafkaConfiguration.RESPONSE_TOPIC_PROPERTY);
        }

        if (car.model().contains("shard-b")) {
            shardKafkaTemplate = shardsKafkaTemplates.get("shard-b");
            topicName = shardsTopicsMap.get("shard-b").get(ShardsKafkaConfiguration.RESPONSE_TOPIC_PROPERTY);
        }

        if (shardsKafkaTemplates == null || topicName == null)
            throw new RuntimeException(String.format("Непонятно в какую шарду пересылать. ShardKafkaTemplate: %s, topicName: %s", shardKafkaTemplate, topicName));

        shardKafkaTemplate.send(topicName, car);
    }


    private boolean isDatabaseAvailable() {
        try {
            return dataSource.getConnection().isValid(10);
        } catch (SQLException e) {
            return false;
        }
    }
}
