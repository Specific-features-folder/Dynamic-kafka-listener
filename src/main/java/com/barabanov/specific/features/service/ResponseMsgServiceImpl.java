package com.barabanov.specific.features.service;

import com.barabanov.specific.features.kafka.dto.Response;
import com.barabanov.specific.features.kafka.shard.ShardWritableTopic;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static com.barabanov.specific.features.kafka.shard.ShardWritableTopic.RESPONSE_TOPIC;

@Slf4j
@RequiredArgsConstructor
@Component
public class ResponseMsgServiceImpl implements ResponseMsgService {

    @Qualifier("shardsWritableTopicsMap")
    private final Map<String, Map<ShardWritableTopic, String>> shardsTopics;
    @Qualifier("shardsKafkaTemplates")
    private final Map<String, KafkaTemplate<String, Object>> shardsKafkaTemplates;


    @Override
    public void handle(Response response) {
        String shardName = ThreadLocalRandom.current().nextInt() % 2 == 0
                ? "shard-a"
                : "shard-b";
        log.info("Обрабатывается response с Message {}. Он перенаправляется в шарду {}", response.message(), shardName);

        Map<ShardWritableTopic, String> shardTopics = shardsTopics.get(shardName);
        KafkaTemplate<String, Object> shardKafkaTemplate = shardsKafkaTemplates.get(shardName);

        shardKafkaTemplate.send(shardTopics.get(RESPONSE_TOPIC), response);
    }
}
