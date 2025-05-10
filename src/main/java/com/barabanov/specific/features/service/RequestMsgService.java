package com.barabanov.specific.features.service;

import com.barabanov.specific.features.kafka.dto.Request;
import com.barabanov.specific.features.kafka.shard.BatchShardKafkaMsgHandler;
import com.barabanov.specific.features.kafka.shard.ShardKafkaMsgHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@Component
public class RequestMsgService implements BatchShardKafkaMsgHandler<Request> {

    @Value("${kafka-topics.request-topic-name}")
    private final String commonRequestTopicName;
    private final KafkaTemplate<String, Object> commonKafkaTemplate;


    @Override
    public void handle(List<Request> msgList, String shardName) {

        log.info("Обрабатывается batch request c messages: {}. Они отправляются в Common",
                msgList.stream()
                        .map(Request::message)
                        .collect(Collectors.joining(",", "{", "}")));

        for (Request request : msgList)
            commonKafkaTemplate.send(commonRequestTopicName, request);
    }
}
