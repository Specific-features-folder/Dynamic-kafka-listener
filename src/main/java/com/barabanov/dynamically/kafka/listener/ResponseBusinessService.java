package com.barabanov.dynamically.kafka.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
@Slf4j
@RequiredArgsConstructor
public class ResponseBusinessService implements BatchKafkaMessageHandler<Response> {

    private final KafkaTemplate<String, Object> commonKafkaTemplate;
    @Value("${kafka-config.request-topic-name}")
    private final String requestCommonTopic;


    @Override
    public void handle(List<Response> msgList) {
        log.info("Обрабатываем сообщения с messages {}", msgList.stream().map(Response::message).toList());
//        if (msg.message().equals("сообщение с ошибкой"))
//            throw new RuntimeException("AAAAA ошибка");

        msgList.forEach(msg -> commonKafkaTemplate.send(requestCommonTopic, msg));
    }
}
