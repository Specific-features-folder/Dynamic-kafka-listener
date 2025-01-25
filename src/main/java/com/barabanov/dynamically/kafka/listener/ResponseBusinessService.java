package com.barabanov.dynamically.kafka.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;


@Service
@Slf4j
@RequiredArgsConstructor
public class ResponseBusinessService implements KafkaMessageHandler<Response> {

    private final KafkaTemplate<String, Object> commonKafkaTemplate;
    @Value("subsystem-request-topic")
    private final String requestCommonTopic;


    @Override
    public void handle(Response msg) {
        log.info("Обрабатываем сообщение {}", msg);
        if (msg.message().equals("сообщение с ошибкой"))
            throw new RuntimeException("AAAAA ошибка");

        commonKafkaTemplate.send(requestCommonTopic, msg);
    }
}
