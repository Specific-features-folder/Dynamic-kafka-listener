package com.barabanov.specific.features.kafka.common;

import com.barabanov.specific.features.kafka.dto.Response;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumer {


    @KafkaListener(topics = "${kafka-topics.response-topic-name}",
            properties = "spring.json.value.default.type = com.barabanov.specific.features.kafka.dto.Response")
    public void handleResponseMsg(Response response) {
        log.info("Получено response сообщение c message: {}", response.message());
    }
}
