package com.barabanov.dynamically.kafka.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;


@Service
@Slf4j
@RequiredArgsConstructor
public class ResponseBusinessService implements KafkaMessageHandler<Response> {

    @Override
    public void handle(Response msg) {
        log.info("Обрабатываем сообщение {}", msg);
    }
}
