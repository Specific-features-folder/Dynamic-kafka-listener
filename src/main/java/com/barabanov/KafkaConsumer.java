package com.barabanov;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    @KafkaListener(topics = "my-topic",
            properties = "spring.json.value.default.type=com.barabanov.Lada")
    public void listenMsg(Lada lada) {
        if (lada.model().equals("error"))
            throw new RuntimeException("Бизнесовая ошибка");
        System.out.println("Считано сообщение из топика: " + lada.toString());
    }
}
