package com.barabanov;

import org.springframework.kafka.annotation.KafkaListener;


public class KafkaConsumer {


    @KafkaListener(topics = "my-topic",
            properties = "spring.json.value.default.type=com.barabanov.Lada")
    public void listenMsg(Lada lada) {
        System.out.println("Считано сообщение из топика: " + lada.toString());
    }
}
