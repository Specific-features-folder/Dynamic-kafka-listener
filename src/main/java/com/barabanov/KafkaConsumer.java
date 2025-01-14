package com.barabanov;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumer {

    private final Instant initTime = Instant.now();
    private final DataSource dataSource;

    @KafkaListener(topics = "car-topic",
            properties = "spring.json.value.default.type=com.barabanov.Car")
    public void listenMsg(Car car) {
//        if (!isDatabaseAvailable())
//            throw new RuntimeException("Ошибка из-за отключенной БД");
//
//        if (car.model().equals("business-error"))
//            throw new RuntimeException("Бизнесовая ошибка");
//        System.out.println("Считано сообщение из топика: " + car.toString());
        log.info("received date: " + Instant.now() + " init date: " + initTime.toString());
    }


    private boolean isDatabaseAvailable() {
        try {
            return dataSource.getConnection().isValid(10);
        } catch (SQLException e) {
            return false;
        }
    }
}
