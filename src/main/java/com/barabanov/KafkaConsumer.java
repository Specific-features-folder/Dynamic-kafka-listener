package com.barabanov;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.SQLException;

@Component
@RequiredArgsConstructor
public class KafkaConsumer {

    private final DataSource dataSource;

    @KafkaListener(topics = "my-topic",
            properties = "spring.json.value.default.type=com.barabanov.Lada")
    public void listenMsg(Lada lada) {
        if (!isDatabaseAvailable())
            throw new RuntimeException("Ошибка из-за отключенной БД");

        if (lada.model().equals("business-error"))
            throw new RuntimeException("Бизнесовая ошибка");
        System.out.println("Считано сообщение из топика: " + lada.toString());
    }


    private boolean isDatabaseAvailable() {
        try {
            return dataSource.getConnection().isValid(10);
        } catch (SQLException e) {
            return false;
        }
    }
}
