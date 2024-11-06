package com.barabanov;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.RecoveryStrategy;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

import javax.sql.DataSource;
import java.sql.SQLException;

@Slf4j
@Component
public class KafkaCommonErrorHandler implements CommonErrorHandler {

    private final RecoveryStrategy recoveryStrategy;
    private final DataSource dataSource;

    public KafkaCommonErrorHandler(DataSource dataSource) {
        this.recoveryStrategy = new MyFailedRecordTracker(null, new FixedBackOff(0, 2l));
        this.dataSource = dataSource;
    }


    @SneakyThrows
    @Override
    public boolean handleOne(Exception exception,
                             ConsumerRecord<?, ?> record,
                             Consumer<?, ?> consumer,
                             MessageListenerContainer container) {

        if (isDatabaseAvailable()) {
            System.out.println("Урааа, подключение есть. Ошибка кая-то бизнесовая, пробуем обработать 3 раза");
            if (recoveryStrategy.recovered(record, exception, container, consumer)) {
                System.out.println("обработали 3 раза. Пропускаем запись!");
                consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset() + 1L);
                consumer.commitSync();
                return true;
            }
            System.out.println("Пока что не двигаем оффсет из-за ошибки, возникшей при доступности БД");
            return false;
        }
        System.out.println("ААААААААтсутствует подключение к БД. Не двигаем оффсет");
        return false;
    }


    @Override
    public void handleOtherException(Exception exception, Consumer<?, ?> consumer, MessageListenerContainer container, boolean batchListener) {
        if (exception instanceof RecordDeserializationException ex) {
            System.out.println("Ошибка при парсинге. Двигаем оффсет");
            consumer.seek(ex.topicPartition(), ex.offset() + 1L);
            consumer.commitSync();
        } else {
            System.out.println("А тут непонятно что делать :/");
        }
    }


    private boolean isDatabaseAvailable() {
        try {
            return dataSource.getConnection().isValid(1); //100 is okay
        } catch (SQLException e) {
            return false;
        }
    }


}