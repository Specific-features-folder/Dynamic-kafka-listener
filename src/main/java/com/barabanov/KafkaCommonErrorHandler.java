package com.barabanov;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.backoff.FixedBackOff;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.time.LocalDateTime;

@Slf4j
@Component
public class KafkaCommonErrorHandler implements CommonErrorHandler {

    private final static long MSG_PROCESSING_ATTEMPTS = 3L;
    private final static int MAX_ERROR_MSG_LENGTH = 4000;
    private final RecoveryStrategy recoveryStrategy;
    private final DataSource dataSource;
    private final TransactionTemplate transactionTemplate;
    private final KafkaListenerErrorRepository kafkaListenerErrorRepository;


    public KafkaCommonErrorHandler(DataSource dataSource,
                                   TransactionTemplate transactionTemplate,
                                   KafkaListenerErrorRepository kafkaListenerErrorRepository) {
        this.recoveryStrategy = new MyFailedRecordTracker(null, new FixedBackOff(0, MSG_PROCESSING_ATTEMPTS - 1));
        this.dataSource = dataSource;
        this.transactionTemplate = transactionTemplate;
        this.kafkaListenerErrorRepository = kafkaListenerErrorRepository;
    }


    @SneakyThrows
    @Override
    public boolean handleOne(Exception exception,
                             ConsumerRecord<?, ?> record,
                             Consumer<?, ?> consumer,
                             MessageListenerContainer container) {

        if (isDatabaseAvailable()) {
            if (recoveryStrategy.recovered(record, exception, container, consumer)) {
                //TODO: выводить ошибку тут
                System.out.printf("Было предпринято " + MSG_PROCESSING_ATTEMPTS + " попыток обработки сообщения при наличии подключения к БД." +
                                " Оффсет будет увеличен! Текущие значения Topic: %s Partition: %s Offset: %s\n",
                        record.topic(), record.partition(), record.offset());
                saveKafkaListenerError(exception, record.value(), record.topic());
                consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset() + 1L);
                consumer.commitSync();
                return true;
            }
            System.out.printf("Производятся повторные попытки обработки соощения из кафки при наличии подключения к БД. Offset не будет увеличен." +
                            " Текущие значения Topic: %s Partition: %s Offset: %s\n",
                    record.topic(), record.partition(), record.offset());
            return false;
        }
        System.out.printf("Во время обработки ошибки при чтении из кафка подключение к БД отсутствует! Offset не будет увеличен." +
                        " Текущие значения Topic: %s Partition: %s Offset: %s\n",
                record.topic(), record.partition(), record.offset());
        return false;
    }


    @Override
    public void handleOtherException(Exception exception, Consumer<?, ?> consumer, MessageListenerContainer container, boolean batchListener) {
        if (exception instanceof RecordDeserializationException ex) {
            TopicPartition topicPartition = ex.topicPartition();
            //TODO: выводить ошибку тут
            System.out.printf("Ошибка при попытке считать сообшение из кафки. Offset будет увеличен!" +
                            " Текущие значения Topic: %s Partition: %s Offset: %s\n",
                    topicPartition.topic(), topicPartition.partition(), ex.offset());
            consumer.seek(topicPartition, ex.offset() + 1L);
            consumer.commitSync();
        } else {
            System.out.println("Неожиданная ошибка при попытке считать сообщение из kafka.\n");
        }
    }


    private boolean isDatabaseAvailable() {
        try {
            return dataSource.getConnection().isValid(10);
        } catch (SQLException e) {
            return false;
        }
    }


    private Long saveKafkaListenerError(Exception e, Object value, String topicName) {
        System.out.printf("Сохраняем сообщение и ошибку возникшую при обработке из топика: %s\n", topicName);
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        String stackTraceAsString = sw.toString();

        return transactionTemplate.execute((transactionStatus) -> kafkaListenerErrorRepository.save(
                        KafkaListenerErrorEntity.builder()
                                .errorStackTrace(
                                        stackTraceAsString.substring(0,
                                                Math.min(stackTraceAsString.length(), MAX_ERROR_MSG_LENGTH)))
                                .kafkaMessageJson(jsonToString(value)) //TODO: тут поправить +objectId +Long заменить на стринг
                                .kafkaTopicName(topicName)
                                .timeOfLastProcessingAttempt(LocalDateTime.now())
                .build()).getObjectId()
        );
    }


    private String jsonToString(Object o) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        try {
            return objectMapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }


}