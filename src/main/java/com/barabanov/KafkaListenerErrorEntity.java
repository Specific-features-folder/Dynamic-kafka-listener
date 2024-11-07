package com.barabanov;

import lombok.*;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.time.LocalDateTime;

@Data
@Builder
@EqualsAndHashCode(exclude = "objectId")
@AllArgsConstructor
@NoArgsConstructor
@Entity(name = "KafkaListenerError")
public class KafkaListenerErrorEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long objectId; //TODO: удалить при внедрении в проект

    private String errorStackTrace;

    private String kafkaMessageJson;

    private String kafkaTopicName;

    private LocalDateTime timeOfLastProcessingAttempt;
}
