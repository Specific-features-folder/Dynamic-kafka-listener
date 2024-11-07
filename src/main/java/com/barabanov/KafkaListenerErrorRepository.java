package com.barabanov;

import org.springframework.data.repository.CrudRepository;

public interface KafkaListenerErrorRepository extends CrudRepository<KafkaListenerErrorEntity, Long> {
}
