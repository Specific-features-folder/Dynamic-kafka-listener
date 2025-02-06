package com.barabanov.dynamically.kafka.listener;

import java.util.List;

public interface BatchKafkaMessageHandler<V> {

    void handle(List<V> msg);
}
