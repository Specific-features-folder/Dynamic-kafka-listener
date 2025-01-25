package com.barabanov.dynamically.kafka.listener;

public interface KafkaMessageHandler<V> {

    void handle(V msg);
}
