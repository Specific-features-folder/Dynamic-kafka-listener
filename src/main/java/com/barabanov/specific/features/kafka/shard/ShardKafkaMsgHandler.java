package com.barabanov.specific.features.kafka.shard;

public interface ShardKafkaMsgHandler<V> {

    void handle(V msg, String shardName);
}
