package com.barabanov.specific.features.kafka.shard;

import java.util.List;

public interface BatchShardKafkaMsgHandler<V> {

    void handle(List<V> msgList, String shardName);
}
