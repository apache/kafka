package org.apache.kafka.streams.processor;

import java.util.List;

public class MultiCaseStreamPartitionerImpl<K, V> extends MultiCastStreamPartitioner<K, V>{

    @Override
    public List<Integer> partitions(String topic, K key, V value, int numPartitions) {
        return null;
    }
}
