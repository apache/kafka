package org.apache.kafka.streams;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.StreamPartitioner;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

public class FixedPartitionPartitioner<K, V> implements StreamPartitioner<K, V> {

    private final int partition;

    public FixedPartitionPartitioner(final int partition) {
        this.partition = partition;
    }

    @SuppressWarnings("removal")
    @Override
    public Optional<Set<Integer>> partitions(final String topic, final K key, final V value, final int numPartitions) {
        throw new AssertionError("Deprecated 4-argument partitions method was called instead of 5-argument method containing headers.");
    }

    @Override
    public Optional<Set<Integer>> partitions(final String topic, final K key, final V value, final Headers headers, final int numPartitions) {
        return Optional.of(Collections.singleton(partition));
    }
}
