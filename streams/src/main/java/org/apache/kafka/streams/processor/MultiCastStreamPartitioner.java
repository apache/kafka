package org.apache.kafka.streams.processor;

import java.util.List;

public abstract class MultiCastStreamPartitioner<K, V> implements StreamPartitioner<K, V>{

    @Override
    public final Integer partition(String topic, K key, V value, int numPartitions) {
        throw new UnsupportedOperationException();
    }

    /**
     * Determine the List of partition numbers to which a record, with the given key and value and the current number
     * of partitions, should be multi-casted to. Note that returning a single valued list with value -1 is a shorthand
     * for broadcasting the record to all the partitions of the topic.
     *
     * @param topic the topic name this record is sent to
     * @param key the key of the record
     * @param value the value of the record
     * @param numPartitions the total number of partitions
     * @return a List of integers between 0 and {@code numPartitions-1}, or [-1] if the record should be pushed to all
     * the partitions of the topic.
     */
    public abstract List<Integer> partitions(String topic, K key, V value, int numPartitions);
}
