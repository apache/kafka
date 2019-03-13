package org.apache.kafka.test;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.internals.RecordCollector;

import java.util.Map;

public class MockRecordCollector implements RecordCollector {
    @Override
    public <K, V> void send(final String topic, final K key, final V value, final Headers headers, final Integer partition, final Long timestamp, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    }

    @Override
    public <K, V> void send(final String topic, final K key, final V value, final Headers headers, final Long timestamp, final Serializer<K> keySerializer, final Serializer<V> valueSerializer, final StreamPartitioner<? super K, ? super V> partitioner) {

    }

    @Override
    public void init(final Producer<byte[], byte[]> producer) {

    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

    @Override
    public Map<TopicPartition, Long> offsets() {
        return null;
    }
}
