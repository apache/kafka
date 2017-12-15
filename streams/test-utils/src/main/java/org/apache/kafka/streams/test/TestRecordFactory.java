/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.test;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Factory to create {@link TestRecord}s for a single topic with given key and value {@link Serializer}s.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
// TODO add annotation -- need to decide which
public class TestRecordFactory<K, V> {
    private final String topicName;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final Time time;

    /**
     * Create a new factory for the given topic.
     * <p>
     * Note: you should <em>not</em> provide {@link SystemTime} as time object as this might result in slow down data
     * generation.
     * If you want to test with system time semantics you should rather set {@link WallclockTimestampExtractor}
     * in your {@link StreamsConfig} that you pass into {@link TopologyTestDriver} and generate you records with
     * any random "dummy" timestamps.
     *
     * @param topicName the topic name used for all generated {@link TestRecord}s
     * @param keySerializer the key serializer
     * @param valueSerializer the value serializer
     * @param time the time use to get record timestamps if no timestamps is specified explicitly (can be {@code null})
     */
    public TestRecordFactory(final String topicName,
                             final Serializer<K> keySerializer,
                             final Serializer<V> valueSerializer,
                             final Time time) {
        Objects.requireNonNull(keySerializer, "keySerializer cannot be null");
        Objects.requireNonNull(valueSerializer, "valueSerializer cannot be null");
        this.topicName = topicName;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.time = time;
    }

    /**
     * Create a {@link TestRecord} with the given key, value, and timestamp.
     *
     * @param key the record key
     * @param value the record value
     * @param timestamp the record timestamp
     * @return the generated {@link TestRecord}
     */
    public TestRecord<K, V> create(final K key,
                                   final V value,
                                   final long timestamp) {
        return new TestRecord<>(topicName, key, value, timestamp, keySerializer, valueSerializer);
    }

    /**
     * Create a {@link TestRecord} with the given key and value.
     * The timestamp will be generated from the constructor provided {@link Time} object.
     *
     * @param key the record key
     * @param value the record value
     * @return the generated {@link TestRecord}
     */
    public TestRecord<K, V> create(final K key,
                                   final V value) {
        return new TestRecord<>(topicName, key, value, time.milliseconds(), keySerializer, valueSerializer);
    }

    /**
     * Create a {@link TestRecord} with {@code null}-key and the given value and timestamp.
     *
     * @param value the record value
     * @return the generated {@link TestRecord}
     */
    public TestRecord<K, V> create(final V value,
                                   final long timestamp) {
        return new TestRecord<>(topicName, null, value, timestamp, keySerializer, valueSerializer);
    }

    /**
     * Create a {@link TestRecord} with {@code null}-key and the given value.
     * The timestamp will be generated from the constructor provided {@link Time} object.
     *
     * @param value the record value
     * @return the generated {@link TestRecord}
     */
    public TestRecord<K, V> create(final V value) {
        return new TestRecord<>(topicName, null, value, time.milliseconds(), keySerializer, valueSerializer);
    }

    /**
     * Create a {@link TestRecord} with the given key, value, and timestamp.
     *
     * @param keyValue the record key and value
     * @param timestamp the record timestamp
     * @return the generated {@link TestRecord}
     */
    public TestRecord<K, V> create(final KeyValue<K, V> keyValue,
                                   final long timestamp) {
        return new TestRecord<>(topicName, keyValue.key, keyValue.value, timestamp, keySerializer, valueSerializer);
    }

    /**
     * Create a {@link TestRecord} with the given key and value.
     * The timestamp will be generated from the constructor provided {@link Time} object.
     *
     * @param keyValue the record key and value
     * @return the generated {@link TestRecord}
     */
    public TestRecord<K, V> create(final KeyValue<K, V> keyValue) {
        return new TestRecord<>(topicName, keyValue.key, keyValue.value, time.milliseconds(), keySerializer, valueSerializer);
    }

    /**
     * Creates {@link TestRecord}s with the given key and value.
     * The timestamp will be generated from the constructor provided {@link Time} object.
     * Note: using {@link MockTime} you can specify an auto-increment to get different timestamp for consecutive created records.
     *
     * @param keyValues the record keys and values
     * @return the generated {@link TestRecord}
     */
    public List<TestRecord<K, V>> create(final List<KeyValue<K, V>> keyValues) {
        final List<TestRecord<K, V>> records = new ArrayList<>(keyValues.size());

        for (final KeyValue<K, V> keyValue : keyValues) {
            records.add(new TestRecord<>(topicName, keyValue.key, keyValue.value, time.milliseconds(), keySerializer, valueSerializer));
        }

        return records;
    }

    /**
     * Creates {@link TestRecord}s with the given key and value.
     * The timestamp will be generated from the constructor provided {@link Time} object.
     * For each generated record, the time is advanced by 1.
     *
     * @param keyValues the record keys and values
     * @param startTimestamp the timestamp for the first generated record
     * @return the generated {@link TestRecord}
     */
    public List<TestRecord<K, V>> create(final List<KeyValue<K, V>> keyValues,
                                         final long startTimestamp) {
        return create(keyValues, startTimestamp, 1);
    }

    /**
     * Creates {@link TestRecord}s with the given key and value.
     *
     * @param keyValues the record keys and values
     * @param startTimestamp the timestamp for the first generated record
     * @param advanceMs the time difference between two consecutive generated records
     * @return the generated {@link TestRecord}
     */
    public List<TestRecord<K, V>> create(final List<KeyValue<K, V>> keyValues,
                                         final long startTimestamp,
                                         final long advanceMs) {
        final List<TestRecord<K, V>> records = new ArrayList<>(keyValues.size());

        long timestamp = startTimestamp;
        for (final KeyValue<K, V> keyValue : keyValues) {
            records.add(new TestRecord<>(topicName, keyValue.key, keyValue.value, timestamp, keySerializer, valueSerializer));
            timestamp += advanceMs;
        }

        return records;
    }

}
