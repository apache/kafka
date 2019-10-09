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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Factory to create {@link ConsumerRecord consumer records} for a single single-partitioned topic with given key and
 * value {@link Serializer serializers}.
 *
 * @deprecated Since 2.4 use methods of {@link TestInputTopic} instead
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 *
 * @see TopologyTestDriver
 */
@Deprecated
public class ConsumerRecordFactory<K, V> {
    private final String topicName;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private long timeMs;
    private final long advanceMs;

    /**
     * Create a new factory for the given topic.
     * Uses current system time as start timestamp.
     * Auto-advance is disabled.
     *
     * @param keySerializer the key serializer
     * @param valueSerializer the value serializer
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecordFactory(final Serializer<K> keySerializer,
                                 final Serializer<V> valueSerializer) {
        this(null, keySerializer, valueSerializer, System.currentTimeMillis());
    }

    /**
     * Create a new factory for the given topic.
     * Uses current system time as start timestamp.
     * Auto-advance is disabled.
     *
     * @param defaultTopicName the default topic name used for all generated {@link ConsumerRecord consumer records}
     * @param keySerializer the key serializer
     * @param valueSerializer the value serializer
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecordFactory(final String defaultTopicName,
                                 final Serializer<K> keySerializer,
                                 final Serializer<V> valueSerializer) {
        this(defaultTopicName, keySerializer, valueSerializer, System.currentTimeMillis());
    }

    /**
     * Create a new factory for the given topic.
     * Auto-advance is disabled.
     *
     * @param keySerializer the key serializer
     * @param valueSerializer the value serializer
     * @param startTimestampMs the initial timestamp for generated records
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecordFactory(final Serializer<K> keySerializer,
                                 final Serializer<V> valueSerializer,
                                 final long startTimestampMs) {
        this(null, keySerializer, valueSerializer, startTimestampMs, 0L);
    }

    /**
     * Create a new factory for the given topic.
     * Auto-advance is disabled.
     *
     * @param defaultTopicName the topic name used for all generated {@link ConsumerRecord consumer records}
     * @param keySerializer the key serializer
     * @param valueSerializer the value serializer
     * @param startTimestampMs the initial timestamp for generated records
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecordFactory(final String defaultTopicName,
                                 final Serializer<K> keySerializer,
                                 final Serializer<V> valueSerializer,
                                 final long startTimestampMs) {
        this(defaultTopicName, keySerializer, valueSerializer, startTimestampMs, 0L);
    }

    /**
     * Create a new factory for the given topic.
     *
     * @param keySerializer the key serializer
     * @param valueSerializer the value serializer
     * @param startTimestampMs the initial timestamp for generated records
     * @param autoAdvanceMs the time increment pre generated record
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecordFactory(final Serializer<K> keySerializer,
                                 final Serializer<V> valueSerializer,
                                 final long startTimestampMs,
                                 final long autoAdvanceMs) {
        this(null, keySerializer, valueSerializer, startTimestampMs, autoAdvanceMs);
    }

    /**
     * Create a new factory for the given topic.
     *
     * @param defaultTopicName the topic name used for all generated {@link ConsumerRecord consumer records}
     * @param keySerializer the key serializer
     * @param valueSerializer the value serializer
     * @param startTimestampMs the initial timestamp for generated records
     * @param autoAdvanceMs the time increment pre generated record
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecordFactory(final String defaultTopicName,
                                 final Serializer<K> keySerializer,
                                 final Serializer<V> valueSerializer,
                                 final long startTimestampMs,
                                 final long autoAdvanceMs) {
        Objects.requireNonNull(keySerializer, "keySerializer cannot be null");
        Objects.requireNonNull(valueSerializer, "valueSerializer cannot be null");
        this.topicName = defaultTopicName;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        timeMs = startTimestampMs;
        advanceMs = autoAdvanceMs;
    }

    /**
     * Advances the internally tracked time.
     *
     * @param advanceMs the amount of time to advance
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void advanceTimeMs(final long advanceMs) {
        if (advanceMs < 0) {
            throw new IllegalArgumentException("advanceMs must be positive");
        }
        timeMs += advanceMs;
    }

    /**
     * Create a {@link ConsumerRecord} with the given topic name, key, value, headers, and timestamp.
     * Does not auto advance internally tracked time.
     *
     * @param topicName the topic name
     * @param key the record key
     * @param value the record value
     * @param headers the record headers
     * @param timestampMs the record timestamp
     * @return the generated {@link ConsumerRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecord<byte[], byte[]> create(final String topicName,
                                                 final K key,
                                                 final V value,
                                                 final Headers headers,
                                                 final long timestampMs) {
        Objects.requireNonNull(topicName, "topicName cannot be null.");
        Objects.requireNonNull(headers, "headers cannot be null.");
        final byte[] serializedKey = keySerializer.serialize(topicName, headers, key);
        final byte[] serializedValue = valueSerializer.serialize(topicName, headers, value);
        return new ConsumerRecord<>(
            topicName,
            -1,
            -1L,
            timestampMs,
            TimestampType.CREATE_TIME,
            (long) ConsumerRecord.NULL_CHECKSUM,
            serializedKey == null ? 0 : serializedKey.length,
            serializedValue == null ? 0 : serializedValue.length,
            serializedKey,
            serializedValue,
            headers);
    }

    /**
     * Create a {@link ConsumerRecord} with the given topic name and given topic, key, value, and timestamp.
     * Does not auto advance internally tracked time.
     *
     * @param topicName the topic name
     * @param key the record key
     * @param value the record value
     * @param timestampMs the record timestamp
     * @return the generated {@link ConsumerRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecord<byte[], byte[]> create(final String topicName,
                                                 final K key,
                                                 final V value,
                                                 final long timestampMs) {
        return create(topicName, key, value, new RecordHeaders(), timestampMs);
    }

    /**
     * Create a {@link ConsumerRecord} with default topic name and given key, value, and timestamp.
     * Does not auto advance internally tracked time.
     *
     * @param key the record key
     * @param value the record value
     * @param timestampMs the record timestamp
     * @return the generated {@link ConsumerRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecord<byte[], byte[]> create(final K key,
                                                 final V value,
                                                 final long timestampMs) {
        return create(key, value, new RecordHeaders(), timestampMs);
    }

    /**
     * Create a {@link ConsumerRecord} with default topic name and given key, value, headers, and timestamp.
     * Does not auto advance internally tracked time.
     *
     * @param key the record key
     * @param value the record value
     * @param headers the record headers
     * @param timestampMs the record timestamp
     * @return the generated {@link ConsumerRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecord<byte[], byte[]> create(final K key,
                                                 final V value,
                                                 final Headers headers,
                                                 final long timestampMs) {
        if (topicName == null) {
            throw new IllegalStateException("ConsumerRecordFactory was created without defaultTopicName. " +
                "Use #create(String topicName, K key, V value, long timestampMs) instead.");
        }
        return create(topicName, key, value, headers, timestampMs);
    }

    /**
     * Create a {@link ConsumerRecord} with the given topic name, key, and value.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param topicName the topic name
     * @param key the record key
     * @param value the record value
     * @return the generated {@link ConsumerRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecord<byte[], byte[]> create(final String topicName,
                                                 final K key,
                                                 final V value) {
        final long timestamp = timeMs;
        timeMs += advanceMs;
        return create(topicName, key, value, new RecordHeaders(), timestamp);
    }

    /**
     * Create a {@link ConsumerRecord} with the given topic name, key, value, and headers.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param topicName the topic name
     * @param key the record key
     * @param value the record value
     * @param headers the record headers
     * @return the generated {@link ConsumerRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecord<byte[], byte[]> create(final String topicName,
                                                 final K key,
                                                 final V value,
                                                 final Headers headers) {
        final long timestamp = timeMs;
        timeMs += advanceMs;
        return create(topicName, key, value, headers, timestamp);
    }

    /**
     * Create a {@link ConsumerRecord} with default topic name and given key and value.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param key the record key
     * @param value the record value
     * @return the generated {@link ConsumerRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecord<byte[], byte[]> create(final K key,
                                                 final V value) {
        return create(key, value, new RecordHeaders());
    }

    /**
     * Create a {@link ConsumerRecord} with default topic name and given key, value, and headers.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param key the record key
     * @param value the record value
     * @param headers the record headers
     * @return the generated {@link ConsumerRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecord<byte[], byte[]> create(final K key,
                                                 final V value,
                                                 final Headers headers) {
        if (topicName == null) {
            throw new IllegalStateException("ConsumerRecordFactory was created without defaultTopicName. " +
                "Use #create(String topicName, K key, V value) instead.");
        }
        return create(topicName, key, value, headers);
    }

    /**
     * Create a {@link ConsumerRecord} with {@code null}-key and the given topic name, value, and timestamp.
     * Does not auto advance internally tracked time.
     *
     * @param topicName the topic name
     * @param value the record value
     * @param timestampMs the record timestamp
     * @return the generated {@link ConsumerRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecord<byte[], byte[]> create(final String topicName,
                                                 final V value,
                                                 final long timestampMs) {
        return create(topicName, null, value, new RecordHeaders(), timestampMs);
    }

    /**
     * Create a {@link ConsumerRecord} with {@code null}-key and the given topic name, value, headers, and timestamp.
     * Does not auto advance internally tracked time.
     *
     * @param topicName the topic name
     * @param value the record value
     * @param headers the record headers
     * @param timestampMs the record timestamp
     * @return the generated {@link ConsumerRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecord<byte[], byte[]> create(final String topicName,
                                                 final V value,
                                                 final Headers headers,
                                                 final long timestampMs) {
        return create(topicName, null, value, headers, timestampMs);
    }

    /**
     * Create a {@link ConsumerRecord} with default topic name and {@code null}-key as well as given value and timestamp.
     * Does not auto advance internally tracked time.
     *
     * @param value the record value
     * @param timestampMs the record timestamp
     * @return the generated {@link ConsumerRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecord<byte[], byte[]> create(final V value,
                                                 final long timestampMs) {
        return create(value, new RecordHeaders(), timestampMs);
    }

    /**
     * Create a {@link ConsumerRecord} with default topic name and {@code null}-key as well as given value, headers, and timestamp.
     * Does not auto advance internally tracked time.
     *
     * @param value the record value
     * @param headers the record headers
     * @param timestampMs the record timestamp
     * @return the generated {@link ConsumerRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecord<byte[], byte[]> create(final V value,
                                                 final Headers headers,
                                                 final long timestampMs) {
        if (topicName == null) {
            throw new IllegalStateException("ConsumerRecordFactory was created without defaultTopicName. " +
                "Use #create(String topicName, V value, long timestampMs) instead.");
        }
        return create(topicName, value, headers, timestampMs);
    }

    /**
     * Create a {@link ConsumerRecord} with {@code null}-key and the given topic name, value, and headers.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param topicName the topic name
     * @param value the record value
     * @param headers the record headers
     * @return the generated {@link ConsumerRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecord<byte[], byte[]> create(final String topicName,
                                                 final V value,
                                                 final Headers headers) {
        return create(topicName, null, value, headers);
    }

    /**
     * Create a {@link ConsumerRecord} with {@code null}-key and the given topic name and value.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param topicName the topic name
     * @param value the record value
     * @return the generated {@link ConsumerRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecord<byte[], byte[]> create(final String topicName,
                                                 final V value) {
        return create(topicName, null, value, new RecordHeaders());
    }

    /**
     * Create a {@link ConsumerRecord} with default topic name and {@code null}-key was well as given value.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param value the record value
     * @return the generated {@link ConsumerRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecord<byte[], byte[]> create(final V value) {
        return create(value, new RecordHeaders());
    }

    /**
     * Create a {@link ConsumerRecord} with default topic name and {@code null}-key was well as given value and headers.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param value the record value
     * @param headers the record headers
     * @return the generated {@link ConsumerRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ConsumerRecord<byte[], byte[]> create(final V value,
                                                 final Headers headers) {
        if (topicName == null) {
            throw new IllegalStateException("ConsumerRecordFactory was created without defaultTopicName. " +
                "Use #create(String topicName, V value, long timestampMs) instead.");
        }
        return create(topicName, value, headers);
    }

    /**
     * Creates {@link ConsumerRecord consumer records} with the given topic name, keys, and values.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param topicName the topic name
     * @param keyValues the record keys and values
     * @return the generated {@link ConsumerRecord consumer records}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public List<ConsumerRecord<byte[], byte[]>> create(final String topicName,
                                                       final List<KeyValue<K, V>> keyValues) {
        final List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>(keyValues.size());

        for (final KeyValue<K, V> keyValue : keyValues) {
            records.add(create(topicName, keyValue.key, keyValue.value));
        }

        return records;
    }

    /**
     * Creates {@link ConsumerRecord consumer records} with default topic name as well as given keys and values.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param keyValues the record keys and values
     * @return the generated {@link ConsumerRecord consumer records}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public List<ConsumerRecord<byte[], byte[]>> create(final List<KeyValue<K, V>> keyValues) {
        if (topicName == null) {
            throw new IllegalStateException("ConsumerRecordFactory was created without defaultTopicName. " +
                "Use #create(String topicName, List<KeyValue<K, V>> keyValues) instead.");
        }

        return create(topicName, keyValues);
    }

    /**
     * Creates {@link ConsumerRecord consumer records} with the given topic name, keys, and values.
     * Does not auto advance internally tracked time.
     *
     * @param topicName the topic name
     * @param keyValues the record keys and values
     * @param startTimestamp the timestamp for the first generated record
     * @param advanceMs the time difference between two consecutive generated records
     * @return the generated {@link ConsumerRecord consumer records}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public List<ConsumerRecord<byte[], byte[]>> create(final String topicName,
                                                       final List<KeyValue<K, V>> keyValues,
                                                       final long startTimestamp,
                                                       final long advanceMs) {
        if (advanceMs < 0) {
            throw new IllegalArgumentException("advanceMs must be positive");
        }

        final List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>(keyValues.size());

        long timestamp = startTimestamp;
        for (final KeyValue<K, V> keyValue : keyValues) {
            records.add(create(topicName, keyValue.key, keyValue.value, new RecordHeaders(), timestamp));
            timestamp += advanceMs;
        }

        return records;
    }

    /**
     * Creates {@link ConsumerRecord consumer records} with default topic name as well as given keys and values.
     * Does not auto advance internally tracked time.
     *
     * @param keyValues the record keys and values
     * @param startTimestamp the timestamp for the first generated record
     * @param advanceMs the time difference between two consecutive generated records
     * @return the generated {@link ConsumerRecord consumer records}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public List<ConsumerRecord<byte[], byte[]>> create(final List<KeyValue<K, V>> keyValues,
                                                       final long startTimestamp,
                                                       final long advanceMs) {
        if (topicName == null) {
            throw new IllegalStateException("ConsumerRecordFactory was created without defaultTopicName. " +
                "Use #create(String topicName, List<KeyValue<K, V>> keyValues, long startTimestamp, long advanceMs) instead.");
        }

        return create(topicName, keyValues, startTimestamp, advanceMs);
    }

    /**
     * Creates {@link ConsumerRecord consumer records} with the given topic name, keys and values.
     * For each generated record, the time is advanced by 1.
     * Does not auto advance internally tracked time.
     *
     * @param topicName the topic name
     * @param keyValues the record keys and values
     * @param startTimestamp the timestamp for the first generated record
     * @return the generated {@link ConsumerRecord consumer records}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public List<ConsumerRecord<byte[], byte[]>> create(final String topicName,
                                                       final List<KeyValue<K, V>> keyValues,
                                                       final long startTimestamp) {
        return create(topicName, keyValues, startTimestamp, 1);
    }

    /**
     * Creates {@link ConsumerRecord consumer records} with the given keys and values.
     * For each generated record, the time is advanced by 1.
     * Does not auto advance internally tracked time.
     *
     * @param keyValues the record keys and values
     * @param startTimestamp the timestamp for the first generated record
     * @return the generated {@link ConsumerRecord consumer records}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public List<ConsumerRecord<byte[], byte[]>> create(final List<KeyValue<K, V>> keyValues,
                                                       final long startTimestamp) {
        if (topicName == null) {
            throw new IllegalStateException("ConsumerRecordFactory was created without defaultTopicName. " +
                "Use #create(String topicName, List<KeyValue<K, V>> keyValues, long startTimestamp) instead.");
        }

        return create(topicName, keyValues, startTimestamp, 1);
    }

}
