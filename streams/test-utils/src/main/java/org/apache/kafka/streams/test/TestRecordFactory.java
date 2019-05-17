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

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Factory to create {@link TestRecord records} for a single single-partitioned topic with given key and
 * value {@link Serializer serializers}.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 *
 * @see org.apache.kafka.streams.TopologyTestDriver
 */
@InterfaceStability.Evolving
public class TestRecordFactory<K, V> {
    private final String topicName;
    private long timeMs;
    private final long advanceMs;

    /**
     * Create a new factory for the given topic.
     * Uses current system time as start timestamp.
     * Auto-advance is disabled.
     *
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecordFactory() {
        this(null, System.currentTimeMillis());
    }

    /**
     * Create a new factory for the given topic.
     * Uses current system time as start timestamp.
     * Auto-advance is disabled.
     *
     * @param defaultTopicName the default topic name used for all generated {@link TestRecord records}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecordFactory(final String defaultTopicName) {
        this(defaultTopicName, System.currentTimeMillis());
    }

    /**
     * Create a new factory for the given topic.
     * Auto-advance is disabled.
     *
     * @param startTimestampMs the initial timestamp for generated records
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecordFactory(final long startTimestampMs) {
        this(null, startTimestampMs, 0L);
    }

    /**
     * Create a new factory for the given topic.
     * Auto-advance is disabled.
     *
     * @param defaultTopicName the topic name used for all generated {@link TestRecord records}
     * @param startTimestampMs the initial timestamp for generated records
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecordFactory(final String defaultTopicName,
                             final long startTimestampMs) {
        this(defaultTopicName, startTimestampMs, 0L);
    }

    /**
     * Create a new factory for the given topic.
     *
     * @param startTimestampMs the initial timestamp for generated records
     * @param autoAdvanceMs the time increment pre generated record
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecordFactory(final long startTimestampMs,
                             final long autoAdvanceMs) {
        this(null, startTimestampMs, autoAdvanceMs);
    }

    /**
     * Create a new factory for the given topic.
     *
     * @param defaultTopicName the topic name used for all generated {@link TestRecord records}
     * @param startTimestampMs the initial timestamp for generated records
     * @param autoAdvanceMs the time increment pre generated record
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecordFactory(final String defaultTopicName,
                             final long startTimestampMs,
                             final long autoAdvanceMs) {
        this.topicName = defaultTopicName;
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
     * Create a {@link TestRecord} with the given topic name, key, value, headers, and timestamp.
     * Does not auto advance internally tracked time.
     *
     * @param topicName the topic name
     * @param key the record key
     * @param value the record value
     * @param headers the record headers
     * @param timestampMs the record timestamp
     * @return the generated {@link TestRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecord<K, V> create(final String topicName,
                                   final K key,
                                   final V value,
                                                 final Headers headers,
                                                 final long timestampMs) {
        Objects.requireNonNull(topicName, "topicName cannot be null.");
        Objects.requireNonNull(headers, "headers cannot be null.");
        return new TestRecord<>(
            topicName,
            timestampMs,
            key,
            value,
            headers);
    }

    /**
     * Create a {@link TestRecord} with the given topic name and given topic, key, value, and timestamp.
     * Does not auto advance internally tracked time.
     *
     * @param topicName the topic name
     * @param key the record key
     * @param value the record value
     * @param timestampMs the record timestamp
     * @return the generated {@link TestRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecord<K, V> create(final String topicName,
                                                 final K key,
                                                 final V value,
                                                 final long timestampMs) {
        return create(topicName, key, value, new RecordHeaders(), timestampMs);
    }

    /**
     * Create a {@link TestRecord} with default topic name and given key, value, and timestamp.
     * Does not auto advance internally tracked time.
     *
     * @param key the record key
     * @param value the record value
     * @param timestampMs the record timestamp
     * @return the generated {@link TestRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecord<K, V> create(final K key,
                                                 final V value,
                                                 final long timestampMs) {
        return create(key, value, new RecordHeaders(), timestampMs);
    }

    /**
     * Create a {@link TestRecord} with default topic name and given key, value, headers, and timestamp.
     * Does not auto advance internally tracked time.
     *
     * @param key the record key
     * @param value the record value
     * @param headers the record headers
     * @param timestampMs the record timestamp
     * @return the generated {@link TestRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecord<K, V> create(final K key,
                                                 final V value,
                                                 final Headers headers,
                                                 final long timestampMs) {
        if (topicName == null) {
            throw new IllegalStateException("TestRecordFactory was created without defaultTopicName. " +
                "Use #create(String topicName, K key, V value, long timestampMs) instead.");
        }
        return create(topicName, key, value, headers, timestampMs);
    }

    /**
     * Create a {@link TestRecord} with the given topic name, key, and value.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param topicName the topic name
     * @param key the record key
     * @param value the record value
     * @return the generated {@link TestRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecord<K, V> create(final String topicName,
                                                 final K key,
                                                 final V value) {
        final long timestamp = timeMs;
        timeMs += advanceMs;
        return create(topicName, key, value, new RecordHeaders(), timestamp);
    }

    /**
     * Create a {@link TestRecord} with the given topic name, key, value, and headers.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param topicName the topic name
     * @param key the record key
     * @param value the record value
     * @param headers the record headers
     * @return the generated {@link TestRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecord<K, V> create(final String topicName,
                                                 final K key,
                                                 final V value,
                                                 final Headers headers) {
        final long timestamp = timeMs;
        timeMs += advanceMs;
        return create(topicName, key, value, headers, timestamp);
    }

    /**
     * Create a {@link TestRecord} with default topic name and given key and value.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param key the record key
     * @param value the record value
     * @return the generated {@link TestRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecord<K, V> create(final K key,
                                                 final V value) {
        return create(key, value, new RecordHeaders());
    }

    /**
     * Create a {@link TestRecord} with default topic name and given key, value, and headers.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param key the record key
     * @param value the record value
     * @param headers the record headers
     * @return the generated {@link TestRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecord<K, V> create(final K key,
                                                 final V value,
                                                 final Headers headers) {
        if (topicName == null) {
            throw new IllegalStateException("TestRecordFactory was created without defaultTopicName. " +
                "Use #create(String topicName, K key, V value) instead.");
        }
        return create(topicName, key, value, headers);
    }

    /**
     * Create a {@link TestRecord} with {@code null}-key and the given topic name, value, and timestamp.
     * Does not auto advance internally tracked time.
     *
     * @param topicName the topic name
     * @param value the record value
     * @param timestampMs the record timestamp
     * @return the generated {@link TestRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecord<K, V> create(final String topicName,
                                                 final V value,
                                                 final long timestampMs) {
        return create(topicName, null, value, new RecordHeaders(), timestampMs);
    }

    /**
     * Create a {@link TestRecord} with {@code null}-key and the given topic name, value, headers, and timestamp.
     * Does not auto advance internally tracked time.
     *
     * @param topicName the topic name
     * @param value the record value
     * @param headers the record headers
     * @param timestampMs the record timestamp
     * @return the generated {@link TestRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecord<K, V> create(final String topicName,
                                                 final V value,
                                                 final Headers headers,
                                                 final long timestampMs) {
        return create(topicName, null, value, headers, timestampMs);
    }

    /**
     * Create a {@link TestRecord} with default topic name and {@code null}-key as well as given value and timestamp.
     * Does not auto advance internally tracked time.
     *
     * @param value the record value
     * @param timestampMs the record timestamp
     * @return the generated {@link TestRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecord<K, V> create(final V value,
                                                 final long timestampMs) {
        return create(value, new RecordHeaders(), timestampMs);
    }

    /**
     * Create a {@link TestRecord} with default topic name and {@code null}-key as well as given value, headers, and timestamp.
     * Does not auto advance internally tracked time.
     *
     * @param value the record value
     * @param headers the record headers
     * @param timestampMs the record timestamp
     * @return the generated {@link TestRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecord<K, V> create(final V value,
                                                 final Headers headers,
                                                 final long timestampMs) {
        if (topicName == null) {
            throw new IllegalStateException("TestRecordFactory was created without defaultTopicName. " +
                "Use #create(String topicName, V value, long timestampMs) instead.");
        }
        return create(topicName, value, headers, timestampMs);
    }

    /**
     * Create a {@link TestRecord} with {@code null}-key and the given topic name, value, and headers.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param topicName the topic name
     * @param value the record value
     * @param headers the record headers
     * @return the generated {@link TestRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecord<K, V> create(final String topicName,
                                                 final V value,
                                                 final Headers headers) {
        return create(topicName, null, value, headers);
    }

    /**
     * Create a {@link TestRecord} with {@code null}-key and the given topic name and value.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param topicName the topic name
     * @param value the record value
     * @return the generated {@link TestRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecord<K, V> create(final String topicName,
                                                 final V value) {
        return create(topicName, null, value, new RecordHeaders());
    }

    /**
     * Create a {@link TestRecord} with default topic name and {@code null}-key was well as given value.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param value the record value
     * @return the generated {@link TestRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecord<K, V> create(final V value) {
        return create(value, new RecordHeaders());
    }

    /**
     * Create a {@link TestRecord} with default topic name and {@code null}-key was well as given value and headers.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param value the record value
     * @param headers the record headers
     * @return the generated {@link TestRecord}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecord<K, V> create(final V value,
                                                 final Headers headers) {
        if (topicName == null) {
            throw new IllegalStateException("TestRecordFactory was created without defaultTopicName. " +
                "Use #create(String topicName, V value, long timestampMs) instead.");
        }
        return create(topicName, value, headers);
    }

    /**
     * Creates {@link TestRecord records} with the given topic name, keys, and values.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param topicName the topic name
     * @param keyValues the record keys and values
     * @return the generated {@link TestRecord records}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public List<TestRecord<K, V>> create(final String topicName,
                                                       final List<KeyValue<K, V>> keyValues) {
        final List<TestRecord<K, V>> records = new ArrayList<>(keyValues.size());

        for (final KeyValue<K, V> keyValue : keyValues) {
            records.add(create(topicName, keyValue.key, keyValue.value));
        }

        return records;
    }

    /**
     * Creates {@link TestRecord records} with default topic name as well as given keys and values.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param keyValues the record keys and values
     * @return the generated {@link TestRecord records}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public List<TestRecord<K, V>> create(final List<KeyValue<K, V>> keyValues) {
        if (topicName == null) {
            throw new IllegalStateException("TestRecordFactory was created without defaultTopicName. " +
                "Use #create(String topicName, List<KeyValue<K, V>> keyValues) instead.");
        }

        return create(topicName, keyValues);
    }

    /**
     * Creates {@link TestRecord records} with the given topic name, keys, and values.
     * Does not auto advance internally tracked time.
     *
     * @param topicName the topic name
     * @param keyValues the record keys and values
     * @param startTimestamp the timestamp for the first generated record
     * @param advanceMs the time difference between two consecutive generated records
     * @return the generated {@link TestRecord records}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public List<TestRecord<K, V>> create(final String topicName,
                                                       final List<KeyValue<K, V>> keyValues,
                                                       final long startTimestamp,
                                                       final long advanceMs) {
        if (advanceMs < 0) {
            throw new IllegalArgumentException("advanceMs must be positive");
        }

        final List<TestRecord<K, V>> records = new ArrayList<>(keyValues.size());

        long timestamp = startTimestamp;
        for (final KeyValue<K, V> keyValue : keyValues) {
            records.add(create(topicName, keyValue.key, keyValue.value, new RecordHeaders(), timestamp));
            timestamp += advanceMs;
        }

        return records;
    }

    /**
     * Creates {@link TestRecord records} with default topic name as well as given keys and values.
     * Does not auto advance internally tracked time.
     *
     * @param keyValues the record keys and values
     * @param startTimestamp the timestamp for the first generated record
     * @param advanceMs the time difference between two consecutive generated records
     * @return the generated {@link TestRecord records}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public List<TestRecord<K, V>> create(final List<KeyValue<K, V>> keyValues,
                                                       final long startTimestamp,
                                                       final long advanceMs) {
        if (topicName == null) {
            throw new IllegalStateException("TestRecordFactory was created without defaultTopicName. " +
                "Use #create(String topicName, List<KeyValue<K, V>> keyValues, long startTimestamp, long advanceMs) instead.");
        }

        return create(topicName, keyValues, startTimestamp, advanceMs);
    }

    /**
     * Creates {@link TestRecord records} with the given topic name, keys and values.
     * For each generated record, the time is advanced by 1.
     * Does not auto advance internally tracked time.
     *
     * @param topicName the topic name
     * @param keyValues the record keys and values
     * @param startTimestamp the timestamp for the first generated record
     * @return the generated {@link TestRecord records}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public List<TestRecord<K, V>> create(final String topicName,
                                                       final List<KeyValue<K, V>> keyValues,
                                                       final long startTimestamp) {
        return create(topicName, keyValues, startTimestamp, 1);
    }

    /**
     * Creates {@link TestRecord records} with the given keys and values.
     * For each generated record, the time is advanced by 1.
     * Does not auto advance internally tracked time.
     *
     * @param keyValues the record keys and values
     * @param startTimestamp the timestamp for the first generated record
     * @return the generated {@link TestRecord records}
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public List<TestRecord<K, V>> create(final List<KeyValue<K, V>> keyValues,
                                                       final long startTimestamp) {
        if (topicName == null) {
            throw new IllegalStateException("TestRecordFactory was created without defaultTopicName. " +
                "Use #create(String topicName, List<KeyValue<K, V>> keyValues, long startTimestamp) instead.");
        }

        return create(topicName, keyValues, startTimestamp, 1);
    }

}
