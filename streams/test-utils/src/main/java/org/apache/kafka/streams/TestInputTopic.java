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
package org.apache.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.streams.test.TestRecordFactory;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * TestInputTopic is used to pipe records to topic in {@link TopologyTestDriver}.
 * This class combines functionality of {@link TopologyTestDriver} and {@link ConsumerRecordFactory}.
 * To use {@code TestInputTopic} create new class with topicName and correct Serdes or Serializers
 * In actual test code, you can pipe new message values, keys and values or list of {@link KeyValue}
 * without needing to pass serdes each time. You need to have own TestInputTopic object for each topic.
 *
 *
 * <h2>Processing messages</h2>
 * <pre>{@code
 *     private TestInputTopic<String, String> inputTopic;
 *     ...
 *     inputTopic = new TestInputTopic<>(testDriver, inputTopic, new Serdes.StringSerde(), new Serdes.StringSerde());
 *     ...
 *     inputTopic.pipeInput("Hello");
 * }</pre>
 *
 * @param <K> the type of the Kafka key
 * @param <V> the type of the Kafka value
 * @see TopologyTestDriver
 */

public class TestInputTopic<K, V> {
    private final TopologyTestDriver driver;
    private final String topic;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    //Timing
    private long timeMs;
    private long advanceMs;

    /**
     * Create a test input topic to pipe messages in.
     * Uses current system time as start timestamp.
     * Auto-advance is disabled.
     *
     * @param driver     TopologyTestDriver to use
     * @param topicName  the topic name used
     * @param keySerde   the key serializer
     * @param valueSerde the value serializer
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestInputTopic(final TopologyTestDriver driver,
                          final String topicName,
                          final Serde<K> keySerde,
                          final Serde<V> valueSerde) {
        this(driver, topicName, keySerde.serializer(), valueSerde.serializer());
    }

    /**
     * Create a test input topic to pipe messages in.
     * Validate inputs
     *
     * @param driver    TopologyTestDriver to use
     * @param topicName the topic name used
     * @param keySerializer   the key serializer
     * @param valueSerializer the value serializer
     */
    @SuppressWarnings("WeakerAccess")
    protected TestInputTopic(final TopologyTestDriver driver,
                             final String topicName,
                             final Serializer<K> keySerializer,
                             final Serializer<V> valueSerializer) {
        Objects.requireNonNull(driver, "TopologyTestDriver cannot be null");
        Objects.requireNonNull(topicName, "topicName cannot be null");
        Objects.requireNonNull(keySerializer, "keySerializer cannot be null");
        Objects.requireNonNull(valueSerializer, "valueSerializer cannot be null");
        this.driver = driver;
        this.topic = topicName;
        this.keySerializer=keySerializer;
        this.valueSerializer=valueSerializer;
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

    public void configureTiming(final long startTimestampMs,
                                final long autoAdvanceMs) {
        timeMs = startTimestampMs;
        if (autoAdvanceMs < 0) {
            throw new IllegalArgumentException("advanceMs must be positive");
        }
        advanceMs = autoAdvanceMs;
    }

    private long getTimestampAndAdvanced() {
        final long timestamp = timeMs;
        timeMs += advanceMs;
        return timestamp;
    }

    public void pipeInput(final TestRecord<K, V> record) {
        //if record timestamp not set get timestamp and advance
        long timestamp = record.timestamp() == null ? getTimestampAndAdvanced() : record.timestamp();
        driver.pipeRecord(topic, record, keySerializer, valueSerializer, timestamp);
    }

    /**
     * Send an input message with the given value on the topic and then commit the messages.
     *
     * @param value the record value
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeInput(final V value) {
        pipeInput(new TestRecord<>(value));
    }

    /**
     * Send an input message with the given key and value on the topic and then commit the messages.
     *
     * @param key   the record key
     * @param value the record value
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeInput(final K key, final V value) {
        pipeInput(new TestRecord<>(key, value));
    }

    /**
     * Send an input message with the given key and timestamp on the topic and then commit the messages.
     * Does not auto advance internally tracked time.
     *
     * @param value       the record value
     * @param timestampMs the record timestamp
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeInput(final V value,
                          final long timestampMs) {
        pipeInput(new TestRecord<>(null, value, timestampMs));
    }

    /**
     * Send an input message with the given key, value and timestamp on the topic and then commit the messages.
     * Does not auto advance internally tracked time.
     *
     * @param key         the record key
     * @param value       the record value
     * @param timestampMs the record timestamp
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeInput(final K key,
                          final V value,
                          final long timestampMs) {
        pipeInput(new TestRecord<>(key, value, timestampMs));
    }

    /**
     * Send an input message with the given key, value and headers on the topic and then commit the messages.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param key     the record key
     * @param value   the record value
     * @param headers the record headers
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeInput(final K key,
                          final V value,
                          final Headers headers) {
        pipeInput(new TestRecord<>(key, value, headers));
    }


    /**
     * Send an input message with the given key, value, timestamp and headers on the topic and then commit the messages.
     * Does not auto advance internally tracked time.
     *
     * @param key         the record key
     * @param value       the record value
     * @param headers     the record headers
     * @param timestampMs the record timestamp
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeInput(final K key,
                          final V value,
                          final Headers headers,
                          final long timestampMs) {
        pipeInput(new TestRecord<>(key, value, headers, timestampMs));
    }

    /**
     * Send input messages with the given KeyValue  list on the topic  then commit each message individually.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param records the list of TestRecord records
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeRecordList(final List<? extends TestRecord<K, V>> records) {
        for (final TestRecord<K, V> record : records) {
            pipeInput(record);
        }
    }

    /**
     * Send input messages with the given KeyValue  list on the topic  then commit each message individually.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param keyValues the list of KeyValue records
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeKeyValueList(final List<KeyValue<K, V>> keyValues) {
        for (final KeyValue<K, V> keyValue : keyValues) {
            pipeInput(keyValue.key, keyValue.value);
        }
    }

    /**
     * Send input messages with the given value list on the topic then commit each message individually.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param values the list of KeyValue records
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeValueList(final List<V> values) {
        for (final V value : values) {
            pipeInput(value);
        }
    }

    /**
     * Send input messages with the given KeyValue  list on the topic  then commit each message individually.
     * Does not auto advance internally tracked time.
     *
     * @param keyValues      the list of KeyValue records
     * @param startTimestamp the timestamp for the first generated record
     * @param advanceMs      the time difference between two consecutive generated records
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeKeyValueList(final List<KeyValue<K, V>> keyValues,
                                 final long startTimestamp,
                                 final long advanceMs) {
        long timeMs = startTimestamp;
        for (final KeyValue<K, V> keyValue : keyValues) {
            pipeInput(keyValue.key, keyValue.value, timeMs);
            timeMs += advanceMs;
        }
    }

    /**
     * Send input messages with the given value list on the topic then commit each message individually.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param values         the list of KeyValue records
     * @param startTimestamp the timestamp for the first generated record
     * @param advanceMs      the time difference between two consecutive generated records
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeValueList(final List<V> values,
                              final long startTimestamp,
                              final long advanceMs) {
        long timeMs = startTimestamp;
        for (final V value : values) {
            pipeInput(value, timeMs);
            timeMs += advanceMs;
        }
    }

    @Override
    public String toString() {
        return "TestInputTopic{topic='" + topic + "'}";
    }
}
