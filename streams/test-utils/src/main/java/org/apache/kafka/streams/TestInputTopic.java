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

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.test.TestRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * TestInputTopic is used to pipe records to topic in {@link TopologyTestDriver}.
 * To use {@code TestInputTopic} create new class {@link TopologyTestDriver#createInputTopic(String, Serializer, Serializer)}
 * In actual test code, you can pipe new message values, keys and values or list of {@link KeyValue}
 * You need to have own TestInputTopic object for each topic.
 *
 *
 * <h2>Processing messages</h2>
 * <pre>{@code
 *     private TestInputTopic<Long, String> inputTopic;
 *     ...
 *     inputTopic = testDriver.createInputTopic(INPUT_TOPIC, longSerde, stringSerde);
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
    private Instant currentTime;
    private final Duration advanceDuration;

    /**
     * Create a test input topic to pipe messages in.
     * Uses current system time as start timestamp.
     * Auto-advance is disabled.
     *
     * @param driver     TopologyTestDriver to use
     * @param topicName  the topic name used
     * @param keySerializer   the key serializer
     * @param valueSerializer the value serializer
     */
    TestInputTopic(final TopologyTestDriver driver,
                          final String topicName,
                           final Serializer<K> keySerializer,
                           final Serializer<V> valueSerializer) {
        this(driver, topicName, keySerializer, valueSerializer, Instant.now(), Duration.ZERO);
    }

    /**
     * Create a test input topic to pipe messages in.
     * Validate inputs
     *
     * @param driver    TopologyTestDriver to use
     * @param topicName the topic name used
     * @param keySerializer   the key serializer
     * @param valueSerializer the value serializer
     * @param startTimestamp the initial timestamp for records
     * @param autoAdvance the time increment per record
     */
    TestInputTopic(final TopologyTestDriver driver,
                             final String topicName,
                             final Serializer<K> keySerializer,
                             final Serializer<V> valueSerializer,
                             final Instant startTimestamp,
                             final Duration autoAdvance) {
        Objects.requireNonNull(driver, "TopologyTestDriver cannot be null");
        Objects.requireNonNull(topicName, "topicName cannot be null");
        Objects.requireNonNull(keySerializer, "keySerializer cannot be null");
        Objects.requireNonNull(valueSerializer, "valueSerializer cannot be null");
        Objects.requireNonNull(startTimestamp, "startTimestamp cannot be null");
        Objects.requireNonNull(autoAdvance, "autoAdvance cannot be null");
        this.driver = driver;
        this.topic = topicName;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.currentTime = startTimestamp;
        if (autoAdvance.isNegative()) {
            throw new IllegalArgumentException("autoAdvance must be positive");
        }
        this.advanceDuration = autoAdvance;

    }

    /**
     * Advances the internally tracked time.
     *
     * @param advance the duration of time to advance
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void advanceTime(final Duration advance) {
        if (advance.isNegative()) {
            throw new IllegalArgumentException("advance must be positive");
        }
        currentTime = currentTime.plus(advance);
    }

    private Instant getTimestampAndAdvanced() {
        final Instant timestamp = currentTime;
        currentTime = currentTime.plus(advanceDuration);
        return timestamp;
    }

    /**
     * Send an input message with the given record on the topic and then commit the messages.
     *
     * @param record the record to sent
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeInput(final TestRecord<K, V> record) {
        //if record timestamp not set get timestamp and advance
        final Instant timestamp = (record.getRecordTime() == null) ? getTimestampAndAdvanced() : record.getRecordTime();
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
     * @param timestamp the record timestamp
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeInput(final V value,
                          final Instant timestamp) {
        pipeInput(new TestRecord<K, V>(null, value, timestamp));
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
        pipeInput(new TestRecord<K, V>(key, value, null, timestampMs));
    }

    /**
     * Send an input message with the given key, value and timestamp on the topic and then commit the messages.
     * Does not auto advance internally tracked time.
     *
     * @param key         the record key
     * @param value       the record value
     * @param timestamp the record timestamp
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeInput(final K key,
                          final V value,
                          final Instant timestamp) {
        pipeInput(new TestRecord<K, V>(key, value, timestamp));
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
     * @param advance        the time difference between two consecutive generated records
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeKeyValueList(final List<KeyValue<K, V>> keyValues,
                                 final Instant startTimestamp,
                                 final Duration advance) {
        Instant recordTime = startTimestamp;
        for (final KeyValue<K, V> keyValue : keyValues) {
            pipeInput(keyValue.key, keyValue.value, recordTime);
            recordTime = recordTime.plus(advance);
        }
    }

    /**
     * Send input messages with the given value list on the topic then commit each message individually.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param values         the list of KeyValue records
     * @param startTimestamp the timestamp for the first generated record
     * @param advance        the time difference between two consecutive generated records
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeValueList(final List<V> values,
                              final Instant startTimestamp,
                              final Duration advance) {
        Instant recordTime = startTimestamp;
        for (final V value : values) {
            pipeInput(value, recordTime);
            recordTime = recordTime.plus(advance);
        }
    }

    @Override
    public String toString() {
        return "TestInputTopic{topic='" + topic + "'}";
    }
}
