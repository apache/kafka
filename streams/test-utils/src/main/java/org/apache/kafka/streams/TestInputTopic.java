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
import java.util.StringJoiner;

/**
 * {@code TestInputTopic} is used to pipe records to topic in {@link TopologyTestDriver}.
 * To use {@code TestInputTopic} create a new instance via
 * {@link TopologyTestDriver#createInputTopic(String, Serializer, Serializer)}.
 * In actual test code, you can pipe new record values, keys and values or list of {@link KeyValue} pairs.
 * If you have multiple source topics, you need to create a {@code TestInputTopic} for each.
 *
 * <h2>Processing messages</h2>
 * <pre>{@code
 *     private TestInputTopic<Long, String> inputTopic;
 *     ...
 *     inputTopic = testDriver.createInputTopic(INPUT_TOPIC, longSerializer, stringSerializer);
 *     ...
 *     inputTopic.pipeInput("Hello");
 * }</pre>
 *
 * @param <K> the type of the record key
 * @param <V> the type of the record value
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
     * Advances the internally tracked event time of this input topic.
     * Each time a record without explicitly defined timestamp is piped,
     * the current topic event time is used as record timestamp.
     * <p>
     * Note: advancing the event time on the input topic, does not advance the tracked stream time in
     * {@link TopologyTestDriver} as long as no new input records are piped.
     * Furthermore, it does not advance the wall-clock time of {@link TopologyTestDriver}.
     *
     * @param advance the duration of time to advance
     */
    public void advanceTime(final Duration advance) {
        if (advance.isNegative()) {
            throw new IllegalArgumentException("advance must be positive");
        }
        currentTime = currentTime.plus(advance);
    }

    private Instant getTimestampAndAdvance() {
        final Instant timestamp = currentTime;
        currentTime = currentTime.plus(advanceDuration);
        return timestamp;
    }

    /**
     * Send an input record with the given record on the topic and then commit the records.
     * May auto advance topic time.
     *
     * @param record the record to sent
     */
    public void pipeInput(final TestRecord<K, V> record) {
        //if record timestamp not set get timestamp and advance
        final Instant timestamp = (record.getRecordTime() == null) ? getTimestampAndAdvance() : record.getRecordTime();
        driver.pipeRecord(topic, record, keySerializer, valueSerializer, timestamp);
    }

    /**
     * Send an input record with the given value on the topic and then commit the records.
     * May auto advance topic time.
     *
     * @param value the record value
     */
    public void pipeInput(final V value) {
        pipeInput(new TestRecord<>(value));
    }

    /**
     * Send an input record with the given key and value on the topic and then commit the records.
     * May auto advance topic time
     *
     * @param key   the record key
     * @param value the record value
     */
    public void pipeInput(final K key,
                          final V value) {
        pipeInput(new TestRecord<>(key, value));
    }

    /**
     * Send an input record with the given value and timestamp on the topic and then commit the records.
     * Does not auto advance internally tracked time.
     *
     * @param value       the record value
     * @param timestamp the record timestamp
     */
    public void pipeInput(final V value,
                          final Instant timestamp) {
        pipeInput(new TestRecord<>(null, value, timestamp));
    }

    /**
     * Send an input record with the given key, value and timestamp on the topic and then commit the records.
     * Does not auto advance internally tracked time.
     *
     * @param key         the record key
     * @param value       the record value
     * @param timestampMs the record timestamp
     */
    public void pipeInput(final K key,
                          final V value,
                          final long timestampMs) {
        pipeInput(new TestRecord<>(key, value, null, timestampMs));
    }

    /**
     * Send an input record with the given key, value and timestamp on the topic and then commit the records.
     * Does not auto advance internally tracked time.
     *
     * @param key         the record key
     * @param value       the record value
     * @param timestamp the record timestamp
     */
    public void pipeInput(final K key,
                          final V value,
                          final Instant timestamp) {
        pipeInput(new TestRecord<>(key, value, timestamp));
    }

    /**
     * Send input records with the given KeyValue  list on the topic  then commit each record individually.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param records the list of TestRecord records
     */
    public void pipeRecordList(final List<? extends TestRecord<K, V>> records) {
        for (final TestRecord<K, V> record : records) {
            pipeInput(record);
        }
    }

    /**
     * Send input records with the given KeyValue list on the topic then commit each record individually.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance based on
     * {@link #TestInputTopic(TopologyTestDriver, String, Serializer, Serializer, Instant, Duration) autoAdvance} setting.
     *
     * @param keyValues the {@link List} of {@link KeyValue} records
     */
    public void pipeKeyValueList(final List<KeyValue<K, V>> keyValues) {
        for (final KeyValue<K, V> keyValue : keyValues) {
            pipeInput(keyValue.key, keyValue.value);
        }
    }

    /**
     * Send input records with the given value list on the topic then commit each record individually.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance based on
     * {@link #TestInputTopic(TopologyTestDriver, String, Serializer, Serializer, Instant, Duration) autoAdvance} setting.
     *
     * @param values the {@link List} of {@link KeyValue} records
     */
    public void pipeValueList(final List<V> values) {
        for (final V value : values) {
            pipeInput(value);
        }
    }

    /**
     * Send input records with the given {@link KeyValue} list on the topic then commit each record individually.
     * Does not auto advance internally tracked time.
     *
     * @param keyValues      the {@link List} of {@link KeyValue} records
     * @param startTimestamp the timestamp for the first generated record
     * @param advance        the time difference between two consecutive generated records
     */
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
     * Send input records with the given value list on the topic then commit each record individually.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance based on
     * {@link #TestInputTopic(TopologyTestDriver, String, Serializer, Serializer, Instant, Duration) autoAdvance} setting.
     *
     * @param values         the {@link List} of values
     * @param startTimestamp the timestamp for the first generated record
     * @param advance        the time difference between two consecutive generated records
     */
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
        return new StringJoiner(", ", TestInputTopic.class.getSimpleName() + "[", "]")
                .add("topic='" + topic + "'")
                .add("keySerializer=" + keySerializer.getClass().getSimpleName())
                .add("valueSerializer=" + valueSerializer.getClass().getSimpleName())
                .toString();
    }
}
