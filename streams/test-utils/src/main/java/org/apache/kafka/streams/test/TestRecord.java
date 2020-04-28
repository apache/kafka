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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;

import java.time.Instant;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * A key/value pair, including timestamp and record headers, to be sent to or received from {@link TopologyTestDriver}.
 * If [a] record does not contain a timestamp,
 * {@link TestInputTopic} will auto advance it's time when the record is piped.
 */
public class TestRecord<K, V> {
    private final Headers headers;
    private final K key;
    private final V value;
    private final Instant recordTime;

    /**
     * Creates a record.
     *
     * @param key The key that will be included in the record
     * @param value The value of the record
     * @param headers the record headers that will be included in the record
     * @param recordTime The timestamp of the record.
     */
    public TestRecord(final K key, final V value, final Headers headers, final Instant recordTime) {
        this.key = key;
        this.value = value;
        this.recordTime = recordTime;
        this.headers = new RecordHeaders(headers);
    }

    /**
     * Creates a record.
     * 
     * @param key The key that will be included in the record
     * @param value The value of the record
     * @param headers the record headers that will be included in the record
     * @param timestampMs The timestamp of the record, in milliseconds since the beginning of the epoch.
     */
    public TestRecord(final K key, final V value, final Headers headers, final Long timestampMs) {
        if (timestampMs != null) {
            if (timestampMs < 0) {
                throw new IllegalArgumentException(
                    String.format("Invalid timestamp: %d. Timestamp should always be non-negative or null.", timestampMs));
            }
            this.recordTime = Instant.ofEpochMilli(timestampMs);
        } else {
            this.recordTime = null;
        }
        this.key = key;
        this.value = value;
        this.headers = new RecordHeaders(headers);
    }

    /**
     * Creates a record.
     *
     * @param key The key of the record
     * @param value The value of the record
     * @param recordTime The timestamp of the record as Instant.
     */
    public TestRecord(final K key, final V value, final Instant recordTime) {
        this(key, value, null, recordTime);
    }

    /**
     * Creates a record.
     *
     * @param key The key of the record
     * @param value The value of the record
     * @param headers The record headers that will be included in the record
     */
    public TestRecord(final K key, final V value, final Headers headers) {
        this.key = key;
        this.value = value;
        this.headers = new RecordHeaders(headers);
        this.recordTime = null;
    }
    
    /**
     * Creates a record.
     *
     * @param key The key of the record
     * @param value The value of the record
     */
    public TestRecord(final K key, final V value) {
        this.key = key;
        this.value = value;
        this.headers = new RecordHeaders();
        this.recordTime = null;
    }

    /**
     * Create a record with {@code null} key.
     *
     * @param value The value of the record
     */
    public TestRecord(final V value) {
        this(null, value);
    }

    /**
     * Create a {@code TestRecord} from a {@link ConsumerRecord}.
     *
     * @param record The v
     */
    public TestRecord(final ConsumerRecord<K, V> record) {
        Objects.requireNonNull(record);
        this.key = record.key();
        this.value = record.value();
        this.headers = record.headers();
        this.recordTime = Instant.ofEpochMilli(record.timestamp());
    }

    /**
     * Create a {@code TestRecord} from a {@link ProducerRecord}.
     *
     * @param record The record contents
     */
    public TestRecord(final ProducerRecord<K, V> record) {
        Objects.requireNonNull(record);
        this.key = record.key();
        this.value = record.value();
        this.headers = record.headers();
        this.recordTime = Instant.ofEpochMilli(record.timestamp());
    }

    /**
     * @return The headers.
     */
    public Headers headers() {
        return headers;
    }

    /**
     * @return The key (or {@code null} if no key is specified).
     */
    public K key() {
        return key;
    }

    /**
     * @return The value.
     */
    public V value() {
        return value;
    }

    /**
     * @return The timestamp, which is in milliseconds since epoch.
     */
    public Long timestamp() {
        return this.recordTime == null ? null : this.recordTime.toEpochMilli();
    }

    /**
     * @return The headers.
     */
    public Headers getHeaders() {
        return headers;
    }

    /**
     * @return The key (or null if no key is specified)
     */
    public K getKey() {
        return key;
    }

    /**
     * @return The value.
     */
    public V getValue() {
        return value;
    }

    /**
     * @return The timestamp.
     */
    public Instant getRecordTime() {
        return recordTime;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", TestRecord.class.getSimpleName() + "[", "]")
                .add("key=" + key)
                .add("value=" + value)
                .add("headers=" + headers)
                .add("recordTime=" + recordTime)
                .toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TestRecord<?, ?> that = (TestRecord<?, ?>) o;
        return Objects.equals(headers, that.headers) &&
            Objects.equals(key, that.key) &&
            Objects.equals(value, that.value) &&
            Objects.equals(recordTime, that.recordTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(headers, key, value, recordTime);
    }
}
