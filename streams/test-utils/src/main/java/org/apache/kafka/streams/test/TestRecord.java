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
import org.apache.kafka.common.annotation.InterfaceAudience;
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
@InterfaceAudience.Public
public class TestRecord<K, V> {
    private static final int NO_PARTITION = -1;
    private final Headers headers;
    private final K key;
    private final V value;
    private final Instant recordTime;
    /**
     * The partition this record is assigned to.
     * A value of {@code -1} is a sentinel meaning "no explicit partition set" and is used
     * only on <em>input</em> records created without an explicit partition argument.
     * Output records read from {@link org.apache.kafka.streams.TestOutputTopic#readRecordsToList()}
     * always carry the real resolved partition ({@code >= 0}).
     */
    private final int partition;

    /**
     * Creates a record with an explicit partition.
     *
     * @param key The key that will be included in the record
     * @param value The value of the record
     * @param headers The record headers that will be included in the record
     * @param recordTime The timestamp of the record
     * @param partition The partition this record is assigned to
     */
    public TestRecord(final K key, final V value, final Headers headers, final Instant recordTime, final int partition) {
        this.key = key;
        this.value = value;
        this.recordTime = recordTime;
        this.headers = new RecordHeaders(headers);
        if (partition < NO_PARTITION) {
            throw new IllegalArgumentException(
                String.format("Invalid partition: %d. Partition number should always be non-negative or %d.", partition, NO_PARTITION));
        }
        this.partition = partition;
    }

    /**
     * Creates a record.
     * Partition defaults to {@code -1} (no explicit partition set).
     * 
     * @param key The key that will be included in the record
     * @param value The value of the record
     * @param headers The record headers that will be included in the record
     * @param recordTime The timestamp of the record
     */
    public TestRecord(final K key, final V value, final Headers headers, final Instant recordTime) {
        this(key, value, headers, recordTime, NO_PARTITION);
    }

    /**
     * Creates a record.
     * Partition defaults to {@code -1} (no explicit partition set).
     *
     * @param key The key that will be included in the record
     * @param value The value of the record
     * @param headers The record headers that will be included in the record
     * @param timestampMs The timestamp of the record, in milliseconds since the beginning of the epoch
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
        this.partition = NO_PARTITION;
    }

    /**
     * Creates a record.
     * Partition defaults to {@code -1} (no explicit partition set).
     *
     * @param key The key of the record
     * @param value The value of the record
     * @param recordTime The timestamp of the record as Instant
     */
    public TestRecord(final K key, final V value, final Instant recordTime) {
        this(key, value, null, recordTime, NO_PARTITION);
    }

    /**
     * Creates a record.
     * Partition defaults to {@code -1} (no explicit partition set).
     *
     * @param key The key of the record
     * @param value The value of the record
     * @param headers The record headers that will be included in the record
     */
    public TestRecord(final K key, final V value, final Headers headers) {
        this(key, value, headers, (Instant) null, NO_PARTITION);
    }

    /**
     * Creates a record.
     * Partition defaults to {@code -1} (no explicit partition set).
     *
     * @param key The key of the record
     * @param value The value of the record
     */
    public TestRecord(final K key, final V value) {
        this.key = key;
        this.value = value;
        this.headers = new RecordHeaders();
        this.recordTime = null;
        this.partition = NO_PARTITION;
    }

    /**
     * Create a record with {@code null} key.
     * Partition defaults to {@code -1} (no explicit partition set).
     *
     * @param value The value of the record
     */
    public TestRecord(final V value) {
        this(null, value);
    }

    /**
     * Create a {@code TestRecord} from a {@link ConsumerRecord}.
     * The partition is taken from {@link ConsumerRecord#partition()}.
     *
     * @param record The consumer record
     */
    public TestRecord(final ConsumerRecord<K, V> record) {
        Objects.requireNonNull(record);
        this.key = record.key();
        this.value = record.value();
        this.headers = record.headers();
        this.recordTime = record.timestamp() < 0 ? null : Instant.ofEpochMilli(record.timestamp());
        final int partition = record.partition();
        if (partition < 0) {
            throw new IllegalArgumentException(
                    String.format("Invalid partition: %d. Partition number should always be non-negative.", partition));
        }
        this.partition = partition;
    }

    /**
     * Create a {@code TestRecord} from a {@link ProducerRecord}.
     * If the producer record carries an explicit partition it is used; otherwise defaults to {@code -1}.
     *
     * @param record The record contents
     */
    public TestRecord(final ProducerRecord<K, V> record) {
        Objects.requireNonNull(record);
        this.key = record.key();
        this.value = record.value();
        this.headers = record.headers();
        final Long timestamp = record.timestamp();
        if (timestamp != null && timestamp < 0) {
            throw new IllegalArgumentException(
                String.format("Invalid timestamp: %d. Timestamp should always be non-negative or null.", timestamp));
        }
        this.recordTime = timestamp == null ? null : Instant.ofEpochMilli(timestamp);
        final Integer partition = record.partition();
        if (partition != null && partition < 0) {
            throw new IllegalArgumentException(
                String.format("Invalid partition: %d. Partition number should always be non-negative or null.", partition));
        }
        this.partition = partition != null ? partition : NO_PARTITION;
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
     * @return the partition number, or {@code -1} if no partition was explicitly set
     */
    public int partition() {
        return partition;
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

    /**
     * @return the partition number, or {@code -1} if no partition was explicitly set
     */
    public int getPartition() {
        return partition;
    }

    /**
     * Compares this record to {@code otherRecord} without considering the {@code partition} field.
     *
     * <p>Use this in tests that do not care about which partition a record was routed to:
     * <pre>{@code
     * assertTrue(expected.equalsIgnorePartition(actual));
     * }</pre>
     *
     * @param otherRecord the record to compare against; {@code null} returns {@code false}
     * @return {@code true} if all fields except {@code partition} are equal
     */
    public boolean equalsIgnorePartition(final TestRecord<K, V> otherRecord) {
        return otherRecord != null && (this == otherRecord || equalsFields(otherRecord));
    }

    private boolean equalsFields(final TestRecord<K, V> otherRecord) {
        return Objects.equals(headers, otherRecord.headers)
            && Objects.equals(key, otherRecord.key)
            && Objects.equals(value, otherRecord.value)
            && Objects.equals(recordTime, otherRecord.recordTime);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", TestRecord.class.getSimpleName() + "[", "]")
            .add("key=" + key)
            .add("value=" + value)
            .add("headers=" + headers)
            .add("recordTime=" + recordTime)
            .add("partition=" + partition)
            .toString();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TestRecord<K, V> that = (TestRecord<K, V>) o;
        return equalsFields(that) && partition == that.partition;
    }

    @Override
    public int hashCode() {
        return Objects.hash(headers, key, value, recordTime, partition);
    }
}
