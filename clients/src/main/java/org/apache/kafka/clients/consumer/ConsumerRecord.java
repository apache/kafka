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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.RecordBatch;

import java.util.ConcurrentModificationException;
import java.util.Optional;

/**
 * A key/value pair to be received from Kafka. This also consists of a topic name and
 * a partition number from which the record is being received, an offset that points
 * to the record in a Kafka partition, and a timestamp as marked by the corresponding ProducerRecord.
 * <p>
 *
 * <h3>Thread Safety</h3>
 * This consumer record is <b>not thread-safe</b>. Concurrent access to a {@code ConsumerRecord} instance by
 * multiple threads may result in undefined behavior, including but not limited to the following:
 * <ul>
 *   <li>Throwing {@link ConcurrentModificationException} (e.g., when concurrently modifying {@link #headers()}).</li>
 *   <li>Data corruption or logical errors (e.g., inconsistent state of {@code headers} or {@code value}).</li>
 *   <li>Visibility issues (e.g., modifications by one thread not being visible to another thread).</li>
 * </ul>
 *
 * <p>
 * In particular, the {@link #headers()} method returns a mutable collection of headers. If multiple
 * threads access or modify these headers concurrently, it may lead to race conditions or inconsistent
 * states. It is the responsibility of the user to ensure that multi-threaded access is properly synchronized.
 *
 * <p>
 * However, each individual {@link org.apache.kafka.common.header.Header} instance
 * is <b>read thread-safe</b>; that is, it is safe for multiple threads to read the same header's key or value concurrently
 * as long as no thread modifies it.
 *
 * <p>
 * Refer to the {@link KafkaConsumer} documentation for more details on multi-threaded consumption and processing strategies.
 */
public class ConsumerRecord<K, V> {
    public static final long NO_TIMESTAMP = RecordBatch.NO_TIMESTAMP;
    public static final int NULL_SIZE = -1;

    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final TimestampType timestampType;
    private final int serializedKeySize;
    private final int serializedValueSize;
    private final Headers headers;
    private final K key;
    private final V value;
    private final Optional<Integer> leaderEpoch;
    private final Optional<Short> deliveryCount;

    /**
     * Creates a record to be received from a specified topic and partition (provided for
     * compatibility with Kafka 0.9 before the message format supported timestamps and before
     * serialized metadata were exposed).
     *
     * @param topic The topic this record is received from
     * @param partition The partition of the topic this record is received from
     * @param offset The offset of this record in the corresponding Kafka partition
     * @param key The key of the record, if one exists (null is allowed)
     * @param value The record contents
     */
    public ConsumerRecord(String topic,
                          int partition,
                          long offset,
                          K key,
                          V value) {
        this(topic, partition, offset, NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, NULL_SIZE, NULL_SIZE, key, value,
            new RecordHeaders(), Optional.empty());
    }

    /**
     * Creates a record to be received from a specified topic and partition.
     *
     * @param topic The topic this record is received from
     * @param partition The partition of the topic this record is received from
     * @param offset The offset of this record in the corresponding Kafka partition
     * @param timestamp The timestamp of the record.
     * @param timestampType The timestamp type
     * @param serializedKeySize The length of the serialized key
     * @param serializedValueSize The length of the serialized value
     * @param key The key of the record, if one exists (null is allowed)
     * @param value The record contents
     * @param headers The headers of the record
     * @param leaderEpoch Optional leader epoch of the record (may be empty for legacy record formats)
     */
    public ConsumerRecord(String topic,
                          int partition,
                          long offset,
                          long timestamp,
                          TimestampType timestampType,
                          int serializedKeySize,
                          int serializedValueSize,
                          K key,
                          V value,
                          Headers headers,
                          Optional<Integer> leaderEpoch) {
        this(topic, partition, offset, timestamp, timestampType, serializedKeySize, serializedValueSize, key, value,
            headers, leaderEpoch, Optional.empty());
    }

    /**
     * Creates a record to be received from a specified topic and partition.
     *
     * @param topic The topic this record is received from
     * @param partition The partition of the topic this record is received from
     * @param offset The offset of this record in the corresponding Kafka partition
     * @param timestamp The timestamp of the record.
     * @param timestampType The timestamp type
     * @param serializedKeySize The length of the serialized key
     * @param serializedValueSize The length of the serialized value
     * @param key The key of the record, if one exists (null is allowed)
     * @param value The record contents
     * @param headers The headers of the record
     * @param leaderEpoch Optional leader epoch of the record (may be empty for legacy record formats)
     * @param deliveryCount Optional delivery count of the record (may be empty when deliveries not counted)
     */
    public ConsumerRecord(String topic,
                          int partition,
                          long offset,
                          long timestamp,
                          TimestampType timestampType,
                          int serializedKeySize,
                          int serializedValueSize,
                          K key,
                          V value,
                          Headers headers,
                          Optional<Integer> leaderEpoch,
                          Optional<Short> deliveryCount) {
        if (topic == null)
            throw new IllegalArgumentException("Topic cannot be null");
        if (headers == null)
            throw new IllegalArgumentException("Headers cannot be null");

        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.timestampType = timestampType;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.key = key;
        this.value = value;
        this.headers = headers;
        this.leaderEpoch = leaderEpoch;
        this.deliveryCount = deliveryCount;
    }

    /**
     * The topic this record is received from (never null)
     */
    public String topic() {
        return this.topic;
    }

    /**
     * The partition from which this record is received
     */
    public int partition() {
        return this.partition;
    }

    /**
     * The headers (never null)
     */
    public Headers headers() {
        return headers;
    }
    
    /**
     * The key (or null if no key is specified)
     */
    public K key() {
        return key;
    }

    /**
     * The value
     */
    public V value() {
        return value;
    }

    /**
     * The position of this record in the corresponding Kafka partition.
     */
    public long offset() {
        return offset;
    }

    /**
     * The timestamp of this record, in milliseconds elapsed since unix epoch.
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * The timestamp type of this record
     */
    public TimestampType timestampType() {
        return timestampType;
    }

    /**
     * The size of the serialized, uncompressed key in bytes. If key is null, the returned size
     * is -1.
     */
    public int serializedKeySize() {
        return this.serializedKeySize;
    }

    /**
     * The size of the serialized, uncompressed value in bytes. If value is null, the
     * returned size is -1.
     */
    public int serializedValueSize() {
        return this.serializedValueSize;
    }

    /**
     * Get the leader epoch for the record if available
     *
     * @return the leader epoch or empty for legacy record formats
     */
    public Optional<Integer> leaderEpoch() {
        return leaderEpoch;
    }

    /**
     * Get the delivery count for the record if available. Deliveries
     * are counted for records delivered by share groups.
     *
     * @return the delivery count or empty when deliveries not counted
     */
    public Optional<Short> deliveryCount() {
        return deliveryCount;
    }

    @Override
    public String toString() {
        return "ConsumerRecord(topic = " + topic
               + ", partition = " + partition
               + ", leaderEpoch = " + leaderEpoch.orElse(null)
               + ", offset = " + offset
               + ", " + timestampType + " = " + timestamp
               + ", deliveryCount = " + deliveryCount.orElse(null)
               + ", serialized key size = "  + serializedKeySize
               + ", serialized value size = " + serializedValueSize
               + ", headers = " + headers
               + ", key = " + key
               + ", value = " + value + ")";
    }
}
