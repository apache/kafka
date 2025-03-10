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
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.Arrays;
import java.util.Objects;

/**
 * A key/value pair to be sent to Kafka. This consists of a topic name to which the record is being sent, an optional
 * partition number, and an optional key and value.
 * <p>
 * If a valid partition number is specified that partition will be used when sending the record. If no partition is
 * specified but a key is present a partition will be chosen using a hash of the key. If neither key nor partition is
 * present a partition will be assigned in a round-robin fashion. Note that partition numbers are 0-indexed.
 * <p>
 * The record also has an associated timestamp. If the user did not provide a timestamp, the producer will stamp the
 * record with its current time. The timestamp eventually used by Kafka depends on the timestamp type configured for
 * the topic.
 * <li>
 * If the topic is configured to use {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime},
 * the timestamp in the producer record will be used by the broker.
 * </li>
 * <li>
 * If the topic is configured to use {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime},
 * the timestamp in the producer record will be overwritten by the broker with the broker local time when it appends the
 * message to its log.
 * </li>
 * <p>
 * In either of the cases above, the timestamp that has actually been used will be returned to user in
 * {@link RecordMetadata}
 */
@InterfaceAudience.Public
public class ProducerRecord<K, V> {

    private final String topic;
    private final Integer partition;
    private final Headers headers;
    private final K key;
    private final V value;
    private final Long timestamp;
    private final byte[] rawSerializedHeaders;

    /**
     * Creates a record with a specified timestamp to be sent to a specified topic and partition
     * 
     * @param topic The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param timestamp The timestamp of the record, in milliseconds since epoch. If null, the producer will assign
     *                  the timestamp using System.currentTimeMillis().
     * @param key The key that will be included in the record
     * @param value The record contents
     * @param headers the headers that will be included in the record
     */
    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value, Iterable<Header> headers) {
        if (topic == null)
            throw new IllegalArgumentException("Topic cannot be null.");
        if (timestamp != null && timestamp < 0)
            throw new IllegalArgumentException(
                    String.format("Invalid timestamp: %d. Timestamp should always be non-negative or null.", timestamp));
        if (partition != null && partition < 0)
            throw new IllegalArgumentException(
                    String.format("Invalid partition: %d. Partition number should always be non-negative or null.", partition));
        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.headers = new RecordHeaders(headers);
        this.rawSerializedHeaders = null;
    }

    /**
     * Creates a record carrying pre-serialized header bytes. When this constructor is used, the
     * producer writes {@code rawSerializedHeaders} directly into the record batch without
     * deserializing or re-serializing individual headers. The {@code rawSerializedHeaders} must
     * use the standard Kafka header wire format: {@code [count(varint)][header1][header2]...},
     * or be empty (length 0) for zero headers.
     *
     * <p>This is intended for internal use (e.g., Kafka Streams changelog writing) where headers
     * are already available in serialized form and the deserialization round-trip should be avoided.
     */
    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value, byte[] rawSerializedHeaders) {
        if (topic == null)
            throw new IllegalArgumentException("Topic cannot be null.");
        if (timestamp != null && timestamp < 0)
            throw new IllegalArgumentException(
                    String.format("Invalid timestamp: %d. Timestamp should always be non-negative or null.", timestamp));
        if (partition != null && partition < 0)
            throw new IllegalArgumentException(
                    String.format("Invalid partition: %d. Partition number should always be non-negative or null.", partition));
        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.headers = new RecordHeaders();
        this.rawSerializedHeaders = rawSerializedHeaders;
    }

    /**
     * Creates a record with a specified timestamp to be sent to a specified topic and partition
     *
     * @param topic The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param timestamp The timestamp of the record, in milliseconds since epoch. If null, the producer will assign the
     *                  timestamp using System.currentTimeMillis().
     * @param key The key that will be included in the record
     * @param value The record contents
     */
    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value) {
        this(topic, partition, timestamp, key, value, (Iterable<Header>) null);
    }

    /**
     * Creates a record to be sent to a specified topic and partition
     *
     * @param topic The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param key The key that will be included in the record
     * @param value The record contents
     * @param headers The headers that will be included in the record
     */
    public ProducerRecord(String topic, Integer partition, K key, V value, Iterable<Header> headers) {
        this(topic, partition, (Long) null, key, value, headers);
    }
    
    /**
     * Creates a record to be sent to a specified topic and partition
     *
     * @param topic The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param key The key that will be included in the record
     * @param value The record contents
     */
    public ProducerRecord(String topic, Integer partition, K key, V value) {
        this(topic, partition, (Long) null, key, value, (Iterable<Header>) null);
    }
    
    /**
     * Create a record to be sent to Kafka
     * 
     * @param topic The topic the record will be appended to
     * @param key The key that will be included in the record
     * @param value The record contents
     */
    public ProducerRecord(String topic, K key, V value) {
        this(topic, null, (Long) null, key, value, (Iterable<Header>) null);
    }
    
    /**
     * Create a record with no key
     * 
     * @param topic The topic this record should be sent to
     * @param value The record contents
     */
    public ProducerRecord(String topic, V value) {
        this(topic, null, (Long) null, null, value, (Iterable<Header>) null);
    }

    /**
     * @return The topic this record is being sent to
     */
    public String topic() {
        return topic;
    }

    /**
     * @return The headers
     */
    public Headers headers() {
        return headers;
    }

    /**
     * @return The key (or null if no key is specified)
     */
    public K key() {
        return key;
    }

    /**
     * @return The value
     */
    public V value() {
        return value;
    }

    /**
     * @return The timestamp, which is in milliseconds since epoch.
     */
    public Long timestamp() {
        return timestamp;
    }

    /**
     * @return The partition to which the record will be sent (or null if no partition was specified)
     */
    public Integer partition() {
        return partition;
    }

    /**
     * @return Pre-serialized header bytes in Kafka wire format, or null if headers were
     *         provided as {@link Headers} objects via the standard constructors.
     */
    public byte[] rawSerializedHeaders() {
        return rawSerializedHeaders;
    }

    @Override
    public String toString() {
        String headers = this.headers == null ? "null" : this.headers.toString();
        String key = this.key == null ? "null" : this.key.toString();
        String value = this.value == null ? "null" : this.value.toString();
        String timestamp = this.timestamp == null ? "null" : this.timestamp.toString();
        return "ProducerRecord(topic=" + topic + ", partition=" + partition + ", headers=" + headers + ", key=" + key + ", value=" + value +
            ", timestamp=" + timestamp + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        else if (!(o instanceof ProducerRecord))
            return false;

        ProducerRecord<?, ?> that = (ProducerRecord<?, ?>) o;

        return Objects.equals(key, that.key) &&
            Objects.equals(partition, that.partition) &&
            Objects.equals(topic, that.topic) &&
            Objects.equals(headers, that.headers) &&
            Objects.equals(value, that.value) &&
            Objects.equals(timestamp, that.timestamp) &&
            Arrays.equals(rawSerializedHeaders, that.rawSerializedHeaders);
    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + (partition != null ? partition.hashCode() : 0);
        result = 31 * result + (headers != null ? headers.hashCode() : 0);
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(rawSerializedHeaders);
        return result;
    }
}
