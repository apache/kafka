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
package org.apache.kafka.connect.connector;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;

import java.util.Objects;

/**
 * <p>
 * Base class for records containing data to be copied to/from Kafka. This corresponds closely to
 * Kafka's {@link org.apache.kafka.clients.producer.ProducerRecord ProducerRecord} and {@link org.apache.kafka.clients.consumer.ConsumerRecord ConsumerRecord} classes, and holds the data that may be used by both
 * sources and sinks (topic, kafkaPartition, key, value, timestamp, headers). Although both implementations include a
 * notion of offset, it is not included here because they differ in type.
 * </p>
 */
public abstract class ConnectRecord<R extends ConnectRecord<R>> {
    private final String topic;
    private final Integer kafkaPartition;
    private final Schema keySchema;
    private final Object key;
    private final Schema valueSchema;
    private final Object value;
    private final Long timestamp;
    private final Headers headers;

    public ConnectRecord(String topic, Integer kafkaPartition,
                         Schema keySchema, Object key,
                         Schema valueSchema, Object value,
                         Long timestamp) {
        this(topic, kafkaPartition, keySchema, key, valueSchema, value, timestamp, new ConnectHeaders());
    }

    public ConnectRecord(String topic, Integer kafkaPartition,
                         Schema keySchema, Object key,
                         Schema valueSchema, Object value,
                         Long timestamp, Iterable<Header> headers) {
        this.topic = topic;
        this.kafkaPartition = kafkaPartition;
        this.keySchema = keySchema;
        this.key = key;
        this.valueSchema = valueSchema;
        this.value = value;
        this.timestamp = timestamp;
        if (headers instanceof ConnectHeaders) {
            this.headers = (ConnectHeaders) headers;
        } else {
            this.headers = new ConnectHeaders(headers);
        }
    }

    public String topic() {
        return topic;
    }

    public Integer kafkaPartition() {
        return kafkaPartition;
    }

    public Object key() {
        return key;
    }

    public Schema keySchema() {
        return keySchema;
    }

    public Object value() {
        return value;
    }

    public Schema valueSchema() {
        return valueSchema;
    }

    public Long timestamp() {
        return timestamp;
    }

    /**
     * Get the headers for this record.
     *
     * @return the headers; never null
     */
    public Headers headers() {
        return headers;
    }

    /**
     * Create a new record of the same type as itself, with the specified parameter values. All other fields in this record will be copied
     * over to the new record. Since the headers are mutable, the resulting record will have a copy of this record's headers.
     *
     * @param topic the name of the topic; may be null
     * @param kafkaPartition the partition number for the Kafka topic; may be null
     * @param keySchema the schema for the key; may be null
     * @param key the key; may be null
     * @param valueSchema the schema for the value; may be null
     * @param value the value; may be null
     * @param timestamp the timestamp; may be null
     * @return the new record
     */
    public abstract R newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key, Schema valueSchema, Object value, Long timestamp);

    /**
     * Create a new record of the same type as itself, with the specified parameter values. All other fields in this record will be copied
     * over to the new record.
     *
     * @param topic the name of the topic; may be null
     * @param kafkaPartition the partition number for the Kafka topic; may be null
     * @param keySchema the schema for the key; may be null
     * @param key the key; may be null
     * @param valueSchema the schema for the value; may be null
     * @param value the value; may be null
     * @param timestamp the timestamp; may be null
     * @param headers the headers; may be null or empty
     * @return the new record
     */
    public abstract R newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key, Schema valueSchema, Object value, Long timestamp, Iterable<Header> headers);

    @Override
    public String toString() {
        return "ConnectRecord{" +
                "topic='" + topic + '\'' +
                ", kafkaPartition=" + kafkaPartition +
                ", key=" + key +
                ", keySchema=" + keySchema +
                ", value=" + value +
                ", valueSchema=" + valueSchema +
                ", timestamp=" + timestamp +
                ", headers=" + headers +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ConnectRecord<?> that = (ConnectRecord<?>) o;

        return Objects.equals(kafkaPartition, that.kafkaPartition)
               && Objects.equals(topic, that.topic)
               && Objects.equals(keySchema, that.keySchema)
               && Objects.equals(key, that.key)
               && Objects.equals(valueSchema, that.valueSchema)
               && Objects.equals(value, that.value)
               && Objects.equals(timestamp, that.timestamp)
               && Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + (kafkaPartition != null ? kafkaPartition.hashCode() : 0);
        result = 31 * result + (keySchema != null ? keySchema.hashCode() : 0);
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (valueSchema != null ? valueSchema.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        result = 31 * result + headers.hashCode();
        return result;
    }
}
