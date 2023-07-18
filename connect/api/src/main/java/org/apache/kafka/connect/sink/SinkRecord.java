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
package org.apache.kafka.connect.sink;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;
import java.util.Objects;

/**
 * SinkRecord is a {@link ConnectRecord} that has been read from Kafka and includes the original Kafka record's
 * topic, partition and offset (before any {@link Transformation transformations} have been applied)
 * in addition to the standard fields. This information should be used by the {@link SinkTask} to coordinate
 * offset commits.
 * <p>
 * It also includes the {@link TimestampType}, which may be {@link TimestampType#NO_TIMESTAMP_TYPE}, and the relevant
 * timestamp, which may be {@code null}.
 */
public class SinkRecord extends ConnectRecord<SinkRecord> {
    private final long kafkaOffset;
    private final TimestampType timestampType;
    private final String originalTopic;
    private final Integer originalKafkaPartition;
    private final long originalKafkaOffset;

    public SinkRecord(String topic, int partition, Schema keySchema, Object key, Schema valueSchema, Object value, long kafkaOffset) {
        this(topic, partition, keySchema, key, valueSchema, value, kafkaOffset, null, TimestampType.NO_TIMESTAMP_TYPE);
    }

    public SinkRecord(String topic, int partition, Schema keySchema, Object key, Schema valueSchema, Object value, long kafkaOffset,
                      Long timestamp, TimestampType timestampType) {
        this(topic, partition, keySchema, key, valueSchema, value, kafkaOffset, timestamp, timestampType, null);
    }

    public SinkRecord(String topic, int partition, Schema keySchema, Object key, Schema valueSchema, Object value, long kafkaOffset,
                      Long timestamp, TimestampType timestampType, Iterable<Header> headers) {
        this(topic, partition, keySchema, key, valueSchema, value, kafkaOffset, timestamp, timestampType, headers, topic, partition, kafkaOffset);
    }

    /**
     * This constructor is intended for use by the Connect runtime only and plugins (sink connectors or transformations)
     * should not use this directly outside testing code.
     */
    public SinkRecord(String topic, int partition, Schema keySchema, Object key, Schema valueSchema, Object value, long kafkaOffset,
                      Long timestamp, TimestampType timestampType, Iterable<Header> headers, String originalTopic,
                      Integer originalKafkaPartition, long originalKafkaOffset) {
        super(topic, partition, keySchema, key, valueSchema, value, timestamp, headers);
        this.kafkaOffset = kafkaOffset;
        this.timestampType = timestampType;
        this.originalTopic = originalTopic;
        this.originalKafkaPartition = originalKafkaPartition;
        this.originalKafkaOffset = originalKafkaOffset;
    }

    public long kafkaOffset() {
        return kafkaOffset;
    }

    public TimestampType timestampType() {
        return timestampType;
    }

    /**
     * Get the original topic for this sink record, before any {@link Transformation transformations} were applied.
     * In order to be compatible with transformations that mutate topic names, this method should be used
     * by sink tasks instead of {@link #topic()} for any internal offset tracking purposes (for instance, reporting
     * offsets to the Connect runtime via {@link SinkTask#preCommit(Map)}).
     * <p>
     * This method was added in Apache Kafka 3.6. Sink connectors that use this method but want to maintain backward
     * compatibility in order to be able to be deployed on older Connect runtimes should guard the call to this method
     * with a try-catch block, since calling this method will result in a {@link NoSuchMethodError} when the sink
     * connector is deployed to Connect runtimes older than Kafka 3.6.
     * For example:
     * <pre>{@code
     * String originalTopic;
     * try {
     *     originalTopic = record.originalTopic();
     * } catch (NoSuchMethodError e) {
     *     log.warn("This connector is not compatible with SMTs that mutate topic names, topic partitions or offset values on this version of Kafka Connect");
     *     originalTopic = record.topic();
     * }
     * }
     * </pre>
     * <p>
     * Note that sink connectors that do their own offset tracking will be incompatible with SMTs that mutate topic
     * names when deployed to older Connect runtimes that do not support this method.
     *
     * @return the topic for this record before any transformations were applied
     *
     * @since 3.6
     */
    public String originalTopic() {
        return originalTopic;
    }

    /**
     * Get the original topic partition for this sink record, before any {@link Transformation transformations} were applied.
     * In order to be compatible with transformations that mutate topic partitions, this method should be used
     * by sink tasks instead of {@link #kafkaPartition()} for any internal offset tracking purposes (for instance, reporting
     * offsets to the Connect runtime via {@link SinkTask#preCommit(Map)}).
     * <p>
     * This method was added in Apache Kafka 3.6. Sink connectors that use this method but want to maintain backward
     * compatibility in order to be able to be deployed on older Connect runtimes should guard the call to this method
     * with a try-catch block, since calling this method will result in a {@link NoSuchMethodError} when the sink
     * connector is deployed to Connect runtimes older than Kafka 3.6.
     * For example:
     * <pre>{@code
     * String originalKafkaPartition;
     * try {
     *     originalKafkaPartition = record.originalKafkaPartition();
     * } catch (NoSuchMethodError e) {
     *     log.warn("This connector is not compatible with SMTs that mutate topic names, topic partitions or offset values on this version of Kafka Connect");
     *     originalKafkaPartition = record.kafkaPartition();
     * }
     * }
     * </pre>
     * <p>
     * Note that sink connectors that do their own offset tracking will be incompatible with SMTs that mutate topic
     * partitions when deployed to older Connect runtimes that do not support this method.
     *
     * @return the topic partition for this record before any transformations were applied
     *
     * @since 3.6
     */
    public Integer originalKafkaPartition() {
        return originalKafkaPartition;
    }

    /**
     * Get the original offset for this sink record, before any {@link Transformation transformations} were applied.
     * In order to be compatible with transformations that mutate offset values, this method should be used
     * by sink tasks instead of {@link #kafkaOffset()} for any internal offset tracking purposes (for instance, reporting
     * offsets to the Connect runtime via {@link SinkTask#preCommit(Map)}).
     * <p>
     * This method was added in Apache Kafka 3.6. Sink connectors that use this method but want to maintain backward
     * compatibility in order to be able to be deployed on older Connect runtimes should guard the call to this method
     * with a try-catch block, since calling this method will result in a {@link NoSuchMethodError} when the sink
     * connector is deployed to Connect runtimes older than Kafka 3.6.
     * For example:
     * <pre>{@code
     * String originalKafkaOffset;
     * try {
     *     originalKafkaOffset = record.originalKafkaOffset();
     * } catch (NoSuchMethodError e) {
     *     log.warn("This connector is not compatible with SMTs that mutate topic names, topic partitions or offset values on this version of Kafka Connect");
     *     originalKafkaOffset = record.kafkaOffset();
     * }
     * }
     * </pre>
     * <p>
     * Note that sink connectors that do their own offset tracking will be incompatible with SMTs that mutate offset
     * values when deployed to older Connect runtimes that do not support this method.
     *
     * @return the offset for this record before any transformations were applied
     *
     * @since 3.6
     */
    public long originalKafkaOffset() {
        return originalKafkaOffset;
    }

    @Override
    public SinkRecord newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key, Schema valueSchema, Object value, Long timestamp) {
        return newRecord(topic, kafkaPartition, keySchema, key, valueSchema, value, timestamp, headers().duplicate());
    }

    @Override
    public SinkRecord newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key, Schema valueSchema, Object value,
                                Long timestamp, Iterable<Header> headers) {
        return new SinkRecord(topic, kafkaPartition, keySchema, key, valueSchema, value, kafkaOffset, timestamp, timestampType, headers,
                originalTopic, originalKafkaPartition, originalKafkaOffset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        SinkRecord that = (SinkRecord) o;

        return kafkaOffset == that.kafkaOffset &&
                timestampType == that.timestampType &&
                Objects.equals(originalTopic, that.originalTopic) &&
                Objects.equals(originalKafkaPartition, that.originalKafkaPartition) &&
                originalKafkaOffset == that.originalKafkaOffset;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Long.hashCode(kafkaOffset);
        result = 31 * result + timestampType.hashCode();
        result = 31 * result + originalTopic.hashCode();
        result = 31 * result + originalKafkaPartition.hashCode();
        result = 31 * result + Long.hashCode(originalKafkaOffset);
        return result;
    }

    @Override
    public String toString() {
        return "SinkRecord{" +
                "kafkaOffset=" + kafkaOffset +
                ", timestampType=" + timestampType +
                ", originalTopic=" + originalTopic +
                ", originalKafkaPartition=" + originalKafkaPartition +
                ", originalKafkaOffset=" + originalKafkaOffset +
                "} " + super.toString();
    }
}
