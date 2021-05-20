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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ProduceResponse;

/**
 * The metadata for a record that has been acknowledged by the server
 */
public final class RecordMetadata {

    /**
     * Partition value for record without partition assigned
     */
    public static final int UNKNOWN_PARTITION = -1;

    private final long offset;
    // The timestamp of the message.
    // If LogAppendTime is used for the topic, the timestamp will be the timestamp returned by the broker.
    // If CreateTime is used for the topic, the timestamp is the timestamp in the corresponding ProducerRecord if the
    // user provided one. Otherwise, it will be the producer local time when the producer record was handed to the
    // producer.
    private final long timestamp;
    private final int serializedKeySize;
    private final int serializedValueSize;
    private final TopicPartition topicPartition;

    /**
     * Creates a new instance with the provided parameters.
     */
    public RecordMetadata(TopicPartition topicPartition, long baseOffset, int batchIndex, long timestamp,
                          int serializedKeySize, int serializedValueSize) {
        // ignore the batchIndex if the base offset is -1, since this indicates the offset is unknown
        this.offset = baseOffset == -1 ? baseOffset : baseOffset + batchIndex;
        this.timestamp = timestamp;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.topicPartition = topicPartition;
    }

    /**
     * Creates a new instance with the provided parameters.
     *
     * @deprecated use constructor without `checksum` parameter. This constructor will be removed in
     *             Apache Kafka 4.0 (deprecated since 3.0).
     */
    @Deprecated
    public RecordMetadata(TopicPartition topicPartition, long baseOffset, long batchIndex, long timestamp,
                          Long checksum, int serializedKeySize, int serializedValueSize) {
        this(topicPartition, baseOffset, batchIndexToInt(batchIndex), timestamp, serializedKeySize, serializedValueSize);
    }

    private static int batchIndexToInt(long batchIndex) {
        if (batchIndex > Integer.MAX_VALUE)
            throw new IllegalArgumentException("batchIndex is larger than Integer.MAX_VALUE: " + batchIndex);
        return (int) batchIndex;
    }

    /**
     * Indicates whether the record metadata includes the offset.
     * @return true if the offset is included in the metadata, false otherwise.
     */
    public boolean hasOffset() {
        return this.offset != ProduceResponse.INVALID_OFFSET;
    }

    /**
     * The offset of the record in the topic/partition.
     * @return the offset of the record, or -1 if {{@link #hasOffset()}} returns false.
     */
    public long offset() {
        return this.offset;
    }

    /**
     * Indicates whether the record metadata includes the timestamp.
     * @return true if a valid timestamp exists, false otherwise.
     */
    public boolean hasTimestamp() {
        return this.timestamp != RecordBatch.NO_TIMESTAMP;
    }

    /**
     * The timestamp of the record in the topic/partition.
     *
     * @return the timestamp of the record, or -1 if the {{@link #hasTimestamp()}} returns false.
     */
    public long timestamp() {
        return this.timestamp;
    }

    /**
     * The size of the serialized, uncompressed key in bytes. If key is null, the returned size
     * is -1.
     */
    public int serializedKeySize() {
        return this.serializedKeySize;
    }

    /**
     * The size of the serialized, uncompressed value in bytes. If value is null, the returned
     * size is -1.
     */
    public int serializedValueSize() {
        return this.serializedValueSize;
    }

    /**
     * The topic the record was appended to
     */
    public String topic() {
        return this.topicPartition.topic();
    }

    /**
     * The partition the record was sent to
     */
    public int partition() {
        return this.topicPartition.partition();
    }

    @Override
    public String toString() {
        return topicPartition.toString() + "@" + offset;
    }
}
