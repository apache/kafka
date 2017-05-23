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
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.RecordBatch;

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

    private volatile Long checksum;

    public RecordMetadata(TopicPartition topicPartition, long baseOffset, long relativeOffset, long timestamp,
                          Long checksum, int serializedKeySize, int serializedValueSize) {
        // ignore the relativeOffset if the base offset is -1,
        // since this indicates the offset is unknown
        this.offset = baseOffset == -1 ? baseOffset : baseOffset + relativeOffset;
        this.timestamp = timestamp;
        this.checksum = checksum;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.topicPartition = topicPartition;
    }

    @Deprecated
    public RecordMetadata(TopicPartition topicPartition, long baseOffset, long relativeOffset) {
        this(topicPartition, baseOffset, relativeOffset, RecordBatch.NO_TIMESTAMP, null, -1, -1);
    }

    /**
     * The offset of the record in the topic/partition.
     */
    public long offset() {
        return this.offset;
    }

    /**
     * The timestamp of the record in the topic/partition.
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * The checksum (CRC32) of the record.
     *
     * @deprecated As of Kafka 0.11.0. Because of the potential for message format conversion on the broker, the
     *             computed checksum may not match what was stored on the broker, or what will be returned to the consumer.
     *             It is therefore unsafe to depend on this checksum for end-to-end delivery guarantees. Additionally,
     *             message format v2 does not support a record-level checksum. To maintain compatibility, a partial
     *             checksum computed from the record timestamp, serialized key size, and serialized value size is
     *             returned instead, but obviously this should not be depended on for end-to-end reliability.
     */
    @Deprecated
    public long checksum() {
        if (checksum == null)
            this.checksum = DefaultRecordBatch.computePartialChecksum(timestamp, serializedKeySize, serializedValueSize);
        return this.checksum;
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
