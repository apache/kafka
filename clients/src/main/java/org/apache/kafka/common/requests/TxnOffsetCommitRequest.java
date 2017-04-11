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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class TxnOffsetCommitRequest extends AbstractRequest {
    private static final String CONSUMER_GROUP_ID_KEY_NAME = "consumer_group_id";
    private static final String PID_KEY_NAME = "producer_id";
    private static final String EPOCH_KEY_NAME = "producer_epoch";
    private static final String RETENTION_TIME_KEY_NAME = "retention_time";
    private static final String TOPIC_PARTITIONS_KEY_NAME = "topics";
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partitions";
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String OFFSET_KEY_NAME = "offset";
    private static final String METADATA_KEY_NAME = "metadata";

    public static class Builder extends AbstractRequest.Builder<TxnOffsetCommitRequest> {
        private final String consumerGroupId;
        private final long producerId;
        private final short producerEpoch;
        private final long retentionTimeMs;
        private final Map<TopicPartition, CommittedOffset> offsets;

        public Builder(String consumerGroupId, long producerId, short producerEpoch, long retentionTimeMs,
                       Map<TopicPartition, CommittedOffset> offsets) {
            super(ApiKeys.TXN_OFFSET_COMMIT);
            this.consumerGroupId = consumerGroupId;
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.retentionTimeMs = retentionTimeMs;
            this.offsets = offsets;
        }

        @Override
        public TxnOffsetCommitRequest build(short version) {
            return new TxnOffsetCommitRequest(version, consumerGroupId, producerId, producerEpoch, retentionTimeMs, offsets);
        }
    }

    private final String consumerGroupId;
    private final long producerId;
    private final short producerEpoch;
    private final long retentionTimeMs;
    private final Map<TopicPartition, CommittedOffset> offsets;

    public TxnOffsetCommitRequest(short version, String consumerGroupId, long producerId, short producerEpoch,
                                  long retentionTimeMs, Map<TopicPartition, CommittedOffset> offsets) {
        super(version);
        this.consumerGroupId = consumerGroupId;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.retentionTimeMs = retentionTimeMs;
        this.offsets = offsets;
    }

    public TxnOffsetCommitRequest(Struct struct, short version) {
        super(version);
        this.consumerGroupId = struct.getString(CONSUMER_GROUP_ID_KEY_NAME);
        this.producerId = struct.getLong(PID_KEY_NAME);
        this.producerEpoch = struct.getShort(EPOCH_KEY_NAME);
        this.retentionTimeMs = struct.getLong(RETENTION_TIME_KEY_NAME);

        Map<TopicPartition, CommittedOffset> offsets = new HashMap<>();
        Object[] topicPartitionsArray = struct.getArray(TOPIC_PARTITIONS_KEY_NAME);
        for (Object topicPartitionObj : topicPartitionsArray) {
            Struct topicPartitionStruct = (Struct) topicPartitionObj;
            String topic = topicPartitionStruct.getString(TOPIC_KEY_NAME);
            for (Object partitionObj : topicPartitionStruct.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionStruct = (Struct) partitionObj;
                TopicPartition partition = new TopicPartition(topic, partitionStruct.getInt(PARTITION_KEY_NAME));
                long offset = partitionStruct.getLong(OFFSET_KEY_NAME);
                String metadata = partitionStruct.getString(METADATA_KEY_NAME);
                offsets.put(partition, new CommittedOffset(offset, metadata));
            }
        }
        this.offsets = offsets;
    }

    public String consumerGroupId() {
        return consumerGroupId;
    }

    public long producerId() {
        return producerId;
    }

    public short producerEpoch() {
        return producerEpoch;
    }

    public long retentionTimeMs() {
        return retentionTimeMs;
    }

    public Map<TopicPartition, CommittedOffset> offsets() {
        return offsets;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.TXN_OFFSET_COMMIT.requestSchema(version()));
        struct.set(CONSUMER_GROUP_ID_KEY_NAME, consumerGroupId);
        struct.set(PID_KEY_NAME, producerId);
        struct.set(EPOCH_KEY_NAME, producerEpoch);
        struct.set(RETENTION_TIME_KEY_NAME, retentionTimeMs);

        Map<String, Map<Integer, CommittedOffset>> mappedPartitionOffsets = CollectionUtils.groupDataByTopic(offsets);
        Object[] partitionsArray = new Object[mappedPartitionOffsets.size()];
        int i = 0;
        for (Map.Entry<String, Map<Integer, CommittedOffset>> topicAndPartitions : mappedPartitionOffsets.entrySet()) {
            Struct topicPartitionsStruct = struct.instance(TOPIC_PARTITIONS_KEY_NAME);
            topicPartitionsStruct.set(TOPIC_KEY_NAME, topicAndPartitions.getKey());

            Map<Integer, CommittedOffset> partitionOffsets = topicAndPartitions.getValue();
            Object[] partitionOffsetsArray = new Object[partitionOffsets.size()];
            int j = 0;
            for (Map.Entry<Integer, CommittedOffset> partitionOffset : partitionOffsets.entrySet()) {
                Struct partitionOffsetStruct = topicPartitionsStruct.instance(PARTITIONS_KEY_NAME);
                partitionOffsetStruct.set(PARTITION_KEY_NAME, partitionOffset.getKey());
                CommittedOffset committedOffset = partitionOffset.getValue();
                partitionOffsetStruct.set(OFFSET_KEY_NAME, committedOffset.offset);
                partitionOffsetStruct.set(METADATA_KEY_NAME, committedOffset.metadata);
                partitionOffsetsArray[j++] = partitionOffsetStruct;
            }
            topicPartitionsStruct.set(PARTITIONS_KEY_NAME, partitionOffsetsArray);
            partitionsArray[i++] = topicPartitionsStruct;
        }

        struct.set(TOPIC_PARTITIONS_KEY_NAME, partitionsArray);
        return struct;
    }

    @Override
    public TxnOffsetCommitResponse getErrorResponse(Throwable e) {
        Errors error = Errors.forException(e);
        Map<TopicPartition, Errors> errors = new HashMap<>(offsets.size());
        for (TopicPartition partition : offsets.keySet())
            errors.put(partition, error);
        return new TxnOffsetCommitResponse(errors);
    }

    public static TxnOffsetCommitRequest parse(ByteBuffer buffer, short version) {
        return new TxnOffsetCommitRequest(ApiKeys.TXN_OFFSET_COMMIT.parseRequest(version, buffer), version);
    }

    public static class CommittedOffset {
        private final long offset;
        private final String metadata;

        public CommittedOffset(long offset, String metadata) {
            this.offset = offset;
            this.metadata = metadata;
        }

        @Override
        public String toString() {
            return "CommittedOffset(" +
                    "offset=" + offset +
                    ", metadata='" + metadata + "')";
        }

        public long offset() {
            return offset;
        }

        public String metadata() {
            return metadata;
        }
    }

}
