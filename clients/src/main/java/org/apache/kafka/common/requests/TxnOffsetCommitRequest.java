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
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.GROUP_ID;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.PRODUCER_EPOCH;
import static org.apache.kafka.common.protocol.CommonFields.PRODUCER_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.CommonFields.TRANSACTIONAL_ID;
import static org.apache.kafka.common.protocol.types.Type.INT64;
import static org.apache.kafka.common.protocol.types.Type.NULLABLE_STRING;

public class TxnOffsetCommitRequest extends AbstractRequest {
    private static final String TOPICS_KEY_NAME = "topics";
    private static final String PARTITIONS_KEY_NAME = "partitions";
    private static final String OFFSET_KEY_NAME = "offset";
    private static final String METADATA_KEY_NAME = "metadata";

    private static final Schema TXN_OFFSET_COMMIT_PARTITION_OFFSET_METADATA_REQUEST_V0 = new Schema(
            PARTITION_ID,
            new Field(OFFSET_KEY_NAME, INT64),
            new Field(METADATA_KEY_NAME, NULLABLE_STRING));

    private static final Schema TXN_OFFSET_COMMIT_REQUEST_V0 = new Schema(
            TRANSACTIONAL_ID,
            GROUP_ID,
            PRODUCER_ID,
            PRODUCER_EPOCH,
            new Field(TOPICS_KEY_NAME, new ArrayOf(new Schema(
                    TOPIC_NAME,
                    new Field(PARTITIONS_KEY_NAME, new ArrayOf(TXN_OFFSET_COMMIT_PARTITION_OFFSET_METADATA_REQUEST_V0)))),
                    "The partitions to write markers for."));

    public static Schema[] schemaVersions() {
        return new Schema[]{TXN_OFFSET_COMMIT_REQUEST_V0};
    }

    public static class Builder extends AbstractRequest.Builder<TxnOffsetCommitRequest> {
        private final String transactionalId;
        private final String consumerGroupId;
        private final long producerId;
        private final short producerEpoch;
        private final Map<TopicPartition, CommittedOffset> offsets;

        public Builder(String transactionalId, String consumerGroupId, long producerId, short producerEpoch,
                       Map<TopicPartition, CommittedOffset> offsets) {
            super(ApiKeys.TXN_OFFSET_COMMIT);
            this.transactionalId = transactionalId;
            this.consumerGroupId = consumerGroupId;
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.offsets = offsets;
        }

        public String consumerGroupId() {
            return consumerGroupId;
        }

        public Map<TopicPartition, CommittedOffset> offsets() {
            return offsets;
        }

        @Override
        public TxnOffsetCommitRequest build(short version) {
            return new TxnOffsetCommitRequest(version, transactionalId, consumerGroupId, producerId, producerEpoch, offsets);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=TxnOffsetCommitRequest").
                    append(", transactionalId=").append(transactionalId).
                    append(", producerId=").append(producerId).
                    append(", producerEpoch=").append(producerEpoch).
                    append(", consumerGroupId=").append(consumerGroupId).
                    append(", offsets=").append(offsets).
                    append(")");
            return bld.toString();
        }
    }

    private final String transactionalId;
    private final String consumerGroupId;
    private final long producerId;
    private final short producerEpoch;
    private final Map<TopicPartition, CommittedOffset> offsets;

    public TxnOffsetCommitRequest(short version, String transactionalId, String consumerGroupId, long producerId,
                                  short producerEpoch, Map<TopicPartition, CommittedOffset> offsets) {
        super(version);
        this.transactionalId = transactionalId;
        this.consumerGroupId = consumerGroupId;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.offsets = offsets;
    }

    public TxnOffsetCommitRequest(Struct struct, short version) {
        super(version);
        this.transactionalId = struct.get(TRANSACTIONAL_ID);
        this.consumerGroupId = struct.get(GROUP_ID);
        this.producerId = struct.get(PRODUCER_ID);
        this.producerEpoch = struct.get(PRODUCER_EPOCH);

        Map<TopicPartition, CommittedOffset> offsets = new HashMap<>();
        Object[] topicPartitionsArray = struct.getArray(TOPICS_KEY_NAME);
        for (Object topicPartitionObj : topicPartitionsArray) {
            Struct topicPartitionStruct = (Struct) topicPartitionObj;
            String topic = topicPartitionStruct.get(TOPIC_NAME);
            for (Object partitionObj : topicPartitionStruct.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionStruct = (Struct) partitionObj;
                TopicPartition partition = new TopicPartition(topic, partitionStruct.get(PARTITION_ID));
                long offset = partitionStruct.getLong(OFFSET_KEY_NAME);
                String metadata = partitionStruct.getString(METADATA_KEY_NAME);
                offsets.put(partition, new CommittedOffset(offset, metadata));
            }
        }
        this.offsets = offsets;
    }

    public String transactionalId() {
        return transactionalId;
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

    public Map<TopicPartition, CommittedOffset> offsets() {
        return offsets;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.TXN_OFFSET_COMMIT.requestSchema(version()));
        struct.set(TRANSACTIONAL_ID, transactionalId);
        struct.set(GROUP_ID, consumerGroupId);
        struct.set(PRODUCER_ID, producerId);
        struct.set(PRODUCER_EPOCH, producerEpoch);

        Map<String, Map<Integer, CommittedOffset>> mappedPartitionOffsets = CollectionUtils.groupDataByTopic(offsets);
        Object[] partitionsArray = new Object[mappedPartitionOffsets.size()];
        int i = 0;
        for (Map.Entry<String, Map<Integer, CommittedOffset>> topicAndPartitions : mappedPartitionOffsets.entrySet()) {
            Struct topicPartitionsStruct = struct.instance(TOPICS_KEY_NAME);
            topicPartitionsStruct.set(TOPIC_NAME, topicAndPartitions.getKey());

            Map<Integer, CommittedOffset> partitionOffsets = topicAndPartitions.getValue();
            Object[] partitionOffsetsArray = new Object[partitionOffsets.size()];
            int j = 0;
            for (Map.Entry<Integer, CommittedOffset> partitionOffset : partitionOffsets.entrySet()) {
                Struct partitionOffsetStruct = topicPartitionsStruct.instance(PARTITIONS_KEY_NAME);
                partitionOffsetStruct.set(PARTITION_ID, partitionOffset.getKey());
                CommittedOffset committedOffset = partitionOffset.getValue();
                partitionOffsetStruct.set(OFFSET_KEY_NAME, committedOffset.offset);
                partitionOffsetStruct.set(METADATA_KEY_NAME, committedOffset.metadata);
                partitionOffsetsArray[j++] = partitionOffsetStruct;
            }
            topicPartitionsStruct.set(PARTITIONS_KEY_NAME, partitionOffsetsArray);
            partitionsArray[i++] = topicPartitionsStruct;
        }

        struct.set(TOPICS_KEY_NAME, partitionsArray);
        return struct;
    }

    @Override
    public TxnOffsetCommitResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        Map<TopicPartition, Errors> errors = new HashMap<>(offsets.size());
        for (TopicPartition partition : offsets.keySet())
            errors.put(partition, error);
        return new TxnOffsetCommitResponse(throttleTimeMs, errors);
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
