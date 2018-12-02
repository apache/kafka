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
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.protocol.CommonFields.COMMITTED_LEADER_EPOCH;
import static org.apache.kafka.common.protocol.CommonFields.COMMITTED_METADATA;
import static org.apache.kafka.common.protocol.CommonFields.COMMITTED_OFFSET;
import static org.apache.kafka.common.protocol.CommonFields.GROUP_ID;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.PRODUCER_EPOCH;
import static org.apache.kafka.common.protocol.CommonFields.PRODUCER_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.CommonFields.TRANSACTIONAL_ID;

public class TxnOffsetCommitRequest extends AbstractRequest {
    // top level fields
    private static final Field.ComplexArray TOPICS = new Field.ComplexArray("topics",
            "Topics to commit offsets");

    // topic level fields
    private static final Field.ComplexArray PARTITIONS = new Field.ComplexArray("partitions",
            "Partitions to commit offsets");

    private static final Field PARTITIONS_V0 = PARTITIONS.withFields(
            PARTITION_ID,
            COMMITTED_OFFSET,
            COMMITTED_METADATA);

    private static final Field TOPICS_V0 = TOPICS.withFields(
            TOPIC_NAME,
            PARTITIONS_V0);

    private static final Schema TXN_OFFSET_COMMIT_REQUEST_V0 = new Schema(
            TRANSACTIONAL_ID,
            GROUP_ID,
            PRODUCER_ID,
            PRODUCER_EPOCH,
            TOPICS_V0);

    // V1 bump used to indicate that on quota violation brokers send out responses before throttling.
    private static final Schema TXN_OFFSET_COMMIT_REQUEST_V1 = TXN_OFFSET_COMMIT_REQUEST_V0;

    // V2 adds the leader epoch to the partition data
    private static final Field PARTITIONS_V2 = PARTITIONS.withFields(
            PARTITION_ID,
            COMMITTED_OFFSET,
            COMMITTED_LEADER_EPOCH,
            COMMITTED_METADATA);

    private static final Field TOPICS_V2 = TOPICS.withFields(
            TOPIC_NAME,
            PARTITIONS_V2);

    private static final Schema TXN_OFFSET_COMMIT_REQUEST_V2 = new Schema(
            TRANSACTIONAL_ID,
            GROUP_ID,
            PRODUCER_ID,
            PRODUCER_EPOCH,
            TOPICS_V2);

    public static Schema[] schemaVersions() {
        return new Schema[]{TXN_OFFSET_COMMIT_REQUEST_V0, TXN_OFFSET_COMMIT_REQUEST_V1, TXN_OFFSET_COMMIT_REQUEST_V2};
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
        super(ApiKeys.TXN_OFFSET_COMMIT, version);
        this.transactionalId = transactionalId;
        this.consumerGroupId = consumerGroupId;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.offsets = offsets;
    }

    public TxnOffsetCommitRequest(Struct struct, short version) {
        super(ApiKeys.TXN_OFFSET_COMMIT, version);
        this.transactionalId = struct.get(TRANSACTIONAL_ID);
        this.consumerGroupId = struct.get(GROUP_ID);
        this.producerId = struct.get(PRODUCER_ID);
        this.producerEpoch = struct.get(PRODUCER_EPOCH);

        Map<TopicPartition, CommittedOffset> offsets = new HashMap<>();
        Object[] topicPartitionsArray = struct.get(TOPICS);
        for (Object topicPartitionObj : topicPartitionsArray) {
            Struct topicPartitionStruct = (Struct) topicPartitionObj;
            String topic = topicPartitionStruct.get(TOPIC_NAME);
            for (Object partitionObj : topicPartitionStruct.get(PARTITIONS)) {
                Struct partitionStruct = (Struct) partitionObj;
                TopicPartition partition = new TopicPartition(topic, partitionStruct.get(PARTITION_ID));
                long offset = partitionStruct.get(COMMITTED_OFFSET);
                String metadata = partitionStruct.get(COMMITTED_METADATA);
                Optional<Integer> leaderEpoch = RequestUtils.getLeaderEpoch(partitionStruct, COMMITTED_LEADER_EPOCH);
                offsets.put(partition, new CommittedOffset(offset, metadata, leaderEpoch));
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

        Map<String, Map<Integer, CommittedOffset>> mappedPartitionOffsets = CollectionUtils.groupPartitionDataByTopic(offsets);
        Object[] partitionsArray = new Object[mappedPartitionOffsets.size()];
        int i = 0;
        for (Map.Entry<String, Map<Integer, CommittedOffset>> topicAndPartitions : mappedPartitionOffsets.entrySet()) {
            Struct topicPartitionsStruct = struct.instance(TOPICS);
            topicPartitionsStruct.set(TOPIC_NAME, topicAndPartitions.getKey());

            Map<Integer, CommittedOffset> partitionOffsets = topicAndPartitions.getValue();
            Object[] partitionOffsetsArray = new Object[partitionOffsets.size()];
            int j = 0;
            for (Map.Entry<Integer, CommittedOffset> partitionOffset : partitionOffsets.entrySet()) {
                Struct partitionOffsetStruct = topicPartitionsStruct.instance(PARTITIONS);
                partitionOffsetStruct.set(PARTITION_ID, partitionOffset.getKey());
                CommittedOffset committedOffset = partitionOffset.getValue();
                partitionOffsetStruct.set(COMMITTED_OFFSET, committedOffset.offset);
                partitionOffsetStruct.set(COMMITTED_METADATA, committedOffset.metadata);
                RequestUtils.setLeaderEpochIfExists(partitionOffsetStruct, COMMITTED_LEADER_EPOCH,
                        committedOffset.leaderEpoch);
                partitionOffsetsArray[j++] = partitionOffsetStruct;
            }
            topicPartitionsStruct.set(PARTITIONS, partitionOffsetsArray);
            partitionsArray[i++] = topicPartitionsStruct;
        }

        struct.set(TOPICS, partitionsArray);
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
        public final long offset;
        public final String metadata;
        public final Optional<Integer> leaderEpoch;

        public CommittedOffset(long offset, String metadata, Optional<Integer> leaderEpoch) {
            this.offset = offset;
            this.metadata = metadata;
            this.leaderEpoch = leaderEpoch;
        }

        @Override
        public String toString() {
            return "CommittedOffset(" +
                    "offset=" + offset +
                    ", leaderEpoch=" + leaderEpoch +
                    ", metadata='" + metadata + "')";
        }
    }

}
