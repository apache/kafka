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
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Optional;

import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.CommonFields.CURRENT_LEADER_EPOCH;

public class RDMAConsumeAddressRequest extends AbstractRequest {

    public static final int CONSUMER_REPLICA_ID = -1;
    public static final int DEBUGGING_REPLICA_ID = -2;


    // top level fields
    private static final Field.Int32 REPLICA_ID = new Field.Int32("replica_id",
            "Broker id of the follower. For normal consumers, use -1.");
    private static final Field.ComplexArray TOPICS = new Field.ComplexArray("topics",
            "Topics to list offsets.");

    // topic level fields
    private static final Field.ComplexArray PARTITIONS = new Field.ComplexArray("partitions",
            "Partitions to list offsets.");

    // partition level fields
    private static final Field.Int64 START_OFFSET = new Field.Int64("start_offset", "Message offset.");

    private static final Field.Int64 BASE_OFFSET = new Field.Int64("base_offset", "Message offset.");


    private static final Field PARTITIONS_V0 = PARTITIONS.withFields(
            PARTITION_ID,
            START_OFFSET);

    private static final Field TOPICS_V0 = TOPICS.withFields(
            TOPIC_NAME,
            PARTITIONS_V0);


    private static final Field.ComplexArray TOPICS_TO_FORGET = new Field.ComplexArray("topics_to_forget",
            "Topics to fetch offsets. If the topic array is null fetch offsets for all topics.");


    private static final Field.ComplexArray FORGET_TOPICS_DATA = new Field.ComplexArray("topics_to_forget",
            "Topics to fetch offsets. If the topic array is null fetch offsets for all topics.");

    private static final Field FORGET_TOPICS_DATA_V0 = FORGET_TOPICS_DATA.withFields(
            PARTITION_ID, BASE_OFFSET);

    private static final Field TOPICS_TO_FORGET_V0 = TOPICS_TO_FORGET.withFields(
            TOPIC_NAME,
            FORGET_TOPICS_DATA_V0);




    private static final Schema RDMA_ADDRESS_REQUEST_V0 = new Schema(
            REPLICA_ID,
            TOPICS_V0,
            TOPICS_TO_FORGET_V0);

    public static Schema[] schemaVersions() {
        return new Schema[] {RDMA_ADDRESS_REQUEST_V0};
    }

    private final int replicaId;
    private final Map<TopicPartition, PartitionData> fetchAddresses;
    private final Map<TopicPartition, Long>  toForget; // Long for baseOffset

    public static class Builder extends AbstractRequest.Builder<RDMAConsumeAddressRequest> {
        private final int replicaId;
        private Map<TopicPartition, PartitionData> fetchAddresses = new HashMap<>();
        private Map<TopicPartition, Long> toForget = new HashMap<>();

        public static Builder forConsumer() {
            short minVersion = 0;
            return new Builder(minVersion, ApiKeys.CONSUMER_RDMA_REGISTER.latestVersion(), CONSUMER_REPLICA_ID);
        }

        private Builder(short oldestAllowedVersion,
                        short latestAllowedVersion,
                        int replicaId) {
            super(ApiKeys.CONSUMER_RDMA_REGISTER, oldestAllowedVersion, latestAllowedVersion);
            this.replicaId = replicaId;
        }

        public Builder setTargetOffsets(Map<TopicPartition, PartitionData> fetchAddresses) {
            this.fetchAddresses = fetchAddresses;
            return this;
        }

        public Builder setToForgetOffsets(Map<TopicPartition, Long> toForget) {
            this.toForget = toForget;
            return this;
        }

        @Override
        public RDMAConsumeAddressRequest build(short version) {
            return new RDMAConsumeAddressRequest(replicaId, fetchAddresses, toForget, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=RDMAConsumeAddressRequest")
               .append(", replicaId=").append(replicaId);
            if (fetchAddresses != null) {
                bld.append(", partitionTimestamps=").append(fetchAddresses);
            }
            bld.append(")");
            return bld.toString();
        }
    }

    public static final class PartitionData {
        public final long startOffset;
        public final Optional<Integer> currentLeaderEpoch;

        public PartitionData(long startOffset, Optional<Integer> currentLeaderEpoch) {
            this.startOffset = startOffset;
            this.currentLeaderEpoch = currentLeaderEpoch;
        }

        public PartitionData(long timestamp) {
            this(timestamp,  Optional.empty());
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("{start_offset: ").append(startOffset).
                    append(", currentLeaderEpoch: ").append(currentLeaderEpoch).
                    append("}");
            return bld.toString();
        }
    }

    /**
     * Private constructor with a specified version.
     */
    @SuppressWarnings("unchecked")
    private RDMAConsumeAddressRequest(int replicaId,
                               Map<TopicPartition, PartitionData> targetTimes,
                               Map<TopicPartition, Long>   toForget,
                               short version) {
        super(ApiKeys.CONSUMER_RDMA_REGISTER, version);
        this.replicaId = replicaId;
        this.fetchAddresses = targetTimes;
        this.toForget = toForget;
    }

    public RDMAConsumeAddressRequest(Struct struct, short version) {
        super(ApiKeys.CONSUMER_RDMA_REGISTER, version);
        replicaId = struct.get(REPLICA_ID);
        fetchAddresses = new HashMap<>();
        for (Object topicResponseObj : struct.get(TOPICS)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.get(TOPIC_NAME);
            for (Object partitionResponseObj : topicResponse.get(PARTITIONS)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.get(PARTITION_ID);
                long startOffset = partitionResponse.get(START_OFFSET);
                TopicPartition tp = new TopicPartition(topic, partition);
                Optional<Integer> currentLeaderEpoch = RequestUtils.getLeaderEpoch(partitionResponse, CURRENT_LEADER_EPOCH);
                PartitionData partitionData = new PartitionData(startOffset, currentLeaderEpoch);
                fetchAddresses.put(tp, partitionData);
            }
        }

        toForget = new HashMap<>();
        for (Object topicStructObj : struct.get(TOPICS_TO_FORGET)) {
            Struct topicStruct = (Struct) topicStructObj;
            String topic = topicStruct.get(TOPIC_NAME);
            for (Object partitionStructObj : topicStruct.get(FORGET_TOPICS_DATA)) {
                Struct partitionStruct = (Struct) partitionStructObj;
                int partition = partitionStruct.get(PARTITION_ID);
                long baseOffset = partitionStruct.get(BASE_OFFSET);
                toForget.put(new TopicPartition(topic, partition), baseOffset);
            }
        }


    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Map<TopicPartition, RDMAConsumeAddressResponse.PartitionData> responseData = new HashMap<>();
        short versionId = version();

        RDMAConsumeAddressResponse.PartitionData partitionError =
                new RDMAConsumeAddressResponse.PartitionData(Errors.forException(e), 0, 0, 0, 0, 0, 0, 0, 0, 0, false, 0, 0);
        for (TopicPartition partition : fetchAddresses.keySet()) {
            responseData.put(partition, partitionError);
        }

        switch (version()) {
            case 0:
                return new RDMAConsumeAddressResponse("", 0, throttleTimeMs, responseData);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.CONSUMER_RDMA_REGISTER.latestVersion()));
        }
    }

    public int replicaId() {
        return replicaId;
    }


    public Map<TopicPartition, PartitionData> fetchAddresses() {
        return fetchAddresses;
    }

    public Map<TopicPartition, Long> getToForget() {
        return toForget;
    }


    public static RDMAConsumeAddressRequest parse(ByteBuffer buffer, short version) {
        return new RDMAConsumeAddressRequest(ApiKeys.CONSUMER_RDMA_REGISTER.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        short version = version();
        Struct struct = new Struct(ApiKeys.CONSUMER_RDMA_REGISTER.requestSchema(version));
        Map<String, Map<Integer, PartitionData>> topicsData = CollectionUtils.groupPartitionDataByTopic(fetchAddresses);

        struct.set(REPLICA_ID, replicaId);

        List<Struct> topicArray = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, PartitionData>> topicEntry: topicsData.entrySet()) {
            Struct topicData = struct.instance(TOPICS);
            topicData.set(TOPIC_NAME, topicEntry.getKey());
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, PartitionData> partitionEntry : topicEntry.getValue().entrySet()) {
                PartitionData offsetPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS);
                partitionData.set(PARTITION_ID, partitionEntry.getKey());
                partitionData.set(START_OFFSET, offsetPartitionData.startOffset);
                RequestUtils.setLeaderEpochIfExists(partitionData, CURRENT_LEADER_EPOCH,
                        offsetPartitionData.currentLeaderEpoch);
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(TOPICS, topicArray.toArray());


        Map<String, Map<Integer, Long>> baseOffsetsByTopic = CollectionUtils.groupPartitionDataByTopic(toForget);

        List<Struct> topicStructArray = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, Long>> offsetsByTopicEntry : baseOffsetsByTopic.entrySet()) {
            Struct topicStruct = struct.instance(TOPICS_TO_FORGET);
            topicStruct.set(TOPIC_NAME, offsetsByTopicEntry.getKey());
            List<Struct> partitionStructArray = new ArrayList<>();
            for (Map.Entry<Integer, Long> offsetsByPartitionEntry : offsetsByTopicEntry.getValue().entrySet()) {
                Struct partitionStruct = topicStruct.instance(FORGET_TOPICS_DATA);
                partitionStruct.set(PARTITION_ID, offsetsByPartitionEntry.getKey());
                partitionStruct.set(BASE_OFFSET, offsetsByPartitionEntry.getValue());
                partitionStructArray.add(partitionStruct);
            }
            topicStruct.set(FORGET_TOPICS_DATA, partitionStructArray.toArray());
            topicStructArray.add(topicStruct);
        }
        struct.set(TOPICS_TO_FORGET, topicStructArray.toArray());

        return struct;
    }
}
