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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.common.protocol.CommonFields.CURRENT_LEADER_EPOCH;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;

public class ListOffsetRequest extends AbstractRequest {
    public static final long EARLIEST_TIMESTAMP = -2L;
    public static final long LATEST_TIMESTAMP = -1L;

    public static final int CONSUMER_REPLICA_ID = -1;
    public static final int DEBUGGING_REPLICA_ID = -2;

    // top level fields
    private static final Field.Int32 REPLICA_ID = new Field.Int32("replica_id",
            "Broker id of the follower. For normal consumers, use -1.");
    private static final Field.Int8 ISOLATION_LEVEL = new Field.Int8("isolation_level",
            "This setting controls the visibility of transactional records. " +
                    "Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED " +
                    "(isolation_level = 1), non-transactional and COMMITTED transactional records are visible. " +
                    "To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current " +
                    "LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the " +
                    "result, which allows consumers to discard ABORTED transactional records");
    private static final Field.ComplexArray TOPICS = new Field.ComplexArray("topics",
            "Topics to list offsets.");

    // topic level fields
    private static final Field.ComplexArray PARTITIONS = new Field.ComplexArray("partitions",
            "Partitions to list offsets.");

    // partition level fields
    private static final Field.Int64 TIMESTAMP = new Field.Int64("timestamp",
            "The target timestamp for the partition.");
    private static final Field.Int32 MAX_NUM_OFFSETS = new Field.Int32("max_num_offsets",
            "Maximum offsets to return.");

    private static final Field PARTITIONS_V0 = PARTITIONS.withFields(
            PARTITION_ID,
            TIMESTAMP,
            MAX_NUM_OFFSETS);

    private static final Field TOPICS_V0 = TOPICS.withFields(
            TOPIC_NAME,
            PARTITIONS_V0);

    private static final Schema LIST_OFFSET_REQUEST_V0 = new Schema(
            REPLICA_ID,
            TOPICS_V0);

    // V1 removes max_num_offsets
    private static final Field PARTITIONS_V1 = PARTITIONS.withFields(
            PARTITION_ID,
            TIMESTAMP);

    private static final Field TOPICS_V1 = TOPICS.withFields(
            TOPIC_NAME,
            PARTITIONS_V1);

    private static final Schema LIST_OFFSET_REQUEST_V1 = new Schema(
            REPLICA_ID,
            TOPICS_V1);

    // V2 adds a field for the isolation level
    private static final Schema LIST_OFFSET_REQUEST_V2 = new Schema(
            REPLICA_ID,
            ISOLATION_LEVEL,
            TOPICS_V1);

    // V3 bump used to indicate that on quota violation brokers send out responses before throttling.
    private static final Schema LIST_OFFSET_REQUEST_V3 = LIST_OFFSET_REQUEST_V2;

    // V4 introduces the current leader epoch, which is used for fencing
    private static final Field PARTITIONS_V4 = PARTITIONS.withFields(
            PARTITION_ID,
            CURRENT_LEADER_EPOCH,
            TIMESTAMP);

    private static final Field TOPICS_V4 = TOPICS.withFields(
            TOPIC_NAME,
            PARTITIONS_V4);

    private static final Schema LIST_OFFSET_REQUEST_V4 = new Schema(
            REPLICA_ID,
            ISOLATION_LEVEL,
            TOPICS_V4);

    // V5 bump to include new possible error code (OFFSET_NOT_AVAILABLE)
    private static final Schema LIST_OFFSET_REQUEST_V5 = LIST_OFFSET_REQUEST_V4;

    public static Schema[] schemaVersions() {
        return new Schema[] {LIST_OFFSET_REQUEST_V0, LIST_OFFSET_REQUEST_V1, LIST_OFFSET_REQUEST_V2,
            LIST_OFFSET_REQUEST_V3, LIST_OFFSET_REQUEST_V4, LIST_OFFSET_REQUEST_V5};
    }

    private final int replicaId;
    private final IsolationLevel isolationLevel;
    private final Map<TopicPartition, PartitionData> partitionTimestamps;
    private final Set<TopicPartition> duplicatePartitions;

    public static class Builder extends AbstractRequest.Builder<ListOffsetRequest> {
        private final int replicaId;
        private final IsolationLevel isolationLevel;
        private Map<TopicPartition, PartitionData> partitionTimestamps = new HashMap<>();

        public static Builder forReplica(short allowedVersion, int replicaId) {
            return new Builder((short) 0, allowedVersion, replicaId, IsolationLevel.READ_UNCOMMITTED);
        }

        public static Builder forConsumer(boolean requireTimestamp, IsolationLevel isolationLevel) {
            short minVersion = 0;
            if (isolationLevel == IsolationLevel.READ_COMMITTED)
                minVersion = 2;
            else if (requireTimestamp)
                minVersion = 1;
            return new Builder(minVersion, ApiKeys.LIST_OFFSETS.latestVersion(), CONSUMER_REPLICA_ID, isolationLevel);
        }

        private Builder(short oldestAllowedVersion,
                        short latestAllowedVersion,
                        int replicaId,
                        IsolationLevel isolationLevel) {
            super(ApiKeys.LIST_OFFSETS, oldestAllowedVersion, latestAllowedVersion);
            this.replicaId = replicaId;
            this.isolationLevel = isolationLevel;
        }

        public Builder setTargetTimes(Map<TopicPartition, PartitionData> partitionTimestamps) {
            this.partitionTimestamps = partitionTimestamps;
            return this;
        }

        @Override
        public ListOffsetRequest build(short version) {
            return new ListOffsetRequest(replicaId, partitionTimestamps, isolationLevel, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=ListOffsetRequest")
               .append(", replicaId=").append(replicaId);
            if (partitionTimestamps != null) {
                bld.append(", partitionTimestamps=").append(partitionTimestamps);
            }
            bld.append(", isolationLevel=").append(isolationLevel);
            bld.append(")");
            return bld.toString();
        }
    }

    public static final class PartitionData {
        public final long timestamp;
        @Deprecated
        public final int maxNumOffsets; // only supported in v0
        public final Optional<Integer> currentLeaderEpoch;

        private PartitionData(long timestamp, int maxNumOffsets, Optional<Integer> currentLeaderEpoch) {
            this.timestamp = timestamp;
            this.maxNumOffsets = maxNumOffsets;
            this.currentLeaderEpoch = currentLeaderEpoch;
        }

        @Deprecated
        public PartitionData(long timestamp, int maxNumOffsets) {
            this(timestamp, maxNumOffsets, Optional.empty());
        }

        public PartitionData(long timestamp, Optional<Integer> currentLeaderEpoch) {
            this(timestamp, 1, currentLeaderEpoch);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("{timestamp: ").append(timestamp).
                    append(", maxNumOffsets: ").append(maxNumOffsets).
                    append(", currentLeaderEpoch: ").append(currentLeaderEpoch).
                    append("}");
            return bld.toString();
        }
    }

    /**
     * Private constructor with a specified version.
     */
    @SuppressWarnings("unchecked")
    private ListOffsetRequest(int replicaId,
                              Map<TopicPartition, PartitionData> targetTimes,
                              IsolationLevel isolationLevel,
                              short version) {
        super(ApiKeys.LIST_OFFSETS, version);
        this.replicaId = replicaId;
        this.isolationLevel = isolationLevel;
        this.partitionTimestamps = targetTimes;
        this.duplicatePartitions = Collections.emptySet();
    }

    public ListOffsetRequest(Struct struct, short version) {
        super(ApiKeys.LIST_OFFSETS, version);
        Set<TopicPartition> duplicatePartitions = new HashSet<>();
        replicaId = struct.get(REPLICA_ID);
        isolationLevel = struct.hasField(ISOLATION_LEVEL) ?
                IsolationLevel.forId(struct.get(ISOLATION_LEVEL)) :
                IsolationLevel.READ_UNCOMMITTED;
        partitionTimestamps = new HashMap<>();
        for (Object topicResponseObj : struct.get(TOPICS)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.get(TOPIC_NAME);
            for (Object partitionResponseObj : topicResponse.get(PARTITIONS)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.get(PARTITION_ID);
                long timestamp = partitionResponse.get(TIMESTAMP);
                TopicPartition tp = new TopicPartition(topic, partition);

                int maxNumOffsets = partitionResponse.getOrElse(MAX_NUM_OFFSETS, 1);
                Optional<Integer> currentLeaderEpoch = RequestUtils.getLeaderEpoch(partitionResponse, CURRENT_LEADER_EPOCH);
                PartitionData partitionData = new PartitionData(timestamp, maxNumOffsets, currentLeaderEpoch);
                if (partitionTimestamps.put(tp, partitionData) != null)
                    duplicatePartitions.add(tp);
            }
        }
        this.duplicatePartitions = duplicatePartitions;
    }

    @Override
    @SuppressWarnings("deprecation")
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Map<TopicPartition, ListOffsetResponse.PartitionData> responseData = new HashMap<>();
        short versionId = version();

        ListOffsetResponse.PartitionData partitionError = versionId == 0 ?
                new ListOffsetResponse.PartitionData(Errors.forException(e), Collections.emptyList()) :
                new ListOffsetResponse.PartitionData(Errors.forException(e), -1L, -1L, Optional.empty());
        for (TopicPartition partition : partitionTimestamps.keySet()) {
            responseData.put(partition, partitionError);
        }

        switch (version()) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
                return new ListOffsetResponse(throttleTimeMs, responseData);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.LIST_OFFSETS.latestVersion()));
        }
    }

    public int replicaId() {
        return replicaId;
    }

    public IsolationLevel isolationLevel() {
        return isolationLevel;
    }

    public Map<TopicPartition, PartitionData> partitionTimestamps() {
        return partitionTimestamps;
    }

    public Set<TopicPartition> duplicatePartitions() {
        return duplicatePartitions;
    }

    public static ListOffsetRequest parse(ByteBuffer buffer, short version) {
        return new ListOffsetRequest(ApiKeys.LIST_OFFSETS.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        short version = version();
        Struct struct = new Struct(ApiKeys.LIST_OFFSETS.requestSchema(version));
        Map<String, Map<Integer, PartitionData>> topicsData = CollectionUtils.groupPartitionDataByTopic(partitionTimestamps);

        struct.set(REPLICA_ID, replicaId);
        struct.setIfExists(ISOLATION_LEVEL, isolationLevel.id());

        List<Struct> topicArray = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, PartitionData>> topicEntry: topicsData.entrySet()) {
            Struct topicData = struct.instance(TOPICS);
            topicData.set(TOPIC_NAME, topicEntry.getKey());
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, PartitionData> partitionEntry : topicEntry.getValue().entrySet()) {
                PartitionData offsetPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS);
                partitionData.set(PARTITION_ID, partitionEntry.getKey());
                partitionData.set(TIMESTAMP, offsetPartitionData.timestamp);
                partitionData.setIfExists(MAX_NUM_OFFSETS, offsetPartitionData.maxNumOffsets);
                RequestUtils.setLeaderEpochIfExists(partitionData, CURRENT_LEADER_EPOCH,
                        offsetPartitionData.currentLeaderEpoch);
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(TOPICS, topicArray.toArray());
        return struct;
    }
}
