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
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.GENERATION_ID;
import static org.apache.kafka.common.protocol.CommonFields.GROUP_ID;
import static org.apache.kafka.common.protocol.CommonFields.MEMBER_ID;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;

/**
 * This wrapper supports both v0 and v1 of OffsetCommitRequest.
 */
public class OffsetCommitRequest extends AbstractRequest {
    private static final String TOPICS_KEY_NAME = "topics";

    // topic level field names
    private static final String PARTITIONS_KEY_NAME = "partitions";

    // partition level field names
    private static final Field.Int64 COMMIT_OFFSET = new Field.Int64("offset",
            "Message offset to be committed.");
    private static final Field.NullableStr COMMIT_METADATA = new Field.NullableStr("metadata",
            "Any associated metadata the client wants to keep.");

    private static final Schema OFFSET_COMMIT_REQUEST_PARTITION_V0 = new Schema(
            PARTITION_ID,
            COMMIT_OFFSET,
            COMMIT_METADATA);

    private static final Schema OFFSET_COMMIT_REQUEST_TOPIC_V0 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(OFFSET_COMMIT_REQUEST_PARTITION_V0), "Partitions to commit offsets."));

    private static final Schema OFFSET_COMMIT_REQUEST_V0 = new Schema(
            GROUP_ID,
            new Field(TOPICS_KEY_NAME, new ArrayOf(OFFSET_COMMIT_REQUEST_TOPIC_V0), "Topics to commit offsets."));

    // V1 adds timestamp and group membership information (generation and memberId)
    @Deprecated
    private static final Field.Int64 COMMIT_TIMESTAMP = new Field.Int64("timestamp", "Timestamp of the commit");

    private static final Schema OFFSET_COMMIT_REQUEST_PARTITION_V1 = new Schema(
            PARTITION_ID,
            COMMIT_OFFSET,
            COMMIT_TIMESTAMP,
            COMMIT_METADATA);

    private static final Schema OFFSET_COMMIT_REQUEST_TOPIC_V1 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(OFFSET_COMMIT_REQUEST_PARTITION_V1), "Partitions to commit offsets."));

    private static final Schema OFFSET_COMMIT_REQUEST_V1 = new Schema(
            GROUP_ID,
            GENERATION_ID,
            MEMBER_ID,
            new Field(TOPICS_KEY_NAME, new ArrayOf(OFFSET_COMMIT_REQUEST_TOPIC_V1), "Topics to commit offsets."));

    // V2 adds retention time
    private static final Field.Int64 RETENTION_TIME = new Field.Int64("retention_time",
            "Time period in ms to retain the offset.");

    private static final Schema OFFSET_COMMIT_REQUEST_PARTITION_V2 = new Schema(
            PARTITION_ID,
            COMMIT_OFFSET,
            COMMIT_METADATA);

    private static final Schema OFFSET_COMMIT_REQUEST_TOPIC_V2 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(OFFSET_COMMIT_REQUEST_PARTITION_V2), "Partitions to commit offsets."));

    private static final Schema OFFSET_COMMIT_REQUEST_V2 = new Schema(
            GROUP_ID,
            GENERATION_ID,
            MEMBER_ID,
            RETENTION_TIME,
            new Field(TOPICS_KEY_NAME, new ArrayOf(OFFSET_COMMIT_REQUEST_TOPIC_V2), "Topics to commit offsets."));

    // V3 adds throttle time
    private static final Schema OFFSET_COMMIT_REQUEST_V3 = OFFSET_COMMIT_REQUEST_V2;

    // V4 bump used to indicate that on quota violation brokers send out responses before throttling.
    private static final Schema OFFSET_COMMIT_REQUEST_V4 = OFFSET_COMMIT_REQUEST_V3;

    // V5 removes the retention time which is now controlled only by a broker configuration
    private static final Schema OFFSET_COMMIT_REQUEST_V5 = new Schema(
            GROUP_ID,
            GENERATION_ID,
            MEMBER_ID,
            new Field(TOPICS_KEY_NAME, new ArrayOf(OFFSET_COMMIT_REQUEST_TOPIC_V2), "Topics to commit offsets."));

    // V6 adds the leader epoch to the partition data
    private static final Field.Int32 LEADER_EPOCH = new Field.Int32("leader_epoch",
            "The leader epoch, if provided is derived from the last consumed record. " +
                    "This is used by the consumer to check for log truncation and to ensure partition " +
                    "metadata is up to date following a group rebalance.");

    private static final Schema OFFSET_COMMIT_REQUEST_PARTITION_V6 = new Schema(
            PARTITION_ID,
            LEADER_EPOCH,
            COMMIT_OFFSET,
            COMMIT_METADATA);

    private static final Schema OFFSET_COMMIT_REQUEST_TOPIC_V6 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(OFFSET_COMMIT_REQUEST_PARTITION_V6), "Partitions to commit offsets."));

    private static final Schema OFFSET_COMMIT_REQUEST_V6 = new Schema(
            GROUP_ID,
            GENERATION_ID,
            MEMBER_ID,
            new Field(TOPICS_KEY_NAME, new ArrayOf(OFFSET_COMMIT_REQUEST_TOPIC_V6), "Topics to commit offsets."));

    public static Schema[] schemaVersions() {
        return new Schema[] {OFFSET_COMMIT_REQUEST_V0, OFFSET_COMMIT_REQUEST_V1, OFFSET_COMMIT_REQUEST_V2,
            OFFSET_COMMIT_REQUEST_V3, OFFSET_COMMIT_REQUEST_V4, OFFSET_COMMIT_REQUEST_V5, OFFSET_COMMIT_REQUEST_V6};
    }

    // default values for the current version
    public static final int DEFAULT_GENERATION_ID = -1;
    public static final String DEFAULT_MEMBER_ID = "";
    public static final long DEFAULT_RETENTION_TIME = -1L;

    // default values for old versions,
    // will be removed after these versions are deprecated
    @Deprecated
    public static final long DEFAULT_TIMESTAMP = -1L;            // for V0, V1

    private final String groupId;
    private final String memberId;
    private final int generationId;
    private final long retentionTime;
    private final Map<TopicPartition, PartitionData> offsetData;

    public static final class PartitionData {
        @Deprecated
        public final long timestamp;                // for V1

        public final long offset;
        public final int leaderEpoch;
        public final String metadata;

        private PartitionData(long offset, int leaderEpoch, long timestamp, String metadata) {
            this.offset = offset;
            this.leaderEpoch = leaderEpoch;
            this.timestamp = timestamp;
            this.metadata = metadata;
        }

        @Deprecated
        public PartitionData(long offset, long timestamp, String metadata) {
            this(offset, RecordBatch.NO_PARTITION_LEADER_EPOCH, timestamp, metadata);
        }

        public PartitionData(long offset, int leaderEpoch, String metadata) {
            this(offset, leaderEpoch, DEFAULT_TIMESTAMP, metadata);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(timestamp=").append(timestamp).
                    append(", offset=").append(offset).
                    append(", leaderEpoch=").append(leaderEpoch).
                    append(", metadata=").append(metadata).
                    append(")");
            return bld.toString();
        }
    }

    public static class Builder extends AbstractRequest.Builder<OffsetCommitRequest> {
        private final String groupId;
        private final Map<TopicPartition, PartitionData> offsetData;
        private String memberId = DEFAULT_MEMBER_ID;
        private int generationId = DEFAULT_GENERATION_ID;

        public Builder(String groupId, Map<TopicPartition, PartitionData> offsetData) {
            super(ApiKeys.OFFSET_COMMIT);
            this.groupId = groupId;
            this.offsetData = offsetData;
        }

        public Builder setMemberId(String memberId) {
            this.memberId = memberId;
            return this;
        }

        public Builder setGenerationId(int generationId) {
            this.generationId = generationId;
            return this;
        }

        @Override
        public OffsetCommitRequest build(short version) {
            if (version == 0) {
                return new OffsetCommitRequest(groupId, DEFAULT_GENERATION_ID, DEFAULT_MEMBER_ID,
                        DEFAULT_RETENTION_TIME, offsetData, version);
            } else {
                return new OffsetCommitRequest(groupId, generationId, memberId, DEFAULT_RETENTION_TIME,
                        offsetData, version);
            }
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=OffsetCommitRequest").
                append(", groupId=").append(groupId).
                append(", memberId=").append(memberId).
                append(", generationId=").append(generationId).
                append(", offsetData=").append(offsetData).
                append(")");
            return bld.toString();
        }
    }

    private OffsetCommitRequest(String groupId, int generationId, String memberId, long retentionTime,
                                Map<TopicPartition, PartitionData> offsetData, short version) {
        super(version);
        this.groupId = groupId;
        this.generationId = generationId;
        this.memberId = memberId;
        this.retentionTime = retentionTime;
        this.offsetData = offsetData;
    }

    public OffsetCommitRequest(Struct struct, short versionId) {
        super(versionId);

        groupId = struct.get(GROUP_ID);

        // These fields only exists in v1.
        generationId = struct.getOrElse(GENERATION_ID, DEFAULT_GENERATION_ID);
        memberId = struct.getOrElse(MEMBER_ID, DEFAULT_MEMBER_ID);

        // This field only exists in v2
        retentionTime = struct.getOrElse(RETENTION_TIME, DEFAULT_RETENTION_TIME);

        offsetData = new HashMap<>();
        for (Object topicDataObj : struct.getArray(TOPICS_KEY_NAME)) {
            Struct topicData = (Struct) topicDataObj;
            String topic = topicData.get(TOPIC_NAME);
            for (Object partitionDataObj : topicData.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionDataStruct = (Struct) partitionDataObj;
                int partition = partitionDataStruct.get(PARTITION_ID);
                long offset = partitionDataStruct.get(COMMIT_OFFSET);
                String metadata = partitionDataStruct.get(COMMIT_METADATA);
                PartitionData partitionOffset;
                // This field only exists in v1
                if (partitionDataStruct.hasField(COMMIT_TIMESTAMP)) {
                    long timestamp = partitionDataStruct.get(COMMIT_TIMESTAMP);
                    partitionOffset = new PartitionData(offset, timestamp, metadata);
                } else {
                    int leaderEpoch = partitionDataStruct.getOrElse(LEADER_EPOCH, RecordBatch.NO_PARTITION_LEADER_EPOCH);
                    partitionOffset = new PartitionData(offset, leaderEpoch, metadata);
                }
                offsetData.put(new TopicPartition(topic, partition), partitionOffset);
            }
        }
    }

    @Override
    public Struct toStruct() {
        short version = version();
        Struct struct = new Struct(ApiKeys.OFFSET_COMMIT.requestSchema(version));
        struct.set(GROUP_ID, groupId);

        Map<String, Map<Integer, PartitionData>> topicsData = CollectionUtils.groupPartitionsByTopic(offsetData);
        List<Struct> topicArray = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, PartitionData>> topicEntry: topicsData.entrySet()) {
            Struct topicData = struct.instance(TOPICS_KEY_NAME);
            topicData.set(TOPIC_NAME, topicEntry.getKey());
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, PartitionData> partitionEntry : topicEntry.getValue().entrySet()) {
                PartitionData fetchPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                partitionData.set(PARTITION_ID, partitionEntry.getKey());
                partitionData.set(COMMIT_OFFSET, fetchPartitionData.offset);
                // Only for v1
                partitionData.setIfExists(COMMIT_TIMESTAMP, fetchPartitionData.timestamp);
                // Only for v6
                partitionData.setIfExists(LEADER_EPOCH, fetchPartitionData.leaderEpoch);
                partitionData.set(COMMIT_METADATA, fetchPartitionData.metadata);
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(TOPICS_KEY_NAME, topicArray.toArray());
        struct.setIfExists(GENERATION_ID, generationId);
        struct.setIfExists(MEMBER_ID, memberId);
        struct.setIfExists(RETENTION_TIME, retentionTime);
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Map<TopicPartition, Errors> responseData = new HashMap<>();
        for (Map.Entry<TopicPartition, PartitionData> entry: offsetData.entrySet()) {
            responseData.put(entry.getKey(), Errors.forException(e));
        }

        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
            case 2:
                return new OffsetCommitResponse(responseData);
            case 3:
            case 4:
            case 5:
            case 6:
                return new OffsetCommitResponse(throttleTimeMs, responseData);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.OFFSET_COMMIT.latestVersion()));
        }
    }

    public String groupId() {
        return groupId;
    }

    public int generationId() {
        return generationId;
    }

    public String memberId() {
        return memberId;
    }

    @Deprecated
    public long retentionTime() {
        return retentionTime;
    }

    public Map<TopicPartition, PartitionData> offsetData() {
        return offsetData;
    }

    public static OffsetCommitRequest parse(ByteBuffer buffer, short version) {
        Schema schema = ApiKeys.OFFSET_COMMIT.requestSchema(version);
        return new OffsetCommitRequest(schema.read(buffer), version);
    }
}
