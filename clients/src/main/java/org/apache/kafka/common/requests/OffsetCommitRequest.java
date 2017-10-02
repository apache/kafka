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
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.INT64;
import static org.apache.kafka.common.protocol.types.Type.NULLABLE_STRING;
import static org.apache.kafka.common.protocol.types.Type.STRING;

/**
 * This wrapper supports both v0 and v1 of OffsetCommitRequest.
 */
public class OffsetCommitRequest extends AbstractRequest {
    private static final String GROUP_ID_KEY_NAME = "group_id";
    private static final String GENERATION_ID_KEY_NAME = "group_generation_id";
    private static final String MEMBER_ID_KEY_NAME = "member_id";
    private static final String TOPICS_KEY_NAME = "topics";
    private static final String RETENTION_TIME_KEY_NAME = "retention_time";

    // topic level field names
    private static final String PARTITIONS_KEY_NAME = "partitions";

    // partition level field names
    private static final String COMMIT_OFFSET_KEY_NAME = "offset";
    private static final String METADATA_KEY_NAME = "metadata";

    @Deprecated
    private static final String TIMESTAMP_KEY_NAME = "timestamp";         // for v0, v1

    /* Offset commit api */
    private static final Schema OFFSET_COMMIT_REQUEST_PARTITION_V0 = new Schema(
            PARTITION_ID,
            new Field(COMMIT_OFFSET_KEY_NAME, INT64, "Message offset to be committed."),
            new Field(METADATA_KEY_NAME, NULLABLE_STRING, "Any associated metadata the client wants to keep."));

    private static final Schema OFFSET_COMMIT_REQUEST_PARTITION_V1 = new Schema(
            PARTITION_ID,
            new Field(COMMIT_OFFSET_KEY_NAME, INT64, "Message offset to be committed."),
            new Field(TIMESTAMP_KEY_NAME, INT64, "Timestamp of the commit"),
            new Field(METADATA_KEY_NAME, NULLABLE_STRING, "Any associated metadata the client wants to keep."));

    private static final Schema OFFSET_COMMIT_REQUEST_PARTITION_V2 = new Schema(
            PARTITION_ID,
            new Field(COMMIT_OFFSET_KEY_NAME, INT64, "Message offset to be committed."),
            new Field(METADATA_KEY_NAME, NULLABLE_STRING, "Any associated metadata the client wants to keep."));

    private static final Schema OFFSET_COMMIT_REQUEST_TOPIC_V0 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(OFFSET_COMMIT_REQUEST_PARTITION_V0), "Partitions to commit offsets."));

    private static final Schema OFFSET_COMMIT_REQUEST_TOPIC_V1 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(OFFSET_COMMIT_REQUEST_PARTITION_V1), "Partitions to commit offsets."));

    private static final Schema OFFSET_COMMIT_REQUEST_TOPIC_V2 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(OFFSET_COMMIT_REQUEST_PARTITION_V2), "Partitions to commit offsets."));

    private static final Schema OFFSET_COMMIT_REQUEST_V0 = new Schema(
            new Field(GROUP_ID_KEY_NAME, STRING, "The group id."),
            new Field(TOPICS_KEY_NAME, new ArrayOf(OFFSET_COMMIT_REQUEST_TOPIC_V0), "Topics to commit offsets."));

    private static final Schema OFFSET_COMMIT_REQUEST_V1 = new Schema(
            new Field(GROUP_ID_KEY_NAME, STRING, "The group id."),
            new Field(GENERATION_ID_KEY_NAME, INT32, "The generation of the group."),
            new Field(MEMBER_ID_KEY_NAME, STRING, "The member id assigned by the group coordinator."),
            new Field(TOPICS_KEY_NAME, new ArrayOf(OFFSET_COMMIT_REQUEST_TOPIC_V1), "Topics to commit offsets."));

    private static final Schema OFFSET_COMMIT_REQUEST_V2 = new Schema(
            new Field(GROUP_ID_KEY_NAME, STRING, "The group id."),
            new Field(GENERATION_ID_KEY_NAME, INT32, "The generation of the consumer group."),
            new Field(MEMBER_ID_KEY_NAME, STRING, "The consumer id assigned by the group coordinator."),
            new Field(RETENTION_TIME_KEY_NAME, INT64, "Time period in ms to retain the offset."),
            new Field(TOPICS_KEY_NAME, new ArrayOf(OFFSET_COMMIT_REQUEST_TOPIC_V2), "Topics to commit offsets."));

    /* v3 request is same as v2. Throttle time has been added to response */
    private static final Schema OFFSET_COMMIT_REQUEST_V3 = OFFSET_COMMIT_REQUEST_V2;

    public static Schema[] schemaVersions() {
        return new Schema[] {OFFSET_COMMIT_REQUEST_V0, OFFSET_COMMIT_REQUEST_V1, OFFSET_COMMIT_REQUEST_V2,
            OFFSET_COMMIT_REQUEST_V3};
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
        public final String metadata;

        @Deprecated
        public PartitionData(long offset, long timestamp, String metadata) {
            this.offset = offset;
            this.timestamp = timestamp;
            this.metadata = metadata;
        }

        public PartitionData(long offset, String metadata) {
            this(offset, DEFAULT_TIMESTAMP, metadata);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(timestamp=").append(timestamp).
                append(", offset=").append(offset).
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
        private long retentionTime = DEFAULT_RETENTION_TIME;

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

        public Builder setRetentionTime(long retentionTime) {
            this.retentionTime = retentionTime;
            return this;
        }

        @Override
        public OffsetCommitRequest build(short version) {
            switch (version) {
                case 0:
                    return new OffsetCommitRequest(groupId, DEFAULT_GENERATION_ID, DEFAULT_MEMBER_ID,
                            DEFAULT_RETENTION_TIME, offsetData, version);
                case 1:
                case 2:
                case 3:
                    long retentionTime = version == 1 ? DEFAULT_RETENTION_TIME : this.retentionTime;
                    return new OffsetCommitRequest(groupId, generationId, memberId, retentionTime, offsetData, version);
                default:
                    throw new UnsupportedVersionException("Unsupported version " + version);
            }
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=OffsetCommitRequest").
                append(", groupId=").append(groupId).
                append(", memberId=").append(memberId).
                append(", generationId=").append(generationId).
                append(", retentionTime=").append(retentionTime).
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

        groupId = struct.getString(GROUP_ID_KEY_NAME);
        // This field only exists in v1.
        if (struct.hasField(GENERATION_ID_KEY_NAME))
            generationId = struct.getInt(GENERATION_ID_KEY_NAME);
        else
            generationId = DEFAULT_GENERATION_ID;

        // This field only exists in v1.
        if (struct.hasField(MEMBER_ID_KEY_NAME))
            memberId = struct.getString(MEMBER_ID_KEY_NAME);
        else
            memberId = DEFAULT_MEMBER_ID;

        // This field only exists in v2
        if (struct.hasField(RETENTION_TIME_KEY_NAME))
            retentionTime = struct.getLong(RETENTION_TIME_KEY_NAME);
        else
            retentionTime = DEFAULT_RETENTION_TIME;

        offsetData = new HashMap<>();
        for (Object topicDataObj : struct.getArray(TOPICS_KEY_NAME)) {
            Struct topicData = (Struct) topicDataObj;
            String topic = topicData.get(TOPIC_NAME);
            for (Object partitionDataObj : topicData.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionDataStruct = (Struct) partitionDataObj;
                int partition = partitionDataStruct.get(PARTITION_ID);
                long offset = partitionDataStruct.getLong(COMMIT_OFFSET_KEY_NAME);
                String metadata = partitionDataStruct.getString(METADATA_KEY_NAME);
                PartitionData partitionOffset;
                // This field only exists in v1
                if (partitionDataStruct.hasField(TIMESTAMP_KEY_NAME)) {
                    long timestamp = partitionDataStruct.getLong(TIMESTAMP_KEY_NAME);
                    partitionOffset = new PartitionData(offset, timestamp, metadata);
                } else {
                    partitionOffset = new PartitionData(offset, metadata);
                }
                offsetData.put(new TopicPartition(topic, partition), partitionOffset);
            }
        }
    }

    @Override
    public Struct toStruct() {
        short version = version();
        Struct struct = new Struct(ApiKeys.OFFSET_COMMIT.requestSchema(version));
        struct.set(GROUP_ID_KEY_NAME, groupId);

        Map<String, Map<Integer, PartitionData>> topicsData = CollectionUtils.groupDataByTopic(offsetData);
        List<Struct> topicArray = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, PartitionData>> topicEntry: topicsData.entrySet()) {
            Struct topicData = struct.instance(TOPICS_KEY_NAME);
            topicData.set(TOPIC_NAME, topicEntry.getKey());
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, PartitionData> partitionEntry : topicEntry.getValue().entrySet()) {
                PartitionData fetchPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                partitionData.set(PARTITION_ID, partitionEntry.getKey());
                partitionData.set(COMMIT_OFFSET_KEY_NAME, fetchPartitionData.offset);
                // Only for v1
                if (partitionData.hasField(TIMESTAMP_KEY_NAME))
                    partitionData.set(TIMESTAMP_KEY_NAME, fetchPartitionData.timestamp);
                partitionData.set(METADATA_KEY_NAME, fetchPartitionData.metadata);
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(TOPICS_KEY_NAME, topicArray.toArray());
        if (struct.hasField(GENERATION_ID_KEY_NAME))
            struct.set(GENERATION_ID_KEY_NAME, generationId);
        if (struct.hasField(MEMBER_ID_KEY_NAME))
            struct.set(MEMBER_ID_KEY_NAME, memberId);
        if (struct.hasField(RETENTION_TIME_KEY_NAME))
            struct.set(RETENTION_TIME_KEY_NAME, retentionTime);
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
