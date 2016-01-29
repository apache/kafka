/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This wrapper supports both v0 and v1 of OffsetCommitRequest.
 */
public class OffsetCommitRequest extends AbstractRequest {
    
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.OFFSET_COMMIT.id);
    private static final String GROUP_ID_KEY_NAME = "group_id";
    private static final String GENERATION_ID_KEY_NAME = "group_generation_id";
    private static final String MEMBER_ID_KEY_NAME = "member_id";
    private static final String TOPICS_KEY_NAME = "topics";
    private static final String RETENTION_TIME_KEY_NAME = "retention_time";

    // topic level field names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    // partition level field names
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String COMMIT_OFFSET_KEY_NAME = "offset";
    private static final String METADATA_KEY_NAME = "metadata";

    @Deprecated
    private static final String TIMESTAMP_KEY_NAME = "timestamp";         // for v0, v1

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
    }

    /**
     * Constructor for version 0.
     * @param groupId
     * @param offsetData
     */
    @Deprecated
    public OffsetCommitRequest(String groupId, Map<TopicPartition, PartitionData> offsetData) {
        super(new Struct(ProtoUtils.requestSchema(ApiKeys.OFFSET_COMMIT.id, 0)));

        initCommonFields(groupId, offsetData);
        this.groupId = groupId;
        this.generationId = DEFAULT_GENERATION_ID;
        this.memberId = DEFAULT_MEMBER_ID;
        this.retentionTime = DEFAULT_RETENTION_TIME;
        this.offsetData = offsetData;
    }

    /**
     * Constructor for version 1.
     * @param groupId
     * @param generationId
     * @param memberId
     * @param offsetData
     */
    @Deprecated
    public OffsetCommitRequest(String groupId, int generationId, String memberId, Map<TopicPartition, PartitionData> offsetData) {
        super(new Struct(ProtoUtils.requestSchema(ApiKeys.OFFSET_COMMIT.id, 1)));

        initCommonFields(groupId, offsetData);
        struct.set(GENERATION_ID_KEY_NAME, generationId);
        struct.set(MEMBER_ID_KEY_NAME, memberId);
        this.groupId = groupId;
        this.generationId = generationId;
        this.memberId = memberId;
        this.retentionTime = DEFAULT_RETENTION_TIME;
        this.offsetData = offsetData;
    }

    /**
     * Constructor for version 2.
     * @param groupId
     * @param generationId
     * @param memberId
     * @param retentionTime
     * @param offsetData
     */
    public OffsetCommitRequest(String groupId, int generationId, String memberId, long retentionTime, Map<TopicPartition, PartitionData> offsetData) {
        super(new Struct(CURRENT_SCHEMA));

        initCommonFields(groupId, offsetData);
        struct.set(GENERATION_ID_KEY_NAME, generationId);
        struct.set(MEMBER_ID_KEY_NAME, memberId);
        struct.set(RETENTION_TIME_KEY_NAME, retentionTime);
        this.groupId = groupId;
        this.generationId = generationId;
        this.memberId = memberId;
        this.retentionTime = retentionTime;
        this.offsetData = offsetData;
    }

    private void initCommonFields(String groupId, Map<TopicPartition, PartitionData> offsetData) {
        Map<String, Map<Integer, PartitionData>> topicsData = CollectionUtils.groupDataByTopic(offsetData);

        struct.set(GROUP_ID_KEY_NAME, groupId);
        List<Struct> topicArray = new ArrayList<Struct>();

        for (Map.Entry<String, Map<Integer, PartitionData>> topicEntry: topicsData.entrySet()) {
            Struct topicData = struct.instance(TOPICS_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, topicEntry.getKey());
            List<Struct> partitionArray = new ArrayList<Struct>();
            for (Map.Entry<Integer, PartitionData> partitionEntry : topicEntry.getValue().entrySet()) {
                PartitionData fetchPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                partitionData.set(PARTITION_KEY_NAME, partitionEntry.getKey());
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
    }

    public OffsetCommitRequest(Struct struct) {
        super(struct);

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

        offsetData = new HashMap<TopicPartition, PartitionData>();
        for (Object topicDataObj : struct.getArray(TOPICS_KEY_NAME)) {
            Struct topicData = (Struct) topicDataObj;
            String topic = topicData.getString(TOPIC_KEY_NAME);
            for (Object partitionDataObj : topicData.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionDataStruct = (Struct) partitionDataObj;
                int partition = partitionDataStruct.getInt(PARTITION_KEY_NAME);
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
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        Map<TopicPartition, Short> responseData = new HashMap<TopicPartition, Short>();
        for (Map.Entry<TopicPartition, PartitionData> entry: offsetData.entrySet()) {
            responseData.put(entry.getKey(), Errors.forException(e).code());
        }

        switch (versionId) {
            // OffsetCommitResponseV0 == OffsetCommitResponseV1 == OffsetCommitResponseV2
            case 0:
            case 1:
            case 2:
                return new OffsetCommitResponse(responseData);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.OFFSET_COMMIT.id)));
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

    public static OffsetCommitRequest parse(ByteBuffer buffer, int versionId) {
        Schema schema = ProtoUtils.requestSchema(ApiKeys.OFFSET_COMMIT.id, versionId);
        return new OffsetCommitRequest(schema.read(buffer));
    }

    public static OffsetCommitRequest parse(ByteBuffer buffer) {
        return new OffsetCommitRequest(CURRENT_SCHEMA.read(buffer));
    }
}
