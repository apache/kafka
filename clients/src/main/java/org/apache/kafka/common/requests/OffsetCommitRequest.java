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
public class OffsetCommitRequest extends AbstractRequestResponse {
    public static Schema curSchema = ProtoUtils.currentRequestSchema(ApiKeys.OFFSET_COMMIT.id);
    private static String GROUP_ID_KEY_NAME = "group_id";
    private static String GENERATION_ID_KEY_NAME = "group_generation_id";
    private static String CONSUMER_ID_KEY_NAME = "consumer_id";
    private static String TOPICS_KEY_NAME = "topics";

    // topic level field names
    private static String TOPIC_KEY_NAME = "topic";
    private static String PARTITIONS_KEY_NAME = "partitions";

    // partition level field names
    private static String PARTITION_KEY_NAME = "partition";
    private static String COMMIT_OFFSET_KEY_NAME = "offset";
    private static String TIMESTAMP_KEY_NAME = "timestamp";
    private static String METADATA_KEY_NAME = "metadata";

    public static final int DEFAULT_GENERATION_ID = -1;
    public static final String DEFAULT_CONSUMER_ID = "";
    public static final long DEFAULT_TIMESTAMP = -1L;

    private final String groupId;
    private final int generationId;
    private final String consumerId;
    private final Map<TopicPartition, PartitionData> offsetData;

    public static final class PartitionData {
        public final long offset;
        public final long timestamp;
        public final String metadata;

        // for v0
        public PartitionData(long offset, String metadata) {
            this(offset, DEFAULT_TIMESTAMP, metadata);
        }

        public PartitionData(long offset, long timestamp, String metadata) {
            this.offset = offset;
            this.timestamp = timestamp;
            this.metadata = metadata;
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
        initCommonFields(groupId, offsetData, 0);
        this.groupId = groupId;
        this.generationId = DEFAULT_GENERATION_ID;
        this.consumerId = DEFAULT_CONSUMER_ID;
        this.offsetData = offsetData;
    }

    /**
     * Constructor for version 1.
     * @param groupId
     * @param generationId
     * @param consumerId
     * @param offsetData
     */
    public OffsetCommitRequest(String groupId, int generationId, String consumerId, Map<TopicPartition, PartitionData> offsetData) {
        super(new Struct(curSchema));

        initCommonFields(groupId, offsetData, 1);
        struct.set(GENERATION_ID_KEY_NAME, generationId);
        struct.set(CONSUMER_ID_KEY_NAME, consumerId);
        this.groupId = groupId;
        this.generationId = generationId;
        this.consumerId = consumerId;
        this.offsetData = offsetData;
    }

    private void initCommonFields(String groupId, Map<TopicPartition, PartitionData> offsetData, int versionId) {
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
                if (versionId == 1)
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
        offsetData = new HashMap<TopicPartition, PartitionData>();
        for (Object topicResponseObj : struct.getArray(TOPICS_KEY_NAME)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.getString(TOPIC_KEY_NAME);
            for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.getInt(PARTITION_KEY_NAME);
                long offset = partitionResponse.getLong(COMMIT_OFFSET_KEY_NAME);
                long timestamp;
                // timestamp only exists in v1
                if (partitionResponse.hasField(TIMESTAMP_KEY_NAME))
                    timestamp = partitionResponse.getLong(TIMESTAMP_KEY_NAME);
                else
                    timestamp = DEFAULT_TIMESTAMP;
                String metadata = partitionResponse.getString(METADATA_KEY_NAME);
                PartitionData partitionData = new PartitionData(offset, timestamp, metadata);
                offsetData.put(new TopicPartition(topic, partition), partitionData);
            }
        }
        groupId = struct.getString(GROUP_ID_KEY_NAME);
        // This field only exists in v1.
        if (struct.hasField(GENERATION_ID_KEY_NAME))
            generationId = struct.getInt(GENERATION_ID_KEY_NAME);
        else
            generationId = DEFAULT_GENERATION_ID;

        // This field only exists in v1.
        if (struct.hasField(CONSUMER_ID_KEY_NAME))
            consumerId = struct.getString(CONSUMER_ID_KEY_NAME);
        else
            consumerId = DEFAULT_CONSUMER_ID;
    }

    public String groupId() {
        return groupId;
    }

    public int generationId() {
        return generationId;
    }

    public String consumerId() {
        return consumerId;
    }

    public Map<TopicPartition, PartitionData> offsetData() {
        return offsetData;
    }

    public static OffsetCommitRequest parse(ByteBuffer buffer, int versionId) {
        Schema schema = ProtoUtils.requestSchema(ApiKeys.OFFSET_COMMIT.id, versionId);
        return new OffsetCommitRequest(((Struct) schema.read(buffer)));
    }

    public static OffsetCommitRequest parse(ByteBuffer buffer) {
        return new OffsetCommitRequest(((Struct) curSchema.read(buffer)));
    }
}
