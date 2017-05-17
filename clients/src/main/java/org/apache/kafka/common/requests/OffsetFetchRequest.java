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
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OffsetFetchRequest extends AbstractRequest {
    private static final String GROUP_ID_KEY_NAME = "group_id";
    private static final String TOPICS_KEY_NAME = "topics";

    // topic level field names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    // partition level field names
    private static final String PARTITION_KEY_NAME = "partition";

    public static class Builder extends AbstractRequest.Builder<OffsetFetchRequest> {
        private static final List<TopicPartition> ALL_TOPIC_PARTITIONS = null;
        private final String groupId;
        private final List<TopicPartition> partitions;

        public Builder(String groupId, List<TopicPartition> partitions) {
            super(ApiKeys.OFFSET_FETCH);
            this.groupId = groupId;
            this.partitions = partitions;
        }

        public static Builder allTopicPartitions(String groupId) {
            return new Builder(groupId, ALL_TOPIC_PARTITIONS);
        }

        public boolean isAllTopicPartitions() {
            return this.partitions == ALL_TOPIC_PARTITIONS;
        }

        @Override
        public OffsetFetchRequest build(short version) {
            if (isAllTopicPartitions() && version < 2)
                throw new UnsupportedVersionException("The broker only supports OffsetFetchRequest " +
                        "v" + version + ", but we need v2 or newer to request all topic partitions.");
            return new OffsetFetchRequest(groupId, partitions, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=OffsetFetchRequest, ").
                    append("groupId=").append(groupId).
                    append(", partitions=").append(Utils.join(partitions, ",")).
                    append(")");
            return bld.toString();
        }
    }

    private final String groupId;
    private final List<TopicPartition> partitions;

    public static OffsetFetchRequest forAllPartitions(String groupId) {
        return new OffsetFetchRequest.Builder(groupId, null).build((short) 2);
    }

    // v0, v1, and v2 have the same fields.
    private OffsetFetchRequest(String groupId, List<TopicPartition> partitions, short version) {
        super(version);
        this.groupId = groupId;
        this.partitions = partitions;
    }

    public OffsetFetchRequest(Struct struct, short version) {
        super(version);

        Object[] topicArray = struct.getArray(TOPICS_KEY_NAME);
        if (topicArray != null) {
            partitions = new ArrayList<>();
            for (Object topicResponseObj : struct.getArray(TOPICS_KEY_NAME)) {
                Struct topicResponse = (Struct) topicResponseObj;
                String topic = topicResponse.getString(TOPIC_KEY_NAME);
                for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
                    Struct partitionResponse = (Struct) partitionResponseObj;
                    int partition = partitionResponse.getInt(PARTITION_KEY_NAME);
                    partitions.add(new TopicPartition(topic, partition));
                }
            }
        } else
            partitions = null;


        groupId = struct.getString(GROUP_ID_KEY_NAME);
    }

    public OffsetFetchResponse getErrorResponse(Errors error) {
        return getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, error);
    }

    public OffsetFetchResponse getErrorResponse(int throttleTimeMs, Errors error) {
        short versionId = version();

        Map<TopicPartition, OffsetFetchResponse.PartitionData> responsePartitions = new HashMap<>();
        if (versionId < 2) {
            for (TopicPartition partition : this.partitions) {
                responsePartitions.put(partition, new OffsetFetchResponse.PartitionData(
                        OffsetFetchResponse.INVALID_OFFSET,
                        OffsetFetchResponse.NO_METADATA,
                        error));
            }
        }

        switch (versionId) {
            case 0:
            case 1:
            case 2:
                return new OffsetFetchResponse(error, responsePartitions);
            case 3:
                return new OffsetFetchResponse(throttleTimeMs, error, responsePartitions);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.OFFSET_FETCH.latestVersion()));
        }
    }

    @Override
    public OffsetFetchResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return getErrorResponse(throttleTimeMs, Errors.forException(e));
    }

    public String groupId() {
        return groupId;
    }

    public List<TopicPartition> partitions() {
        return partitions;
    }

    public static OffsetFetchRequest parse(ByteBuffer buffer, short version) {
        return new OffsetFetchRequest(ApiKeys.OFFSET_FETCH.parseRequest(version, buffer), version);
    }

    public boolean isAllPartitions() {
        return partitions == null;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.OFFSET_FETCH.requestSchema(version()));
        struct.set(GROUP_ID_KEY_NAME, groupId);
        if (partitions != null) {
            Map<String, List<Integer>> topicsData = CollectionUtils.groupDataByTopic(partitions);

            List<Struct> topicArray = new ArrayList<>();
            for (Map.Entry<String, List<Integer>> entries : topicsData.entrySet()) {
                Struct topicData = struct.instance(TOPICS_KEY_NAME);
                topicData.set(TOPIC_KEY_NAME, entries.getKey());
                List<Struct> partitionArray = new ArrayList<>();
                for (Integer partitionId : entries.getValue()) {
                    Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                    partitionData.set(PARTITION_KEY_NAME, partitionId);
                    partitionArray.add(partitionData);
                }
                topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
                topicArray.add(topicData);
            }
            struct.set(TOPICS_KEY_NAME, topicArray.toArray());
        } else
            struct.set(TOPICS_KEY_NAME, null);

        return struct;
    }
}
