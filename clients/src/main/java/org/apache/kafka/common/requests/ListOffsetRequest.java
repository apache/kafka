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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ListOffsetRequest extends AbstractRequest {
    public static final long EARLIEST_TIMESTAMP = -2L;
    public static final long LATEST_TIMESTAMP = -1L;

    public static final int CONSUMER_REPLICA_ID = -1;
    public static final int DEBUGGING_REPLICA_ID = -2;

    private static final String REPLICA_ID_KEY_NAME = "replica_id";
    private static final String TOPICS_KEY_NAME = "topics";

    // topic level field names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    // partition level field names
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String TIMESTAMP_KEY_NAME = "timestamp";
    private static final String MAX_NUM_OFFSETS_KEY_NAME = "max_num_offsets";

    private final int replicaId;
    private final Map<TopicPartition, PartitionData> offsetData;
    private final Map<TopicPartition, Long> partitionTimestamps;
    private final Set<TopicPartition> duplicatePartitions;

    public static class Builder extends AbstractRequest.Builder<ListOffsetRequest> {
        private final int replicaId;
        private final short minVersion;
        private Map<TopicPartition, PartitionData> offsetData = null;
        private Map<TopicPartition, Long> partitionTimestamps = null;

        public static Builder forReplica(short desiredVersion, int replicaId) {
            return new Builder((short) 0, desiredVersion, replicaId);
        }

        public static Builder forConsumer(boolean requireTimestamp) {
            // If we need a timestamp in the response, the minimum RPC version we can send is v1. Otherwise, v0 is OK.
            short minVersion = requireTimestamp ? (short) 1 : (short) 0;
            return new Builder(minVersion, null, CONSUMER_REPLICA_ID);
        }

        private Builder(short minVersion, Short desiredVersion, int replicaId) {
            super(ApiKeys.LIST_OFFSETS, desiredVersion);
            this.minVersion = minVersion;
            this.replicaId = replicaId;
        }

        public Builder setOffsetData(Map<TopicPartition, PartitionData> offsetData) {
            this.offsetData = offsetData;
            return this;
        }

        public Builder setTargetTimes(Map<TopicPartition, Long> partitionTimestamps) {
            this.partitionTimestamps = partitionTimestamps;
            return this;
        }

        @Override
        public ListOffsetRequest build(short version) {
            if (version < minVersion) {
                throw new UnsupportedVersionException("Cannot create a v" + version + " ListOffsetRequest because " +
                    "we require features supported only in " + minVersion + " or later.");
            }
            if (version == 0) {
                if (offsetData == null) {
                    if (partitionTimestamps == null) {
                        throw new RuntimeException("Must set partitionTimestamps or offsetData when creating a v0 " +
                            "ListOffsetRequest");
                    } else {
                        offsetData = new HashMap<>();
                        for (Map.Entry<TopicPartition, Long> entry: partitionTimestamps.entrySet()) {
                            offsetData.put(entry.getKey(),
                                    new PartitionData(entry.getValue(), 1));
                        }
                        this.partitionTimestamps = null;
                    }
                }
            } else {
                if (offsetData != null) {
                    throw new RuntimeException("Cannot create a v" + version + " ListOffsetRequest with v0 " +
                        "PartitionData.");
                } else if (partitionTimestamps == null) {
                    throw new RuntimeException("Must set partitionTimestamps when creating a v" +
                            version + " ListOffsetRequest");
                }
            }
            Map<TopicPartition, ?> m = (version == 0) ?  offsetData : partitionTimestamps;
            return new ListOffsetRequest(replicaId, m, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=ListOffsetRequest")
               .append(", replicaId=").append(replicaId);
            if (offsetData != null) {
                bld.append(", offsetData=").append(offsetData);
            }
            if (partitionTimestamps != null) {
                bld.append(", partitionTimestamps=").append(partitionTimestamps);
            }
            bld.append(", minVersion=").append(minVersion);
            bld.append(")");
            return bld.toString();
        }
    }

    /**
     * This class is only used by ListOffsetRequest v0 which has been deprecated.
     */
    @Deprecated
    public static final class PartitionData {
        public final long timestamp;
        public final int maxNumOffsets;

        public PartitionData(long timestamp, int maxNumOffsets) {
            this.timestamp = timestamp;
            this.maxNumOffsets = maxNumOffsets;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("{timestamp: ").append(timestamp).
                append(", maxNumOffsets: ").append(maxNumOffsets).
                append("}");
            return bld.toString();
        }
    }

    /**
     * Private constructor with a specified version.
     */
    @SuppressWarnings("unchecked")
    private ListOffsetRequest(int replicaId, Map<TopicPartition, ?> targetTimes, short version) {
        super(version);
        this.replicaId = replicaId;
        this.offsetData = version == 0 ? (Map<TopicPartition, PartitionData>) targetTimes : null;
        this.partitionTimestamps = version >= 1 ? (Map<TopicPartition, Long>) targetTimes : null;
        this.duplicatePartitions = Collections.emptySet();
    }

    public ListOffsetRequest(Struct struct, short version) {
        super(version);
        Set<TopicPartition> duplicatePartitions = new HashSet<>();
        replicaId = struct.getInt(REPLICA_ID_KEY_NAME);
        offsetData = new HashMap<>();
        partitionTimestamps = new HashMap<>();
        for (Object topicResponseObj : struct.getArray(TOPICS_KEY_NAME)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.getString(TOPIC_KEY_NAME);
            for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.getInt(PARTITION_KEY_NAME);
                long timestamp = partitionResponse.getLong(TIMESTAMP_KEY_NAME);
                TopicPartition tp = new TopicPartition(topic, partition);
                if (partitionResponse.hasField(MAX_NUM_OFFSETS_KEY_NAME)) {
                    int maxNumOffsets = partitionResponse.getInt(MAX_NUM_OFFSETS_KEY_NAME);
                    PartitionData partitionData = new PartitionData(timestamp, maxNumOffsets);
                    offsetData.put(tp, partitionData);
                } else {
                    if (partitionTimestamps.put(tp, timestamp) != null)
                        duplicatePartitions.add(tp);
                }
            }
        }
        this.duplicatePartitions = duplicatePartitions;
    }

    @Override
    @SuppressWarnings("deprecation")
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Map<TopicPartition, ListOffsetResponse.PartitionData> responseData = new HashMap<>();

        short versionId = version();
        if (versionId == 0) {
            for (Map.Entry<TopicPartition, PartitionData> entry : offsetData.entrySet()) {
                ListOffsetResponse.PartitionData partitionResponse = new ListOffsetResponse.PartitionData(
                        Errors.forException(e), Collections.<Long>emptyList());
                responseData.put(entry.getKey(), partitionResponse);
            }
        } else {
            for (Map.Entry<TopicPartition, Long> entry : partitionTimestamps.entrySet()) {
                ListOffsetResponse.PartitionData partitionResponse = new ListOffsetResponse.PartitionData(
                        Errors.forException(e), -1L, -1L);
                responseData.put(entry.getKey(), partitionResponse);
            }
        }

        switch (versionId) {
            case 0:
            case 1:
                return new ListOffsetResponse(responseData);
            case 2:
                return new ListOffsetResponse(throttleTimeMs, responseData);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.LIST_OFFSETS.latestVersion()));
        }
    }

    public int replicaId() {
        return replicaId;
    }

    @Deprecated
    public Map<TopicPartition, PartitionData> offsetData() {
        return offsetData;
    }

    public Map<TopicPartition, Long> partitionTimestamps() {
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

        Map<TopicPartition, ?> targetTimes = partitionTimestamps == null ? offsetData : partitionTimestamps;
        Map<String, Map<Integer, Object>> topicsData = CollectionUtils.groupDataByTopic(targetTimes);

        struct.set(REPLICA_ID_KEY_NAME, replicaId);
        List<Struct> topicArray = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, Object>> topicEntry: topicsData.entrySet()) {
            Struct topicData = struct.instance(TOPICS_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, topicEntry.getKey());
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, Object> partitionEntry : topicEntry.getValue().entrySet()) {
                if (version == 0) {
                    PartitionData offsetPartitionData = (PartitionData) partitionEntry.getValue();
                    Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                    partitionData.set(PARTITION_KEY_NAME, partitionEntry.getKey());
                    partitionData.set(TIMESTAMP_KEY_NAME, offsetPartitionData.timestamp);
                    partitionData.set(MAX_NUM_OFFSETS_KEY_NAME, offsetPartitionData.maxNumOffsets);
                    partitionArray.add(partitionData);
                } else {
                    Long timestamp = (Long) partitionEntry.getValue();
                    Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                    partitionData.set(PARTITION_KEY_NAME, partitionEntry.getKey());
                    partitionData.set(TIMESTAMP_KEY_NAME, timestamp);
                    partitionArray.add(partitionData);
                }
            }
            topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(TOPICS_KEY_NAME, topicArray.toArray());
        return struct;
    }
}
