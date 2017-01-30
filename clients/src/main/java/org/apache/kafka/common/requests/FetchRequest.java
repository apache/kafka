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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.Utils;

public class FetchRequest extends AbstractRequest {
    public static final int CONSUMER_REPLICA_ID = -1;
    private static final String REPLICA_ID_KEY_NAME = "replica_id";
    private static final String MAX_WAIT_KEY_NAME = "max_wait_time";
    private static final String MIN_BYTES_KEY_NAME = "min_bytes";
    private static final String TOPICS_KEY_NAME = "topics";

    // request and partition level name
    private static final String MAX_BYTES_KEY_NAME = "max_bytes";

    // topic level field names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    // partition level field names
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String FETCH_OFFSET_KEY_NAME = "fetch_offset";

    // default values for older versions where a request level limit did not exist
    public static final int DEFAULT_RESPONSE_MAX_BYTES = Integer.MAX_VALUE;

    private final int replicaId;
    private final int maxWait;
    private final int minBytes;
    private final int maxBytes;
    private final LinkedHashMap<TopicPartition, PartitionData> fetchData;

    public static final class PartitionData {
        public final long offset;
        public final int maxBytes;

        public PartitionData(long offset, int maxBytes) {
            this.offset = offset;
            this.maxBytes = maxBytes;
        }

        @Override
        public String toString() {
            return "(offset=" + offset + ", maxBytes=" + maxBytes + ")";
        }
    }

    static final class TopicAndPartitionData<T> {
        public final String topic;
        public final LinkedHashMap<Integer, T> partitions;

        public TopicAndPartitionData(String topic) {
            this.topic = topic;
            this.partitions = new LinkedHashMap<>();
        }

        public static <T> List<TopicAndPartitionData<T>> batchByTopic(LinkedHashMap<TopicPartition, T> data) {
            List<TopicAndPartitionData<T>> topics = new ArrayList<>();
            for (Map.Entry<TopicPartition, T> topicEntry : data.entrySet()) {
                String topic = topicEntry.getKey().topic();
                int partition = topicEntry.getKey().partition();
                T partitionData = topicEntry.getValue();
                if (topics.isEmpty() || !topics.get(topics.size() - 1).topic.equals(topic))
                    topics.add(new TopicAndPartitionData<T>(topic));
                topics.get(topics.size() - 1).partitions.put(partition, partitionData);
            }
            return topics;
        }
    }

    public static class Builder extends AbstractRequest.Builder<FetchRequest> {
        private int replicaId = CONSUMER_REPLICA_ID;
        private int maxWait;
        private final int minBytes;
        private int maxBytes = DEFAULT_RESPONSE_MAX_BYTES;
        private LinkedHashMap<TopicPartition, PartitionData> fetchData;

        public Builder(int maxWait, int minBytes, LinkedHashMap<TopicPartition, PartitionData> fetchData) {
            super(ApiKeys.FETCH);
            this.maxWait = maxWait;
            this.minBytes = minBytes;
            this.fetchData = fetchData;
        }

        public Builder setReplicaId(int replicaId) {
            this.replicaId = replicaId;
            return this;
        }

        public Builder setMaxWait(int maxWait) {
            this.maxWait = maxWait;
            return this;
        }

        public Builder setMaxBytes(int maxBytes) {
            this.maxBytes = maxBytes;
            return this;
        }

        public LinkedHashMap<TopicPartition, PartitionData> fetchData() {
            return this.fetchData;
        }

        @Override
        public FetchRequest build() {
            short version = version();
            if (version < 3) {
                maxBytes = -1;
            }

            return new FetchRequest(version, replicaId, maxWait, minBytes,
                    maxBytes, fetchData);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type:FetchRequest").
                    append(", replicaId=").append(replicaId).
                    append(", maxWait=").append(maxWait).
                    append(", minBytes=").append(minBytes).
                    append(", maxBytes=").append(maxBytes).
                    append(", fetchData=").append(Utils.mkString(fetchData)).
                    append(")");
            return bld.toString();
        }
    }

    private FetchRequest(short version, int replicaId, int maxWait, int minBytes, int maxBytes,
                         LinkedHashMap<TopicPartition, PartitionData> fetchData) {
        super(new Struct(ProtoUtils.requestSchema(ApiKeys.FETCH.id, version)), version);
        List<TopicAndPartitionData<PartitionData>> topicsData = TopicAndPartitionData.batchByTopic(fetchData);

        struct.set(REPLICA_ID_KEY_NAME, replicaId);
        struct.set(MAX_WAIT_KEY_NAME, maxWait);
        struct.set(MIN_BYTES_KEY_NAME, minBytes);
        if (version >= 3)
            struct.set(MAX_BYTES_KEY_NAME, maxBytes);
        List<Struct> topicArray = new ArrayList<>();
        for (TopicAndPartitionData<PartitionData> topicEntry : topicsData) {
            Struct topicData = struct.instance(TOPICS_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, topicEntry.topic);
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, PartitionData> partitionEntry : topicEntry.partitions.entrySet()) {
                PartitionData fetchPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                partitionData.set(PARTITION_KEY_NAME, partitionEntry.getKey());
                partitionData.set(FETCH_OFFSET_KEY_NAME, fetchPartitionData.offset);
                partitionData.set(MAX_BYTES_KEY_NAME, fetchPartitionData.maxBytes);
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(TOPICS_KEY_NAME, topicArray.toArray());
        this.replicaId = replicaId;
        this.maxWait = maxWait;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
        this.fetchData = fetchData;
    }

    public FetchRequest(Struct struct, short versionId) {
        super(struct, versionId);
        replicaId = struct.getInt(REPLICA_ID_KEY_NAME);
        maxWait = struct.getInt(MAX_WAIT_KEY_NAME);
        minBytes = struct.getInt(MIN_BYTES_KEY_NAME);
        if (struct.hasField(MAX_BYTES_KEY_NAME))
            maxBytes = struct.getInt(MAX_BYTES_KEY_NAME);
        else
            maxBytes = DEFAULT_RESPONSE_MAX_BYTES;
        fetchData = new LinkedHashMap<>();
        for (Object topicResponseObj : struct.getArray(TOPICS_KEY_NAME)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.getString(TOPIC_KEY_NAME);
            for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.getInt(PARTITION_KEY_NAME);
                long offset = partitionResponse.getLong(FETCH_OFFSET_KEY_NAME);
                int maxBytes = partitionResponse.getInt(MAX_BYTES_KEY_NAME);
                PartitionData partitionData = new PartitionData(offset, maxBytes);
                fetchData.put(new TopicPartition(topic, partition), partitionData);
            }
        }
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData> responseData = new LinkedHashMap<>();

        for (Map.Entry<TopicPartition, PartitionData> entry: fetchData.entrySet()) {
            FetchResponse.PartitionData partitionResponse = new FetchResponse.PartitionData(Errors.forException(e).code(),
                    FetchResponse.INVALID_HIGHWATERMARK, MemoryRecords.EMPTY);

            responseData.put(entry.getKey(), partitionResponse);
        }
        short versionId = version();
        return new FetchResponse(versionId, responseData, 0);
    }

    public int replicaId() {
        return replicaId;
    }

    public int maxWait() {
        return maxWait;
    }

    public int minBytes() {
        return minBytes;
    }

    public int maxBytes() {
        return maxBytes;
    }

    public Map<TopicPartition, PartitionData> fetchData() {
        return fetchData;
    }

    public boolean isFromFollower() {
        return replicaId >= 0;
    }

    public static FetchRequest parse(ByteBuffer buffer, int versionId) {
        return new FetchRequest(ProtoUtils.parseRequest(ApiKeys.FETCH.id, versionId, buffer), (short) versionId);
    }

    public static FetchRequest parse(ByteBuffer buffer) {
        return parse(buffer, ProtoUtils.latestVersion(ApiKeys.FETCH.id));
    }
}
