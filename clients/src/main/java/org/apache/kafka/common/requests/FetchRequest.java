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
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

public class FetchRequest extends AbstractRequest {

    public static final int CONSUMER_REPLICA_ID = -1;
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.FETCH.id);
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
                    topics.add(new TopicAndPartitionData(topic));
                topics.get(topics.size() - 1).partitions.put(partition, partitionData);
            }
            return topics;
        }
    }

    /**
     * Create a non-replica fetch request for versions 0, 1 or 2 (the version is set via the RequestHeader).
     */
    @Deprecated
    public FetchRequest(int maxWait, int minBytes, Map<TopicPartition, PartitionData> fetchData) {
        // Any of 0, 1 or 2 would do here since the schemas for these versions are identical
        this(2, CONSUMER_REPLICA_ID, maxWait, minBytes, DEFAULT_RESPONSE_MAX_BYTES, new LinkedHashMap<>(fetchData));
    }

    /**
     * Create a non-replica fetch request for the current version.
     */
    public FetchRequest(int maxWait, int minBytes, int maxBytes, LinkedHashMap<TopicPartition, PartitionData> fetchData) {
        this(ProtoUtils.latestVersion(ApiKeys.FETCH.id), CONSUMER_REPLICA_ID, maxWait, minBytes, maxBytes, fetchData);
    }

    /**
     * Create a replica fetch request for versions 0, 1 or 2 (the actual version is determined by the RequestHeader).
     */
    @Deprecated
    public static FetchRequest fromReplica(int replicaId, int maxWait, int minBytes,
                                           Map<TopicPartition, PartitionData> fetchData) {
        // Any of 0, 1 or 2 would do here since the schemas for these versions are identical
        return new FetchRequest(2, replicaId, maxWait, minBytes, DEFAULT_RESPONSE_MAX_BYTES, new LinkedHashMap<>(fetchData));
    }

    /**
     * Create a replica fetch request for the current version.
     */
    public static FetchRequest fromReplica(int replicaId, int maxWait, int minBytes, int maxBytes,
                                           LinkedHashMap<TopicPartition, PartitionData> fetchData) {
        return new FetchRequest(ProtoUtils.latestVersion(ApiKeys.FETCH.id), replicaId, maxWait, minBytes, maxBytes, fetchData);
    }

    private FetchRequest(int version, int replicaId, int maxWait, int minBytes, int maxBytes,
                         LinkedHashMap<TopicPartition, PartitionData> fetchData) {
        super(new Struct(ProtoUtils.requestSchema(ApiKeys.FETCH.id, version)));
        List<TopicAndPartitionData<PartitionData>> topicsData = TopicAndPartitionData.batchByTopic(fetchData);

        struct.set(REPLICA_ID_KEY_NAME, replicaId);
        struct.set(MAX_WAIT_KEY_NAME, maxWait);
        struct.set(MIN_BYTES_KEY_NAME, minBytes);
        if (version >= 3)
            struct.set(MAX_BYTES_KEY_NAME, maxBytes);
        List<Struct> topicArray = new ArrayList<Struct>();
        for (TopicAndPartitionData<PartitionData> topicEntry : topicsData) {
            Struct topicData = struct.instance(TOPICS_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, topicEntry.topic);
            List<Struct> partitionArray = new ArrayList<Struct>();
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

    public FetchRequest(Struct struct) {
        super(struct);
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
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        Map<TopicPartition, FetchResponse.PartitionData> responseData = new LinkedHashMap<>();

        for (Map.Entry<TopicPartition, PartitionData> entry: fetchData.entrySet()) {
            FetchResponse.PartitionData partitionResponse = new FetchResponse.PartitionData(Errors.forException(e).code(),
                    FetchResponse.INVALID_HIGHWATERMARK,
                    FetchResponse.EMPTY_RECORD_SET);
            responseData.put(entry.getKey(), partitionResponse);
        }

        switch (versionId) {
            case 0:
                return new FetchResponse(responseData);
            case 1:
            case 2:
            case 3:
                return new FetchResponse(responseData, 0);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.FETCH.id)));
        }
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

    public static FetchRequest parse(ByteBuffer buffer, int versionId) {
        return new FetchRequest(ProtoUtils.parseRequest(ApiKeys.FETCH.id, versionId, buffer));
    }

    public static FetchRequest parse(ByteBuffer buffer) {
        return new FetchRequest(CURRENT_SCHEMA.read(buffer));
    }
}
