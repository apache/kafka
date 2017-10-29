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
import org.apache.kafka.common.record.MemoryRecords;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.INT64;
import static org.apache.kafka.common.protocol.types.Type.INT8;

public class FetchRequest extends AbstractRequest {
    public static final int CONSUMER_REPLICA_ID = -1;
    private static final String REPLICA_ID_KEY_NAME = "replica_id";
    private static final String MAX_WAIT_KEY_NAME = "max_wait_time";
    private static final String MIN_BYTES_KEY_NAME = "min_bytes";
    private static final String ISOLATION_LEVEL_KEY_NAME = "isolation_level";
    private static final String TOPICS_KEY_NAME = "topics";

    // request and partition level name
    private static final String MAX_BYTES_KEY_NAME = "max_bytes";

    // topic level field names
    private static final String PARTITIONS_KEY_NAME = "partitions";

    // partition level field names
    private static final String FETCH_OFFSET_KEY_NAME = "fetch_offset";
    private static final String LOG_START_OFFSET_KEY_NAME = "log_start_offset";

    private static final Schema FETCH_REQUEST_PARTITION_V0 = new Schema(
            PARTITION_ID,
            new Field(FETCH_OFFSET_KEY_NAME, INT64, "Message offset."),
            new Field(MAX_BYTES_KEY_NAME, INT32, "Maximum bytes to fetch."));

    // FETCH_REQUEST_PARTITION_V5 added log_start_offset field - the earliest available offset of partition data that can be consumed.
    private static final Schema FETCH_REQUEST_PARTITION_V5 = new Schema(
            PARTITION_ID,
            new Field(FETCH_OFFSET_KEY_NAME, INT64, "Message offset."),
            new Field(LOG_START_OFFSET_KEY_NAME, INT64, "Earliest available offset of the follower replica. " +
                            "The field is only used when request is sent by follower. "),
            new Field(MAX_BYTES_KEY_NAME, INT32, "Maximum bytes to fetch."));

    private static final Schema FETCH_REQUEST_TOPIC_V0 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(FETCH_REQUEST_PARTITION_V0), "Partitions to fetch."));

    private static final Schema FETCH_REQUEST_TOPIC_V5 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(FETCH_REQUEST_PARTITION_V5), "Partitions to fetch."));

    private static final Schema FETCH_REQUEST_V0 = new Schema(
            new Field(REPLICA_ID_KEY_NAME, INT32, "Broker id of the follower. For normal consumers, use -1."),
            new Field(MAX_WAIT_KEY_NAME, INT32, "Maximum time in ms to wait for the response."),
            new Field(MIN_BYTES_KEY_NAME, INT32, "Minimum bytes to accumulate in the response."),
            new Field(TOPICS_KEY_NAME, new ArrayOf(FETCH_REQUEST_TOPIC_V0), "Topics to fetch."));

    // The V1 Fetch Request body is the same as V0.
    // Only the version number is incremented to indicate a newer client
    private static final Schema FETCH_REQUEST_V1 = FETCH_REQUEST_V0;
    // The V2 Fetch Request body is the same as V1.
    // Only the version number is incremented to indicate the client support message format V1 which uses
    // relative offset and has timestamp.
    private static final Schema FETCH_REQUEST_V2 = FETCH_REQUEST_V1;
    // Fetch Request V3 added top level max_bytes field - the total size of partition data to accumulate in response.
    // The partition ordering is now relevant - partitions will be processed in order they appear in request.
    private static final Schema FETCH_REQUEST_V3 = new Schema(
            new Field(REPLICA_ID_KEY_NAME, INT32, "Broker id of the follower. For normal consumers, use -1."),
            new Field(MAX_WAIT_KEY_NAME, INT32, "Maximum time in ms to wait for the response."),
            new Field(MIN_BYTES_KEY_NAME, INT32, "Minimum bytes to accumulate in the response."),
            new Field(MAX_BYTES_KEY_NAME, INT32, "Maximum bytes to accumulate in the response. Note that this is not an absolute maximum, " +
                    "if the first message in the first non-empty partition of the fetch is larger than this " +
                    "value, the message will still be returned to ensure that progress can be made."),
            new Field(TOPICS_KEY_NAME, new ArrayOf(FETCH_REQUEST_TOPIC_V0), "Topics to fetch in the order provided."));

    // The V4 Fetch Request adds the fetch isolation level and exposes magic v2 (via the response).
    private static final Schema FETCH_REQUEST_V4 = new Schema(
            new Field(REPLICA_ID_KEY_NAME, INT32, "Broker id of the follower. For normal consumers, use -1."),
            new Field(MAX_WAIT_KEY_NAME, INT32, "Maximum time in ms to wait for the response."),
            new Field(MIN_BYTES_KEY_NAME, INT32, "Minimum bytes to accumulate in the response."),
            new Field(MAX_BYTES_KEY_NAME, INT32, "Maximum bytes to accumulate in the response. Note that this is not an absolute maximum, " +
                    "if the first message in the first non-empty partition of the fetch is larger than this " +
                    "value, the message will still be returned to ensure that progress can be made."),
            new Field(ISOLATION_LEVEL_KEY_NAME, INT8, "This setting controls the visibility of transactional records. Using READ_UNCOMMITTED " +
                    "(isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), " +
                    "non-transactional and COMMITTED transactional records are visible. To be more concrete, " +
                    "READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), " +
                    "and enables the inclusion of the list of aborted transactions in the result, which allows " +
                    "consumers to discard ABORTED transactional records"),
            new Field(TOPICS_KEY_NAME, new ArrayOf(FETCH_REQUEST_TOPIC_V0), "Topics to fetch in the order provided."));

    // FETCH_REQUEST_V5 added a per-partition log_start_offset field - the earliest available offset of partition data that can be consumed.
    private static final Schema FETCH_REQUEST_V5 = new Schema(
            new Field(REPLICA_ID_KEY_NAME, INT32, "Broker id of the follower. For normal consumers, use -1."),
            new Field(MAX_WAIT_KEY_NAME, INT32, "Maximum time in ms to wait for the response."),
            new Field(MIN_BYTES_KEY_NAME, INT32, "Minimum bytes to accumulate in the response."),
            new Field(MAX_BYTES_KEY_NAME, INT32, "Maximum bytes to accumulate in the response. Note that this is not an absolute maximum, " +
                    "if the first message in the first non-empty partition of the fetch is larger than this " +
                    "value, the message will still be returned to ensure that progress can be made."),
            new Field(ISOLATION_LEVEL_KEY_NAME, INT8, "This setting controls the visibility of transactional records. Using READ_UNCOMMITTED " +
                    "(isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), " +
                    "non-transactional and COMMITTED transactional records are visible. To be more concrete, " +
                    "READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), " +
                    "and enables the inclusion of the list of aborted transactions in the result, which allows " +
                    "consumers to discard ABORTED transactional records"),
            new Field(TOPICS_KEY_NAME, new ArrayOf(FETCH_REQUEST_TOPIC_V5), "Topics to fetch in the order provided."));

    /**
     * The body of FETCH_REQUEST_V6 is the same as FETCH_REQUEST_V5.
     * The version number is bumped up to indicate that the client supports KafkaStorageException.
     * The KafkaStorageException will be translated to NotLeaderForPartitionException in the response if version <= 5
     */
    private static final Schema FETCH_REQUEST_V6 = FETCH_REQUEST_V5;

    public static Schema[] schemaVersions() {
        return new Schema[]{FETCH_REQUEST_V0, FETCH_REQUEST_V1, FETCH_REQUEST_V2, FETCH_REQUEST_V3, FETCH_REQUEST_V4,
            FETCH_REQUEST_V5, FETCH_REQUEST_V6};
    };

    // default values for older versions where a request level limit did not exist
    public static final int DEFAULT_RESPONSE_MAX_BYTES = Integer.MAX_VALUE;
    public static final long INVALID_LOG_START_OFFSET = -1L;

    private final int replicaId;
    private final int maxWait;
    private final int minBytes;
    private final int maxBytes;
    private final IsolationLevel isolationLevel;
    private final LinkedHashMap<TopicPartition, PartitionData> fetchData;

    public static final class PartitionData {
        public final long fetchOffset;
        public final long logStartOffset;
        public final int maxBytes;

        public PartitionData(long fetchOffset, long logStartOffset, int maxBytes) {
            this.fetchOffset = fetchOffset;
            this.logStartOffset = logStartOffset;
            this.maxBytes = maxBytes;
        }

        @Override
        public String toString() {
            return "(offset=" + fetchOffset + ", logStartOffset=" + logStartOffset + ", maxBytes=" + maxBytes + ")";
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
        private final int maxWait;
        private final int minBytes;
        private final int replicaId;
        private final LinkedHashMap<TopicPartition, PartitionData> fetchData;
        private final IsolationLevel isolationLevel;
        private int maxBytes = DEFAULT_RESPONSE_MAX_BYTES;

        public static Builder forConsumer(int maxWait, int minBytes, LinkedHashMap<TopicPartition, PartitionData> fetchData) {
            return forConsumer(maxWait, minBytes, fetchData, IsolationLevel.READ_UNCOMMITTED);
        }

        public static Builder forConsumer(int maxWait, int minBytes, LinkedHashMap<TopicPartition, PartitionData> fetchData,
                                          IsolationLevel isolationLevel) {
            return new Builder(ApiKeys.FETCH.oldestVersion(), ApiKeys.FETCH.latestVersion(), CONSUMER_REPLICA_ID,
                    maxWait, minBytes, fetchData, isolationLevel);
        }

        public static Builder forReplica(short allowedVersion, int replicaId, int maxWait, int minBytes,
                                         LinkedHashMap<TopicPartition, PartitionData> fetchData) {
            return new Builder(allowedVersion, allowedVersion, replicaId, maxWait, minBytes, fetchData,
                    IsolationLevel.READ_UNCOMMITTED);
        }

        private Builder(short minVersion, short maxVersion, int replicaId, int maxWait, int minBytes,
                        LinkedHashMap<TopicPartition, PartitionData> fetchData, IsolationLevel isolationLevel) {
            super(ApiKeys.FETCH, minVersion, maxVersion);
            this.replicaId = replicaId;
            this.maxWait = maxWait;
            this.minBytes = minBytes;
            this.fetchData = fetchData;
            this.isolationLevel = isolationLevel;
        }

        public LinkedHashMap<TopicPartition, PartitionData> fetchData() {
            return this.fetchData;
        }

        public Builder setMaxBytes(int maxBytes) {
            this.maxBytes = maxBytes;
            return this;
        }

        @Override
        public FetchRequest build(short version) {
            if (version < 3) {
                maxBytes = DEFAULT_RESPONSE_MAX_BYTES;
            }

            return new FetchRequest(version, replicaId, maxWait, minBytes, maxBytes, fetchData, isolationLevel);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=FetchRequest").
                    append(", replicaId=").append(replicaId).
                    append(", maxWait=").append(maxWait).
                    append(", minBytes=").append(minBytes).
                    append(", maxBytes=").append(maxBytes).
                    append(", fetchData=").append(fetchData).
                    append(", isolationLevel=").append(isolationLevel).
                    append(")");
            return bld.toString();
        }
    }

    private FetchRequest(short version, int replicaId, int maxWait, int minBytes, int maxBytes,
                         LinkedHashMap<TopicPartition, PartitionData> fetchData, IsolationLevel isolationLevel) {
        super(version);
        this.replicaId = replicaId;
        this.maxWait = maxWait;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
        this.fetchData = fetchData;
        this.isolationLevel = isolationLevel;
    }

    public FetchRequest(Struct struct, short version) {
        super(version);
        replicaId = struct.getInt(REPLICA_ID_KEY_NAME);
        maxWait = struct.getInt(MAX_WAIT_KEY_NAME);
        minBytes = struct.getInt(MIN_BYTES_KEY_NAME);
        if (struct.hasField(MAX_BYTES_KEY_NAME))
            maxBytes = struct.getInt(MAX_BYTES_KEY_NAME);
        else
            maxBytes = DEFAULT_RESPONSE_MAX_BYTES;

        if (struct.hasField(ISOLATION_LEVEL_KEY_NAME))
            isolationLevel = IsolationLevel.forId(struct.getByte(ISOLATION_LEVEL_KEY_NAME));
        else
            isolationLevel = IsolationLevel.READ_UNCOMMITTED;

        fetchData = new LinkedHashMap<>();
        for (Object topicResponseObj : struct.getArray(TOPICS_KEY_NAME)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.get(TOPIC_NAME);
            for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.get(PARTITION_ID);
                long offset = partitionResponse.getLong(FETCH_OFFSET_KEY_NAME);
                int maxBytes = partitionResponse.getInt(MAX_BYTES_KEY_NAME);
                long logStartOffset = partitionResponse.hasField(LOG_START_OFFSET_KEY_NAME) ?
                    partitionResponse.getLong(LOG_START_OFFSET_KEY_NAME) : INVALID_LOG_START_OFFSET;
                PartitionData partitionData = new PartitionData(offset, logStartOffset, maxBytes);
                fetchData.put(new TopicPartition(topic, partition), partitionData);
            }
        }
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData> responseData = new LinkedHashMap<>();

        for (Map.Entry<TopicPartition, PartitionData> entry: fetchData.entrySet()) {
            FetchResponse.PartitionData partitionResponse = new FetchResponse.PartitionData(Errors.forException(e),
                FetchResponse.INVALID_HIGHWATERMARK, FetchResponse.INVALID_LAST_STABLE_OFFSET, FetchResponse.INVALID_LOG_START_OFFSET,
                null, MemoryRecords.EMPTY);
            responseData.put(entry.getKey(), partitionResponse);
        }
        return new FetchResponse(responseData, throttleTimeMs);
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

    public IsolationLevel isolationLevel() {
        return isolationLevel;
    }

    public static FetchRequest parse(ByteBuffer buffer, short version) {
        return new FetchRequest(ApiKeys.FETCH.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.FETCH.requestSchema(version()));
        List<TopicAndPartitionData<PartitionData>> topicsData = TopicAndPartitionData.batchByTopic(fetchData);

        struct.set(REPLICA_ID_KEY_NAME, replicaId);
        struct.set(MAX_WAIT_KEY_NAME, maxWait);
        struct.set(MIN_BYTES_KEY_NAME, minBytes);
        if (struct.hasField(MAX_BYTES_KEY_NAME))
            struct.set(MAX_BYTES_KEY_NAME, maxBytes);
        if (struct.hasField(ISOLATION_LEVEL_KEY_NAME))
            struct.set(ISOLATION_LEVEL_KEY_NAME, isolationLevel.id());

        List<Struct> topicArray = new ArrayList<>();
        for (TopicAndPartitionData<PartitionData> topicEntry : topicsData) {
            Struct topicData = struct.instance(TOPICS_KEY_NAME);
            topicData.set(TOPIC_NAME, topicEntry.topic);
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, PartitionData> partitionEntry : topicEntry.partitions.entrySet()) {
                PartitionData fetchPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                partitionData.set(PARTITION_ID, partitionEntry.getKey());
                partitionData.set(FETCH_OFFSET_KEY_NAME, fetchPartitionData.fetchOffset);
                if (partitionData.hasField(LOG_START_OFFSET_KEY_NAME))
                    partitionData.set(LOG_START_OFFSET_KEY_NAME, fetchPartitionData.logStartOffset);
                partitionData.set(MAX_BYTES_KEY_NAME, fetchPartitionData.maxBytes);
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(TOPICS_KEY_NAME, topicArray.toArray());
        return struct;
    }
}
