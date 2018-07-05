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
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.INT64;
import static org.apache.kafka.common.protocol.types.Type.INT8;
import static org.apache.kafka.common.requests.FetchMetadata.FINAL_EPOCH;
import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;

public class FetchRequest extends AbstractRequest {
    public static final int CONSUMER_REPLICA_ID = -1;
    private static final String REPLICA_ID_KEY_NAME = "replica_id";
    private static final String MAX_WAIT_KEY_NAME = "max_wait_time";
    private static final String MIN_BYTES_KEY_NAME = "min_bytes";
    private static final String ISOLATION_LEVEL_KEY_NAME = "isolation_level";
    private static final String TOPICS_KEY_NAME = "topics";
    private static final String FORGOTTEN_TOPICS_DATA = "forgotten_topics_data";

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

    // FETCH_REQUEST_V7 added incremental fetch requests.
    public static final Field.Int32 SESSION_ID = new Field.Int32("session_id", "The fetch session ID");
    public static final Field.Int32 EPOCH = new Field.Int32("epoch", "The fetch epoch");

    private static final Schema FORGOTTEN_TOPIC_DATA = new Schema(
        TOPIC_NAME,
        new Field(PARTITIONS_KEY_NAME, new ArrayOf(Type.INT32),
            "Partitions to remove from the fetch session."));

    private static final Schema FETCH_REQUEST_V7 = new Schema(
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
        SESSION_ID,
        EPOCH,
        new Field(TOPICS_KEY_NAME, new ArrayOf(FETCH_REQUEST_TOPIC_V5), "Topics to fetch in the order provided."),
        new Field(FORGOTTEN_TOPICS_DATA, new ArrayOf(FORGOTTEN_TOPIC_DATA), "Topics to remove from the fetch session."));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema FETCH_REQUEST_V8 = FETCH_REQUEST_V7;

    public static Schema[] schemaVersions() {
        return new Schema[]{FETCH_REQUEST_V0, FETCH_REQUEST_V1, FETCH_REQUEST_V2, FETCH_REQUEST_V3, FETCH_REQUEST_V4,
            FETCH_REQUEST_V5, FETCH_REQUEST_V6, FETCH_REQUEST_V7, FETCH_REQUEST_V8};
    };

    // default values for older versions where a request level limit did not exist
    public static final int DEFAULT_RESPONSE_MAX_BYTES = Integer.MAX_VALUE;
    public static final long INVALID_LOG_START_OFFSET = -1L;

    private final int replicaId;
    private final int maxWait;
    private final int minBytes;
    private final int maxBytes;
    private final IsolationLevel isolationLevel;

    // Note: the iteration order of this map is significant, since it determines the order
    // in which partitions appear in the message.  For this reason, this map should have a
    // deterministic iteration order, like LinkedHashMap or TreeMap (but unlike HashMap).
    private final Map<TopicPartition, PartitionData> fetchData;

    private final List<TopicPartition> toForget;
    private final FetchMetadata metadata;

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

        @Override
        public int hashCode() {
            return Objects.hash(fetchOffset, logStartOffset, maxBytes);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PartitionData that = (PartitionData) o;
            return Objects.equals(fetchOffset, that.fetchOffset) &&
                Objects.equals(logStartOffset, that.logStartOffset) &&
                Objects.equals(maxBytes, that.maxBytes);
        }
    }

    static final class TopicAndPartitionData<T> {
        public final String topic;
        public final LinkedHashMap<Integer, T> partitions;

        public TopicAndPartitionData(String topic) {
            this.topic = topic;
            this.partitions = new LinkedHashMap<>();
        }

        public static <T> List<TopicAndPartitionData<T>> batchByTopic(Iterator<Map.Entry<TopicPartition, T>> iter) {
            List<TopicAndPartitionData<T>> topics = new ArrayList<>();
            while (iter.hasNext()) {
                Map.Entry<TopicPartition, T> topicEntry = iter.next();
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
        private final Map<TopicPartition, PartitionData> fetchData;
        private IsolationLevel isolationLevel = IsolationLevel.READ_UNCOMMITTED;
        private int maxBytes = DEFAULT_RESPONSE_MAX_BYTES;
        private FetchMetadata metadata = FetchMetadata.LEGACY;
        private List<TopicPartition> toForget = Collections.<TopicPartition>emptyList();

        public static Builder forConsumer(int maxWait, int minBytes, Map<TopicPartition, PartitionData> fetchData) {
            return new Builder(ApiKeys.FETCH.oldestVersion(), ApiKeys.FETCH.latestVersion(),
                CONSUMER_REPLICA_ID, maxWait, minBytes, fetchData);
        }

        public static Builder forReplica(short allowedVersion, int replicaId, int maxWait, int minBytes,
                                         Map<TopicPartition, PartitionData> fetchData) {
            return new Builder(allowedVersion, allowedVersion, replicaId, maxWait, minBytes, fetchData);
        }

        public Builder(short minVersion, short maxVersion, int replicaId, int maxWait, int minBytes,
                        Map<TopicPartition, PartitionData> fetchData) {
            super(ApiKeys.FETCH, minVersion, maxVersion);
            this.replicaId = replicaId;
            this.maxWait = maxWait;
            this.minBytes = minBytes;
            this.fetchData = fetchData;
        }

        public Builder isolationLevel(IsolationLevel isolationLevel) {
            this.isolationLevel = isolationLevel;
            return this;
        }

        public Builder metadata(FetchMetadata metadata) {
            this.metadata = metadata;
            return this;
        }

        public Map<TopicPartition, PartitionData> fetchData() {
            return this.fetchData;
        }

        public Builder setMaxBytes(int maxBytes) {
            this.maxBytes = maxBytes;
            return this;
        }

        public List<TopicPartition> toForget() {
            return toForget;
        }

        public Builder toForget(List<TopicPartition> toForget) {
            this.toForget = toForget;
            return this;
        }

        @Override
        public FetchRequest build(short version) {
            if (version < 3) {
                maxBytes = DEFAULT_RESPONSE_MAX_BYTES;
            }

            return new FetchRequest(version, replicaId, maxWait, minBytes, maxBytes, fetchData,
                isolationLevel, toForget, metadata);
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
                    append(", toForget=").append(Utils.join(toForget, ", ")).
                    append(", metadata=").append(metadata).
                    append(")");
            return bld.toString();
        }
    }

    private FetchRequest(short version, int replicaId, int maxWait, int minBytes, int maxBytes,
                         Map<TopicPartition, PartitionData> fetchData, IsolationLevel isolationLevel,
                         List<TopicPartition> toForget, FetchMetadata metadata) {
        super(version);
        this.replicaId = replicaId;
        this.maxWait = maxWait;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
        this.fetchData = fetchData;
        this.isolationLevel = isolationLevel;
        this.toForget = toForget;
        this.metadata = metadata;
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
        toForget = new ArrayList<>(0);
        if (struct.hasField(FORGOTTEN_TOPICS_DATA)) {
            for (Object forgottenTopicObj : struct.getArray(FORGOTTEN_TOPICS_DATA)) {
                Struct forgottenTopic = (Struct) forgottenTopicObj;
                String topicName = forgottenTopic.get(TOPIC_NAME);
                for (Object partObj : forgottenTopic.getArray(PARTITIONS_KEY_NAME)) {
                    Integer part = (Integer) partObj;
                    toForget.add(new TopicPartition(topicName, part));
                }
            }
        }
        metadata = new FetchMetadata(struct.getOrElse(SESSION_ID, INVALID_SESSION_ID),
            struct.getOrElse(EPOCH, FINAL_EPOCH));

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
        // The error is indicated in two ways: by setting the same error code in all partitions, and by
        // setting the top-level error code.  The form where we set the same error code in all partitions
        // is needed in order to maintain backwards compatibility with older versions of the protocol
        // in which there was no top-level error code. Note that for incremental fetch responses, there
        // may not be any partitions at all in the response.  For this reason, the top-level error code
        // is essential for them.
        Errors error = Errors.forException(e);
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData> responseData = new LinkedHashMap<>();
        for (Map.Entry<TopicPartition, PartitionData> entry : fetchData.entrySet()) {
            FetchResponse.PartitionData partitionResponse = new FetchResponse.PartitionData(error,
                FetchResponse.INVALID_HIGHWATERMARK, FetchResponse.INVALID_LAST_STABLE_OFFSET,
                FetchResponse.INVALID_LOG_START_OFFSET, null, MemoryRecords.EMPTY);
            responseData.put(entry.getKey(), partitionResponse);
        }
        return new FetchResponse(error, responseData, throttleTimeMs, metadata.sessionId());
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

    public List<TopicPartition> toForget() {
        return toForget;
    }

    public boolean isFromFollower() {
        return replicaId >= 0;
    }

    public IsolationLevel isolationLevel() {
        return isolationLevel;
    }

    public FetchMetadata metadata() {
        return metadata;
    }

    public static FetchRequest parse(ByteBuffer buffer, short version) {
        return new FetchRequest(ApiKeys.FETCH.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.FETCH.requestSchema(version()));
        List<TopicAndPartitionData<PartitionData>> topicsData =
            TopicAndPartitionData.batchByTopic(fetchData.entrySet().iterator());

        struct.set(REPLICA_ID_KEY_NAME, replicaId);
        struct.set(MAX_WAIT_KEY_NAME, maxWait);
        struct.set(MIN_BYTES_KEY_NAME, minBytes);
        if (struct.hasField(MAX_BYTES_KEY_NAME))
            struct.set(MAX_BYTES_KEY_NAME, maxBytes);
        if (struct.hasField(ISOLATION_LEVEL_KEY_NAME))
            struct.set(ISOLATION_LEVEL_KEY_NAME, isolationLevel.id());
        struct.setIfExists(SESSION_ID, metadata.sessionId());
        struct.setIfExists(EPOCH, metadata.epoch());

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
        if (struct.hasField(FORGOTTEN_TOPICS_DATA)) {
            Map<String, List<Integer>> topicsToPartitions = new HashMap<>();
            for (TopicPartition part : toForget) {
                List<Integer> partitions = topicsToPartitions.get(part.topic());
                if (partitions == null) {
                    partitions = new ArrayList<>();
                    topicsToPartitions.put(part.topic(), partitions);
                }
                partitions.add(part.partition());
            }
            List<Struct> toForgetStructs = new ArrayList<>();
            for (Map.Entry<String, List<Integer>> entry : topicsToPartitions.entrySet()) {
                Struct toForgetStruct = struct.instance(FORGOTTEN_TOPICS_DATA);
                toForgetStruct.set(TOPIC_NAME, entry.getKey());
                toForgetStruct.set(PARTITIONS_KEY_NAME, entry.getValue().toArray());
                toForgetStructs.add(toForgetStruct);
            }
            struct.set(FORGOTTEN_TOPICS_DATA, toForgetStructs.toArray());
        }
        return struct;
    }
}
