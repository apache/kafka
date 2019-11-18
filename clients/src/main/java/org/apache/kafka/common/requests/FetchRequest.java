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
import java.util.Optional;

import static org.apache.kafka.common.protocol.CommonFields.CURRENT_LEADER_EPOCH;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.requests.FetchMetadata.FINAL_EPOCH;
import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;

public class FetchRequest extends AbstractRequest {
    public static final int CONSUMER_REPLICA_ID = -1;

    private static final Field.ComplexArray TOPICS = new Field.ComplexArray("topics",
            "Topics to fetch in the order provided.");
    private static final Field.ComplexArray FORGOTTEN_TOPICS = new Field.ComplexArray("forgotten_topics_data",
            "Topics to remove from the fetch session.");
    private static final Field.Int32 MAX_BYTES = new Field.Int32("max_bytes",
            "Maximum bytes to accumulate in the response. Note that this is not an absolute maximum, " +
                    "if the first message in the first non-empty partition of the fetch is larger than this " +
                    "value, the message will still be returned to ensure that progress can be made.");
    private static final Field.Int8 ISOLATION_LEVEL = new Field.Int8("isolation_level",
            "This setting controls the visibility of transactional records. Using READ_UNCOMMITTED " +
                    "(isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), " +
                    "non-transactional and COMMITTED transactional records are visible. To be more concrete, " +
                    "READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), " +
                    "and enables the inclusion of the list of aborted transactions in the result, which allows " +
                    "consumers to discard ABORTED transactional records");
    private static final Field.Int32 SESSION_ID = new Field.Int32("session_id", "The fetch session ID");
    private static final Field.Int32 SESSION_EPOCH = new Field.Int32("session_epoch", "The fetch session epoch");
    private static final Field.Str RACK_ID = new Field.Str("rack_id", "The consumer's rack id");

    // topic level fields
    private static final Field.ComplexArray PARTITIONS = new Field.ComplexArray("partitions",
            "Partitions to fetch.");

    // partition level fields
    private static final Field.Int32 REPLICA_ID = new Field.Int32("replica_id",
            "Broker id of the follower. For normal consumers, use -1.");
    private static final Field.Int64 FETCH_OFFSET = new Field.Int64("fetch_offset", "Message offset.");
    private static final Field.Int32 PARTITION_MAX_BYTES = new Field.Int32("partition_max_bytes",
            "Maximum bytes to fetch.");
    private static final Field.Int32 MAX_WAIT_TIME = new Field.Int32("max_wait_time",
            "Maximum time in ms to wait for the response.");
    private static final Field.Int32 MIN_BYTES = new Field.Int32("min_bytes",
            "Minimum bytes to accumulate in the response.");
    private static final Field.Int64 LOG_START_OFFSET = new Field.Int64("log_start_offset",
            "Earliest available offset of the follower replica. " +
                    "The field is only used when request is sent by follower. ");

    private static final Field PARTITIONS_V0 = PARTITIONS.withFields(
            PARTITION_ID,
            FETCH_OFFSET,
            PARTITION_MAX_BYTES);

    private static final Field TOPICS_V0 = TOPICS.withFields(
            TOPIC_NAME,
            PARTITIONS_V0);

    private static final Schema FETCH_REQUEST_V0 = new Schema(
            REPLICA_ID,
            MAX_WAIT_TIME,
            MIN_BYTES,
            TOPICS_V0);

    // The V1 Fetch Request body is the same as V0.
    // Only the version number is incremented to indicate a newer client
    private static final Schema FETCH_REQUEST_V1 = FETCH_REQUEST_V0;

    // V2 bumped to indicate the client support message format V1 which uses relative offset and has timestamp.
    private static final Schema FETCH_REQUEST_V2 = FETCH_REQUEST_V1;

    // V3 added top level max_bytes field - the total size of partition data to accumulate in response.
    // The partition ordering is now relevant - partitions will be processed in order they appear in request.
    private static final Schema FETCH_REQUEST_V3 = new Schema(
            REPLICA_ID,
            MAX_WAIT_TIME,
            MIN_BYTES,
            MAX_BYTES,
            TOPICS_V0);

    // V4 adds the fetch isolation level and exposes magic v2 (via the response).

    private static final Schema FETCH_REQUEST_V4 = new Schema(
            REPLICA_ID,
            MAX_WAIT_TIME,
            MIN_BYTES,
            MAX_BYTES,
            ISOLATION_LEVEL,
            TOPICS_V0);


    // V5 added log_start_offset field - the earliest available offset of partition data that can be consumed.
    private static final Field PARTITIONS_V5 = PARTITIONS.withFields(
            PARTITION_ID,
            FETCH_OFFSET,
            LOG_START_OFFSET,
            PARTITION_MAX_BYTES);

    private static final Field TOPICS_V5 = TOPICS.withFields(
            TOPIC_NAME,
            PARTITIONS_V5);

    private static final Schema FETCH_REQUEST_V5 = new Schema(
            REPLICA_ID,
            MAX_WAIT_TIME,
            MIN_BYTES,
            MAX_BYTES,
            ISOLATION_LEVEL,
            TOPICS_V5);

    // V6 bumped up to indicate that the client supports KafkaStorageException. The KafkaStorageException will be
    // translated to NotLeaderForPartitionException in the response if version <= 5
    private static final Schema FETCH_REQUEST_V6 = FETCH_REQUEST_V5;

    // V7 added incremental fetch requests.
    private static final Field.Array FORGOTTEN_PARTITIONS = new Field.Array("partitions", Type.INT32,
            "Partitions to remove from the fetch session.");
    private static final Field FORGOTTEN_TOPIC_DATA_V7 = FORGOTTEN_TOPICS.withFields(
            TOPIC_NAME,
            FORGOTTEN_PARTITIONS);

    private static final Schema FETCH_REQUEST_V7 = new Schema(
            REPLICA_ID,
            MAX_WAIT_TIME,
            MIN_BYTES,
            MAX_BYTES,
            ISOLATION_LEVEL,
            SESSION_ID,
            SESSION_EPOCH,
            TOPICS_V5,
            FORGOTTEN_TOPIC_DATA_V7);

    // V8 bump used to indicate that on quota violation brokers send out responses before throttling.
    private static final Schema FETCH_REQUEST_V8 = FETCH_REQUEST_V7;

    // V9 adds the current leader epoch (see KIP-320)
    private static final Field FETCH_REQUEST_PARTITION_V9 = PARTITIONS.withFields(
            PARTITION_ID,
            CURRENT_LEADER_EPOCH,
            FETCH_OFFSET,
            LOG_START_OFFSET,
            PARTITION_MAX_BYTES);

    private static final Field FETCH_REQUEST_TOPIC_V9 = TOPICS.withFields(
            TOPIC_NAME,
            FETCH_REQUEST_PARTITION_V9);

    private static final Schema FETCH_REQUEST_V9 = new Schema(
            REPLICA_ID,
            MAX_WAIT_TIME,
            MIN_BYTES,
            MAX_BYTES,
            ISOLATION_LEVEL,
            SESSION_ID,
            SESSION_EPOCH,
            FETCH_REQUEST_TOPIC_V9,
            FORGOTTEN_TOPIC_DATA_V7);

    // V10 bumped up to indicate ZStandard capability. (see KIP-110)
    private static final Schema FETCH_REQUEST_V10 = FETCH_REQUEST_V9;

    // V11 added rack ID to support read from followers (KIP-392)
    private static final Schema FETCH_REQUEST_V11 = new Schema(
            REPLICA_ID,
            MAX_WAIT_TIME,
            MIN_BYTES,
            MAX_BYTES,
            ISOLATION_LEVEL,
            SESSION_ID,
            SESSION_EPOCH,
            FETCH_REQUEST_TOPIC_V9,
            FORGOTTEN_TOPIC_DATA_V7,
            RACK_ID);

    public static Schema[] schemaVersions() {
        return new Schema[]{FETCH_REQUEST_V0, FETCH_REQUEST_V1, FETCH_REQUEST_V2, FETCH_REQUEST_V3, FETCH_REQUEST_V4,
            FETCH_REQUEST_V5, FETCH_REQUEST_V6, FETCH_REQUEST_V7, FETCH_REQUEST_V8, FETCH_REQUEST_V9,
            FETCH_REQUEST_V10, FETCH_REQUEST_V11};
    }

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
    private final String rackId;

    public static final class PartitionData {
        public final long fetchOffset;
        public final long logStartOffset;
        public final int maxBytes;
        public final Optional<Integer> currentLeaderEpoch;

        public PartitionData(long fetchOffset, long logStartOffset, int maxBytes, Optional<Integer> currentLeaderEpoch) {
            this.fetchOffset = fetchOffset;
            this.logStartOffset = logStartOffset;
            this.maxBytes = maxBytes;
            this.currentLeaderEpoch = currentLeaderEpoch;
        }

        @Override
        public String toString() {
            return "(fetchOffset=" + fetchOffset +
                    ", logStartOffset=" + logStartOffset +
                    ", maxBytes=" + maxBytes +
                    ", currentLeaderEpoch=" + currentLeaderEpoch +
                    ")";
        }

        @Override
        public int hashCode() {
            return Objects.hash(fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PartitionData that = (PartitionData) o;
            return Objects.equals(fetchOffset, that.fetchOffset) &&
                Objects.equals(logStartOffset, that.logStartOffset) &&
                Objects.equals(maxBytes, that.maxBytes) &&
                Objects.equals(currentLeaderEpoch, that.currentLeaderEpoch);
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
        private List<TopicPartition> toForget = Collections.emptyList();
        private String rackId = "";

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

        public Builder rackId(String rackId) {
            this.rackId = rackId;
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
                isolationLevel, toForget, metadata, rackId);
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
                    append(", rackId=").append(rackId).
                    append(")");
            return bld.toString();
        }
    }

    private FetchRequest(short version, int replicaId, int maxWait, int minBytes, int maxBytes,
                         Map<TopicPartition, PartitionData> fetchData, IsolationLevel isolationLevel,
                         List<TopicPartition> toForget, FetchMetadata metadata, String rackId) {
        super(ApiKeys.FETCH, version);
        this.replicaId = replicaId;
        this.maxWait = maxWait;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
        this.fetchData = fetchData;
        this.isolationLevel = isolationLevel;
        this.toForget = toForget;
        this.metadata = metadata;
        this.rackId = rackId;
    }

    public FetchRequest(Struct struct, short version) {
        super(ApiKeys.FETCH, version);
        replicaId = struct.get(REPLICA_ID);
        maxWait = struct.get(MAX_WAIT_TIME);
        minBytes = struct.get(MIN_BYTES);
        maxBytes = struct.getOrElse(MAX_BYTES, DEFAULT_RESPONSE_MAX_BYTES);

        if (struct.hasField(ISOLATION_LEVEL))
            isolationLevel = IsolationLevel.forId(struct.get(ISOLATION_LEVEL));
        else
            isolationLevel = IsolationLevel.READ_UNCOMMITTED;
        toForget = new ArrayList<>(0);
        if (struct.hasField(FORGOTTEN_TOPICS)) {
            for (Object forgottenTopicObj : struct.get(FORGOTTEN_TOPICS)) {
                Struct forgottenTopic = (Struct) forgottenTopicObj;
                String topicName = forgottenTopic.get(TOPIC_NAME);
                for (Object partObj : forgottenTopic.get(FORGOTTEN_PARTITIONS)) {
                    Integer part = (Integer) partObj;
                    toForget.add(new TopicPartition(topicName, part));
                }
            }
        }
        metadata = new FetchMetadata(struct.getOrElse(SESSION_ID, INVALID_SESSION_ID),
            struct.getOrElse(SESSION_EPOCH, FINAL_EPOCH));

        fetchData = new LinkedHashMap<>();
        for (Object topicResponseObj : struct.get(TOPICS)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.get(TOPIC_NAME);
            for (Object partitionResponseObj : topicResponse.get(PARTITIONS)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.get(PARTITION_ID);
                long offset = partitionResponse.get(FETCH_OFFSET);
                int maxBytes = partitionResponse.get(PARTITION_MAX_BYTES);
                long logStartOffset = partitionResponse.getOrElse(LOG_START_OFFSET, INVALID_LOG_START_OFFSET);

                // Current leader epoch added in v9
                Optional<Integer> currentLeaderEpoch = RequestUtils.getLeaderEpoch(partitionResponse, CURRENT_LEADER_EPOCH);
                PartitionData partitionData = new PartitionData(offset, logStartOffset, maxBytes, currentLeaderEpoch);
                fetchData.put(new TopicPartition(topic, partition), partitionData);
            }
        }
        rackId = struct.getOrElse(RACK_ID, "");
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
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> responseData = new LinkedHashMap<>();
        for (Map.Entry<TopicPartition, PartitionData> entry : fetchData.entrySet()) {
            FetchResponse.PartitionData<MemoryRecords> partitionResponse = new FetchResponse.PartitionData<>(error,
                    FetchResponse.INVALID_HIGHWATERMARK, FetchResponse.INVALID_LAST_STABLE_OFFSET,
                    FetchResponse.INVALID_LOG_START_OFFSET, Optional.empty(), null,
                    MemoryRecords.EMPTY);
            responseData.put(entry.getKey(), partitionResponse);
        }
        return new FetchResponse<>(error, responseData, throttleTimeMs, metadata.sessionId());
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

    public String rackId() {
        return rackId;
    }

    public static FetchRequest parse(ByteBuffer buffer, short version) {
        return new FetchRequest(ApiKeys.FETCH.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.FETCH.requestSchema(version()));
        List<TopicAndPartitionData<PartitionData>> topicsData =
            TopicAndPartitionData.batchByTopic(fetchData.entrySet().iterator());

        struct.set(REPLICA_ID, replicaId);
        struct.set(MAX_WAIT_TIME, maxWait);
        struct.set(MIN_BYTES, minBytes);
        struct.setIfExists(MAX_BYTES, maxBytes);
        struct.setIfExists(ISOLATION_LEVEL, isolationLevel.id());
        struct.setIfExists(SESSION_ID, metadata.sessionId());
        struct.setIfExists(SESSION_EPOCH, metadata.epoch());

        List<Struct> topicArray = new ArrayList<>();
        for (TopicAndPartitionData<PartitionData> topicEntry : topicsData) {
            Struct topicData = struct.instance(TOPICS);
            topicData.set(TOPIC_NAME, topicEntry.topic);
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, PartitionData> partitionEntry : topicEntry.partitions.entrySet()) {
                PartitionData fetchPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS);
                partitionData.set(PARTITION_ID, partitionEntry.getKey());
                partitionData.set(FETCH_OFFSET, fetchPartitionData.fetchOffset);
                partitionData.set(PARTITION_MAX_BYTES, fetchPartitionData.maxBytes);
                partitionData.setIfExists(LOG_START_OFFSET, fetchPartitionData.logStartOffset);
                RequestUtils.setLeaderEpochIfExists(partitionData, CURRENT_LEADER_EPOCH, fetchPartitionData.currentLeaderEpoch);
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(TOPICS, topicArray.toArray());
        if (struct.hasField(FORGOTTEN_TOPICS)) {
            Map<String, List<Integer>> topicsToPartitions = new HashMap<>();
            for (TopicPartition part : toForget) {
                List<Integer> partitions = topicsToPartitions.computeIfAbsent(part.topic(), topic -> new ArrayList<>());
                partitions.add(part.partition());
            }
            List<Struct> toForgetStructs = new ArrayList<>();
            for (Map.Entry<String, List<Integer>> entry : topicsToPartitions.entrySet()) {
                Struct toForgetStruct = struct.instance(FORGOTTEN_TOPICS);
                toForgetStruct.set(TOPIC_NAME, entry.getKey());
                toForgetStruct.set(FORGOTTEN_PARTITIONS, entry.getValue().toArray());
                toForgetStructs.add(toForgetStruct);
            }
            struct.set(FORGOTTEN_TOPICS, toForgetStructs.toArray());
        }
        struct.setIfExists(RACK_ID, rackId);
        return struct;
    }
}
