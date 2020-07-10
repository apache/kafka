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

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.kafka.common.protocol.CommonFields.CURRENT_LEADER_EPOCH;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;

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
    public static final Schema FETCH_REQUEST_V2 = FETCH_REQUEST_V1;

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

    // default values for older versions where a request level limit did not exist
    public static final int DEFAULT_RESPONSE_MAX_BYTES = Integer.MAX_VALUE;
    public static final long INVALID_LOG_START_OFFSET = -1L;

    private final FetchRequestData fetchRequestData;

    // These are immutable read-only structures derived from FetchRequestData
    private final Map<TopicPartition, PartitionData> fetchData;
    private final List<TopicPartition> toForget;
    private final FetchMetadata metadata;

    @Override
    public FetchRequestData data() {
        return fetchRequestData;
    }

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
            return fetchOffset == that.fetchOffset &&
                    logStartOffset == that.logStartOffset &&
                    maxBytes == that.maxBytes &&
                    currentLeaderEpoch.equals(that.currentLeaderEpoch);
        }
    }

    private Map<TopicPartition, PartitionData> toPartitionDataMap(List<FetchRequestData.FetchableTopic> fetchableTopics) {
        Map<String, List<FetchRequestData.FetchableTopic>> topicsByName = fetchableTopics.stream()
            .collect(Collectors.groupingBy(FetchRequestData.FetchableTopic::topic, LinkedHashMap::new, Collectors.toList()));

        Map<TopicPartition, PartitionData> result = new LinkedHashMap<>();
        topicsByName.forEach((topicName, groupedFetchableTopics) ->
            groupedFetchableTopics.stream()
                .flatMap(fetchableTopic -> fetchableTopic.partitions().stream())
                .forEach(fetchPartition ->
                    result.put(new TopicPartition(topicName, fetchPartition.partition()),
                        new PartitionData(fetchPartition.fetchOffset(), fetchPartition.logStartOffset(),
                            fetchPartition.partitionMaxBytes(), Optional.of(fetchPartition.currentLeaderEpoch())))
                )
        );

        return Collections.unmodifiableMap(result);
    }

    private List<TopicPartition> toForgottonTopicList(List<FetchRequestData.ForgottenTopic> forgottenTopics) {
        List<TopicPartition> result = new ArrayList<>();
        forgottenTopics.forEach(forgottenTopic ->
            forgottenTopic.partitions().forEach(partitionId ->
                result.add(new TopicPartition(forgottenTopic.topic(), partitionId))
            )
        );
        return result;
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

            FetchRequestData fetchRequestData = new FetchRequestData();
            fetchRequestData.setReplicaId(replicaId);
            fetchRequestData.setMaxWaitTime(maxWait);
            fetchRequestData.setMinBytes(minBytes);
            fetchRequestData.setMaxBytes(maxBytes);
            fetchRequestData.setIsolationLevel(isolationLevel.id());
            fetchRequestData.setForgottenTopicsData(new ArrayList<>());
            toForget.stream()
                .collect(Collectors.groupingBy(TopicPartition::topic, LinkedHashMap::new, Collectors.toList()))
                .forEach((topic, partitions) ->
                    fetchRequestData.forgottenTopicsData().add(new FetchRequestData.ForgottenTopic()
                        .setTopic(topic)
                        .setPartitions(partitions.stream().map(TopicPartition::partition).collect(Collectors.toList())))
                );
            fetchRequestData.setTopics(new ArrayList<>());
            fetchData.entrySet().stream()
                .collect(Collectors.groupingBy(entry -> entry.getKey().topic(), LinkedHashMap::new, Collectors.toList()))
                .forEach((topic, entries) -> {
                    FetchRequestData.FetchableTopic fetchableTopic = new FetchRequestData.FetchableTopic()
                        .setTopic(topic)
                        .setPartitions(new ArrayList<>());
                    entries.forEach(entry ->
                        fetchableTopic.partitions().add(
                            new FetchRequestData.FetchPartition().setPartition(entry.getKey().partition())
                                .setCurrentLeaderEpoch(entry.getValue().currentLeaderEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                                .setFetchOffset(entry.getValue().fetchOffset)
                                .setLogStartOffset(entry.getValue().logStartOffset)
                                .setPartitionMaxBytes(entry.getValue().maxBytes))
                    );
                    fetchRequestData.topics().add(fetchableTopic);
                });
            if (metadata != null) {
                fetchRequestData.setEpoch(metadata.epoch());
                fetchRequestData.setSessionId(metadata.sessionId());
            }
            fetchRequestData.setRackId(rackId);

            return new FetchRequest(fetchRequestData, version);
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

    public FetchRequest(FetchRequestData fetchRequestData, short version) {
        super(ApiKeys.FETCH, version);
        this.fetchRequestData = fetchRequestData;
        this.fetchData = toPartitionDataMap(fetchRequestData.topics());
        this.toForget = toForgottonTopicList(fetchRequestData.forgottenTopicsData());
        this.metadata = new FetchMetadata(fetchRequestData.sessionId(), fetchRequestData.epoch());
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
                FetchResponse.INVALID_LOG_START_OFFSET, Optional.empty(), null, MemoryRecords.EMPTY);
            responseData.put(entry.getKey(), partitionResponse);
        }
        return new FetchResponse<>(error, responseData, throttleTimeMs, fetchRequestData.sessionId());
    }

    public int replicaId() {
        return fetchRequestData.replicaId();
    }

    public int maxWait() {
        return fetchRequestData.maxWaitTime();
    }

    public int minBytes() {
        return fetchRequestData.minBytes();
    }

    public int maxBytes() {
        return fetchRequestData.maxBytes();
    }

    public Map<TopicPartition, PartitionData> fetchData() {
        return fetchData;
    }

    public List<TopicPartition> toForget() {
        return toForget;
    }

    public boolean isFromFollower() {
        return replicaId() >= 0;
    }

    public IsolationLevel isolationLevel() {
        return IsolationLevel.forId(fetchRequestData.isolationLevel());
    }

    public FetchMetadata metadata() {
        return metadata;
    }

    public String rackId() {
        return fetchRequestData.rackId();
    }

    public static FetchRequest parse(ByteBuffer buffer, short version) {
        ByteBufferAccessor accessor = new ByteBufferAccessor(buffer);
        FetchRequestData message = new FetchRequestData();
        message.read(accessor, version);
        return new FetchRequest(message, version);
    }

    @Override
    protected Struct toStruct() {
        return fetchRequestData.toStruct(version());
    }
}
