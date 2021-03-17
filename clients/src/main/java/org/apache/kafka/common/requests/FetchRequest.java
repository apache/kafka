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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class FetchRequest extends AbstractRequest {

    public static final int CONSUMER_REPLICA_ID = -1;

    // default values for older versions where a request level limit did not exist
    public static final int DEFAULT_RESPONSE_MAX_BYTES = Integer.MAX_VALUE;
    public static final long INVALID_LOG_START_OFFSET = -1L;

    private final FetchRequestData data;

    // These are immutable read-only structures derived from FetchRequestData
    private final Map<TopicPartition, PartitionData> fetchData;
    private final FetchMetadata metadata;

    public static final class PartitionData {
        public final long fetchOffset;
        public final long logStartOffset;
        public final int maxBytes;
        public final Optional<Integer> currentLeaderEpoch;
        public final Optional<Integer> lastFetchedEpoch;

        public PartitionData(
            long fetchOffset,
            long logStartOffset,
            int maxBytes,
            Optional<Integer> currentLeaderEpoch
        ) {
            this(fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch, Optional.empty());
        }

        public PartitionData(
            long fetchOffset,
            long logStartOffset,
            int maxBytes,
            Optional<Integer> currentLeaderEpoch,
            Optional<Integer> lastFetchedEpoch
        ) {
            this.fetchOffset = fetchOffset;
            this.logStartOffset = logStartOffset;
            this.maxBytes = maxBytes;
            this.currentLeaderEpoch = currentLeaderEpoch;
            this.lastFetchedEpoch = lastFetchedEpoch;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PartitionData that = (PartitionData) o;
            return fetchOffset == that.fetchOffset &&
                logStartOffset == that.logStartOffset &&
                maxBytes == that.maxBytes &&
                Objects.equals(currentLeaderEpoch, that.currentLeaderEpoch) &&
                Objects.equals(lastFetchedEpoch, that.lastFetchedEpoch);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch, lastFetchedEpoch);
        }

        @Override
        public String toString() {
            return "PartitionData(" +
                "fetchOffset=" + fetchOffset +
                ", logStartOffset=" + logStartOffset +
                ", maxBytes=" + maxBytes +
                ", currentLeaderEpoch=" + currentLeaderEpoch +
                ", lastFetchedEpoch=" + lastFetchedEpoch +
                ')';
        }
    }

    private static Optional<Integer> optionalEpoch(int rawEpochValue) {
        if (rawEpochValue < 0) {
            return Optional.empty();
        } else {
            return Optional.of(rawEpochValue);
        }
    }

    // Only used when version is lower than 13.
    private Map<TopicPartition, PartitionData> toPartitionDataMap(List<FetchRequestData.FetchTopic> fetchableTopics) {
        Map<TopicPartition, PartitionData> result = new LinkedHashMap<>();
        fetchableTopics.forEach(fetchTopic -> fetchTopic.partitions().forEach(fetchPartition -> {
            result.put(new TopicPartition(fetchTopic.topic(), fetchPartition.partition()),
                    new PartitionData(
                            fetchPartition.fetchOffset(),
                            fetchPartition.logStartOffset(),
                            fetchPartition.partitionMaxBytes(),
                            optionalEpoch(fetchPartition.currentLeaderEpoch()),
                            optionalEpoch(fetchPartition.lastFetchedEpoch())
                    ));
        }));
        return Collections.unmodifiableMap(result);
    }

    /**
     *  The following methods are new to version 13. They support sending Fetch requests using topic ID rather
     *  than topic name. Since the sender and receiver of the fetch request may have different topic IDs in
     *  their caches, there is a possibility for some topic IDs to be unresolved on the receiving end. These
     *  methods and classes try to resolve the topic IDs and keep track of unresolved partitions and their errors.
     */

    // Holds information on partitions whose topic IDs were unable to be resolved when the Fetch request
    // was received.
    public static final class UnresolvedPartitions {
        private final Uuid id;
        private final Map<Integer, PartitionData> partitionData;

        public UnresolvedPartitions(Uuid id, Map<Integer, PartitionData> partitionData) {
            this.id = id;
            this.partitionData = partitionData;
        }

        public Uuid id() {
            return id;
        }

        public Map<Integer, PartitionData> partitionData() {
            return partitionData;
        }
    }

    // For Fetch versions 13+, this object holds the fetchData map used in previous Fetch iterations,
    // a list of UnresolvedPartitions, and a map containing all unresolved IDs.
    // For Fetch versions < 13, this will only wrap the fetchData map.
    public static final class FetchDataAndError {
        private final Map<TopicPartition, PartitionData> fetchData;
        private final List<UnresolvedPartitions> unresolvedPartitions;

        public FetchDataAndError(Map<TopicPartition, PartitionData> fetchData, List<UnresolvedPartitions> unresolvedPartitions) {
            this.fetchData = fetchData;
            this.unresolvedPartitions = unresolvedPartitions;
        }

        public final Map<TopicPartition, PartitionData> fetchData() {
            return fetchData;
        }

        public final List<UnresolvedPartitions> unresolvedPartitions() {
            return unresolvedPartitions;
        }
    }

    // Only used when Fetch is version 13 or greater and ID errors can be ignored.
    private Map<TopicPartition, PartitionData> toPartitionDataMap(List<FetchRequestData.FetchTopic> fetchableTopics, Map<Uuid, String> topicNames) {
        Map<TopicPartition, PartitionData> result = new LinkedHashMap<>();
        fetchableTopics.forEach(fetchTopic -> {
            String name = topicNames.get(fetchTopic.topicId());
            if (name != null) {
                fetchTopic.partitions().forEach(fetchPartition ->
                    result.put(new TopicPartition(name, fetchPartition.partition()),
                        new PartitionData(
                                fetchPartition.fetchOffset(),
                                fetchPartition.logStartOffset(),
                                fetchPartition.partitionMaxBytes(),
                                optionalEpoch(fetchPartition.currentLeaderEpoch()),
                                optionalEpoch(fetchPartition.lastFetchedEpoch())
                        )
                    )
                );
            }
        });
        return Collections.unmodifiableMap(result);
    }

    // Only used when Fetch is version 13 or greater.
    private FetchDataAndError toPartitionDataMapAndError(List<FetchRequestData.FetchTopic> fetchableTopics, Map<Uuid, String> topicNames) {
        Map<TopicPartition, PartitionData> fetchData = new LinkedHashMap<>();
        List<UnresolvedPartitions> unresolvedPartitions = new LinkedList<>();
        fetchableTopics.forEach(fetchTopic -> {
            String name = topicNames.get(fetchTopic.topicId());
            if (name != null) {
                // If topic name is resolved, simply add to fetchData map
                fetchTopic.partitions().forEach(fetchPartition ->
                        fetchData.put(new TopicPartition(name, fetchPartition.partition()),
                                new PartitionData(
                                        fetchPartition.fetchOffset(),
                                        fetchPartition.logStartOffset(),
                                        fetchPartition.partitionMaxBytes(),
                                        optionalEpoch(fetchPartition.currentLeaderEpoch()),
                                        optionalEpoch(fetchPartition.lastFetchedEpoch())
                                )
                        )
                );
            } else {
                // If topic name is not resolved, add to unresolvedPartitions list
                unresolvedPartitions.add(new UnresolvedPartitions(fetchTopic.topicId(), fetchTopic.partitions().stream().collect(Collectors.toMap(
                        FetchRequestData.FetchPartition::partition, fetchPartition -> new PartitionData(
                        fetchPartition.fetchOffset(),
                        fetchPartition.logStartOffset(),
                        fetchPartition.partitionMaxBytes(),
                        optionalEpoch(fetchPartition.currentLeaderEpoch()),
                        optionalEpoch(fetchPartition.lastFetchedEpoch()))))));
            }
        });
        return new FetchDataAndError(fetchData, unresolvedPartitions);
    }

    public static class Builder extends AbstractRequest.Builder<FetchRequest> {
        private final int maxWait;
        private final int minBytes;
        private final int replicaId;
        private final Map<TopicPartition, PartitionData> fetchData;
        private final Map<String, Uuid> topicIds;
        private IsolationLevel isolationLevel = IsolationLevel.READ_UNCOMMITTED;
        private int maxBytes = DEFAULT_RESPONSE_MAX_BYTES;
        private FetchMetadata metadata = FetchMetadata.LEGACY;
        private List<TopicPartition> toForget = Collections.emptyList();
        private String rackId = "";

        public static Builder forConsumer(short maxVersion, int maxWait, int minBytes, Map<TopicPartition, PartitionData> fetchData,
                                          Map<String, Uuid> topicIds) {
            return new Builder(ApiKeys.FETCH.oldestVersion(), maxVersion,
                CONSUMER_REPLICA_ID, maxWait, minBytes, fetchData, topicIds);
        }

        public static Builder forReplica(short allowedVersion, int replicaId, int maxWait, int minBytes,
                                         Map<TopicPartition, PartitionData> fetchData, Map<String, Uuid> topicIds) {
            return new Builder(allowedVersion, allowedVersion, replicaId, maxWait, minBytes, fetchData, topicIds);
        }

        public Builder(short minVersion, short maxVersion, int replicaId, int maxWait, int minBytes,
                        Map<TopicPartition, PartitionData> fetchData, Map<String, Uuid> topicIds) {
            super(ApiKeys.FETCH, minVersion, maxVersion);
            this.replicaId = replicaId;
            this.maxWait = maxWait;
            this.minBytes = minBytes;
            this.fetchData = fetchData;
            this.topicIds = topicIds;
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
            fetchRequestData.setMaxWaitMs(maxWait);
            fetchRequestData.setMinBytes(minBytes);
            fetchRequestData.setMaxBytes(maxBytes);
            fetchRequestData.setIsolationLevel(isolationLevel.id());
            fetchRequestData.setForgottenTopicsData(new ArrayList<>());
            toForget.stream()
                .collect(Collectors.groupingBy(TopicPartition::topic, LinkedHashMap::new, Collectors.toList()))
                .forEach((topic, partitions) ->
                    fetchRequestData.forgottenTopicsData().add(new FetchRequestData.ForgottenTopic()
                        .setTopic(topic)
                        .setTopicId(topicIds.getOrDefault(topic, Uuid.ZERO_UUID))
                        .setPartitions(partitions.stream().map(TopicPartition::partition).collect(Collectors.toList())))
                );
            fetchRequestData.setTopics(new ArrayList<>());

            // We collect the partitions in a single FetchTopic only if they appear sequentially in the fetchData
            FetchRequestData.FetchTopic fetchTopic = null;
            for (Map.Entry<TopicPartition, PartitionData> entry : fetchData.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                PartitionData partitionData = entry.getValue();

                if (fetchTopic == null || !topicPartition.topic().equals(fetchTopic.topic())) {
                    fetchTopic = new FetchRequestData.FetchTopic()
                       .setTopic(topicPartition.topic())
                       .setTopicId(topicIds.getOrDefault(topicPartition.topic(), Uuid.ZERO_UUID))
                       .setPartitions(new ArrayList<>());
                    fetchRequestData.topics().add(fetchTopic);
                }

                FetchRequestData.FetchPartition fetchPartition = new FetchRequestData.FetchPartition()
                    .setPartition(topicPartition.partition())
                    .setCurrentLeaderEpoch(partitionData.currentLeaderEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                    .setLastFetchedEpoch(partitionData.lastFetchedEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                    .setFetchOffset(partitionData.fetchOffset)
                    .setLogStartOffset(partitionData.logStartOffset)
                    .setPartitionMaxBytes(partitionData.maxBytes);

                fetchTopic.partitions().add(fetchPartition);
            }

            if (metadata != null) {
                fetchRequestData.setSessionEpoch(metadata.epoch());
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
        if (version < 13) {
            this.data = fetchRequestData;
            this.fetchData = toPartitionDataMap(fetchRequestData.topics());
            this.metadata = new FetchMetadata(fetchRequestData.sessionId(), fetchRequestData.sessionEpoch());
        } else {
            this.data = fetchRequestData;
            // We need topic name map to fill these data structures, so we will build on fetchData() and toForget()
            this.fetchData = Collections.emptyMap();
            this.metadata = new FetchMetadata(fetchRequestData.sessionId(), fetchRequestData.sessionEpoch());
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
        if (version() < 13) {
            LinkedHashMap<TopicPartition, FetchResponseData.PartitionData> responseData = new LinkedHashMap<>();
            for (Map.Entry<TopicPartition, PartitionData> entry : fetchData.entrySet()) {
                responseData.put(entry.getKey(), FetchResponse.partitionResponse(entry.getKey().partition(), error));
            }
            return FetchResponse.of(error, throttleTimeMs, data.sessionId(), responseData);
        }
        List<FetchResponseData.FetchableTopicResponse> topicResponseList = new ArrayList<>();
        data.topics().forEach(topic -> {
            List<FetchResponseData.PartitionData> partitionResponses = topic.partitions().stream().map(partition ->
                    FetchResponse.partitionResponse(partition.partition(), error)).collect(Collectors.toList());
            topicResponseList.add(new FetchResponseData.FetchableTopicResponse()
                    .setTopicId(topic.topicId())
                    .setPartitions(partitionResponses));
        });
        return new FetchResponse(new FetchResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code())
                .setSessionId(data.sessionId())
                .setResponses(topicResponseList));
    }

    public int replicaId() {
        return data.replicaId();
    }

    public int maxWait() {
        return data.maxWaitMs();
    }

    public int minBytes() {
        return data.minBytes();
    }

    public int maxBytes() {
        return data.maxBytes();
    }

    // Ignores unresolved topic IDs when request version is 13+.
    public Map<TopicPartition, PartitionData> fetchData(Map<Uuid, String> topicNames) {
        if (version() < 13)
            return fetchData;
        return toPartitionDataMap(data.topics(), topicNames);
    }

    public FetchDataAndError fetchDataAndError(Map<Uuid, String> topicNames) {
        if (version() < 13)
            return new FetchDataAndError(fetchData, Collections.emptyList());
        return toPartitionDataMapAndError(data.topics(), topicNames);
    }

    public List<FetchRequestData.ForgottenTopic> forgottenTopics(Map<Uuid, String> topicNames) {
        if (version() >= 13)
          data.forgottenTopicsData().forEach(forgottenTopic ->
                  forgottenTopic.setTopic(topicNames.getOrDefault(forgottenTopic.topicId(), "")));
        return data.forgottenTopicsData();
    }

    public boolean isFromFollower() {
        return replicaId() >= 0;
    }

    public IsolationLevel isolationLevel() {
        return IsolationLevel.forId(data.isolationLevel());
    }

    public FetchMetadata metadata() {
        return metadata;
    }

    public String rackId() {
        return data.rackId();
    }

    public static FetchRequest parse(ByteBuffer buffer, short version) {
        return new FetchRequest(new FetchRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public FetchRequestData data() {
        return data;
    }
}
