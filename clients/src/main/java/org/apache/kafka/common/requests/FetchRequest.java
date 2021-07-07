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
import org.apache.kafka.common.errors.UnknownTopicIdException;
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
    private volatile LinkedHashMap<TopicPartition, PartitionData> fetchData = null;
    private volatile List<TopicPartition> toForget = null;

    // This is an immutable read-only structures derived from FetchRequestData
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
        this.data = fetchRequestData;
        this.metadata = new FetchMetadata(fetchRequestData.sessionId(), fetchRequestData.sessionEpoch());
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        // For versions 13+ the error is indicated by setting the top-level error code, and no partitions will be returned.
        // For earlier versions, the error is indicated in two ways: by setting the same error code in all partitions,
        // and by setting the top-level error code.  The form where we set the same error code in all partitions
        // is needed in order to maintain backwards compatibility with older versions of the protocol
        // in which there was no top-level error code. Note that for incremental fetch responses, there
        // may not be any partitions at all in the response.  For this reason, the top-level error code
        // is essential for them.
        Errors error = Errors.forException(e);
        List<FetchResponseData.FetchableTopicResponse> topicResponseList = new ArrayList<>();
        // For version 13+, we know the client can handle a top level error code, so we don't need to send back partitions too.
        if (version() < 13) {
            data.topics().forEach(topic -> {
                List<FetchResponseData.PartitionData> partitionResponses = topic.partitions().stream().map(partition ->
                        FetchResponse.partitionResponse(partition.partition(), error)).collect(Collectors.toList());
                topicResponseList.add(new FetchResponseData.FetchableTopicResponse()
                        .setTopic(topic.topic())
                        .setTopicId(topic.topicId())
                        .setPartitions(partitionResponses));
            });
        }
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

    // For versions < 13, builds the partitionData map using only the FetchRequestData.
    // For versions 13+, builds the partitionData map using both the FetchRequestData and a mapping of topic IDs to names.
    // Throws UnknownTopicIdException for versions 13+ if the topic ID was unknown to the server.
    public Map<TopicPartition, PartitionData> fetchData(Map<Uuid, String> topicNames) throws UnknownTopicIdException {
        if (fetchData == null) {
            synchronized (this) {
                if (fetchData == null) {
                    fetchData = new LinkedHashMap<>();
                    short version = version();
                    data.topics().forEach(fetchTopic -> {
                        String name;
                        if (version < 13) {
                            name = fetchTopic.topic(); // can't be null
                        } else {
                            name = topicNames.get(fetchTopic.topicId());
                        }
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
                            throw new UnknownTopicIdException(String.format("Topic Id %s in FetchRequest was unknown to the server", fetchTopic.topicId()));
                        }
                    });
                }
            }
        }
        return fetchData;
    }

    // For versions 13+, throws UnknownTopicIdException if the topic ID was unknown to the server.
    public List<TopicPartition> forgottenTopics(Map<Uuid, String> topicNames) throws UnknownTopicIdException {
        if (toForget == null) {
            synchronized (this) {
                if (toForget == null) {
                    toForget = new ArrayList<>();
                    data.forgottenTopicsData().forEach(forgottenTopic -> {
                        String name;
                        if (version() < 13) {
                            name = forgottenTopic.topic(); // can't be null
                        } else {
                            name = topicNames.get(forgottenTopic.topicId());
                        }
                        if (name == null) {
                            throw new UnknownTopicIdException(String.format("Topic Id %s in FetchRequest was unknown to the server", forgottenTopic.topicId()));
                        }
                        forgottenTopic.partitions().forEach(partitionId -> toForget.add(new TopicPartition(name, partitionId)));
                    });
                }
            }
        }
        return toForget;
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
