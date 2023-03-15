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
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchRequestData.ForgottenTopic;
import org.apache.kafka.common.message.FetchRequestData.ReplicaState;
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

    public static final int ORDINARY_CONSUMER_ID = -1;
    public static final int DEBUGGING_CONSUMER_ID = -2;
    public static final int FUTURE_LOCAL_REPLICA_ID = -3;

    private final FetchRequestData data;
    private volatile LinkedHashMap<TopicIdPartition, PartitionData> fetchData = null;
    private volatile List<TopicIdPartition> toForget = null;

    // This is an immutable read-only structures derived from FetchRequestData
    private final FetchMetadata metadata;

    public static final class PartitionData {
        public final Uuid topicId;
        public final long fetchOffset;
        public final long logStartOffset;
        public final int maxBytes;
        public final Optional<Integer> currentLeaderEpoch;
        public final Optional<Integer> lastFetchedEpoch;

        public PartitionData(
            Uuid topicId,
            long fetchOffset,
            long logStartOffset,
            int maxBytes,
            Optional<Integer> currentLeaderEpoch
        ) {
            this(topicId, fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch, Optional.empty());
        }

        public PartitionData(
            Uuid topicId,
            long fetchOffset,
            long logStartOffset,
            int maxBytes,
            Optional<Integer> currentLeaderEpoch,
            Optional<Integer> lastFetchedEpoch
        ) {
            this.topicId = topicId;
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
            return Objects.equals(topicId, that.topicId) &&
                fetchOffset == that.fetchOffset &&
                logStartOffset == that.logStartOffset &&
                maxBytes == that.maxBytes &&
                Objects.equals(currentLeaderEpoch, that.currentLeaderEpoch) &&
                Objects.equals(lastFetchedEpoch, that.lastFetchedEpoch);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicId, fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch, lastFetchedEpoch);
        }

        @Override
        public String toString() {
            return "PartitionData(" +
                "topicId=" + topicId +
                ", fetchOffset=" + fetchOffset +
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

    // It is only used by KafkaRaftClient for downgrading the FetchRequest.
    public static class SimpleBuilder extends AbstractRequest.Builder<FetchRequest> {
        private final FetchRequestData fetchRequestData;

        public SimpleBuilder(FetchRequestData fetchRequestData) {
            super(ApiKeys.FETCH);
            this.fetchRequestData = fetchRequestData;
        }

        @Override
        public FetchRequest build(short version) {
            if (fetchRequestData.replicaId() >= 0) {
                throw new IllegalStateException("The replica id should be placed in the replicaState of a fetchRequestData");
            }

            if (version < 15) {
                fetchRequestData.setReplicaId(fetchRequestData.replicaState().replicaId());
                fetchRequestData.setReplicaState(new ReplicaState());
            }
            return new FetchRequest(fetchRequestData, version);
        }
    }

    public static class Builder extends AbstractRequest.Builder<FetchRequest> {
        private final int maxWait;
        private final int minBytes;
        private final int replicaId;
        private final long replicaEpoch;
        private final Map<TopicPartition, PartitionData> toFetch;
        private IsolationLevel isolationLevel = IsolationLevel.READ_UNCOMMITTED;
        private int maxBytes = DEFAULT_RESPONSE_MAX_BYTES;
        private FetchMetadata metadata = FetchMetadata.LEGACY;
        private List<TopicIdPartition> removed = Collections.emptyList();
        private List<TopicIdPartition> replaced = Collections.emptyList();
        private String rackId = "";

        public static Builder forConsumer(short maxVersion, int maxWait, int minBytes, Map<TopicPartition, PartitionData> fetchData) {
            return new Builder(ApiKeys.FETCH.oldestVersion(), maxVersion,
                CONSUMER_REPLICA_ID,  -1, maxWait, minBytes, fetchData);
        }

        public static Builder forReplica(short allowedVersion, int replicaId, long replicaEpoch, int maxWait, int minBytes,
                                         Map<TopicPartition, PartitionData> fetchData) {
            return new Builder(allowedVersion, allowedVersion, replicaId, replicaEpoch, maxWait, minBytes, fetchData);
        }

        public Builder(short minVersion, short maxVersion, int replicaId, long replicaEpoch, int maxWait, int minBytes,
                       Map<TopicPartition, PartitionData> fetchData) {
            super(ApiKeys.FETCH, minVersion, maxVersion);
            this.replicaId = replicaId;
            this.replicaEpoch = replicaEpoch;
            this.maxWait = maxWait;
            this.minBytes = minBytes;
            this.toFetch = fetchData;
        }

        public Builder isolationLevel(IsolationLevel isolationLevel) {
            this.isolationLevel = isolationLevel;
            return this;
        }

        // Visible for testing
        public FetchMetadata metadata() {
            return this.metadata;
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
            return this.toFetch;
        }

        public Builder setMaxBytes(int maxBytes) {
            this.maxBytes = maxBytes;
            return this;
        }

        public List<TopicIdPartition> removed() {
            return removed;
        }

        public Builder removed(List<TopicIdPartition> removed) {
            this.removed = removed;
            return this;
        }

        public List<TopicIdPartition> replaced() {
            return replaced;
        }

        public Builder replaced(List<TopicIdPartition> replaced) {
            this.replaced = replaced;
            return this;
        }

        private void addToForgottenTopicMap(List<TopicIdPartition> toForget, Map<String, FetchRequestData.ForgottenTopic> forgottenTopicMap) {
            toForget.forEach(topicIdPartition -> {
                FetchRequestData.ForgottenTopic forgottenTopic = forgottenTopicMap.get(topicIdPartition.topic());
                if (forgottenTopic == null) {
                    forgottenTopic = new ForgottenTopic()
                            .setTopic(topicIdPartition.topic())
                            .setTopicId(topicIdPartition.topicId());
                    forgottenTopicMap.put(topicIdPartition.topic(), forgottenTopic);
                }
                forgottenTopic.partitions().add(topicIdPartition.partition());
            });
        }

        @Override
        public FetchRequest build(short version) {
            if (version < 3) {
                maxBytes = DEFAULT_RESPONSE_MAX_BYTES;
            }

            FetchRequestData fetchRequestData = new FetchRequestData();
            fetchRequestData.setMaxWaitMs(maxWait);
            fetchRequestData.setMinBytes(minBytes);
            fetchRequestData.setMaxBytes(maxBytes);
            fetchRequestData.setIsolationLevel(isolationLevel.id());
            fetchRequestData.setForgottenTopicsData(new ArrayList<>());
            if (version < 15) {
                fetchRequestData.setReplicaId(replicaId);
            } else {
                fetchRequestData.setReplicaState(new ReplicaState()
                    .setReplicaId(replicaId)
                    .setReplicaEpoch(replicaEpoch));
            }

            Map<String, FetchRequestData.ForgottenTopic> forgottenTopicMap = new LinkedHashMap<>();
            addToForgottenTopicMap(removed, forgottenTopicMap);

            // If a version older than v13 is used, topic-partition which were replaced
            // by a topic-partition with the same name but a different topic ID are not
            // sent out in the "forget" set in order to not remove the newly added
            // partition in the "fetch" set.
            if (version >= 13) {
                addToForgottenTopicMap(replaced, forgottenTopicMap);
            }

            forgottenTopicMap.forEach((topic, forgottenTopic) -> fetchRequestData.forgottenTopicsData().add(forgottenTopic));

            // We collect the partitions in a single FetchTopic only if they appear sequentially in the fetchData
            fetchRequestData.setTopics(new ArrayList<>());
            FetchRequestData.FetchTopic fetchTopic = null;
            for (Map.Entry<TopicPartition, PartitionData> entry : toFetch.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                PartitionData partitionData = entry.getValue();

                if (fetchTopic == null || !topicPartition.topic().equals(fetchTopic.topic())) {
                    fetchTopic = new FetchRequestData.FetchTopic()
                       .setTopic(topicPartition.topic())
                       .setTopicId(partitionData.topicId)
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
                    append(", fetchData=").append(toFetch).
                    append(", isolationLevel=").append(isolationLevel).
                    append(", removed=").append(Utils.join(removed, ", ")).
                    append(", replaced=").append(Utils.join(replaced, ", ")).
                    append(", metadata=").append(metadata).
                    append(", rackId=").append(rackId).
                    append(")");
            return bld.toString();
        }
    }

    public static int replicaId(FetchRequestData fetchRequestData) {
        return fetchRequestData.replicaId() != -1 ? fetchRequestData.replicaId() : fetchRequestData.replicaState().replicaId();
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
        if (version() < 15) {
            return data.replicaId();
        }
        return data.replicaState().replicaId();
    }

    public long replicaEpoch() {
        return data.replicaState().replicaEpoch();
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
    public Map<TopicIdPartition, PartitionData> fetchData(Map<Uuid, String> topicNames) {
        if (fetchData == null) {
            synchronized (this) {
                if (fetchData == null) {
                    // Assigning the lazy-initialized `fetchData` in the last step
                    // to avoid other threads accessing a half-initialized object.
                    final LinkedHashMap<TopicIdPartition, PartitionData> fetchDataTmp = new LinkedHashMap<>();
                    final short version = version();
                    data.topics().forEach(fetchTopic -> {
                        String name;
                        if (version < 13) {
                            name = fetchTopic.topic(); // can't be null
                        } else {
                            name = topicNames.get(fetchTopic.topicId());
                        }
                        fetchTopic.partitions().forEach(fetchPartition ->
                                // Topic name may be null here if the topic name was unable to be resolved using the topicNames map.
                                fetchDataTmp.put(new TopicIdPartition(fetchTopic.topicId(), new TopicPartition(name, fetchPartition.partition())),
                                        new PartitionData(
                                                fetchTopic.topicId(),
                                                fetchPartition.fetchOffset(),
                                                fetchPartition.logStartOffset(),
                                                fetchPartition.partitionMaxBytes(),
                                                optionalEpoch(fetchPartition.currentLeaderEpoch()),
                                                optionalEpoch(fetchPartition.lastFetchedEpoch())
                                        )
                                )
                        );
                    });
                    fetchData = fetchDataTmp;
                }
            }
        }
        return fetchData;
    }

    // For versions < 13, builds the forgotten topics list using only the FetchRequestData.
    // For versions 13+, builds the forgotten topics list using both the FetchRequestData and a mapping of topic IDs to names.
    public List<TopicIdPartition> forgottenTopics(Map<Uuid, String> topicNames) {
        if (toForget == null) {
            synchronized (this) {
                if (toForget == null) {
                    // Assigning the lazy-initialized `toForget` in the last step
                    // to avoid other threads accessing a half-initialized object.
                    final List<TopicIdPartition> toForgetTmp = new ArrayList<>();
                    data.forgottenTopicsData().forEach(forgottenTopic -> {
                        String name;
                        if (version() < 13) {
                            name = forgottenTopic.topic(); // can't be null
                        } else {
                            name = topicNames.get(forgottenTopic.topicId());
                        }
                        // Topic name may be null here if the topic name was unable to be resolved using the topicNames map.
                        forgottenTopic.partitions().forEach(partitionId -> toForgetTmp.add(new TopicIdPartition(forgottenTopic.topicId(), new TopicPartition(name, partitionId))));
                    });
                    toForget = toForgetTmp;
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

    // Broker ids are non-negative int.
    public static boolean isValidBrokerId(int brokerId) {
        return brokerId >= 0;
    }

    public static boolean isConsumer(int replicaId) {
        return replicaId < 0 && replicaId != FUTURE_LOCAL_REPLICA_ID;
    }

    public static String describeReplicaId(int replicaId) {
        switch (replicaId) {
            case ORDINARY_CONSUMER_ID: return "consumer";
            case DEBUGGING_CONSUMER_ID: return "debug consumer";
            case FUTURE_LOCAL_REPLICA_ID: return "future local replica";
            default: {
                if (isValidBrokerId(replicaId))
                    return "replica [" + replicaId + "]";
                else
                    return "invalid replica [" + replicaId + "]";
            }
        }
    }

    @Override
    public FetchRequestData data() {
        return data;
    }
}
