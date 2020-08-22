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
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.Struct;
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

public class FetchRequest extends AbstractRequest {

    public static final int CONSUMER_REPLICA_ID = -1;

    // default values for older versions where a request level limit did not exist
    public static final int DEFAULT_RESPONSE_MAX_BYTES = Integer.MAX_VALUE;
    public static final long INVALID_LOG_START_OFFSET = -1L;

    private final FetchRequestData data;

    // These are immutable read-only structures derived from FetchRequestData
    private final Map<TopicPartition, PartitionData> fetchData;
    private final List<TopicPartition> toForget;
    private final FetchMetadata metadata;

    public FetchRequestData data() {
        return data;
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

    private Map<TopicPartition, PartitionData> toPartitionDataMap(List<FetchRequestData.FetchTopic> fetchableTopics) {
        Map<TopicPartition, PartitionData> result = new LinkedHashMap<>();
        fetchableTopics.forEach(fetchTopic -> fetchTopic.partitions().forEach(fetchPartition -> {
            Optional<Integer> leaderEpoch = Optional.of(fetchPartition.currentLeaderEpoch())
                .filter(epoch -> epoch != RecordBatch.NO_PARTITION_LEADER_EPOCH);
            result.put(new TopicPartition(fetchTopic.topic(), fetchPartition.partition()),
                new PartitionData(fetchPartition.fetchOffset(), fetchPartition.logStartOffset(),
                    fetchPartition.partitionMaxBytes(), leaderEpoch));
        }));
        return Collections.unmodifiableMap(result);
    }

    private List<TopicPartition> toForgottenTopicList(List<FetchRequestData.ForgottenTopic> forgottenTopics) {
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
                        .setPartitions(partitions.stream().map(TopicPartition::partition).collect(Collectors.toList())))
                );
            fetchRequestData.setTopics(new ArrayList<>());

            // We collect the partitions in a single FetchTopic only if they appear sequentially in the fetchData
            FetchRequestData.FetchTopic fetchTopic = null;
            for (Map.Entry<TopicPartition, PartitionData> entry : fetchData.entrySet()) {
                if (fetchTopic == null || !entry.getKey().topic().equals(fetchTopic.topic())) {
                    fetchTopic = new FetchRequestData.FetchTopic()
                       .setTopic(entry.getKey().topic())
                       .setPartitions(new ArrayList<>());
                    fetchRequestData.topics().add(fetchTopic);
                }

                fetchTopic.partitions().add(
                    new FetchRequestData.FetchPartition().setPartition(entry.getKey().partition())
                        .setCurrentLeaderEpoch(entry.getValue().currentLeaderEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                        .setFetchOffset(entry.getValue().fetchOffset)
                        .setLogStartOffset(entry.getValue().logStartOffset)
                        .setPartitionMaxBytes(entry.getValue().maxBytes));
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
        this.fetchData = toPartitionDataMap(fetchRequestData.topics());
        this.toForget = toForgottenTopicList(fetchRequestData.forgottenTopicsData());
        this.metadata = new FetchMetadata(fetchRequestData.sessionId(), fetchRequestData.sessionEpoch());
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
        return new FetchResponse<>(error, responseData, throttleTimeMs, data.sessionId());
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
        return IsolationLevel.forId(data.isolationLevel());
    }

    public FetchMetadata metadata() {
        return metadata;
    }

    public String rackId() {
        return data.rackId();
    }

    @Override
    public ByteBuffer serialize(RequestHeader header) {
        // Unlike the custom FetchResponse#toSend, we don't include the buffer size here. This buffer is passed
        // to a NetworkSend which adds the length value in the eventual serialization

        ObjectSerializationCache cache = new ObjectSerializationCache();
        RequestHeaderData requestHeaderData = header.data();

        int headerSize = requestHeaderData.size(cache, header.headerVersion());
        int bodySize = data.size(cache, header.apiVersion());

        ByteBuffer buffer = ByteBuffer.allocate(headerSize + bodySize);
        ByteBufferAccessor writer = new ByteBufferAccessor(buffer);

        requestHeaderData.write(writer, cache, header.headerVersion());
        data.write(writer, cache, header.apiVersion());

        buffer.rewind();
        return buffer;
    }

    // For testing
    public static FetchRequest parse(ByteBuffer buffer, short version) {
        return new FetchRequest(new FetchRequestData(ApiKeys.FETCH.parseRequest(version, buffer), version), version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }
}
