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

import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseGroup;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartition;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartitions;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopic;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.kafka.common.record.RecordBatch.NO_PARTITION_LEADER_EPOCH;

/**
 * Possible error codes:
 *
 * - Partition errors:
 *   - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}
 *   - {@link Errors#TOPIC_AUTHORIZATION_FAILED}
 *   - {@link Errors#UNSTABLE_OFFSET_COMMIT}
 *
 * - Group or coordinator errors:
 *   - {@link Errors#COORDINATOR_LOAD_IN_PROGRESS}
 *   - {@link Errors#COORDINATOR_NOT_AVAILABLE}
 *   - {@link Errors#NOT_COORDINATOR}
 *   - {@link Errors#GROUP_AUTHORIZATION_FAILED}
 */
public class OffsetFetchResponse extends AbstractResponse {
    public static final long INVALID_OFFSET = -1L;
    public static final String NO_METADATA = "";
    public static final PartitionData UNKNOWN_PARTITION = new PartitionData(INVALID_OFFSET,
                                                                            Optional.empty(),
                                                                            NO_METADATA,
                                                                            Errors.UNKNOWN_TOPIC_OR_PARTITION);
    public static final PartitionData UNAUTHORIZED_PARTITION = new PartitionData(INVALID_OFFSET,
                                                                                 Optional.empty(),
                                                                                 NO_METADATA,
                                                                                 Errors.TOPIC_AUTHORIZATION_FAILED);
    private static final List<Errors> PARTITION_ERRORS = Arrays.asList(
        Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.TOPIC_AUTHORIZATION_FAILED);

    private final OffsetFetchResponseData data;
    private final Errors error;
    private final Map<String, Errors> groupLevelErrors = new HashMap<>();

    public static final class PartitionData {
        public final long offset;
        public final String metadata;
        public final Errors error;
        public final Optional<Integer> leaderEpoch;

        public PartitionData(long offset,
                             Optional<Integer> leaderEpoch,
                             String metadata,
                             Errors error) {
            this.offset = offset;
            this.leaderEpoch = leaderEpoch;
            this.metadata = metadata;
            this.error = error;
        }

        public boolean hasError() {
            return this.error != Errors.NONE;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof PartitionData))
                return false;
            PartitionData otherPartition = (PartitionData) other;
            return Objects.equals(this.offset, otherPartition.offset)
                   && Objects.equals(this.leaderEpoch, otherPartition.leaderEpoch)
                   && Objects.equals(this.metadata, otherPartition.metadata)
                   && Objects.equals(this.error, otherPartition.error);
        }

        @Override
        public String toString() {
            return "PartitionData("
                       + "offset=" + offset
                       + ", leaderEpoch=" + leaderEpoch.orElse(NO_PARTITION_LEADER_EPOCH)
                       + ", metadata=" + metadata
                       + ", error='" + error.toString()
                       + ")";
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, leaderEpoch, metadata, error);
        }
    }

    public OffsetFetchResponse(OffsetFetchResponseData data) {
        super(ApiKeys.OFFSET_FETCH);
        this.data = data;
        this.error = null;
    }

    /**
     * Constructor without throttle time.
     * @param error Potential coordinator or group level error code (for api version 2 and later)
     * @param responseData Fetched offset information grouped by topic-partition
     */
    public OffsetFetchResponse(Errors error, Map<TopicPartition, PartitionData> responseData) {
        this(DEFAULT_THROTTLE_TIME, error, responseData);
    }

    /**
     * Constructor with throttle time for version 0 to 7
     * @param throttleTimeMs The time in milliseconds that this response was throttled
     * @param error Potential coordinator or group level error code (for api version 2 and later)
     * @param responseData Fetched offset information grouped by topic-partition
     */
    public OffsetFetchResponse(int throttleTimeMs, Errors error, Map<TopicPartition, PartitionData> responseData) {
        super(ApiKeys.OFFSET_FETCH);
        Map<String, OffsetFetchResponseTopic> offsetFetchResponseTopicMap = new HashMap<>();
        for (Map.Entry<TopicPartition, PartitionData> entry : responseData.entrySet()) {
            String topicName = entry.getKey().topic();
            OffsetFetchResponseTopic topic = offsetFetchResponseTopicMap.getOrDefault(
                topicName, new OffsetFetchResponseTopic().setName(topicName));
            PartitionData partitionData = entry.getValue();
            topic.partitions().add(new OffsetFetchResponsePartition()
                                       .setPartitionIndex(entry.getKey().partition())
                                       .setErrorCode(partitionData.error.code())
                                       .setCommittedOffset(partitionData.offset)
                                       .setCommittedLeaderEpoch(
                                           partitionData.leaderEpoch.orElse(NO_PARTITION_LEADER_EPOCH))
                                       .setMetadata(partitionData.metadata)
            );
            offsetFetchResponseTopicMap.put(topicName, topic);
        }

        this.data = new OffsetFetchResponseData()
            .setTopics(new ArrayList<>(offsetFetchResponseTopicMap.values()))
            .setErrorCode(error.code())
            .setThrottleTimeMs(throttleTimeMs);
        this.error = error;
    }

    /**
     * Constructor with throttle time for version 8 and above.
     * @param throttleTimeMs The time in milliseconds that this response was throttled
     * @param errors Potential coordinator or group level error code
     * @param responseData Fetched offset information grouped by topic-partition and by group
     */
    public OffsetFetchResponse(int throttleTimeMs,
                               Map<String, Errors> errors,
                               Map<String, Map<TopicPartition, PartitionData>> responseData) {
        super(ApiKeys.OFFSET_FETCH);
        List<OffsetFetchResponseGroup> groupList = new ArrayList<>();
        for (Entry<String, Map<TopicPartition, PartitionData>> entry : responseData.entrySet()) {
            String groupName = entry.getKey();
            Map<TopicPartition, PartitionData> partitionDataMap = entry.getValue();
            Map<String, OffsetFetchResponseTopics> offsetFetchResponseTopicsMap = new HashMap<>();
            for (Entry<TopicPartition, PartitionData> partitionEntry : partitionDataMap.entrySet()) {
                String topicName = partitionEntry.getKey().topic();
                OffsetFetchResponseTopics topic =
                    offsetFetchResponseTopicsMap.getOrDefault(topicName,
                        new OffsetFetchResponseTopics().setName(topicName));
                PartitionData partitionData = partitionEntry.getValue();
                topic.partitions().add(new OffsetFetchResponsePartitions()
                    .setPartitionIndex(partitionEntry.getKey().partition())
                    .setErrorCode(partitionData.error.code())
                    .setCommittedOffset(partitionData.offset)
                    .setCommittedLeaderEpoch(
                        partitionData.leaderEpoch.orElse(NO_PARTITION_LEADER_EPOCH))
                    .setMetadata(partitionData.metadata));
                offsetFetchResponseTopicsMap.put(topicName, topic);
            }
            groupList.add(new OffsetFetchResponseGroup()
                .setGroupId(groupName)
                .setTopics(new ArrayList<>(offsetFetchResponseTopicsMap.values()))
                .setErrorCode(errors.get(groupName).code()));
            groupLevelErrors.put(groupName, errors.get(groupName));
        }
        this.data = new OffsetFetchResponseData()
            .setGroups(groupList)
            .setThrottleTimeMs(throttleTimeMs);
        this.error = null;
    }

    public OffsetFetchResponse(OffsetFetchResponseData data, short version) {
        super(ApiKeys.OFFSET_FETCH);
        this.data = data;
        // for version 2 and later use the top-level error code (in ERROR_CODE_KEY_NAME) from the response.
        // for older versions there is no top-level error in the response and all errors are partition errors,
        // so if there is a group or coordinator error at the partition level use that as the top-level error.
        // this way clients can depend on the top-level error regardless of the offset fetch version.
        // we return the error differently starting with version 8, so we will only populate the
        // error field if we are between version 2 and 7. if we are in version 8 or greater, then
        // we will populate the map of group id to error codes.
        if (version < 8) {
            this.error = version >= 2 ? Errors.forCode(data.errorCode()) : topLevelError(data);
        } else {
            for (OffsetFetchResponseGroup group : data.groups()) {
                this.groupLevelErrors.put(group.groupId(), Errors.forCode(group.errorCode()));
            }
            this.error = null;
        }
    }

    private static Errors topLevelError(OffsetFetchResponseData data) {
        for (OffsetFetchResponseTopic topic : data.topics()) {
            for (OffsetFetchResponsePartition partition : topic.partitions()) {
                Errors partitionError = Errors.forCode(partition.errorCode());
                if (partitionError != Errors.NONE && !PARTITION_ERRORS.contains(partitionError)) {
                    return partitionError;
                }
            }
        }
        return Errors.NONE;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    public boolean hasError() {
        return error != Errors.NONE;
    }

    public boolean groupHasError(String groupId) {
        Errors error = groupLevelErrors.get(groupId);
        if (error == null) {
            return this.error != null && this.error != Errors.NONE;
        }
        return error != Errors.NONE;
    }

    public Errors error() {
        return error;
    }

    public Errors groupLevelError(String groupId) {
        if (error != null) {
            return error;
        }
        return groupLevelErrors.get(groupId);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> counts = new HashMap<>();
        if (!groupLevelErrors.isEmpty()) {
            // built response with v8 or above
            for (Map.Entry<String, Errors> entry : groupLevelErrors.entrySet()) {
                updateErrorCounts(counts, entry.getValue());
            }
            for (OffsetFetchResponseGroup group : data.groups()) {
                group.topics().forEach(topic ->
                    topic.partitions().forEach(partition ->
                        updateErrorCounts(counts, Errors.forCode(partition.errorCode()))));
            }
        } else {
            // built response with v0-v7
            updateErrorCounts(counts, error);
            data.topics().forEach(topic ->
                topic.partitions().forEach(partition ->
                    updateErrorCounts(counts, Errors.forCode(partition.errorCode()))));
        }
        return counts;
    }

    // package-private for testing purposes
    Map<TopicPartition, PartitionData> responseDataV0ToV7() {
        Map<TopicPartition, PartitionData> responseData = new HashMap<>();
        for (OffsetFetchResponseTopic topic : data.topics()) {
            for (OffsetFetchResponsePartition partition : topic.partitions()) {
                responseData.put(new TopicPartition(topic.name(), partition.partitionIndex()),
                                 new PartitionData(partition.committedOffset(),
                                                   RequestUtils.getLeaderEpoch(partition.committedLeaderEpoch()),
                                                   partition.metadata(),
                                                   Errors.forCode(partition.errorCode()))
                );
            }
        }
        return responseData;
    }

    private Map<TopicPartition, PartitionData> buildResponseData(String groupId) {
        Map<TopicPartition, PartitionData> responseData = new HashMap<>();
        OffsetFetchResponseGroup group = data
            .groups()
            .stream()
            .filter(g -> g.groupId().equals(groupId))
            .collect(Collectors.toList())
            .get(0);
        for (OffsetFetchResponseTopics topic : group.topics()) {
            for (OffsetFetchResponsePartitions partition : topic.partitions()) {
                responseData.put(new TopicPartition(topic.name(), partition.partitionIndex()),
                    new PartitionData(partition.committedOffset(),
                        RequestUtils.getLeaderEpoch(partition.committedLeaderEpoch()),
                        partition.metadata(),
                        Errors.forCode(partition.errorCode()))
                );
            }
        }
        return responseData;
    }

    public Map<TopicPartition, PartitionData> partitionDataMap(String groupId) {
        if (groupLevelErrors.isEmpty()) {
            return responseDataV0ToV7();
        }
        return buildResponseData(groupId);
    }

    public static OffsetFetchResponse parse(ByteBuffer buffer, short version) {
        return new OffsetFetchResponse(new OffsetFetchResponseData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public OffsetFetchResponseData data() {
        return data;
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 4;
    }
}
