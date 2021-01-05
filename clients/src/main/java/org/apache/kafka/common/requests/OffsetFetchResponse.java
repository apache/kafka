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
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartition;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopic;
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

    /**
     * Constructor for all versions without throttle time.
     * @param error Potential coordinator or group level error code (for api version 2 and later)
     * @param responseData Fetched offset information grouped by topic-partition
     */
    public OffsetFetchResponse(Errors error, Map<TopicPartition, PartitionData> responseData) {
        this(DEFAULT_THROTTLE_TIME, error, responseData);
    }

    /**
     * Constructor with throttle time
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

    public OffsetFetchResponse(OffsetFetchResponseData data, short version) {
        super(ApiKeys.OFFSET_FETCH);
        this.data = data;
        // for version 2 and later use the top-level error code (in ERROR_CODE_KEY_NAME) from the response.
        // for older versions there is no top-level error in the response and all errors are partition errors,
        // so if there is a group or coordinator error at the partition level use that as the top-level error.
        // this way clients can depend on the top-level error regardless of the offset fetch version.
        this.error = version >= 2 ? Errors.forCode(data.errorCode()) : topLevelError(data);
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

    public boolean hasError() {
        return error != Errors.NONE;
    }

    public Errors error() {
        return error;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> counts = new HashMap<>();
        updateErrorCounts(counts, error);
        data.topics().forEach(topic ->
                topic.partitions().forEach(partition ->
                        updateErrorCounts(counts, Errors.forCode(partition.errorCode()))));
        return counts;
    }

    public Map<TopicPartition, PartitionData> responseData() {
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
