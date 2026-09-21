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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.record.internal.RecordBatch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class TxnOffsetCommitRequest extends AbstractRequest {
    public static final short LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2 = 4;

    /**
     * @return true if the given version returns {@code GROUP_ID_NOT_FOUND} directly when the
     *         group is not found; false if the legacy mapping to {@code ILLEGAL_GENERATION}
     *         is used (KIP-1319).
     */
    public static boolean supportsGroupIdNotFoundError(short version) {
        return version >= 6;
    }

    /**
     * @return true if the given version returns {@code STALE_MEMBER_EPOCH} directly when the
     *         member epoch is stale; false if the legacy mapping to {@code ILLEGAL_GENERATION}
     *         is used (KIP-1319).
     */
    public static boolean supportsStaleMemberEpochError(short version) {
        return version >= 6;
    }

    private final TxnOffsetCommitRequestData data;

    public static class Builder extends AbstractRequest.Builder<TxnOffsetCommitRequest> {

        public final TxnOffsetCommitRequestData data;

        private Builder(
            final TxnOffsetCommitRequestData data,
            final short oldestAllowedVersion,
            final short latestAllowedVersion
        ) {
            super(ApiKeys.TXN_OFFSET_COMMIT, oldestAllowedVersion, latestAllowedVersion);
            this.data = data;
        }

        public static Builder forTopicNames(
            final TxnOffsetCommitRequestData data,
            final boolean isTransactionV2Enabled
        ) {
            return new Builder(
                data,
                ApiKeys.TXN_OFFSET_COMMIT.oldestVersion(),
                isTransactionV2Enabled ? (short) 5 : LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2
            );
        }

        public static Builder forTopicIdsOrNames(
            final TxnOffsetCommitRequestData data,
            final boolean isTransactionV2Enabled
        ) {
            return new Builder(
                data,
                ApiKeys.TXN_OFFSET_COMMIT.oldestVersion(),
                isTransactionV2Enabled
                    ? ApiKeys.TXN_OFFSET_COMMIT.latestVersion()
                    : LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2
            );
        }

        @Override
        public TxnOffsetCommitRequest build(short version) {
            if (version < 3 && groupMetadataSet()) {
                throw new UnsupportedVersionException("Broker doesn't support group metadata commit API on version " + version
                    + ", minimum supported request version is 3 which requires brokers to be on version 2.5 or above.");
            }
            if (version >= 6) {
                for (TxnOffsetCommitRequestTopic topic : data.topics()) {
                    if (topic.topicId() == null || topic.topicId().equals(Uuid.ZERO_UUID)) {
                        throw new UnsupportedVersionException("The broker TxnOffsetCommit api version " +
                            version + " does require usage of topic ids.");
                    }
                }
            } else {
                for (TxnOffsetCommitRequestTopic topic : data.topics()) {
                    if (topic.name() == null || topic.name().isEmpty()) {
                        throw new UnsupportedVersionException("The broker TxnOffsetCommit api version " +
                            version + " does require usage of topic names.");
                    }
                }
            }
            return new TxnOffsetCommitRequest(data, version);
        }

        private boolean groupMetadataSet() {
            return !data.memberId().equals(JoinGroupRequest.UNKNOWN_MEMBER_ID) ||
                       data.generationIdOrMemberEpoch() != JoinGroupRequest.UNKNOWN_GENERATION_ID ||
                       data.groupInstanceId() != null;
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public TxnOffsetCommitRequest(TxnOffsetCommitRequestData data, short version) {
        super(ApiKeys.TXN_OFFSET_COMMIT, version);
        this.data = data;
    }

    public Map<TopicPartition, CommittedOffset> offsets() {
        List<TxnOffsetCommitRequestTopic> topics = data.topics();
        Map<TopicPartition, CommittedOffset> offsetMap = new HashMap<>();
        for (TxnOffsetCommitRequestTopic topic : topics) {
            for (TxnOffsetCommitRequestPartition partition : topic.partitions()) {
                offsetMap.put(new TopicPartition(topic.name(), partition.partitionIndex()),
                              new CommittedOffset(partition.committedOffset(),
                                                  partition.committedMetadata(),
                                                  RequestUtils.getLeaderEpoch(partition.committedLeaderEpoch()))
                );
            }
        }
        return offsetMap;
    }

    public static List<TxnOffsetCommitRequestTopic> getTopics(
        Map<TopicPartition, CommittedOffset> pendingTxnOffsetCommits
    ) {
        return getTopics(pendingTxnOffsetCommits, Map.of());
    }

    public static List<TxnOffsetCommitRequestTopic> getTopics(
        Map<TopicPartition, CommittedOffset> pendingTxnOffsetCommits,
        Map<String, Uuid> topicIds
    ) {
        Map<String, List<TxnOffsetCommitRequestPartition>> topicPartitionMap = new HashMap<>();
        for (Map.Entry<TopicPartition, CommittedOffset> entry : pendingTxnOffsetCommits.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            CommittedOffset offset = entry.getValue();

            List<TxnOffsetCommitRequestPartition> partitions =
                topicPartitionMap.getOrDefault(topicPartition.topic(), new ArrayList<>());
            partitions.add(new TxnOffsetCommitRequestPartition()
                               .setPartitionIndex(topicPartition.partition())
                               .setCommittedOffset(offset.offset)
                               .setCommittedLeaderEpoch(offset.leaderEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                               .setCommittedMetadata(offset.metadata)
            );
            topicPartitionMap.put(topicPartition.topic(), partitions);
        }
        return topicPartitionMap.entrySet().stream()
                   .map(entry -> new TxnOffsetCommitRequestTopic()
                                     .setName(entry.getKey())
                                     .setTopicId(topicIds.getOrDefault(entry.getKey(), Uuid.ZERO_UUID))
                                     .setPartitions(entry.getValue()))
                   .collect(Collectors.toList());
    }

    @Override
    public TxnOffsetCommitRequestData data() {
        return data;
    }

    @Override
    public TxnOffsetCommitResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new TxnOffsetCommitResponse(getErrorResponse(data, Errors.forException(e)).setThrottleTimeMs(throttleTimeMs));
    }

    @Override
    public TxnOffsetCommitResponse getErrorResponse(Throwable e) {
        return getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, e);
    }

    public static TxnOffsetCommitResponseData getErrorResponse(
        TxnOffsetCommitRequestData request,
        Errors error
    ) {
        TxnOffsetCommitResponseData response = new TxnOffsetCommitResponseData();
        request.topics().forEach(topic -> {
            TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic responseTopic = new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
                .setTopicId(topic.topicId())
                .setName(topic.name());
            response.topics().add(responseTopic);

            topic.partitions().forEach(partition ->
                responseTopic.partitions().add(new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                    .setPartitionIndex(partition.partitionIndex())
                    .setErrorCode(error.code()))
            );
        });
        return response;
    }

    public static TxnOffsetCommitRequest parse(Readable readable, short version) {
        return new TxnOffsetCommitRequest(new TxnOffsetCommitRequestData(
            readable, version), version);
    }

    public static class CommittedOffset {
        public final long offset;
        public final String metadata;
        public final Optional<Integer> leaderEpoch;

        public CommittedOffset(long offset, String metadata, Optional<Integer> leaderEpoch) {
            this.offset = offset;
            this.metadata = metadata;
            this.leaderEpoch = leaderEpoch;
        }

        @Override
        public String toString() {
            return "CommittedOffset(" +
                    "offset=" + offset +
                    ", leaderEpoch=" + leaderEpoch +
                    ", metadata='" + metadata + "')";
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof CommittedOffset)) {
                return false;
            }
            CommittedOffset otherOffset = (CommittedOffset) other;

            return this.offset == otherOffset.offset
                       && this.leaderEpoch.equals(otherOffset.leaderEpoch)
                       && Objects.equals(this.metadata, otherOffset.metadata);
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, leaderEpoch, metadata);
        }
    }
}
