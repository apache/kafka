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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class ShareFetchRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<ShareFetchRequest> {

        private final ShareFetchRequestData data;

        public Builder(ShareFetchRequestData data) {
            this(data, false);
        }

        public Builder(ShareFetchRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.SHARE_FETCH, enableUnstableLastVersion);
            this.data = data;
        }

        public static Builder forConsumer(int maxWait, int minBytes, Map<TopicPartition, TopicIdPartition> send,
                                          Map<TopicIdPartition, List<ShareFetchRequestData.AcknowledgementBatch>> acknowledgements) {
            ShareFetchRequestData data = new ShareFetchRequestData();
            data.setMaxWaitMs(maxWait);
            data.setMinBytes(minBytes);

            // We collect the partitions in a single FetchTopic only if they appear sequentially in the fetchData
            data.setTopics(new ArrayList<>());
            ShareFetchRequestData.FetchTopic fetchTopic = null;
            for (TopicIdPartition topicPartition : send.values()) {
                if (fetchTopic == null || !topicPartition.topicId().equals(fetchTopic.topicId())) {
                    fetchTopic = new ShareFetchRequestData.FetchTopic()
                            .setTopicId(topicPartition.topicId())
                            .setPartitions(new ArrayList<>());
                    data.topics().add(fetchTopic);
                }
                ShareFetchRequestData.FetchPartition fetchPartition = new ShareFetchRequestData.FetchPartition()
                        .setPartitionIndex(topicPartition.partition())
                        .setCurrentLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH);

                // Get the list of Acknowledgments for the current partition
                List<ShareFetchRequestData.AcknowledgementBatch> acknowledgementBatches = acknowledgements.get(topicPartition);
                if (acknowledgementBatches != null) {
                    fetchPartition.setAcknowledgementBatches(acknowledgementBatches);
                }


                fetchTopic.partitions().add(fetchPartition);
            }

            return new Builder(data, true);
        }

        public Builder forShareSession(String groupId, ShareFetchMetadata metadata) {
            data.setGroupId(groupId);
            if (metadata != null) {
                data.setShareSessionEpoch(metadata.epoch());
                data.setMemberId(metadata.memberId().toString());
            }
            return this;
        }

        @Override
        public ShareFetchRequest build(short version) {
            return new ShareFetchRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }

        public Builder setMaxBytes(int maxBytes) {
            data.setMaxBytes(maxBytes);
            return this;
        }
    }

    private final ShareFetchRequestData data;
    private volatile LinkedHashMap<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData = null;
    private volatile List<TopicIdPartition> toForget = null;

    public ShareFetchRequest(ShareFetchRequestData data, short version) {
        super(ApiKeys.SHARE_FETCH, version);
        this.data = data;
    }

    @Override
    public ShareFetchRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new ShareFetchResponse(new ShareFetchResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code()));
    }

    public static ShareFetchRequest parse(ByteBuffer buffer, short version) {
        return new ShareFetchRequest(
            new ShareFetchRequestData(new ByteBufferAccessor(buffer), version),
            version
        );
    }

    public static final class SharePartitionData {
        public final Uuid topicId;
        public final int maxBytes;
        public final Optional<Integer> currentLeaderEpoch;

        public SharePartitionData(
                Uuid topicId,
                int maxBytes,
                Optional<Integer> currentLeaderEpoch
        ) {
            this.topicId = topicId;
            this.maxBytes = maxBytes;
            this.currentLeaderEpoch = currentLeaderEpoch;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ShareFetchRequest.SharePartitionData that = (ShareFetchRequest.SharePartitionData) o;
            return Objects.equals(topicId, that.topicId) &&
                    maxBytes == that.maxBytes &&
                    Objects.equals(currentLeaderEpoch, that.currentLeaderEpoch);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicId, maxBytes, currentLeaderEpoch);
        }

        @Override
        public String toString() {
            return "SharePartitionData(" +
                    "topicId=" + topicId +
                    ", maxBytes=" + maxBytes +
                    ", currentLeaderEpoch=" + currentLeaderEpoch +
                    ')';
        }
    }

    public int minBytes() {
        return data.minBytes();
    }

    public int maxBytes() {
        return data.maxBytes();
    }

    public int maxWait() {
        return data.maxWaitMs();
    }

    private static Optional<Integer> optionalEpoch(int rawEpochValue) {
        if (rawEpochValue < 0) {
            return Optional.empty();
        } else {
            return Optional.of(rawEpochValue);
        }
    }

    public Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData(Map<Uuid, String> topicNames) {
        if (shareFetchData == null) {
            synchronized (this) {
                if (shareFetchData == null) {
                    // Assigning the lazy-initialized `shareFetchData` in the last step
                    // to avoid other threads accessing a half-initialized object.
                    final LinkedHashMap<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchDataTmp = new LinkedHashMap<>();
                    data.topics().forEach(shareFetchTopic -> {
                        String name = topicNames.get(shareFetchTopic.topicId());
                        shareFetchTopic.partitions().forEach(shareFetchPartition ->
                                // Topic name may be null here if the topic name was unable to be resolved using the topicNames map.
                                shareFetchDataTmp.put(new TopicIdPartition(shareFetchTopic.topicId(), new TopicPartition(name, shareFetchPartition.partitionIndex())),
                                        new ShareFetchRequest.SharePartitionData(
                                                shareFetchTopic.topicId(),
                                                shareFetchPartition.partitionMaxBytes(),
                                                optionalEpoch(shareFetchPartition.currentLeaderEpoch())
                                        )
                                )
                        );
                    });
                    shareFetchData = shareFetchDataTmp;
                }
            }
        }
        return shareFetchData;
    }

    public List<TopicIdPartition> forgottenTopics(Map<Uuid, String> topicNames) {
        if (toForget == null) {
            synchronized (this) {
                if (toForget == null) {
                    // Assigning the lazy-initialized `toForget` in the last step
                    // to avoid other threads accessing a half-initialized object.
                    final List<TopicIdPartition> toForgetTmp = new ArrayList<>();
                    data.forgottenTopicsData().forEach(forgottenTopic -> {
                        String name = topicNames.get(forgottenTopic.topicId());
                        // Topic name may be null here if the topic name was unable to be resolved using the topicNames map.
                        forgottenTopic.partitions().forEach(partitionId -> toForgetTmp.add(new TopicIdPartition(forgottenTopic.topicId(), new TopicPartition(name, partitionId))));
                    });
                    toForget = toForgetTmp;
                }
            }
        }
        return toForget;
    }
}