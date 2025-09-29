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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

        public static Builder forConsumer(String groupId, ShareRequestMetadata metadata,
                                          int maxWait, int minBytes, int maxBytes, int fetchSize,
                                          List<TopicIdPartition> send, List<TopicIdPartition> forget,
                                          Map<TopicIdPartition, List<ShareFetchRequestData.AcknowledgementBatch>> acknowledgementsMap) {
            ShareFetchRequestData data = new ShareFetchRequestData();
            data.setGroupId(groupId);
            int ackOnlyPartitionMaxBytes = fetchSize;
            boolean isClosingShareSession = false;
            if (metadata != null) {
                data.setMemberId(metadata.memberId().toString());
                data.setShareSessionEpoch(metadata.epoch());
                if (metadata.isFinalEpoch()) {
                    isClosingShareSession = true;
                    ackOnlyPartitionMaxBytes = 0;
                }
            }
            data.setMaxWaitMs(maxWait);
            data.setMinBytes(minBytes);
            data.setMaxBytes(maxBytes);

            // Build a map of topics to fetch keyed by topic ID, and within each a map of partitions keyed by index
            Map<Uuid, Map<Integer, ShareFetchRequestData.FetchPartition>> fetchMap = new HashMap<>();

            // First, start by adding the list of topic-partitions we are fetching
            if (!isClosingShareSession) {
                for (TopicIdPartition tip : send) {
                    Map<Integer, ShareFetchRequestData.FetchPartition> partMap = fetchMap.computeIfAbsent(tip.topicId(), k -> new HashMap<>());
                    ShareFetchRequestData.FetchPartition fetchPartition = new ShareFetchRequestData.FetchPartition()
                            .setPartitionIndex(tip.partition())
                            .setPartitionMaxBytes(fetchSize);
                    partMap.put(tip.partition(), fetchPartition);
                }
            }

            // Next, add acknowledgements that we are piggybacking onto the fetch. Generally, the list of
            // topic-partitions will be a subset, but if the assignment changes, there might be new entries to add
            for (Map.Entry<TopicIdPartition, List<ShareFetchRequestData.AcknowledgementBatch>> acknowledgeEntry : acknowledgementsMap.entrySet()) {
                TopicIdPartition tip = acknowledgeEntry.getKey();
                Map<Integer, ShareFetchRequestData.FetchPartition> partMap = fetchMap.computeIfAbsent(tip.topicId(), k -> new HashMap<>());
                ShareFetchRequestData.FetchPartition fetchPartition = partMap.get(tip.partition());
                if (fetchPartition == null) {
                    fetchPartition = new ShareFetchRequestData.FetchPartition()
                            .setPartitionIndex(tip.partition())
                            .setPartitionMaxBytes(ackOnlyPartitionMaxBytes);
                    partMap.put(tip.partition(), fetchPartition);
                }
                fetchPartition.setAcknowledgementBatches(acknowledgeEntry.getValue());
            }

            // Build up the data to fetch
            if (!fetchMap.isEmpty()) {
                data.setTopics(new ArrayList<>());
                fetchMap.forEach((topicId, partMap) -> {
                    ShareFetchRequestData.FetchTopic fetchTopic = new ShareFetchRequestData.FetchTopic()
                            .setTopicId(topicId)
                            .setPartitions(new ArrayList<>());
                    partMap.forEach((index, fetchPartition) -> fetchTopic.partitions().add(fetchPartition));
                    data.topics().add(fetchTopic);
                });
            }

            Builder builder = new Builder(data, true);
            // And finally, forget the topic-partitions that are no longer in the session
            if (!forget.isEmpty()) {
                data.setForgottenTopicsData(new ArrayList<>());
                builder.updateForgottenData(forget);
            }

            return builder;
        }

        public void updateForgottenData(List<TopicIdPartition> forget) {
            Map<Uuid, List<Integer>> forgetMap = new HashMap<>();
            for (TopicIdPartition tip : forget) {
                List<Integer> partList = forgetMap.computeIfAbsent(tip.topicId(), k -> new ArrayList<>());
                partList.add(tip.partition());
            }
            forgetMap.forEach((topicId, partList) -> {
                ShareFetchRequestData.ForgottenTopic forgetTopic = new ShareFetchRequestData.ForgottenTopic()
                        .setTopicId(topicId)
                        .setPartitions(new ArrayList<>());
                partList.forEach(index -> forgetTopic.partitions().add(index));
                data.forgottenTopicsData().add(forgetTopic);
            });
        }

        public ShareFetchRequestData data() {
            return data;
        }

        @Override
        public ShareFetchRequest build(short version) {
            return new ShareFetchRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
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

        public SharePartitionData(
                Uuid topicId,
                int maxBytes
        ) {
            this.topicId = topicId;
            this.maxBytes = maxBytes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ShareFetchRequest.SharePartitionData that = (ShareFetchRequest.SharePartitionData) o;
            return Objects.equals(topicId, that.topicId) &&
                    maxBytes == that.maxBytes;
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicId, maxBytes);
        }

        @Override
        public String toString() {
            return "SharePartitionData(" +
                    "topicId=" + topicId +
                    ", maxBytes=" + maxBytes +
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

    public Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData(Map<Uuid, String> topicNames) {
        if (shareFetchData == null) {
            synchronized (this) {
                if (shareFetchData == null) {
                    // Assigning the lazy-initialized `shareFetchData` in the last step
                    // to avoid other threads accessing a half-initialized object.
                    final LinkedHashMap<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchDataTmp = new LinkedHashMap<>();
                    data.topics().forEach(shareFetchTopic -> {
                        String name = topicNames.get(shareFetchTopic.topicId());
                        shareFetchTopic.partitions().forEach(shareFetchPartition -> {
                            // Topic name may be null here if the topic name was unable to be resolved using the topicNames map.
                            shareFetchDataTmp.put(new TopicIdPartition(shareFetchTopic.topicId(), new TopicPartition(name, shareFetchPartition.partitionIndex())),
                                    new ShareFetchRequest.SharePartitionData(
                                            shareFetchTopic.topicId(),
                                            shareFetchPartition.partitionMaxBytes()
                                    )
                            );
                        });
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