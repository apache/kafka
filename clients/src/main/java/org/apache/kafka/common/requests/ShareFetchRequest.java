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

import org.apache.kafka.clients.consumer.internals.ShareAcquireMode;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ShareFetchRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<ShareFetchRequest> {

        private final ShareFetchRequestData data;

        public Builder(ShareFetchRequestData data) {
            super(ApiKeys.SHARE_FETCH);
            this.data = data;
        }

        public static Builder forConsumer(String groupId, ShareRequestMetadata metadata,
                                          int maxWait, int minBytes, int maxBytes, int maxRecords,
                                          int batchSize, byte shareAcquireMode, boolean isRenewAck,
                                          List<TopicIdPartition> send, List<TopicIdPartition> forget,
                                          Map<TopicIdPartition, List<ShareFetchRequestData.AcknowledgementBatch>> acknowledgementsMap) {
            ShareFetchRequestData data = new ShareFetchRequestData();
            data.setGroupId(groupId);
            boolean isClosingShareSession = false;
            if (metadata != null) {
                data.setMemberId(metadata.memberId().toString());
                data.setShareSessionEpoch(metadata.epoch());
                if (metadata.isFinalEpoch()) {
                    isClosingShareSession = true;
                }
            }
            data.setMaxWaitMs(maxWait);
            data.setMinBytes(minBytes);
            data.setMaxBytes(maxBytes);
            data.setMaxRecords(maxRecords);
            data.setBatchSize(batchSize);
            data.setShareAcquireMode(shareAcquireMode);
            data.setIsRenewAck(isRenewAck);

            // Build a map of topics to fetch keyed by topic ID, and within each a map of partitions keyed by index
            ShareFetchRequestData.FetchTopicCollection fetchTopics = new ShareFetchRequestData.FetchTopicCollection();

            // First, start by adding the list of topic-partitions we are fetching
            if (!isClosingShareSession) {
                for (TopicIdPartition tip : send) {
                    ShareFetchRequestData.FetchTopic fetchTopic = fetchTopics.find(tip.topicId());
                    if (fetchTopic == null) {
                        fetchTopic = new ShareFetchRequestData.FetchTopic()
                                .setTopicId(tip.topicId())
                                .setPartitions(new ShareFetchRequestData.FetchPartitionCollection());
                        fetchTopics.add(fetchTopic);
                    }
                    ShareFetchRequestData.FetchPartition fetchPartition = fetchTopic.partitions().find(tip.partition());
                    if (fetchPartition == null) {
                        fetchPartition = new ShareFetchRequestData.FetchPartition()
                            .setPartitionIndex(tip.partition());
                        fetchTopic.partitions().add(fetchPartition);
                    }
                }
            }

            // Next, add acknowledgements that we are piggybacking onto the fetch. Generally, the list of
            // topic-partitions will be a subset, but if the assignment changes, there might be new entries to add
            for (Map.Entry<TopicIdPartition, List<ShareFetchRequestData.AcknowledgementBatch>> acknowledgeEntry : acknowledgementsMap.entrySet()) {
                TopicIdPartition tip = acknowledgeEntry.getKey();
                ShareFetchRequestData.FetchTopic fetchTopic = fetchTopics.find(tip.topicId());
                if (fetchTopic == null) {
                    fetchTopic = new ShareFetchRequestData.FetchTopic()
                            .setTopicId(tip.topicId())
                            .setPartitions(new ShareFetchRequestData.FetchPartitionCollection());
                    fetchTopics.add(fetchTopic);
                }
                ShareFetchRequestData.FetchPartition fetchPartition = fetchTopic.partitions().find(tip.partition());
                if (fetchPartition == null) {
                    fetchPartition = new ShareFetchRequestData.FetchPartition()
                            .setPartitionIndex(tip.partition());
                    fetchTopic.partitions().add(fetchPartition);
                }
                fetchPartition.setAcknowledgementBatches(acknowledgeEntry.getValue());
            }

            // Build up the data to fetch
            data.setTopics(fetchTopics);

            Builder builder = new Builder(data);
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
            if (version < 2) {
                // The v1 does not support AcknowledgeType RENEW.
                if (data.isRenewAck()) {
                    throw new UnsupportedVersionException("The v1 ShareFetch does not support AcknowledgeType.RENEW");
                }
                // The v1 only supports ShareAcquireMode.BATCH_OPTIMIZED.
                if (data.shareAcquireMode() != ShareAcquireMode.BATCH_OPTIMIZED.id()) {
                    throw new UnsupportedVersionException("The v1 ShareFetch only supports ShareAcquireMode.BATCH_OPTIMIZED");
                }
            }
            return new ShareFetchRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ShareFetchRequestData data;
    private volatile List<TopicIdPartition> shareFetchData = null;
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
        return ShareFetchResponse.of(error, throttleTimeMs, new LinkedHashMap<>(), List.of(), 0);
    }

    public static ShareFetchRequest parse(Readable readable, short version) {
        return new ShareFetchRequest(
                new ShareFetchRequestData(readable, version),
                version
        );
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

    public List<TopicIdPartition> shareFetchData(Map<Uuid, String> topicNames) {
        if (shareFetchData == null) {
            synchronized (this) {
                if (shareFetchData == null) {
                    // Assigning the lazy-initialized `shareFetchData` in the last step
                    // to avoid other threads accessing a half-initialized object.
                    final List<TopicIdPartition> shareFetchDataTmp = new ArrayList<>();
                    data.topics().forEach(shareFetchTopic -> {
                        String name = topicNames.get(shareFetchTopic.topicId());
                        shareFetchTopic.partitions().forEach(shareFetchPartition -> {
                            // Topic name may be null here if the topic name was unable to be resolved using the topicNames map.
                            shareFetchDataTmp.add(new TopicIdPartition(shareFetchTopic.topicId(), shareFetchPartition.partitionIndex(), name));
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