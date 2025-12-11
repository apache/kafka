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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility methods for preparing requests to the OffsetsForLeaderEpoch API and handling responses.
 */
public final class OffsetsForLeaderEpochUtils {

    private static final Logger LOG = LoggerFactory.getLogger(OffsetsForLeaderEpochUtils.class);

    private OffsetsForLeaderEpochUtils() {}

    static AbstractRequest.Builder<OffsetsForLeaderEpochRequest> prepareRequest(
            Map<TopicIdPartition, SubscriptionState.FetchPosition> requestData) {
        OffsetForLeaderTopicCollection topics = new OffsetForLeaderTopicCollection(requestData.size());
        requestData.forEach((topicIdPartition, fetchPosition) ->
                fetchPosition.offsetEpoch.ifPresent(fetchEpoch -> {
                    OffsetForLeaderTopic topic = topics.find(topicIdPartition.topic());
                    if (topic == null) {
                        topic = new OffsetForLeaderTopic().setTopic(topicIdPartition.topic())
                                .setTopicId(topicIdPartition.topicId());
                        topics.add(topic);
                    }
                    topic.partitions().add(new OffsetForLeaderPartition()
                            .setPartition(topicIdPartition.partition())
                            .setLeaderEpoch(fetchEpoch)
                            .setCurrentLeaderEpoch(fetchPosition.currentLeader.epoch
                                    .orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                    );
                })
        );
        return OffsetsForLeaderEpochRequest.Builder.forConsumer(topics);
    }

    /**
     * Find the TopicIdPartition from the request data that matches the response.
     * This handles backward compatibility when the response may not include topic IDs.
     *
     * @param requestPartitions The set of partitions from the request
     * @param responseTopicId The topic ID from the response (may be null or ZERO_UUID)
     * @param responseTopicName The topic name from the response
     * @param responsePartition The partition number from the response
     * @return The matching TopicIdPartition, or null if not found
     */
    private static TopicIdPartition findTopicIdPartition(
            Set<TopicIdPartition> requestPartitions,
            Uuid responseTopicId,
            String responseTopicName,
            int responsePartition) {

        for (TopicIdPartition requestPartition : requestPartitions) {
            // Check if partition number matches
            if (requestPartition.partition() != responsePartition) {
                continue;
            }

            // If response has a valid topic ID, try to match by topic ID
            if (responseTopicId != null && !responseTopicId.equals(Uuid.ZERO_UUID)) {
                if (requestPartition.topicId().equals(responseTopicId)) {
                    return requestPartition;
                }
            }

            // Fall back to matching by topic name
            if (responseTopicName != null && requestPartition.topic().equals(responseTopicName)) {
                return requestPartition;
            }
        }

        return null;
    }

    public static OffsetForEpochResult handleResponse(
            Map<TopicIdPartition, SubscriptionState.FetchPosition> requestData,
            OffsetsForLeaderEpochResponse response) {

        Set<TopicIdPartition> partitionsToRetry = new HashSet<>(requestData.keySet());
        Set<String> unauthorizedTopics = new HashSet<>();
        Map<TopicIdPartition, EpochEndOffset> endOffsets = new HashMap<>();

        for (OffsetForLeaderTopicResult topic : response.data().topics()) {
            for (EpochEndOffset partition : topic.partitions()) {
                // Try to match the response partition with the request data
                // Handle both topic ID and topic name for backward compatibility
                TopicIdPartition topicIdPartition = findTopicIdPartition(
                    requestData.keySet(),
                    topic.topicId(),
                    topic.topic(),
                    partition.partition()
                );

                if (topicIdPartition == null) {
                    LOG.warn("Received unrequested topic or partition (topicId={}, topic={}, partition={}) from response, ignoring.",
                        topic.topicId(), topic.topic(), partition.partition());
                    continue;
                }

                Errors error = Errors.forCode(partition.errorCode());
                switch (error) {
                    case NONE:
                        LOG.debug("Handling OffsetsForLeaderEpoch response for {}. Got offset {} for epoch {}.",
                                topicIdPartition, partition.endOffset(), partition.leaderEpoch());
                        endOffsets.put(topicIdPartition, partition);
                        partitionsToRetry.remove(topicIdPartition);
                        break;
                    case NOT_LEADER_OR_FOLLOWER:
                    case REPLICA_NOT_AVAILABLE:
                    case KAFKA_STORAGE_ERROR:
                    case OFFSET_NOT_AVAILABLE:
                    case LEADER_NOT_AVAILABLE:
                    case FENCED_LEADER_EPOCH:
                    case UNKNOWN_LEADER_EPOCH:
                        LOG.debug("Attempt to fetch offsets for partition {} failed due to {}, retrying.",
                                topicIdPartition, error);
                        break;
                    case UNKNOWN_TOPIC_OR_PARTITION:
                        LOG.warn("Received unknown topic or partition error in OffsetsForLeaderEpoch request for partition {}.",
                                topicIdPartition);
                        break;
                    case TOPIC_AUTHORIZATION_FAILED:
                        unauthorizedTopics.add(topicIdPartition.topic());
                        partitionsToRetry.remove(topicIdPartition);
                        break;
                    default:
                        LOG.warn("Attempt to fetch offsets for partition {} failed due to: {}, retrying.",
                                topicIdPartition, error.message());
                }
            }
        }

        if (!unauthorizedTopics.isEmpty())
            throw new TopicAuthorizationException(unauthorizedTopics);

        return new OffsetForEpochResult(endOffsets, partitionsToRetry.stream()
                .map(TopicIdPartition::topicPartition)
                .collect(Collectors.toSet()));
    }

    static class OffsetForEpochResult {
        private final Map<TopicIdPartition, EpochEndOffset> endOffsets;
        private final Set<TopicPartition> partitionsToRetry;

        OffsetForEpochResult(Map<TopicIdPartition, EpochEndOffset> endOffsets, Set<TopicPartition> partitionsNeedingRetry) {
            this.endOffsets = endOffsets;
            this.partitionsToRetry = partitionsNeedingRetry;
        }

        public Map<TopicIdPartition, EpochEndOffset> endOffsets() {
            return endOffsets;
        }

        public Set<TopicPartition> partitionsToRetry() {
            return partitionsToRetry;
        }
    }
}
