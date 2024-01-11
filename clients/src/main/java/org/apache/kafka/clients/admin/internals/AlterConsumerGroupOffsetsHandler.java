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
package org.apache.kafka.clients.admin.internals;

import static java.util.Collections.singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class AlterConsumerGroupOffsetsHandler extends AdminApiHandler.Batched<CoordinatorKey, Map<TopicPartition, Errors>> {

    private final CoordinatorKey groupId;
    private final Map<TopicPartition, OffsetAndMetadata> offsets;
    private final Logger log;
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;

    public AlterConsumerGroupOffsetsHandler(
        String groupId,
        Map<TopicPartition, OffsetAndMetadata> offsets,
        LogContext logContext
    ) {
        this.groupId = CoordinatorKey.byGroupId(groupId);
        this.offsets = offsets;
        this.log = logContext.logger(AlterConsumerGroupOffsetsHandler.class);
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
    }

    @Override
    public String apiName() {
        return "offsetCommit";
    }

    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, Errors>> newFuture(
        String groupId
    ) {
        return AdminApiFuture.forKeys(Collections.singleton(CoordinatorKey.byGroupId(groupId)));
    }

    private void validateKeys(Set<CoordinatorKey> groupIds) {
        if (!groupIds.equals(singleton(groupId))) {
            throw new IllegalArgumentException("Received unexpected group ids " + groupIds +
                " (expected only " + singleton(groupId) + ")");
        }
    }

    @Override
    public OffsetCommitRequest.Builder buildBatchedRequest(
        int coordinatorId,
        Set<CoordinatorKey> groupIds
    ) {
        validateKeys(groupIds);

        Map<String, OffsetCommitRequestTopic> offsetData = new HashMap<>();
        offsets.forEach((topicPartition, offsetAndMetadata) -> {
            OffsetCommitRequestTopic topic = offsetData.computeIfAbsent(
                topicPartition.topic(),
                key -> new OffsetCommitRequestTopic().setName(topicPartition.topic())
            );

            topic.partitions().add(new OffsetCommitRequestPartition()
                .setCommittedOffset(offsetAndMetadata.offset())
                .setCommittedLeaderEpoch(offsetAndMetadata.leaderEpoch().orElse(-1))
                .setCommittedMetadata(offsetAndMetadata.metadata())
                .setPartitionIndex(topicPartition.partition()));
        });

        OffsetCommitRequestData data = new OffsetCommitRequestData()
            .setGroupId(groupId.idValue)
            .setTopics(new ArrayList<>(offsetData.values()));

        return new OffsetCommitRequest.Builder(data);
    }

    @Override
    public ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> handleResponse(
        Node coordinator,
        Set<CoordinatorKey> groupIds,
        AbstractResponse abstractResponse
    ) {
        validateKeys(groupIds);

        final OffsetCommitResponse response = (OffsetCommitResponse) abstractResponse;
        final Set<CoordinatorKey> groupsToUnmap = new HashSet<>();
        final Set<CoordinatorKey> groupsToRetry = new HashSet<>();
        final Map<TopicPartition, Errors> partitionResults = new HashMap<>();

        for (OffsetCommitResponseTopic topic : response.data().topics()) {
            for (OffsetCommitResponsePartition partition : topic.partitions()) {
                TopicPartition topicPartition = new TopicPartition(topic.name(), partition.partitionIndex());
                Errors error = Errors.forCode(partition.errorCode());

                if (error != Errors.NONE) {
                    handleError(
                        groupId,
                        topicPartition,
                        error,
                        partitionResults,
                        groupsToUnmap,
                        groupsToRetry
                    );
                } else {
                    partitionResults.put(topicPartition, error);
                }
            }
        }

        if (groupsToUnmap.isEmpty() && groupsToRetry.isEmpty()) {
            return ApiResult.completed(groupId, partitionResults);
        } else {
            return ApiResult.unmapped(new ArrayList<>(groupsToUnmap));
        }
    }

    private void handleError(
        CoordinatorKey groupId,
        TopicPartition topicPartition,
        Errors error,
        Map<TopicPartition, Errors> partitionResults,
        Set<CoordinatorKey> groupsToUnmap,
        Set<CoordinatorKey> groupsToRetry
    ) {
        switch (error) {
            // If the coordinator is in the middle of loading, or rebalance is in progress, then we just need to retry.
            case COORDINATOR_LOAD_IN_PROGRESS:
            case REBALANCE_IN_PROGRESS:
                log.debug("OffsetCommit request for group id {} returned error {}. Will retry.",
                    groupId.idValue, error);
                groupsToRetry.add(groupId);
                break;

            // If the coordinator is not available, then we unmap and retry.
            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                log.debug("OffsetCommit request for group id {} returned error {}. Will rediscover the coordinator and retry.",
                    groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;

            // Group level errors.
            case INVALID_GROUP_ID:
            case INVALID_COMMIT_OFFSET_SIZE:
            case GROUP_AUTHORIZATION_FAILED:
            case GROUP_ID_NOT_FOUND:
            // Member level errors.
            case UNKNOWN_MEMBER_ID:
            case STALE_MEMBER_EPOCH:
                log.debug("OffsetCommit request for group id {} failed due to error {}.",
                    groupId.idValue, error);
                partitionResults.put(topicPartition, error);
                break;

            // TopicPartition level errors.
            case UNKNOWN_TOPIC_OR_PARTITION:
            case OFFSET_METADATA_TOO_LARGE:
            case TOPIC_AUTHORIZATION_FAILED:
                log.debug("OffsetCommit request for group id {} and partition {} failed due" +
                    " to error {}.", groupId.idValue, topicPartition, error);
                partitionResults.put(topicPartition, error);
                break;

            // Unexpected errors.
            default:
                log.error("OffsetCommit request for group id {} and partition {} failed due" +
                    " to unexpected error {}.", groupId.idValue, topicPartition, error);
                partitionResults.put(topicPartition, error);
        }
    }
}
