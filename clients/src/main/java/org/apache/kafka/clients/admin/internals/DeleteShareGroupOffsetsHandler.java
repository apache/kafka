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

import org.apache.kafka.clients.admin.DeleteShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.DeleteShareGroupOffsetsRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.DeleteShareGroupOffsetsRequest;
import org.apache.kafka.common.requests.DeleteShareGroupOffsetsResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class is the handler for {@link KafkaAdminClient#deleteShareGroupOffsets(String, Set, DeleteShareGroupOffsetsOptions)} call
 */
public class DeleteShareGroupOffsetsHandler extends AdminApiHandler.Batched<CoordinatorKey, Map<TopicPartition, ApiException>> {

    private final CoordinatorKey groupId;

    private final Logger log;

    private final Set<TopicPartition> partitions;

    private final CoordinatorStrategy lookupStrategy;

    public DeleteShareGroupOffsetsHandler(String groupId, Set<TopicPartition> partitions, LogContext logContext) {
        this.groupId = CoordinatorKey.byGroupId(groupId);
        this.partitions = partitions;
        this.log = logContext.logger(DeleteShareGroupOffsetsHandler.class);
        this.lookupStrategy = new CoordinatorStrategy(FindCoordinatorRequest.CoordinatorType.GROUP, logContext);
    }

    @Override
    public String apiName() {
        return "deleteShareGroupOffsets";
    }

    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, ApiException>> newFuture(String groupId) {
        return AdminApiFuture.forKeys(Collections.singleton(CoordinatorKey.byGroupId(groupId)));
    }

    private void validateKeys(Set<CoordinatorKey> groupIds) {
        if (!groupIds.equals(Collections.singleton(groupId))) {
            throw new IllegalArgumentException("Received unexpected group ids " + groupIds +
                " (expected only " + Collections.singleton(groupId) + ")");
        }
    }

    @Override
    DeleteShareGroupOffsetsRequest.Builder buildBatchedRequest(int brokerId, Set<CoordinatorKey> groupIds) {
        validateKeys(groupIds);

        final List<DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic> topics =
            new ArrayList<>();
        partitions.stream().collect(Collectors.groupingBy(TopicPartition::topic)).forEach((topic, topicPartitions) -> topics.add(
            new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                .setTopicName(topic)
                .setPartitions(topicPartitions.stream()
                    .map(TopicPartition::partition)
                    .collect(Collectors.toList())
                )
        ));

        return new DeleteShareGroupOffsetsRequest.Builder(
            new DeleteShareGroupOffsetsRequestData()
                .setGroupId(groupId.idValue)
                .setTopics(topics)
        );
    }

    @Override
    public ApiResult<CoordinatorKey, Map<TopicPartition, ApiException>> handleResponse(
        Node coordinator,
        Set<CoordinatorKey> groupIds,
        AbstractResponse abstractResponse
    ) {
        validateKeys(groupIds);

        final DeleteShareGroupOffsetsResponse response = (DeleteShareGroupOffsetsResponse) abstractResponse;

        final Errors groupError = Errors.forCode(response.data().errorCode());
        final String groupErrorMessage = response.data().errorMessage();

        if (groupError != Errors.NONE) {
            final Set<CoordinatorKey> groupsToUnmap = new HashSet<>();
            final Map<CoordinatorKey, Throwable> groupsFailed = new HashMap<>();
            handleGroupError(groupId, groupError, groupErrorMessage, groupsFailed, groupsToUnmap);

            return new ApiResult<>(Collections.emptyMap(), groupsFailed, new ArrayList<>(groupsToUnmap));
        } else {
            final Map<TopicPartition, ApiException> partitionResults = new HashMap<>();
            response.data().responses().forEach(topic ->
                topic.partitions().forEach(partition -> {
                    if (partition.errorCode() != Errors.NONE.code()) {
                        final Errors partitionError = Errors.forCode(partition.errorCode());
                        final String partitionErrorMessage = partition.errorMessage();
                        log.debug("DeleteShareGroupOffsets request for group id {}, topic {} and partition {} failed and returned error {}." + partitionErrorMessage,
                            groupId.idValue, topic.topicName(), partition.partitionIndex(), partitionError);
                    }
                    partitionResults.put(
                        new TopicPartition(topic.topicName(), partition.partitionIndex()),
                        Errors.forCode(partition.errorCode()).exception(partition.errorMessage())
                    );
                })
            );

            return ApiResult.completed(groupId, partitionResults);
        }
    }

    private void handleGroupError(
        CoordinatorKey groupId,
        Errors error,
        String errorMessage,
        Map<CoordinatorKey, Throwable> failed,
        Set<CoordinatorKey> groupsToUnmap
    ) {
        switch (error) {
            case COORDINATOR_LOAD_IN_PROGRESS:
            case REBALANCE_IN_PROGRESS:
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug("DeleteShareGroupOffsets request for group id {} failed because the coordinator" +
                    " is still in the process of loading state. Will retry. " + errorMessage, groupId.idValue);
                break;
            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug("DeleteShareGroupOffsets request for group id {} returned error {}. Will rediscover the coordinator and retry. " + errorMessage,
                    groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;
            case INVALID_GROUP_ID:
            case GROUP_ID_NOT_FOUND:
            case NON_EMPTY_GROUP:
            case INVALID_REQUEST:
            case UNKNOWN_SERVER_ERROR:
            case KAFKA_STORAGE_ERROR:
            case GROUP_AUTHORIZATION_FAILED:
                log.debug("DeleteShareGroupOffsets request for group id {} failed due to error {}. " + errorMessage, groupId.idValue, error);
                failed.put(groupId, error.exception(errorMessage));
                break;
            default:
                log.error("DeleteShareGroupOffsets request for group id {} failed due to unexpected error {}. " + errorMessage, groupId.idValue, error);
                failed.put(groupId, error.exception(errorMessage));
        }
    }
}