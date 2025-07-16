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

import org.apache.kafka.clients.admin.AlterShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.AlterShareGroupOffsetsRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AlterShareGroupOffsetsRequest;
import org.apache.kafka.common.requests.AlterShareGroupOffsetsResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class is the handler for {@link KafkaAdminClient#alterShareGroupOffsets(String, Map, AlterShareGroupOffsetsOptions)} call
 */
public class AlterShareGroupOffsetsHandler extends AdminApiHandler.Batched<CoordinatorKey, Map<TopicPartition, ApiException>> {

    private final CoordinatorKey groupId;

    private final Logger log;

    private final Map<TopicPartition, Long> offsets;

    private final CoordinatorStrategy lookupStrategy;

    public AlterShareGroupOffsetsHandler(String groupId, Map<TopicPartition, Long> offsets, LogContext logContext) {
        this.groupId = CoordinatorKey.byGroupId(groupId);
        this.offsets = offsets;
        this.log = logContext.logger(AlterShareGroupOffsetsHandler.class);
        this.lookupStrategy = new CoordinatorStrategy(FindCoordinatorRequest.CoordinatorType.GROUP, logContext);
    }

    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, ApiException>> newFuture(String groupId) {
        return AdminApiFuture.forKeys(Set.of(CoordinatorKey.byGroupId(groupId)));
    }

    private void validateKeys(Set<CoordinatorKey> groupIds) {
        if (!groupIds.equals(Set.of(groupId))) {
            throw new IllegalArgumentException("Received unexpected group ids " + groupIds +
                " (expected only " + Set.of(groupId) + ")");
        }
    }

    @Override
    AlterShareGroupOffsetsRequest.Builder buildBatchedRequest(int brokerId, Set<CoordinatorKey> groupIds) {
        var data = new AlterShareGroupOffsetsRequestData().setGroupId(groupId.idValue);
        offsets.forEach((tp, offset) -> {
            var topic = data.topics().find(tp.topic());
            if (topic == null) {
                topic = new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestTopic()
                        .setTopicName(tp.topic());
                data.topics().add(topic);
            }
            topic.partitions().add(new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestPartition()
                    .setPartitionIndex(tp.partition())
                    .setStartOffset(offset));
        });
        return new AlterShareGroupOffsetsRequest.Builder(data);
    }

    @Override
    public String apiName() {
        return "alterShareGroupOffsets";
    }

    @Override
    public ApiResult<CoordinatorKey, Map<TopicPartition, ApiException>> handleResponse(Node broker, Set<CoordinatorKey> keys, AbstractResponse abstractResponse) {
        validateKeys(keys);

        AlterShareGroupOffsetsResponse response = (AlterShareGroupOffsetsResponse) abstractResponse;
        final Set<CoordinatorKey> groupsToUnmap = new HashSet<>();
        final Set<CoordinatorKey> groupsToRetry = new HashSet<>();
        final Map<TopicPartition, ApiException> partitionResults = new HashMap<>();

        if (response.data().errorCode() != Errors.NONE.code()) {
            final Errors topLevelError = Errors.forCode(response.data().errorCode());
            final String topLevelErrorMessage = response.data().errorMessage();

            offsets.forEach((topicPartition, offset) ->
                handleError(
                    groupId,
                    topicPartition,
                    topLevelError,
                    topLevelErrorMessage,
                    partitionResults,
                    groupsToUnmap,
                    groupsToRetry
                ));
        } else {
            response.data().responses().forEach(topic -> topic.partitions().forEach(partition -> {
                final Errors partitionError = Errors.forCode(partition.errorCode());
                if (partitionError != Errors.NONE) {
                    String errorMessageToLog = partition.errorMessage() == null ? "" : partition.errorMessage();
                    log.debug("AlterShareGroupOffsets request for group id {} and topic-partition {}-{} failed and returned error {}. {}",
                        groupId.idValue, topic.topicName(), partition.partitionIndex(), partitionError.name(), errorMessageToLog);
                }
                partitionResults.put(new TopicPartition(topic.topicName(), partition.partitionIndex()), partitionError.exception(partition.errorMessage()));
            }));
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
        String errorMessage,
        Map<TopicPartition, ApiException> partitionResults,
        Set<CoordinatorKey> groupsToUnmap,
        Set<CoordinatorKey> groupsToRetry
    ) {
        String errorMessageToLog = errorMessage == null ? "" : errorMessage;
        switch (error) {
            case COORDINATOR_LOAD_IN_PROGRESS:
            case REBALANCE_IN_PROGRESS:
                log.debug("AlterShareGroupOffsets request for group id {} returned error {}. Will retry. {}",
                        groupId.idValue, error, errorMessageToLog);
                groupsToRetry.add(groupId);
                break;
            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                log.debug("AlterShareGroupOffsets request for group id {} returned error {}. Will rediscover the coordinator and retry. {}",
                        groupId.idValue, error, errorMessageToLog);
                groupsToUnmap.add(groupId);
                break;
            case GROUP_ID_NOT_FOUND:
            case NON_EMPTY_GROUP:
            case INVALID_REQUEST:
            case UNKNOWN_SERVER_ERROR:
            case KAFKA_STORAGE_ERROR:
            case GROUP_AUTHORIZATION_FAILED:
                log.debug("AlterShareGroupOffsets request for group id {} failed due to error {}. {}",
                        groupId.idValue, error, errorMessageToLog);
                partitionResults.put(topicPartition, error.exception(errorMessage));
                break;
            default:
                log.error("AlterShareGroupOffsets request for group id {} failed due to unexpected error {}. {}",
                        groupId.idValue, error, errorMessageToLog);
                partitionResults.put(topicPartition, error.exception(errorMessage));
        }
    }

    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }
}
