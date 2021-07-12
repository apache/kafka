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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class ListConsumerGroupOffsetsHandler implements AdminApiHandler<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> {

    private final CoordinatorKey groupId;
    private final List<TopicPartition> partitions;
    private final Logger log;
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;

    public ListConsumerGroupOffsetsHandler(
        String groupId,
        List<TopicPartition> partitions,
        LogContext logContext
    ) {
        this.groupId = CoordinatorKey.byGroupId(groupId);
        this.partitions = partitions;
        this.log = logContext.logger(ListConsumerGroupOffsetsHandler.class);
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
    }

    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> newFuture(
        String groupId
    ) {
        return AdminApiFuture.forKeys(Collections.singleton(CoordinatorKey.byGroupId(groupId)));
    }

    @Override
    public String apiName() {
        return "offsetFetch";
    }

    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    @Override
    public OffsetFetchRequest.Builder buildRequest(int coordinatorId, Set<CoordinatorKey> keys) {
        // Set the flag to false as for admin client request,
        // we don't need to wait for any pending offset state to clear.
        return new OffsetFetchRequest.Builder(groupId.idValue, false, partitions, false);
    }

    @Override
    public ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> handleResponse(
        Node coordinator,
        Set<CoordinatorKey> groupIds,
        AbstractResponse abstractResponse
    ) {
        final OffsetFetchResponse response = (OffsetFetchResponse) abstractResponse;
        Map<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> completed = new HashMap<>();
        Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        final Set<CoordinatorKey> groupsToUnmap = new HashSet<>();
        final Set<CoordinatorKey> groupsToRetry = new HashSet<>();

        Errors responseError = response.groupLevelError(groupId.idValue);
        if (responseError != Errors.NONE) {
            handleGroupError(groupId, responseError, failed, groupsToUnmap, groupsToRetry);
        } else {
            final Map<TopicPartition, OffsetAndMetadata> groupOffsetsListing = new HashMap<>();
            Map<TopicPartition, OffsetFetchResponse.PartitionData> partitionDataMap =
                response.partitionDataMap(groupId.idValue);
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : partitionDataMap.entrySet()) {
                final TopicPartition topicPartition = entry.getKey();
                OffsetFetchResponse.PartitionData partitionData = entry.getValue();
                final Errors error = partitionData.error;

                if (error == Errors.NONE) {
                    final long offset = partitionData.offset;
                    final String metadata = partitionData.metadata;
                    final Optional<Integer> leaderEpoch = partitionData.leaderEpoch;
                    // Negative offset indicates that the group has no committed offset for this partition
                    if (offset < 0) {
                        groupOffsetsListing.put(topicPartition, null);
                    } else {
                        groupOffsetsListing.put(topicPartition, new OffsetAndMetadata(offset, leaderEpoch, metadata));
                    }
                } else {
                    handlePartitionError(groupId, topicPartition, error, groupsToUnmap, groupsToRetry);
                }
            }
            completed.put(groupId, groupOffsetsListing);
        }

        if (groupsToUnmap.isEmpty() && groupsToRetry.isEmpty()) {
            return new ApiResult<>(
                completed,
                failed,
                Collections.emptyList()
            );
        } else {
            // retry the request, so don't send completed/failed results back
            return new ApiResult<>(
                Collections.emptyMap(),
                Collections.emptyMap(),
                new ArrayList<>(groupsToUnmap)
            );
        }
    }

    private void handleGroupError(
        CoordinatorKey groupId,
        Errors error,
        Map<CoordinatorKey, Throwable> failed,
        Set<CoordinatorKey> groupsToUnmap,
        Set<CoordinatorKey> groupsToRetry
    ) {
        switch (error) {
            case GROUP_AUTHORIZATION_FAILED:
                log.error("Received authorization failure for group {} in `{}` response", groupId,
                    apiName(), error.exception());
                failed.put(groupId, error.exception());
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug("`{}` request for group {} failed because the coordinator " +
                    "is still in the process of loading state. Will retry", apiName(), groupId);
                groupsToRetry.add(groupId);
                break;
            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug("`{}` request for group {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry", apiName(), groupId, error);
                groupsToUnmap.add(groupId);
                break;

            default:
                final String unexpectedErrorMsg = String.format("Received unexpected error for group %s in `%s` response",
                    groupId, apiName());
                log.error(unexpectedErrorMsg, error.exception());
                failed.put(groupId, error.exception(unexpectedErrorMsg));
        }
    }

    private void handlePartitionError(
        CoordinatorKey groupId,
        TopicPartition topicPartition,
        Errors error,
        Set<CoordinatorKey> groupsToUnmap,
        Set<CoordinatorKey> groupsToRetry
    ) {
        switch (error) {
            case COORDINATOR_LOAD_IN_PROGRESS:
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug("`{}` request for group {} failed because the coordinator " +
                    "is still in the process of loading state. Will retry", apiName(), groupId);
                groupsToRetry.add(groupId);
                break;
            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug("`{}` request for group {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry", apiName(), groupId, error);
                groupsToUnmap.add(groupId);
                break;
            case UNKNOWN_TOPIC_OR_PARTITION:
            case TOPIC_AUTHORIZATION_FAILED:
            case UNSTABLE_OFFSET_COMMIT:
                log.warn("`{}` request for group {} returned error {} in partition {}. Skipping return offset for it.",
                    apiName(), groupId, error, topicPartition);
                break;
            default:
                log.error("`{}` request for group {} returned unexpected error {} in partition {}. Skipping return offset for it.",
                    apiName(), groupId, error, topicPartition);
        }
    }

}