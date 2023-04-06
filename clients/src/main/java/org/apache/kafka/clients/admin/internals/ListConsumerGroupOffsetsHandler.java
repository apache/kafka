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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
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

    private final boolean requireStable;
    private final Map<String, ListConsumerGroupOffsetsSpec> groupSpecs;
    private final Logger log;
    private final CoordinatorStrategy lookupStrategy;

    public ListConsumerGroupOffsetsHandler(
        Map<String, ListConsumerGroupOffsetsSpec> groupSpecs,
        boolean requireStable,
        LogContext logContext
    ) {
        this.log = logContext.logger(ListConsumerGroupOffsetsHandler.class);
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
        this.groupSpecs = groupSpecs;
        this.requireStable = requireStable;
    }

    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> newFuture(Collection<String> groupIds) {
        return AdminApiFuture.forKeys(coordinatorKeys(groupIds));
    }

    @Override
    public String apiName() {
        return "offsetFetch";
    }

    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    private void validateKeys(Set<CoordinatorKey> groupIds) {
        Set<CoordinatorKey> keys = coordinatorKeys(groupSpecs.keySet());
        if (!keys.containsAll(groupIds)) {
            throw new IllegalArgumentException("Received unexpected group ids " + groupIds +
                    " (expected one of " + keys + ")");
        }
    }

    private static Set<CoordinatorKey> coordinatorKeys(Collection<String> groupIds) {
        return groupIds.stream()
           .map(CoordinatorKey::byGroupId)
           .collect(Collectors.toSet());
    }

    public OffsetFetchRequest.Builder buildBatchedRequest(Set<CoordinatorKey> groupIds) {
        // Create a map that only contains the consumer groups owned by the coordinator.
        Map<String, List<TopicPartition>> coordinatorGroupIdToTopicPartitions = new HashMap<>(groupIds.size());
        groupIds.forEach(g -> {
            ListConsumerGroupOffsetsSpec spec = groupSpecs.get(g.idValue);
            List<TopicPartition> partitions = spec.topicPartitions() != null ? new ArrayList<>(spec.topicPartitions()) : null;
            coordinatorGroupIdToTopicPartitions.put(g.idValue, partitions);
        });

        return new OffsetFetchRequest.Builder(coordinatorGroupIdToTopicPartitions, requireStable, false);
    }

    @Override
    public Collection<RequestAndKeys<CoordinatorKey>> buildRequest(int brokerId, Set<CoordinatorKey> groupIds) {
        validateKeys(groupIds);

        // When the OffsetFetchRequest fails with NoBatchedOffsetFetchRequestException, we completely disable
        // the batching end-to-end, including the FindCoordinatorRequest.
        if (lookupStrategy.batch()) {
            return Collections.singletonList(new RequestAndKeys<>(buildBatchedRequest(groupIds), groupIds));
        } else {
            return groupIds.stream().map(groupId -> {
                Set<CoordinatorKey> keys = Collections.singleton(groupId);
                return new RequestAndKeys<>(buildBatchedRequest(keys), keys);
            }).collect(Collectors.toList());
        }
    }

    @Override
    public ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> handleResponse(
        Node coordinator,
        Set<CoordinatorKey> groupIds,
        AbstractResponse abstractResponse
    ) {
        validateKeys(groupIds);

        final OffsetFetchResponse response = (OffsetFetchResponse) abstractResponse;

        Map<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> completed = new HashMap<>();
        Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        List<CoordinatorKey> unmapped = new ArrayList<>();
        for (CoordinatorKey coordinatorKey : groupIds) {
            String group = coordinatorKey.idValue;
            if (response.groupHasError(group)) {
                handleGroupError(CoordinatorKey.byGroupId(group), response.groupLevelError(group), failed, unmapped);
            } else {
                final Map<TopicPartition, OffsetAndMetadata> groupOffsetsListing = new HashMap<>();
                Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData = response.partitionDataMap(group);
                for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> partitionEntry : responseData.entrySet()) {
                    final TopicPartition topicPartition = partitionEntry.getKey();
                    OffsetFetchResponse.PartitionData partitionData = partitionEntry.getValue();
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
                        log.warn("Skipping return offset for {} due to error {}.", topicPartition, error);
                    }
                }
                completed.put(CoordinatorKey.byGroupId(group), groupOffsetsListing);
            }
        }
        return new ApiResult<>(completed, failed, unmapped);
    }

    private void handleGroupError(
        CoordinatorKey groupId,
        Errors error,
        Map<CoordinatorKey, Throwable> failed,
        List<CoordinatorKey> groupsToUnmap
    ) {
        switch (error) {
            case GROUP_AUTHORIZATION_FAILED:
                log.debug("`OffsetFetch` request for group id {} failed due to error {}", groupId.idValue, error);
                failed.put(groupId, error.exception());
                break;
            case COORDINATOR_LOAD_IN_PROGRESS:
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug("`OffsetFetch` request for group id {} failed because the coordinator " +
                    "is still in the process of loading state. Will retry", groupId.idValue);
                break;

            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug("`OffsetFetch` request for group id {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry", groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;

            default:
                log.error("`OffsetFetch` request for group id {} failed due to unexpected error {}", groupId.idValue, error);
                failed.put(groupId, error.exception());
        }
    }
}
