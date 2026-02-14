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

import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
        // Create a request that only contains the consumer groups owned by the coordinator.
        return OffsetFetchRequest.Builder.forTopicNames(
            new OffsetFetchRequestData()
                .setRequireStable(requireStable)
                .setGroups(groupIds.stream().map(groupId -> {
                    ListConsumerGroupOffsetsSpec spec = groupSpecs.get(groupId.idValue);

                    List<OffsetFetchRequestData.OffsetFetchRequestTopics> topics = null;
                    if (spec.topicPartitions() != null) {
                        topics = spec.topicPartitions().stream()
                            .collect(Collectors.groupingBy(TopicPartition::topic))
                            .entrySet()
                            .stream()
                            .map(entry -> new OffsetFetchRequestData.OffsetFetchRequestTopics()
                                .setName(entry.getKey())
                                .setPartitionIndexes(entry.getValue().stream()
                                    .map(TopicPartition::partition)
                                    .collect(Collectors.toList())))
                            .collect(Collectors.toList());
                    }
                    return new OffsetFetchRequestData.OffsetFetchRequestGroup()
                        .setGroupId(groupId.idValue)
                        .setTopics(topics);
                }).collect(Collectors.toList())),
            false
        );
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

        var response = (OffsetFetchResponse) abstractResponse;
        var completed = new HashMap<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>>();
        var failed = new HashMap<CoordinatorKey, Throwable>();
        var unmapped = new ArrayList<CoordinatorKey>();

        for (CoordinatorKey coordinatorKey : groupIds) {
            var groupId = coordinatorKey.idValue;
            var group = response.group(groupId);
            var error = Errors.forCode(group.errorCode());

            if (error != Errors.NONE) {
                handleGroupError(
                    coordinatorKey,
                    error,
                    failed,
                    unmapped
                );
            } else {
                var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();

                group.topics().forEach(topic ->
                    topic.partitions().forEach(partition -> {
                        var tp = new TopicPartition(topic.name(), partition.partitionIndex());
                        var partitionError = Errors.forCode(partition.errorCode());

                        if (partitionError == Errors.NONE) {
                            // Negative offset indicates that the group has no committed offset for this partition.
                            if (partition.committedOffset() < 0) {
                                offsets.put(tp, null);
                            } else {
                                offsets.put(tp, new OffsetAndMetadata(
                                    partition.committedOffset(),
                                    RequestUtils.getLeaderEpoch(partition.committedLeaderEpoch()),
                                    partition.metadata()
                                ));
                            }
                        } else {
                            log.warn("Skipping return offset for {} due to error {}.", tp, partitionError);
                        }
                    })
                );

                completed.put(coordinatorKey, offsets);
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
            case UNKNOWN_MEMBER_ID:
            case STALE_MEMBER_EPOCH:
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
