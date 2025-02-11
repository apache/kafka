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

import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.DescribeShareGroupOffsetsRequest;
import org.apache.kafka.common.requests.DescribeShareGroupOffsetsResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class is the handler for {@link KafkaAdminClient#listShareGroupOffsets(Map, ListShareGroupOffsetsOptions)} call
 */
public class ListShareGroupOffsetsHandler extends AdminApiHandler.Batched<CoordinatorKey, Map<TopicPartition, Long>> {

    private final Map<String, ListShareGroupOffsetsSpec> groupSpecs;
    private final Logger log;
    private final CoordinatorStrategy lookupStrategy;

    public ListShareGroupOffsetsHandler(Map<String, ListShareGroupOffsetsSpec> groupSpecs,
                                        LogContext logContext) {
        this.groupSpecs = groupSpecs;
        this.log = logContext.logger(ListShareGroupOffsetsHandler.class);
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
    }

    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, Long>> newFuture(Collection<String> groupIds) {
        return AdminApiFuture.forKeys(coordinatorKeys(groupIds));
    }

    @Override
    public String apiName() {
        return "describeShareGroupOffsets";
    }

    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    @Override
    public DescribeShareGroupOffsetsRequest.Builder buildBatchedRequest(int coordinatorId, Set<CoordinatorKey> keys) {
        validateKeys(keys);

        List<DescribeShareGroupOffsetsRequestGroup> groups = new ArrayList<>(keys.size());
        keys.forEach(coordinatorKey -> {
            String groupId = coordinatorKey.idValue;
            ListShareGroupOffsetsSpec spec = groupSpecs.get(groupId);
            DescribeShareGroupOffsetsRequestGroup requestGroup = new DescribeShareGroupOffsetsRequestGroup()
                .setGroupId(groupId);

            Map<String, List<Integer>> topicPartitionMap = new HashMap<>();
            spec.topicPartitions().forEach(tp -> topicPartitionMap.computeIfAbsent(tp.topic(), t -> new LinkedList<>()).add(tp.partition()));

            Map<String, DescribeShareGroupOffsetsRequestTopic> requestTopics = new HashMap<>();
            for (TopicPartition tp : spec.topicPartitions()) {
                requestTopics.computeIfAbsent(tp.topic(), t ->
                        new DescribeShareGroupOffsetsRequestTopic()
                            .setTopicName(tp.topic())
                            .setPartitions(new LinkedList<>()))
                    .partitions()
                    .add(tp.partition());
            }
            requestGroup.setTopics(new ArrayList<>(requestTopics.values()));
            groups.add(requestGroup);
        });
        DescribeShareGroupOffsetsRequestData data = new DescribeShareGroupOffsetsRequestData()
            .setGroups(groups);
        return new DescribeShareGroupOffsetsRequest.Builder(data, true);
    }

    @Override
    public ApiResult<CoordinatorKey, Map<TopicPartition, Long>> handleResponse(Node coordinator,
                                                                               Set<CoordinatorKey> groupIds,
                                                                               AbstractResponse abstractResponse) {
        validateKeys(groupIds);

        final DescribeShareGroupOffsetsResponse response = (DescribeShareGroupOffsetsResponse) abstractResponse;
        final Map<CoordinatorKey, Map<TopicPartition, Long>> completed = new HashMap<>();
        final Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        final List<CoordinatorKey> unmapped = new ArrayList<>();

        for (CoordinatorKey coordinatorKey : groupIds) {
            String groupId = coordinatorKey.idValue;
            if (response.hasGroupError(groupId)) {
                handleGroupError(coordinatorKey, response.groupError(groupId), failed, unmapped);
            } else {
                Map<TopicPartition, Long> groupOffsetsListing = new HashMap<>();
                response.data().groups().stream().filter(g -> g.groupId().equals(groupId)).forEach(groupResponse -> {
                    for (DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic topicResponse : groupResponse.topics()) {
                        for (DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition partitionResponse : topicResponse.partitions()) {
                            TopicPartition tp = new TopicPartition(topicResponse.topicName(), partitionResponse.partitionIndex());
                            if (partitionResponse.errorCode() == Errors.NONE.code()) {
                                // Negative offset indicates there is no start offset for this partition
                                if (partitionResponse.startOffset() < 0) {
                                    groupOffsetsListing.put(tp, null);
                                } else {
                                    groupOffsetsListing.put(tp, partitionResponse.startOffset());
                                }
                            } else {
                                log.warn("Skipping return offset for {} due to error {}: {}.", tp, partitionResponse.errorCode(), partitionResponse.errorMessage());
                            }
                        }
                    }
                });

                completed.put(coordinatorKey, groupOffsetsListing);
            }
        }
        return new ApiResult<>(completed, failed, unmapped);
    }

    private static Set<CoordinatorKey> coordinatorKeys(Collection<String> groupIds) {
        return groupIds.stream()
            .map(CoordinatorKey::byGroupId)
            .collect(Collectors.toSet());
    }

    private void validateKeys(Set<CoordinatorKey> groupIds) {
        Set<CoordinatorKey> keys = coordinatorKeys(groupSpecs.keySet());
        if (!keys.containsAll(groupIds)) {
            throw new IllegalArgumentException("Received unexpected group ids " + groupIds +
                " (expected one of " + keys + ")");
        }
    }

    private void handleGroupError(CoordinatorKey groupId,
                                  Throwable exception,
                                  Map<CoordinatorKey, Throwable> failed,
                                  List<CoordinatorKey> groupsToUnmap) {
        Errors error = Errors.forException(exception);
        switch (error) {
            case GROUP_AUTHORIZATION_FAILED:
            case UNKNOWN_MEMBER_ID:
            case STALE_MEMBER_EPOCH:
                log.debug("`DescribeShareGroupOffsets` request for group id {} failed due to error {}", groupId.idValue, error);
                failed.put(groupId, exception);
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug("`DescribeShareGroupOffsets` request for group id {} failed because the coordinator " +
                    "is still in the process of loading state. Will retry", groupId.idValue);
                break;

            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug("`DescribeShareGroupOffsets` request for group id {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry", groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;

            default:
                log.error("`DescribeShareGroupOffsets` request for group id {} failed due to unexpected error {}", groupId.idValue, error);
                failed.put(groupId, exception);
        }
    }
}
