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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.DescribeShareGroupOffsetsRequest;
import org.apache.kafka.common.requests.DescribeShareGroupOffsetsResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;

    public ListShareGroupOffsetsHandler(
        Map<String, ListShareGroupOffsetsSpec> groupSpecs,
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
        List<String> groupIds = keys.stream().map(key -> {
            if (key.type != FindCoordinatorRequest.CoordinatorType.GROUP) {
                throw new IllegalArgumentException("Invalid group coordinator key " + key +
                    " when building `DescribeShareGroupOffsets` request");
            }
            return key.idValue;
        }).collect(Collectors.toList());
        // The DescribeShareGroupOffsetsRequest only includes a single group ID at this point, which is likely a mistake to be fixing a follow-on PR.
        String groupId = groupIds.isEmpty() ? null : groupIds.get(0);
        if (groupId == null) {
            throw new IllegalArgumentException("Missing group id in request");
        }
        ListShareGroupOffsetsSpec spec = groupSpecs.get(groupId);
        List<DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic> topics =
            spec.topicPartitions().stream().map(
                topicPartition -> new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                    .setTopicName(topicPartition.topic())
                    .setPartitions(List.of(topicPartition.partition()))
            ).collect(Collectors.toList());
        DescribeShareGroupOffsetsRequestData data = new DescribeShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(topics);
        return new DescribeShareGroupOffsetsRequest.Builder(data, true);
    }

    @Override
    public ApiResult<CoordinatorKey, Map<TopicPartition, Long>> handleResponse(Node coordinator,
                                                                               Set<CoordinatorKey> groupIds,
                                                                               AbstractResponse abstractResponse) {
        final DescribeShareGroupOffsetsResponse response = (DescribeShareGroupOffsetsResponse) abstractResponse;
        final Map<CoordinatorKey, Map<TopicPartition, Long>> completed = new HashMap<>();
        final Map<CoordinatorKey, Throwable> failed = new HashMap<>();

        for (CoordinatorKey groupId : groupIds) {
            Map<TopicPartition, Long> data = new HashMap<>();
            response.data().responses().stream().map(
                describedTopic ->
                    describedTopic.partitions().stream().map(
                        partition -> {
                            if (partition.errorCode() == Errors.NONE.code())
                                data.put(new TopicPartition(describedTopic.topicName(), partition.partitionIndex()), partition.startOffset());
                            else
                                log.error("Skipping return offset for topic {} partition {} due to error {}.", describedTopic.topicName(), partition.partitionIndex(), Errors.forCode(partition.errorCode()));
                            return data;
                        }
                    ).collect(Collectors.toList())
            ).collect(Collectors.toList());
            completed.put(groupId, data);
        }
        return new ApiResult<>(completed, failed, Collections.emptyList());
    }

    private static Set<CoordinatorKey> coordinatorKeys(Collection<String> groupIds) {
        return groupIds.stream()
            .map(CoordinatorKey::byGroupId)
            .collect(Collectors.toSet());
    }
}
