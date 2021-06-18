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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestPartition;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestTopic;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.OffsetDeleteRequest;
import org.apache.kafka.common.requests.OffsetDeleteResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class DeleteConsumerGroupOffsetsHandler implements AdminApiHandler<CoordinatorKey, Map<TopicPartition, Errors>> {

    private final CoordinatorKey groupId;
    private final Set<TopicPartition> partitions;
    private final Logger log;
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;

    public DeleteConsumerGroupOffsetsHandler(
        String groupId,
        Set<TopicPartition> partitions,
        LogContext logContext
    ) {
        this.groupId = CoordinatorKey.byGroupId(groupId);
        this.partitions = partitions;
        this.log = logContext.logger(DeleteConsumerGroupOffsetsHandler.class);
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
    }

    @Override
    public String apiName() {
        return "offsetDelete";
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

    @Override
    public OffsetDeleteRequest.Builder buildRequest(int coordinatorId, Set<CoordinatorKey> keys) {
        final OffsetDeleteRequestTopicCollection topics = new OffsetDeleteRequestTopicCollection();
        partitions.stream().collect(Collectors.groupingBy(TopicPartition::topic)).forEach((topic, topicPartitions) -> topics.add(
            new OffsetDeleteRequestTopic()
            .setName(topic)
            .setPartitions(topicPartitions.stream()
                .map(tp -> new OffsetDeleteRequestPartition().setPartitionIndex(tp.partition()))
                .collect(Collectors.toList())
            )
        ));

        return new OffsetDeleteRequest.Builder(
            new OffsetDeleteRequestData()
                .setGroupId(groupId.idValue)
                .setTopics(topics)
        );
    }

    @Override
    public ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> handleResponse(
        Node coordinator,
        Set<CoordinatorKey> groupIds,
        AbstractResponse abstractResponse
    ) {
        final OffsetDeleteResponse response = (OffsetDeleteResponse) abstractResponse;
        Map<CoordinatorKey, Map<TopicPartition, Errors>> completed = new HashMap<>();
        Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        List<CoordinatorKey> unmapped = new ArrayList<>();

        final Errors error = Errors.forCode(response.data().errorCode());
        if (error != Errors.NONE) {
            handleError(groupId, error, failed, unmapped);
        } else {
            final Map<TopicPartition, Errors> partitions = new HashMap<>();
            response.data().topics().forEach(topic -> 
                topic.partitions().forEach(partition -> {
                    Errors partitionError = Errors.forCode(partition.errorCode());
                    if (!handleError(groupId, partitionError, failed, unmapped)) {
                        partitions.put(new TopicPartition(topic.name(), partition.partitionIndex()), partitionError);
                    }
                })
            );
            if (!partitions.isEmpty())
                completed.put(groupId, partitions);
        }
        return new ApiResult<>(completed, failed, unmapped);
    }

    private boolean handleError(
        CoordinatorKey groupId,
        Errors error,
        Map<CoordinatorKey, Throwable> failed,
        List<CoordinatorKey> unmapped
    ) {
        switch (error) {
            case GROUP_AUTHORIZATION_FAILED:
            case GROUP_ID_NOT_FOUND:
            case INVALID_GROUP_ID:
                log.error("Received non retriable error for group {} in `DeleteConsumerGroupOffsets` response", groupId,
                        error.exception());
                failed.put(groupId, error.exception());
                return true;
            case COORDINATOR_LOAD_IN_PROGRESS:
            case COORDINATOR_NOT_AVAILABLE:
                return true;
            case NOT_COORDINATOR:
                log.debug("DeleteConsumerGroupOffsets request for group {} returned error {}. Will retry",
                        groupId, error);
                unmapped.add(groupId);
                return true;
            default:
                return false;
        }
    }

}
