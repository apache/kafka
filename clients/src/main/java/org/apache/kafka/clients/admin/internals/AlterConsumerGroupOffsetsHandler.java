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

public class AlterConsumerGroupOffsetsHandler implements AdminApiHandler<CoordinatorKey, Map<TopicPartition, Errors>> {

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

    @Override
    public OffsetCommitRequest.Builder buildRequest(int coordinatorId, Set<CoordinatorKey> keys) {
        List<OffsetCommitRequestTopic> topics = new ArrayList<>();
        Map<String, List<OffsetCommitRequestPartition>> offsetData = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            String topic = entry.getKey().topic();
            OffsetAndMetadata oam = entry.getValue();
            OffsetCommitRequestPartition partition = new OffsetCommitRequestPartition()
                    .setCommittedOffset(oam.offset())
                    .setCommittedLeaderEpoch(oam.leaderEpoch().orElse(-1))
                    .setCommittedMetadata(oam.metadata())
                    .setPartitionIndex(entry.getKey().partition());
            offsetData.computeIfAbsent(topic, key -> new ArrayList<>()).add(partition);
        }
        for (Map.Entry<String, List<OffsetCommitRequestPartition>> entry : offsetData.entrySet()) {
            OffsetCommitRequestTopic topic = new OffsetCommitRequestTopic()
                    .setName(entry.getKey())
                    .setPartitions(entry.getValue());
            topics.add(topic);
        }
        OffsetCommitRequestData data = new OffsetCommitRequestData()
            .setGroupId(groupId.idValue)
            .setTopics(topics);
        return new OffsetCommitRequest.Builder(data);
    }

    @Override
    public ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> handleResponse(
        Node coordinator,
        Set<CoordinatorKey> groupIds,
        AbstractResponse abstractResponse
    ) {
        final OffsetCommitResponse response = (OffsetCommitResponse) abstractResponse;
        Map<CoordinatorKey, Map<TopicPartition, Errors>> completed = new HashMap<>();
        Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        List<CoordinatorKey> unmapped = new ArrayList<>();

        Map<TopicPartition, Errors> partitions = new HashMap<>();
        int totalPartitionCount = 0;
        for (OffsetCommitResponseTopic topic : response.data().topics()) {
            for (OffsetCommitResponsePartition partition : topic.partitions()) {
                TopicPartition tp = new TopicPartition(topic.name(), partition.partitionIndex());
                Errors error = Errors.forCode(partition.errorCode());
                if (error != Errors.NONE) {
                    handleError(groupId, error, failed, unmapped);
                } else {
                    partitions.put(tp, error);
                }
                totalPartitionCount++;
            }
        }
        // only complete this request when:
        // 1. no fail
        // 2. no unmapped
        // 3. all partitions are handled (i.e. no need to retry)
        if (failed.isEmpty() && unmapped.isEmpty() && partitions.size() == totalPartitionCount)
            completed.put(groupId, partitions);

        return new ApiResult<>(completed, failed, unmapped);
    }

    private void handleError(
        CoordinatorKey groupId,
        Errors error,
        Map<CoordinatorKey, Throwable> failed,
        List<CoordinatorKey> unmapped
    ) {
        switch (error) {
            case GROUP_AUTHORIZATION_FAILED:
                log.error("Received authorization failure for group {} in `{}` response", groupId,
                        apiName(), error.exception());
                failed.put(groupId, error.exception());
                break;
            case COORDINATOR_LOAD_IN_PROGRESS:
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug("`{}` request for group {} failed because the coordinator" +
                    " is still in the process of loading state. Will retry.", apiName(), groupId);
                break;
            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug("`{}` request for group {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry.", apiName(), groupId, error);
                unmapped.add(groupId);
                break;
            default:
                final String unexpectedErrorMsg = String.format("Received unexpected error for group %s in `%s` response",
                    groupId, apiName());
                log.error(unexpectedErrorMsg, groupId, apiName(), error.exception());
                failed.put(groupId, error.exception(unexpectedErrorMsg));
        }
    }

}
