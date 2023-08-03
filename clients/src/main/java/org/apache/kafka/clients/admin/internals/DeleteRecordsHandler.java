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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.DeletedRecords;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.internals.AdminApiFuture.SimpleAdminApiFuture;
import org.apache.kafka.clients.admin.internals.AdminApiHandler.Batched;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.DeleteRecordsRequest;
import org.apache.kafka.common.requests.DeleteRecordsResponse;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public final class DeleteRecordsHandler extends Batched<TopicPartition, DeletedRecords> {

    private final Map<TopicPartition, RecordsToDelete> recordsToDelete;
    private final Logger log;
    private final AdminApiLookupStrategy<TopicPartition> lookupStrategy;

    private final int timeout;

    public DeleteRecordsHandler(
            Map<TopicPartition, RecordsToDelete> recordsToDelete,
            LogContext logContext, int timeout
    ) {
        this.recordsToDelete = recordsToDelete;
        this.log = logContext.logger(DeleteRecordsHandler.class);
        this.lookupStrategy = new PartitionLeaderStrategy(logContext);
        this.timeout = timeout;
    }

    @Override
    public String apiName() {
        return "deleteRecords";
    }

    @Override
    public AdminApiLookupStrategy<TopicPartition> lookupStrategy() {
        return this.lookupStrategy;
    }

    public static SimpleAdminApiFuture<TopicPartition, DeletedRecords> newFuture(
            Collection<TopicPartition> topicPartitions
    ) {
        return AdminApiFuture.forKeys(new HashSet<>(topicPartitions));
    }

    @Override
    public DeleteRecordsRequest.Builder buildBatchedRequest(int brokerId, Set<TopicPartition> keys) {
        Map<String, DeleteRecordsRequestData.DeleteRecordsTopic> deletionsForTopic = new HashMap<>();
        for (Map.Entry<TopicPartition, RecordsToDelete> entry: recordsToDelete.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            DeleteRecordsRequestData.DeleteRecordsTopic deleteRecords = deletionsForTopic.computeIfAbsent(
                    topicPartition.topic(),
                    key -> new DeleteRecordsRequestData.DeleteRecordsTopic().setName(topicPartition.topic())
            );
            deleteRecords.partitions().add(new DeleteRecordsRequestData.DeleteRecordsPartition()
                    .setPartitionIndex(topicPartition.partition())
                    .setOffset(entry.getValue().beforeOffset()));
        }

        DeleteRecordsRequestData data = new DeleteRecordsRequestData()
                .setTopics(new ArrayList<>(deletionsForTopic.values()))
                .setTimeoutMs(timeout);
        return new DeleteRecordsRequest.Builder(data);
    }


    @Override
    public ApiResult<TopicPartition, DeletedRecords> handleResponse(
        Node broker,
        Set<TopicPartition> keys,
        AbstractResponse abstractResponse
    ) {
        DeleteRecordsResponse response = (DeleteRecordsResponse) abstractResponse;
        Map<TopicPartition, DeletedRecords> completed = new HashMap<>();
        Map<TopicPartition, Throwable> failed = new HashMap<>();
        List<TopicPartition> unmapped = new ArrayList<>();
        Set<TopicPartition> retriable = new HashSet<>();

        for (DeleteRecordsResponseData.DeleteRecordsTopicResult topicResult: response.data().topics()) {
            for (DeleteRecordsResponseData.DeleteRecordsPartitionResult partitionResult : topicResult.partitions()) {
                Errors error = Errors.forCode(partitionResult.errorCode());
                TopicPartition topicPartition = new TopicPartition(topicResult.name(), partitionResult.partitionIndex());
                if (error == Errors.NONE) {
                    completed.put(topicPartition, new DeletedRecords(partitionResult.lowWatermark()));
                } else {
                    handlePartitionError(topicPartition, error, failed, unmapped, retriable);
                }
            }
        }

        // Sanity-check if the current leader for these partitions returned results for all of them
        for (TopicPartition topicPartition : keys) {
            if (unmapped.isEmpty()
                    && !completed.containsKey(topicPartition)
                    && !failed.containsKey(topicPartition)
                    && !retriable.contains(topicPartition)
            ) {
                ApiException sanityCheckException = new ApiException(
                        "The response from broker " + broker.id() +
                                " did not contain a result for topic partition " + topicPartition);
                log.error(
                        "DeleteRecords request for topic partition {} failed sanity check",
                        topicPartition,
                        sanityCheckException);
                failed.put(topicPartition, sanityCheckException);
            }
        }

        return new ApiResult<>(completed, failed, unmapped);
    }

    private void handlePartitionError(
        TopicPartition topicPartition,
        Errors error,
        Map<TopicPartition, Throwable> failed,
        List<TopicPartition> unmapped,
        Set<TopicPartition> retriable
    ) {
        if (error.exception() instanceof InvalidMetadataException) {
            log.debug(
                "DeleteRecords lookup request for topic partition {} will be retried due to invalid leader metadata {}",
                 topicPartition,
                 error);
            unmapped.add(topicPartition);
        } else if (error.exception() instanceof RetriableException) {
            log.debug(
                "DeleteRecords fulfillment request for topic partition {} will be retried due to {}",
                topicPartition,
                error);
            retriable.add(topicPartition);
        } else if (error.exception() instanceof TopicAuthorizationException) {
            log.error(
                "DeleteRecords request for topic partition {} failed due to an error {}",
                topicPartition,
                error);
            failed.put(topicPartition, error.exception());
        } else {
            log.error(
                "DeleteRecords request for topic partition {} failed due to an unexpected error {}",
                topicPartition,
                error);
            failed.put(topicPartition, error.exception());
        }
    }
}
