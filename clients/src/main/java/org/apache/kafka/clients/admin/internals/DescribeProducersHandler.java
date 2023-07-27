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

import org.apache.kafka.clients.admin.DescribeProducersOptions;
import org.apache.kafka.clients.admin.DescribeProducersResult.PartitionProducerState;
import org.apache.kafka.clients.admin.ProducerState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.DescribeProducersRequestData;
import org.apache.kafka.common.message.DescribeProducersResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.DescribeProducersRequest;
import org.apache.kafka.common.requests.DescribeProducersResponse;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

public class DescribeProducersHandler extends AdminApiHandler.Batched<TopicPartition, PartitionProducerState> {
    private final Logger log;
    private final DescribeProducersOptions options;
    private final AdminApiLookupStrategy<TopicPartition> lookupStrategy;

    public DescribeProducersHandler(
        DescribeProducersOptions options,
        LogContext logContext
    ) {
        this.options = options;
        this.log = logContext.logger(DescribeProducersHandler.class);

        if (options.brokerId().isPresent()) {
            this.lookupStrategy = new StaticBrokerStrategy<>(options.brokerId().getAsInt());
        } else {
            this.lookupStrategy = new PartitionLeaderStrategy(logContext);
        }
    }

    public static AdminApiFuture.SimpleAdminApiFuture<TopicPartition, PartitionProducerState> newFuture(
        Collection<TopicPartition> topicPartitions
    ) {
        return AdminApiFuture.forKeys(new HashSet<>(topicPartitions));
    }

    @Override
    public String apiName() {
        return "describeProducers";
    }

    @Override
    public AdminApiLookupStrategy<TopicPartition> lookupStrategy() {
        return lookupStrategy;
    }

    @Override
    public DescribeProducersRequest.Builder buildBatchedRequest(
        int brokerId,
        Set<TopicPartition> topicPartitions
    ) {
        DescribeProducersRequestData request = new DescribeProducersRequestData();
        DescribeProducersRequest.Builder builder = new DescribeProducersRequest.Builder(request);

        CollectionUtils.groupPartitionsByTopic(
            topicPartitions,
            builder::addTopic,
            (topicRequest, partitionId) -> topicRequest.partitionIndexes().add(partitionId)
        );

        return builder;
    }

    private void handlePartitionError(
        TopicPartition topicPartition,
        ApiError apiError,
        Map<TopicPartition, Throwable> failed,
        List<TopicPartition> unmapped
    ) {
        switch (apiError.error()) {
            case NOT_LEADER_OR_FOLLOWER:
                if (options.brokerId().isPresent()) {
                    // Typically these errors are retriable, but if the user specified the brokerId
                    // explicitly, then they are fatal.
                    int brokerId = options.brokerId().getAsInt();
                    log.error("Not leader error in `DescribeProducers` response for partition {} " +
                        "for brokerId {} set in options", topicPartition, brokerId, apiError.exception());
                    failed.put(topicPartition, apiError.error().exception("Failed to describe active producers " +
                        "for partition " + topicPartition + " on brokerId " + brokerId));
                } else {
                    // Otherwise, we unmap the partition so that we can find the new leader
                    log.debug("Not leader error in `DescribeProducers` response for partition {}. " +
                        "Will retry later.", topicPartition);
                    unmapped.add(topicPartition);
                }
                break;

            case UNKNOWN_TOPIC_OR_PARTITION:
                log.debug("Unknown topic/partition error in `DescribeProducers` response for partition {}. " +
                    "Will retry later.", topicPartition);
                break;

            case INVALID_TOPIC_EXCEPTION:
                log.error("Invalid topic in `DescribeProducers` response for partition {}",
                    topicPartition, apiError.exception());
                failed.put(topicPartition, new InvalidTopicException(
                    "Failed to fetch metadata for partition " + topicPartition
                        + " due to invalid topic error: " + apiError.messageWithFallback(),
                    Collections.singleton(topicPartition.topic())));
                break;

            case TOPIC_AUTHORIZATION_FAILED:
                log.error("Authorization failed in `DescribeProducers` response for partition {}",
                    topicPartition, apiError.exception());
                failed.put(topicPartition, new TopicAuthorizationException("Failed to describe " +
                    "active producers for partition " + topicPartition + " due to authorization failure on topic" +
                    " `" + topicPartition.topic() + "`", Collections.singleton(topicPartition.topic())));
                break;

            default:
                log.error("Unexpected error in `DescribeProducers` response for partition {}",
                    topicPartition, apiError.exception());
                failed.put(topicPartition, apiError.error().exception("Failed to describe active " +
                    "producers for partition " + topicPartition + " due to unexpected error"));
                break;
        }
    }

    @Override
    public ApiResult<TopicPartition, PartitionProducerState> handleResponse(
        Node broker,
        Set<TopicPartition> keys,
        AbstractResponse abstractResponse
    ) {
        DescribeProducersResponse response = (DescribeProducersResponse) abstractResponse;
        Map<TopicPartition, PartitionProducerState> completed = new HashMap<>();
        Map<TopicPartition, Throwable> failed = new HashMap<>();
        List<TopicPartition> unmapped = new ArrayList<>();

        for (DescribeProducersResponseData.TopicResponse topicResponse : response.data().topics()) {
            for (DescribeProducersResponseData.PartitionResponse partitionResponse : topicResponse.partitions()) {
                TopicPartition topicPartition = new TopicPartition(
                    topicResponse.name(), partitionResponse.partitionIndex());

                Errors error = Errors.forCode(partitionResponse.errorCode());
                if (error != Errors.NONE) {
                    ApiError apiError = new ApiError(error, partitionResponse.errorMessage());
                    handlePartitionError(topicPartition, apiError, failed, unmapped);
                    continue;
                }

                List<ProducerState> activeProducers = partitionResponse.activeProducers().stream()
                    .map(activeProducer -> {
                        OptionalLong currentTransactionFirstOffset =
                            activeProducer.currentTxnStartOffset() < 0 ?
                                OptionalLong.empty() :
                                OptionalLong.of(activeProducer.currentTxnStartOffset());
                        OptionalInt coordinatorEpoch =
                            activeProducer.coordinatorEpoch() < 0 ?
                                OptionalInt.empty() :
                                OptionalInt.of(activeProducer.coordinatorEpoch());

                        return new ProducerState(
                            activeProducer.producerId(),
                            activeProducer.producerEpoch(),
                            activeProducer.lastSequence(),
                            activeProducer.lastTimestamp(),
                            coordinatorEpoch,
                            currentTransactionFirstOffset
                        );
                    }).collect(Collectors.toList());

                completed.put(topicPartition, new PartitionProducerState(activeProducers));
            }
        }
        return new ApiResult<>(completed, failed, unmapped);
    }

}
