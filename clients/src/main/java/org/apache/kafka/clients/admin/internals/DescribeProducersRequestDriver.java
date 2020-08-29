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
import org.apache.kafka.clients.admin.DescribeProducersResult;
import org.apache.kafka.clients.admin.DescribeProducersResult.PartitionProducerState;
import org.apache.kafka.clients.admin.DescribeProducersResult.ProducerState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.DescribeProducersRequestData;
import org.apache.kafka.common.message.DescribeProducersResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.DescribeProducersRequest;
import org.apache.kafka.common.requests.DescribeProducersResponse;
import org.apache.kafka.common.utils.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

public class DescribeProducersRequestDriver extends MetadataRequestDriver<PartitionProducerState> {
    private static final Logger log = LoggerFactory.getLogger(DescribeProducersRequestDriver.class);

    private final DescribeProducersOptions options;

    public DescribeProducersRequestDriver(
        Collection<TopicPartition> topicPartitions,
        DescribeProducersOptions options,
        long deadlineMs,
        long retryBackoffMs
    ) {
        super(topicPartitions, deadlineMs, retryBackoffMs);
        this.options = options;

        // If the request options indicate a specific target broker, then we directly
        // map the topic partitions to avoid the unneeded `Metadata` lookup.
        if (options.brokerId().isPresent()) {
            int destinationBrokerId = options.brokerId().getAsInt();
            for (TopicPartition topicPartition : topicPartitions) {
                super.map(topicPartition, destinationBrokerId);
            }
        }
    }

    @Override
    DescribeProducersRequest.Builder buildFulfillmentRequest(Set<TopicPartition> topicPartitions) {
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
        Errors error
    ) {
        switch (error) {
            case NOT_LEADER_OR_FOLLOWER:
                if (options.brokerId().isPresent()) {
                    // Typically these errors are retriable, but if the user specified the brokerId
                    // explicitly, then they are fatal
                    super.completeExceptionally(topicPartition, error.exception("Failed to describe active producers " +
                        "for partition " + topicPartition + " on brokerId " + options.brokerId().getAsInt()));
                } else {
                    // Otherwise, we unmap the partition so that we can find the new leader
                    super.unmap(topicPartition);
                }
                break;

            case UNKNOWN_TOPIC_OR_PARTITION:
                log.debug("Received unknown topic/partition error when trying to describe active producers " +
                    "for partition " + topicPartition + ". Will retry later.");
                break;

            case TOPIC_AUTHORIZATION_FAILED:
                super.completeExceptionally(topicPartition, new TopicAuthorizationException("Failed to describe " +
                    "active producers for partition " + topicPartition + " due to authorization failure on topic" +
                    " `" + topicPartition.topic() + "`", Collections.singleton(topicPartition.topic())));
                break;

            default:
                super.completeExceptionally(topicPartition, error.exception("Failed to describe active " +
                    "producers for partition " + topicPartition + " due to unexpected error"));
                break;

        }
    }

    @Override
    void handleFulfillmentResponse(Set<TopicPartition> keys, AbstractResponse abstractResponse) {
        DescribeProducersResponse response = (DescribeProducersResponse) abstractResponse;

        for (DescribeProducersResponseData.TopicResponse topicResponse : response.data().topics()) {
            for (DescribeProducersResponseData.PartitionResponse partitionResponse : topicResponse.partitions()) {
                TopicPartition topicPartition = new TopicPartition(
                    topicResponse.name(), partitionResponse.partitionIndex());

                Errors error = Errors.forCode(partitionResponse.errorCode());
                if (error != Errors.NONE) {
                    handlePartitionError(topicPartition, error);
                    continue;
                }

                List<ProducerState> activeProducers = partitionResponse.activeProducers().stream()
                    .map(activeProducer -> {
                        OptionalLong currentTransactionFirstOffset =
                            activeProducer.currentTxnStartTimestamp() < 0 ?
                                OptionalLong.empty() :
                                OptionalLong.of(activeProducer.currentTxnStartTimestamp());
                        return new ProducerState(
                            activeProducer.producerId(),
                            activeProducer.producerEpoch(),
                            activeProducer.lastSequence(),
                            activeProducer.lastTimestamp(),
                            currentTransactionFirstOffset
                        );
                    }).collect(Collectors.toList());

                super.complete(topicPartition, new DescribeProducersResult.PartitionProducerState(activeProducers));
            }
        }
    }

}
