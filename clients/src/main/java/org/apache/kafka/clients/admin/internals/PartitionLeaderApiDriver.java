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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Base driver implementation for APIs which target partition leaders.
 */
public abstract class PartitionLeaderApiDriver<V> extends ApiDriver<TopicPartition, V> {
    private static final RequestScope SINGLE_REQUEST_SCOPE = new RequestScope() {
    };

    private final Logger log;

    public PartitionLeaderApiDriver(
        Collection<TopicPartition> futures,
        long deadlineMs,
        long retryBackoffMs,
        LogContext logContext
    ) {
        super(futures, deadlineMs, retryBackoffMs, logContext);
        this.log = logContext.logger(PartitionLeaderApiDriver.class);
    }

    @Override
    RequestScope lookupScope(TopicPartition key) {
        // Metadata requests can group topic partitions arbitrarily, so they can all share
        // the same request context
        return SINGLE_REQUEST_SCOPE;
    }

    @Override
    MetadataRequest.Builder buildLookupRequest(Set<TopicPartition> partitions) {
        List<String> topics = new ArrayList<>();
        for (TopicPartition partition : partitions) {
            topics.add(partition.topic());
        }
        return new MetadataRequest.Builder(topics, false);
    }

    private void handleTopicError(
        String topic,
        Errors topicError,
        Set<TopicPartition> requestPartitions
    ) {
        switch (topicError) {
            case UNKNOWN_TOPIC_OR_PARTITION:
            case LEADER_NOT_AVAILABLE:
                log.debug("Metadata request for topic {} returned topic-level error {}. Will retry",
                    topic, topicError);
                break;


            case TOPIC_AUTHORIZATION_FAILED:
                failAllPartitionsForTopic(topic, requestPartitions, tp -> new TopicAuthorizationException(
                    "Failed to fetch metadata for partition " + tp + " due to topic authorization failure",
                    Collections.singleton(topic)));
                break;

            case INVALID_TOPIC_EXCEPTION:
                failAllPartitionsForTopic(topic, requestPartitions, tp -> new InvalidTopicException(
                    "Failed to fetch metadata for partition " + tp + " due to invalid topic `" + topic + "`"));
                break;

            default:
                failAllPartitionsForTopic(topic, requestPartitions, tp -> new InvalidTopicException(
                    "Failed to fetch metadata for partition " + tp + " due to unexpected error for topic `" + topic + "`"));
        }
    }

    private void failAllPartitionsForTopic(
        String topic,
        Set<TopicPartition> partitions,
        Function<TopicPartition, Throwable> exceptionGenerator
    ) {
        partitions.stream().filter(tp -> tp.topic().equals(topic)).forEach(tp -> {
            super.completeExceptionally(tp, exceptionGenerator.apply(tp));
        });
    }

    private void handlePartitionError(
        TopicPartition topicPartition,
        Errors partitionError
    ) {
        switch (partitionError) {
            case NOT_LEADER_OR_FOLLOWER:
            case REPLICA_NOT_AVAILABLE:
            case LEADER_NOT_AVAILABLE:
            case BROKER_NOT_AVAILABLE:
            case KAFKA_STORAGE_ERROR:
                log.debug("Metadata request for partition {} returned partition-level error {}. Will retry",
                    topicPartition, partitionError);
                break;

            default:
                super.completeExceptionally(topicPartition, partitionError.exception(
                    "Unexpected error during metadata lookup for " + topicPartition));
        }
    }

    @Override
    void handleLookupResponse(Set<TopicPartition> requestPartitions, AbstractResponse abstractResponse) {
        MetadataResponse response = (MetadataResponse) abstractResponse;
        for (MetadataResponse.TopicMetadata topicMetadata : response.topicMetadata()) {
            Errors topicError = topicMetadata.error();
            if (topicError != Errors.NONE) {
                handleTopicError(topicMetadata.topic(), topicError, requestPartitions);
                continue;
            }

            for (MetadataResponse.PartitionMetadata partitionMetadata : topicMetadata.partitionMetadata()) {
                TopicPartition topicPartition = partitionMetadata.topicPartition;
                Errors partitionError = partitionMetadata.error;

                if (!super.contains(topicPartition)) {
                    // The `Metadata` response may include partitions that we are not interested in
                    continue;
                }

                if (partitionError != Errors.NONE) {
                    handlePartitionError(topicPartition, partitionError);
                    continue;
                }

                Optional<Integer> leaderIdOpt = partitionMetadata.leaderId;
                if (leaderIdOpt.isPresent()) {
                    super.map(topicPartition, leaderIdOpt.get());
                } else {
                    log.debug("Metadata request for {} returned no error, but the leader is unknown. Will retry",
                        topicPartition);
                }
            }
        }
    }

}
