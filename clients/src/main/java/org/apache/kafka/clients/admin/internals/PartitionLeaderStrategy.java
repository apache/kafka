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
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Base driver implementation for APIs which target partition leaders.
 */
public class PartitionLeaderStrategy implements AdminApiLookupStrategy<TopicPartition> {
    private static final ApiRequestScope SINGLE_REQUEST_SCOPE = new ApiRequestScope() {
    };

    private final Logger log;

    public PartitionLeaderStrategy(LogContext logContext) {
        this.log = logContext.logger(PartitionLeaderStrategy.class);
    }

    @Override
    public ApiRequestScope lookupScope(TopicPartition key) {
        // Metadata requests can group topic partitions arbitrarily, so they can all share
        // the same request context
        return SINGLE_REQUEST_SCOPE;
    }

    @Override
    public MetadataRequest.Builder buildRequest(Set<TopicPartition> partitions) {
        MetadataRequestData request = new MetadataRequestData();
        request.setAllowAutoTopicCreation(false);
        partitions.stream().map(TopicPartition::topic).distinct().forEach(topic ->
            request.topics().add(new MetadataRequestData.MetadataRequestTopic().setName(topic))
        );
        return new MetadataRequest.Builder(request);
    }

    private void handleTopicError(
        String topic,
        Errors topicError,
        Set<TopicPartition> requestPartitions,
        Map<TopicPartition, Throwable> failed
    ) {
        switch (topicError) {
            case UNKNOWN_TOPIC_OR_PARTITION:
            case LEADER_NOT_AVAILABLE:
            case BROKER_NOT_AVAILABLE:
                log.debug("Metadata request for topic {} returned topic-level error {}. Will retry",
                    topic, topicError);
                break;

            case TOPIC_AUTHORIZATION_FAILED:
                log.error("Received authorization failure for topic {} in `Metadata` response", topic,
                    topicError.exception());
                failAllPartitionsForTopic(topic, requestPartitions, failed, tp -> new TopicAuthorizationException(
                    "Failed to fetch metadata for partition " + tp + " due to topic authorization failure",
                    Collections.singleton(topic)));
                break;

            case INVALID_TOPIC_EXCEPTION:
                log.error("Received invalid topic error for topic {} in `Metadata` response", topic,
                    topicError.exception());
                failAllPartitionsForTopic(topic, requestPartitions, failed, tp -> new InvalidTopicException(
                    "Failed to fetch metadata for partition " + tp + " due to invalid topic `" + topic + "`",
                    Collections.singleton(topic)));
                break;

            default:
                log.error("Received unexpected error for topic {} in `Metadata` response", topic,
                    topicError.exception());
                failAllPartitionsForTopic(topic, requestPartitions, failed, tp -> topicError.exception(
                    "Failed to fetch metadata for partition " + tp + " due to unexpected error for topic `" + topic + "`"));
        }
    }

    private void failAllPartitionsForTopic(
        String topic,
        Set<TopicPartition> partitions,
        Map<TopicPartition, Throwable> failed,
        Function<TopicPartition, Throwable> exceptionGenerator
    ) {
        partitions.stream().filter(tp -> tp.topic().equals(topic)).forEach(tp -> {
            failed.put(tp, exceptionGenerator.apply(tp));
        });
    }

    private void handlePartitionError(
        TopicPartition topicPartition,
        Errors partitionError,
        Map<TopicPartition, Throwable> failed
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
                log.error("Received unexpected error for partition {} in `Metadata` response",
                    topicPartition, partitionError.exception());
                failed.put(topicPartition, partitionError.exception(
                    "Unexpected error during metadata lookup for " + topicPartition));
        }
    }

    @Override
    public LookupResult<TopicPartition> handleResponse(
        Set<TopicPartition> requestPartitions,
        AbstractResponse abstractResponse
    ) {
        MetadataResponse response = (MetadataResponse) abstractResponse;
        Map<TopicPartition, Throwable> failed = new HashMap<>();
        Map<TopicPartition, Integer> mapped = new HashMap<>();

        for (MetadataResponseData.MetadataResponseTopic topicMetadata : response.data().topics()) {
            String topic = topicMetadata.name();
            Errors topicError = Errors.forCode(topicMetadata.errorCode());
            if (topicError != Errors.NONE) {
                handleTopicError(topic, topicError, requestPartitions, failed);
                continue;
            }


            for (MetadataResponseData.MetadataResponsePartition partitionMetadata : topicMetadata.partitions()) {
                TopicPartition topicPartition = new TopicPartition(topic, partitionMetadata.partitionIndex());
                Errors partitionError = Errors.forCode(partitionMetadata.errorCode());

                if (!requestPartitions.contains(topicPartition)) {
                    // The `Metadata` response always returns all partitions for requested
                    // topics, so we have to filter any that we are not interested in.
                    continue;
                }

                if (partitionError != Errors.NONE) {
                    handlePartitionError(topicPartition, partitionError, failed);
                    continue;
                }

                int leaderId = partitionMetadata.leaderId();
                if (leaderId >= 0) {
                    mapped.put(topicPartition, leaderId);
                } else {
                    log.debug("Metadata request for {} returned no error, but the leader is unknown. Will retry",
                        topicPartition);
                }
            }
        }
        return new LookupResult<>(failed, mapped);
    }

}
