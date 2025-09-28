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

package kafka.server.handlers;

import kafka.network.RequestChannel;
import kafka.server.AuthHelper;
import kafka.server.KafkaConfig;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeTopicPartitionsRequest;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.metadata.MetadataCache;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;

/**
 * Handles DescribeTopicPartitions requests by performing cursor validation, authorization checks,
 * topic filtering, and constructing the response with partition metadata.
 */
public class DescribeTopicPartitionsRequestHandler {
    private final MetadataCache metadataCache;
    private final AuthHelper authHelper;
    private final KafkaConfig config;

    /**
     * Creates a new request handler for DescribeTopicPartitions.
     *
     * @param metadataCache metadata lookup for topics
     * @param authHelper    authorization utility
     * @param config        broker configuration
     */
    public DescribeTopicPartitionsRequestHandler(
            final MetadataCache metadataCache,
            final AuthHelper authHelper,
            final KafkaConfig config) {
        this.metadataCache = metadataCache;
        this.authHelper = authHelper;
        this.config = config;
    }

    /**
     * Main entry point for processing DescribeTopicPartitions requests.
     *
     * @param abstractRequest the incoming request
     * @return response data containing metadata for authorized topics
     */
    public DescribeTopicPartitionsResponseData handleDescribeTopicPartitionsRequest(
            final RequestChannel.Request abstractRequest) {

        final DescribeTopicPartitionsRequestData requestData = getRequestData(abstractRequest);
        final String cursorTopicName = requestData.cursor() != null ? requestData.cursor().topicName() : "";
        final Set<String> topicsToDescribe = getTopicsToDescribe(requestData, cursorTopicName);

        validateCursor(requestData.cursor());

        final Set<DescribeTopicPartitionsResponseTopic> unauthorizedTopics = new HashSet<>();
        final Stream<String> authorizedTopics = filterAuthorizedTopics(
                abstractRequest,
                topicsToDescribe,
                unauthorizedTopics,
                requestData.topics().isEmpty()
        );

        final DescribeTopicPartitionsResponseData response =
                buildResponse(authorizedTopics, abstractRequest, requestData, cursorTopicName);

        // Include authorized operations
        response.topics().forEach(topicData ->
                topicData.setTopicAuthorizedOperations(
                        authHelper.authorizedOperations(abstractRequest, new Resource(TOPIC, topicData.name()))
                )
        );

        // Include unauthorized topic metadata
        response.topics().addAll(unauthorizedTopics);
        return response;
    }

    /**
     * Parses the typed request data from the raw abstract request.
     */
    private DescribeTopicPartitionsRequestData getRequestData(final RequestChannel.Request abstractRequest) {
        return ((DescribeTopicPartitionsRequest) abstractRequest.loggableRequest()).data();
    }

    /**
     * Computes the set of topics to describe based on the request and cursor.
     * Filters out topics before the cursor topic (if present).
     *
     * @throws InvalidRequestException if the cursor references a topic not in the requested list
     */
    private Set<String> getTopicsToDescribe(
            final DescribeTopicPartitionsRequestData requestData,
            final String cursorTopicName
    ) {
        final Set<String> topics = new HashSet<>();
        final boolean fetchAllTopics = requestData.topics().isEmpty();
        final DescribeTopicPartitionsRequestData.Cursor cursor = requestData.cursor();

        if (fetchAllTopics) {
            metadataCache.getAllTopics().forEach(topicName -> {
                if (topicName.compareTo(cursorTopicName) >= 0) {
                    topics.add(topicName);
                }
            });
        } else {
            requestData.topics().forEach(topic -> {
                String topicName = topic.name();
                if (topicName.compareTo(cursorTopicName) >= 0) {
                    topics.add(topicName);
                }
            });

            if (cursor != null && !topics.contains(cursor.topicName())) {
                throw new InvalidRequestException(
                        "DescribeTopicPartitionsRequest topic list should contain the cursor topic: " + cursor.topicName());
            }
        }
        return topics;
    }

    /**
     * Validates the cursor's partition index, if a cursor is provided.
     *
     * @throws InvalidRequestException if the partition index is negative
     */
    private void validateCursor(final DescribeTopicPartitionsRequestData.Cursor cursor) {
        if (cursor != null && cursor.partitionIndex() < 0) {
            throw new InvalidRequestException(
                    "Invalid cursor: partition index must be non-negative. Received: " + cursor
            );
        }
    }

    /**
     * Filters the input topics based on authorization.
     * Adds unauthorized topics to the result with masked metadata.
     *
     * @param abstractRequest     the request context
     * @param topicsToDescribe    candidate topics to filter
     * @param unauthorizedTopics  output container for unauthorized topics
     * @param fetchAllTopics      true if request was for all topics
     * @return a stream of authorized topic names
     */
    private Stream<String> filterAuthorizedTopics(
            final RequestChannel.Request abstractRequest,
            final Set<String> topicsToDescribe,
            final Set<DescribeTopicPartitionsResponseTopic> unauthorizedTopics,
            final boolean fetchAllTopics
    ) {
        return topicsToDescribe.stream().sorted().filter(topicName -> {
            final boolean isAuthorized = authHelper.authorize(
                    abstractRequest.context(), DESCRIBE, TOPIC, topicName, true, true, 1
            );

            if (!fetchAllTopics && !isAuthorized) {
                unauthorizedTopics.add(describeTopicPartitionsResponseTopic(
                        Errors.TOPIC_AUTHORIZATION_FAILED, topicName, Uuid.ZERO_UUID, false, List.of()
                ));
            }
            return isAuthorized;
        });
    }

    /**
     * Builds the response containing partition metadata for authorized topics.
     *
     * @param authorizedTopics    stream of authorized topic names
     * @param abstractRequest     the original request
     * @param requestData         the parsed request data
     * @param cursorTopicName     the topic referenced in the cursor (if any)
     */
    private DescribeTopicPartitionsResponseData buildResponse(
            final Stream<String> authorizedTopics,
            final RequestChannel.Request abstractRequest,
            final DescribeTopicPartitionsRequestData requestData,
            final String cursorTopicName
    ) {
        return metadataCache.describeTopicResponse(
                authorizedTopics.iterator(),
                abstractRequest.context().listenerName,
                topicName -> topicName.equals(cursorTopicName) ? requestData.cursor().partitionIndex() : 0,
                Math.max(Math.min(config.maxRequestPartitionSizeLimit(), requestData.responsePartitionLimit()), 1),
                requestData.topics().isEmpty()
        );
    }

    /**
     * Creates a response topic object, typically used to report errors like unauthorized access.
     */
    private DescribeTopicPartitionsResponseTopic describeTopicPartitionsResponseTopic(
            final Errors error,
            final String topic,
            final Uuid topicId,
            final Boolean isInternal,
            final List<DescribeTopicPartitionsResponsePartition> partitionData
    ) {
        return new DescribeTopicPartitionsResponseTopic()
                .setErrorCode(error.code())
                .setName(topic)
                .setTopicId(topicId)
                .setIsInternal(isInternal)
                .setPartitions(partitionData);
    }
}
