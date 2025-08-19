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
import org.apache.kafka.metadata.MetadataCache;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;

/**
 * Handles the DescribeTopicPartitionsRequest, which provides metadata about topic partitions in a Kafka cluster.
 * This handler is responsible for managing authorization checks, cursor validation, and constructing the response data
 * for topics that are authorized for the requestor.
 */
public class DescribeTopicPartitionsRequestHandler {
    private final MetadataCache metadataCache;
    private final AuthHelper authHelper;
    private final KafkaConfig config;

    /**
     * Constructs a new DescribeTopicPartitionsRequestHandler.
     *
     * @param metadataCache The metadata cache used to retrieve topic information.
     * @param authHelper    The authentication helper used to check the authorization for topics.
     * @param config        The Kafka configuration.
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
     * Handles the DescribeTopicPartitionsRequest and constructs a response containing metadata about topic partitions.
     *
     * @param abstractRequest The request containing the metadata request for topic partitions.
     * @return A DescribeTopicPartitionsResponseData containing metadata for the requested topic partitions.
     */
    public DescribeTopicPartitionsResponseData handleDescribeTopicPartitionsRequest(
            final RequestChannel.Request abstractRequest) {
        final DescribeTopicPartitionsRequestData requestData = getRequestData(abstractRequest);

        // Get topics to describe based on request data (all topics or specific ones)
        final String cursorTopicName = requestData.cursor() != null ? requestData.cursor().topicName() : "";
        final Set<String> topicsToDescribe = getTopicsToDescribe(requestData, cursorTopicName);

        // Validate cursor if provided in the request
        validateCursor(requestData.cursor(), topicsToDescribe);

        // Handle topics that are unauthorized for the Describe operation
        final Set<DescribeTopicPartitionsResponseTopic> unauthorizedForDescribeTopicMetadata = new HashSet<>();
        final Stream<String> authorizedTopicsStream = filterAuthorizedTopics(
                abstractRequest,
                topicsToDescribe,
                unauthorizedForDescribeTopicMetadata,
                requestData.topics().isEmpty()
        );

        // Construct the response for authorized topics
        final DescribeTopicPartitionsResponseData response =
                buildResponse(authorizedTopicsStream, abstractRequest, requestData, cursorTopicName);

        // Add unauthorized topics to the response to avoid disclosing their existence
        response.topics().addAll(unauthorizedForDescribeTopicMetadata);
        return response;
    }

    /**
     * Extracts the request data from the abstract request.
     *
     * @param abstractRequest The incoming request.
     * @return The request data for the DescribeTopicPartitionsRequest.
     */
    private DescribeTopicPartitionsRequestData getRequestData(final RequestChannel.Request abstractRequest) {
        return ((DescribeTopicPartitionsRequest) abstractRequest.loggableRequest()).data();
    }

    /**
     * Determines the list of topics to describe based on the provided request data.
     * It can either fetch all topics or only the ones specified in the request.
     *
     * @param requestData The request data containing the list of topics.
     * @return A set of topics to describe.
     */
    private Set<String> getTopicsToDescribe(
            final DescribeTopicPartitionsRequestData requestData,
            final String cursorTopicName
    ) {
        final Set<String> topics = new HashSet<>();
        final boolean fetchAllTopics = requestData.topics().isEmpty();
        final DescribeTopicPartitionsRequestData.Cursor cursor = requestData.cursor();

        // If no topics are specified, fetch all topics that come after the cursor topic
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
                // The topic in cursor must be included in the topic list if provided.
                throw new InvalidRequestException("DescribeTopicPartitionsRequest topic list should contain the cursor topic: " + cursor.topicName());
            }
        }
        return topics;
    }

    /**
     * Validates the cursor from the request. If the cursor is provided, it checks that the partition index is valid
     * and that the topic in the cursor is included in the list of topics.
     *
     * @param cursor           The cursor for pagination, if provided in the request.
     * @param topicsToDescribe The list of topics that the requestor is authorized to describe.
     */
    private void validateCursor(
            final DescribeTopicPartitionsRequestData.Cursor cursor,
            final Set<String> topicsToDescribe
    ) {
        if (cursor != null) {
            // Validate that the partition index in the cursor is valid
            if (cursor.partitionIndex() < 0) {
                throw new InvalidRequestException("DescribeTopicPartitionsRequest cursor partition must be valid: " + cursor);
            }

            // Ensure the cursor topic is included in the list of topics
            if (!topicsToDescribe.contains(cursor.topicName())) {
                throw new InvalidRequestException("DescribeTopicPartitionsRequest topic list should contain the cursor topic: " + cursor.topicName());
            }
        }
    }

    /**
     * Filters the topics based on authorization. It ensures that only topics the requestor is authorized to describe are included.
     * Unauthorized topics are added to the unauthorized topics list.
     *
     * @param abstractRequest                      The incoming request.
     * @param topicsToDescribe                     The list of topics to filter.
     * @param unauthorizedForDescribeTopicMetadata A set to store topics that the requestor is unauthorized to describe.
     * @param fetchAllTopics                       A flag indicating whether to fetch all topics or only specified ones.
     * @return A stream of authorized topic names.
     */
    private Stream<String> filterAuthorizedTopics(
            final RequestChannel.Request abstractRequest,
            final Set<String> topicsToDescribe,
            final Set<DescribeTopicPartitionsResponseTopic> unauthorizedForDescribeTopicMetadata,
            final boolean fetchAllTopics
    ) {
        return topicsToDescribe.stream().sorted().filter(topicName -> {
            // Check authorization for each topic
            final boolean isAuthorized = authHelper.authorize(abstractRequest.context(),
                    DESCRIBE, TOPIC, topicName, true, true, 1
            );
            if (!fetchAllTopics && !isAuthorized) {
                // If unauthorized, add the topic to the unauthorized list with an empty UUID
                unauthorizedForDescribeTopicMetadata.add(describeTopicPartitionsResponseTopic(
                        Errors.TOPIC_AUTHORIZATION_FAILED, topicName, Uuid.ZERO_UUID, false, List.of())
                );
            }
            return isAuthorized;
        });
    }

    /**
     * Constructs the response data based on authorized topics.
     *
     * @param authorizedTopicsStream A stream of authorized topic names.
     * @param abstractRequest        The incoming request.
     * @param requestData            The request data containing the cursor and partition limits.
     * @return The constructed response data with metadata for the authorized topics.
     */
    private DescribeTopicPartitionsResponseData buildResponse(
            final Stream<String> authorizedTopicsStream,
            final RequestChannel.Request abstractRequest,
            final DescribeTopicPartitionsRequestData requestData,
            final String cursorTopicName
    ) {
        return metadataCache.describeTopicResponse(
                authorizedTopicsStream.iterator(),
                abstractRequest.context().listenerName,
                (String topicName) -> topicName.equals(cursorTopicName) ? requestData.cursor().partitionIndex() : 0,
                Math.max(Math.min(config.maxRequestPartitionSizeLimit(), requestData.responsePartitionLimit()), 1),
                requestData.topics().isEmpty()
        );
    }

    /**
     * Constructs a DescribeTopicPartitionsResponseTopic object, which contains metadata about a single topic,
     * including error codes, topic ID, partition data, and whether the topic is internal.
     *
     * @param error         The error that occurred while accessing the topic.
     * @param topic         The name of the topic.
     * @param topicId       The unique identifier for the topic.
     * @param isInternal    Whether the topic is internal or not.
     * @param partitionData The partition data associated with the topic.
     * @return A DescribeTopicPartitionsResponseTopic object with the specified metadata.
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
