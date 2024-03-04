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
import kafka.server.metadata.KRaftMetadataCache;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeTopicPartitionsRequest;
import org.apache.kafka.common.resource.Resource;
import scala.collection.JavaConverters;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;

public class DescribeTopicPartitionsRequestHandler {
    KRaftMetadataCache metadataCache;
    AuthHelper authHelper;
    KafkaConfig config;

    public DescribeTopicPartitionsRequestHandler(
        KRaftMetadataCache metadataCache,
        AuthHelper authHelper,
        KafkaConfig config
    ) {
        this.metadataCache = metadataCache;
        this.authHelper = authHelper;
        this.config = config;
    }

    public DescribeTopicPartitionsResponseData handleDescribeTopicPartitionsRequest(RequestChannel.Request abstractRequest) {
        DescribeTopicPartitionsRequestData request = ((DescribeTopicPartitionsRequest) abstractRequest.loggableRequest()).data();
        Set<String> topics = new HashSet<>();
        boolean fetchAllTopics = request.topics().isEmpty();
        DescribeTopicPartitionsRequestData.Cursor cursor = request.cursor();
        String cursorTopicName = cursor != null ? cursor.topicName() : "";
        if (fetchAllTopics) {
            JavaConverters.asJavaCollection(metadataCache.getAllTopics()).forEach(topicName -> {
                if (topicName.compareTo(cursorTopicName) >= 0) {
                    topics.add(topicName);
                }
            });
        } else {
            request.topics().forEach(topic -> {
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

        // Do not disclose the existence of topics unauthorized for Describe, so we've not even checked if they exist or not
        Set<DescribeTopicPartitionsResponseTopic> unauthorizedForDescribeTopicMetadata = new HashSet<>();

        Stream<String> authorizedTopicsStream = topics.stream().sorted().filter(topicName -> {
            boolean isAuthorized = authHelper.authorize(
                abstractRequest.context(), DESCRIBE, TOPIC, topicName, true, true, 1);
            if (!fetchAllTopics && !isAuthorized) {
                // We should not return topicId when on unauthorized error, so we return zero uuid.
                unauthorizedForDescribeTopicMetadata.add(describeTopicPartitionsResponseTopic(
                    Errors.TOPIC_AUTHORIZATION_FAILED, topicName, Uuid.ZERO_UUID, false, Collections.emptyList())
                );
            }
            return isAuthorized;
        });

        DescribeTopicPartitionsResponseData response = metadataCache.getTopicMetadataForDescribeTopicResponse(
            JavaConverters.asScalaIterator(authorizedTopicsStream.iterator()),
            abstractRequest.context().listenerName,
            (String topicName) -> topicName.equals(cursorTopicName) ? cursor.partitionIndex() : 0,
            Math.min(config.maxRequestPartitionSizeLimit(), request.responsePartitionLimit()),
            fetchAllTopics
        );

        // get topic authorized operations
        response.topics().forEach(topicData ->
            topicData.setTopicAuthorizedOperations(authHelper.authorizedOperations(abstractRequest, new Resource(TOPIC, topicData.name()))));

        response.topics().addAll(unauthorizedForDescribeTopicMetadata);
        return response;
    }

    private DescribeTopicPartitionsResponseTopic describeTopicPartitionsResponseTopic(
        Errors error,
        String topic,
        Uuid topicId,
        Boolean isInternal,
        List<DescribeTopicPartitionsResponsePartition> partitionData
    ) {
        return new DescribeTopicPartitionsResponseTopic()
            .setErrorCode(error.code())
            .setName(topic)
            .setTopicId(topicId)
            .setIsInternal(isInternal)
            .setPartitions(partitionData);
    }
}
