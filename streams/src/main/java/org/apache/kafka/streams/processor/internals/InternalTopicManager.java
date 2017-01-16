/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.streams.errors.StreamsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InternalTopicManager {

    private static final Logger log = LoggerFactory.getLogger(InternalTopicManager.class);
    public static final String CLEANUP_POLICY_PROP = "cleanup.policy";
    public static final String RETENTION_MS = "retention.ms";
    public static final Long WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
    private static final int MAX_TOPIC_READY_TRY = 5;

    private final long windowChangeLogAdditionalRetention;

    private final int replicationFactor;
    private final StreamsKafkaClient streamsKafkaClient;

    public InternalTopicManager(final StreamsKafkaClient streamsKafkaClient, final int replicationFactor, final long windowChangeLogAdditionalRetention) {
        this.streamsKafkaClient = streamsKafkaClient;
        this.replicationFactor = replicationFactor;
        this.windowChangeLogAdditionalRetention = windowChangeLogAdditionalRetention;
    }

    /**
     * Prepares the set of given internal topics. If the topic with the correct number of partitions exists ignores it. For the ones with different number of
     * partitions delete them and create new ones with correct number of partitons along with the non existing topics.
     * @param topic
     */
    public void makeReady(final InternalTopicConfig topic, int numPartitions) {

        Map<InternalTopicConfig, Integer> topics = new HashMap<>();
        topics.put(topic, numPartitions);
        for (int i = 0; i < MAX_TOPIC_READY_TRY; i++) {
            try {
                Collection<MetadataResponse.TopicMetadata> topicMetadatas = streamsKafkaClient.fetchTopicMetadata();
                Map<InternalTopicConfig, Integer> topicsToBeDeleted = getTopicsToBeDeleted(topics, topicMetadatas);
                Map<InternalTopicConfig, Integer> topicsToBeCreated = filterExistingTopics(topics, topicMetadatas);
                topicsToBeCreated.putAll(topicsToBeDeleted);
                streamsKafkaClient.deleteTopics(topicsToBeDeleted);
                streamsKafkaClient.createTopics(topicsToBeCreated, replicationFactor, windowChangeLogAdditionalRetention);
                return;
            } catch (StreamsException ex) {
                log.warn("Could not create internal topics: " + ex.getMessage() + ". Retry #" + i);
            }
        }
        throw new StreamsException("Could not create internal topics.");
    }

    public void close() {
        try {
            streamsKafkaClient.close();
        } catch (IOException e) {
            log.warn("Could not close StreamsKafkaClient.");
        }
    }

    /**
     * Return the non existing topics.
     *
     * @param topicsPartitionsMap
     * @param topicsMetadata
     * @return
     */
    private Map<InternalTopicConfig, Integer> filterExistingTopics(final Map<InternalTopicConfig, Integer> topicsPartitionsMap, Collection<MetadataResponse.TopicMetadata> topicsMetadata) {
        Map<String, Integer> existingTopicNamesPartitions = getExistingTopicNamesPartitions(topicsMetadata);
        Map<InternalTopicConfig, Integer> nonExistingTopics = new HashMap<>();
        // Add the topics that don't exist to the nonExistingTopics.
        for (InternalTopicConfig topic: topicsPartitionsMap.keySet()) {
            if (existingTopicNamesPartitions.get(topic.name()) == null) {
                nonExistingTopics.put(topic, topicsPartitionsMap.get(topic));
            }
        }
        return nonExistingTopics;
    }

    /**
     * Return the topics that exist but have different partiton number to be deleted.
     * @param topicsPartitionsMap
     * @param topicsMetadata
     * @return
     */
    private Map<InternalTopicConfig, Integer> getTopicsToBeDeleted(final Map<InternalTopicConfig, Integer> topicsPartitionsMap, Collection<MetadataResponse.TopicMetadata> topicsMetadata) {
        Map<String, Integer> existingTopicNamesPartitions = getExistingTopicNamesPartitions(topicsMetadata);
        Map<InternalTopicConfig, Integer> deleteTopics = new HashMap<>();
        // Add the topics that don't exist to the nonExistingTopics.
        for (InternalTopicConfig topic: topicsPartitionsMap.keySet()) {
            if (existingTopicNamesPartitions.get(topic.name()) != null) {
                if (existingTopicNamesPartitions.get(topic.name()) != topicsPartitionsMap.get(topic)) {
                    deleteTopics.put(topic, topicsPartitionsMap.get(topic));
                }
            }
        }
        return deleteTopics;
    }

    private Map<String, Integer> getExistingTopicNamesPartitions(Collection<MetadataResponse.TopicMetadata> topicsMetadata) {
        // The names of existing topics
        Map<String, Integer> existingTopicNamesPartitions = new HashMap<>();
        for (MetadataResponse.TopicMetadata topicMetadata: topicsMetadata) {
            existingTopicNamesPartitions.put(topicMetadata.topic(), topicMetadata.partitionMetadata().size());
        }
        return existingTopicNamesPartitions;
    }

}
