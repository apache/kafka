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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.StreamsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class InternalTopicManager {

    private static final Logger log = LoggerFactory.getLogger(InternalTopicManager.class);
    public static final String CLEANUP_POLICY_PROP = "cleanup.policy";
    public static final String RETENTION_MS = "retention.ms";
    public static final Long WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
    private static final int MAX_TOPIC_READY_TRY = 5;
    private final Time time;
    private final long windowChangeLogAdditionalRetention;

    private final int replicationFactor;
    private final StreamsKafkaClient streamsKafkaClient;

    public InternalTopicManager(final StreamsKafkaClient streamsKafkaClient, final int replicationFactor,
                                final long windowChangeLogAdditionalRetention, final Time time) {
        this.streamsKafkaClient = streamsKafkaClient;
        this.replicationFactor = replicationFactor;
        this.windowChangeLogAdditionalRetention = windowChangeLogAdditionalRetention;
        this.time = time;
    }

    /**
     * Prepares a set of given internal topics.
     *
     * If a topic does not exist creates a new topic.
     * If a topic with the correct number of partitions exists ignores it.
     * If a topic exists already but has different number of partitions we fail and throw exception requesting user to reset the app before restarting again.
     */
    public void makeReady(final Map<InternalTopicConfig, Integer> topics) {
        for (int i = 0; i < MAX_TOPIC_READY_TRY; i++) {
            try {
                final MetadataResponse metadata = streamsKafkaClient.fetchMetadata();
                final Map<String, Integer> existingTopicPartitions = fetchExistingPartitionCountByTopic(metadata);
                final Map<InternalTopicConfig, Integer> topicsToBeCreated = validateTopicPartitions(topics, existingTopicPartitions);
                if (topicsToBeCreated.size() > 0) {
                    if (metadata.brokers().size() < replicationFactor) {
                        throw new StreamsException("Found only " + metadata.brokers().size() + " brokers, " +
                            " but replication factor is " + replicationFactor + "." +
                            " Decrease replication factor for internal topics via StreamsConfig parameter \"replication.factor\"" +
                            " or add more brokers to your cluster.");
                    }
                    streamsKafkaClient.createTopics(topicsToBeCreated, replicationFactor, windowChangeLogAdditionalRetention, metadata);
                }
                return;
            } catch (StreamsException ex) {
                log.warn("Could not create internal topics: {} Retry #{}", ex.getMessage(), i);
            }
            // backoff
            time.sleep(100L);
        }
        throw new StreamsException("Could not create internal topics.");
    }

    /**
     * Get the number of partitions for the given topics
     */
    public Map<String, Integer> getNumPartitions(final Set<String> topics) {
        for (int i = 0; i < MAX_TOPIC_READY_TRY; i++) {
            try {
                final MetadataResponse metadata = streamsKafkaClient.fetchMetadata();
                final Map<String, Integer> existingTopicPartitions = fetchExistingPartitionCountByTopic(metadata);
                existingTopicPartitions.keySet().retainAll(topics);

                return existingTopicPartitions;
            } catch (StreamsException ex) {
                log.warn("Could not get number of partitions: {} Retry #{}", ex.getMessage(), i);
            }
            // backoff
            time.sleep(100L);
        }
        throw new StreamsException("Could not get number of partitions.");
    }

    public void close() {
        try {
            streamsKafkaClient.close();
        } catch (IOException e) {
            log.warn("Could not close StreamsKafkaClient.");
        }
    }

    /**
     * Check the existing topics to have correct number of partitions; and return the non existing topics to be created
     */
    private Map<InternalTopicConfig, Integer> validateTopicPartitions(final Map<InternalTopicConfig, Integer> topicsPartitionsMap,
                                                                      final Map<String, Integer> existingTopicNamesPartitions) {
        final Map<InternalTopicConfig, Integer> topicsToBeCreated = new HashMap<>();
        for (Map.Entry<InternalTopicConfig, Integer> entry : topicsPartitionsMap.entrySet()) {
            InternalTopicConfig topic = entry.getKey();
            Integer partition = entry.getValue();
            if (existingTopicNamesPartitions.containsKey(topic.name())) {
                if (!existingTopicNamesPartitions.get(topic.name()).equals(partition)) {
                    throw new StreamsException("Existing internal topic " + topic.name() + " has invalid partitions." +
                            " Expected: " + partition + " Actual: " + existingTopicNamesPartitions.get(topic.name()) +
                            ". Use 'kafka.tools.StreamsResetter' tool to clean up invalid topics before processing.");
                }
            } else {
                topicsToBeCreated.put(topic, partition);
            }
        }

        return topicsToBeCreated;
    }

    private Map<String, Integer> fetchExistingPartitionCountByTopic(final MetadataResponse metadata) {
        // The names of existing topics and corresponding partition counts
        final Map<String, Integer> existingPartitionCountByTopic = new HashMap<>();
        final Collection<MetadataResponse.TopicMetadata> topicsMetadata = metadata.topicMetadata();

        for (MetadataResponse.TopicMetadata topicMetadata: topicsMetadata) {
            existingPartitionCountByTopic.put(topicMetadata.topic(), topicMetadata.partitionMetadata().size());
        }

        return existingPartitionCountByTopic;
    }
}
