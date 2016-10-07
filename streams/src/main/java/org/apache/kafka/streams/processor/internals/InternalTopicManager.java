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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InternalTopicManager {

    public static final String CLEANUP_POLICY_PROP = "cleanup.policy";
    public static final String RETENTION_MS = "retention.ms";
    public static final Long WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);

    private final long windowChangeLogAdditionalRetention;

    private final int replicationFactor;
    private final StreamsKafkaClient streamsKafkaClient;

    public InternalTopicManager(final StreamsKafkaClient streamsKafkaClient, final int replicationFactor, final long windowChangeLogAdditionalRetention) {
        this.streamsKafkaClient = streamsKafkaClient;
        this.replicationFactor = replicationFactor;
        this.windowChangeLogAdditionalRetention = windowChangeLogAdditionalRetention;
    }

    public void makeReady(final InternalTopicConfig topic, int numPartitions) {
        Map<InternalTopicConfig, Integer> topics = new HashMap<>();
        topics.put(topic, numPartitions);
        makeReady(topics);
    }

    /**
     * Prepares the set of given internal topics. If the topic with the correct number of partitions exists ignores it. For the ones with different number of
     * partitions delete them and create new ones with correct number of partitons along with the non existing topics.
     * @param topics
     */
    public void makeReady(final Map<InternalTopicConfig, Integer> topics) {

        //TODO: Add loop/ inspect th error codes
        Collection<MetadataResponse.TopicMetadata> topicMetadatas = streamsKafkaClient.fetchTopicMetadata();
        Map<InternalTopicConfig, Integer> topicsToBeDeleted = streamsKafkaClient.getTopicsToBeDeleted(topics, topicMetadatas);
        Map<InternalTopicConfig, Integer> topicsToBeCreated = streamsKafkaClient.filterExistingTopics(topics, topicMetadatas);
        topicsToBeCreated.putAll(topicsToBeDeleted);
        streamsKafkaClient.deleteTopics(topicsToBeDeleted);
        streamsKafkaClient.createTopics(topicsToBeCreated, replicationFactor, windowChangeLogAdditionalRetention);
    }

    public void close() throws IOException {
        streamsKafkaClient.close();
    }

}
