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


import java.io.IOException;
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

    /**
     * If the topic exists do nothing (we don't change partititions or delete the existing topics).
     * Otherwise create the new topic.
     * @param topic
     * @param numPartitions
     */
    public void makeReady(final InternalTopicConfig topic, final int numPartitions) {

        if (!streamsKafkaClient.topicExists(topic.name())) {
            streamsKafkaClient.createTopic(topic, numPartitions, replicationFactor, windowChangeLogAdditionalRetention);
            // Make sure the topic was created.
            if (!streamsKafkaClient.topicExists(topic.name())) {
                throw new StreamsException("Cound not create topic: " + topic.name());
            }
        } else {
            final MetadataResponse.TopicMetadata topicMetadata = streamsKafkaClient.getTopicMetadata(topic.name());
            if (topicMetadata != null) {
                if (topicMetadata.error().code() != 0) {
                    throw new StreamsException("Topic metadata request returned with error code " + topicMetadata.error().code() + ": " + topicMetadata.error().message());
                }
                if (topicMetadata.partitionMetadata().size() != numPartitions) {
                    // Delete the topic and create a new one with the requested number of partitions.
                    streamsKafkaClient.deleteTopic(topic);
                    streamsKafkaClient.createTopic(topic, numPartitions, replicationFactor, windowChangeLogAdditionalRetention);
                    if (!streamsKafkaClient.topicExists(topic.name())) {
                        throw new StreamsException("Deleted the topic " + topic.name() + " but could not create it again.");
                    }

                }
            } else {
                throw new StreamsException("Could not fetch the topic metadata.");
            }

        }
    }

    public void close() throws IOException {
        streamsKafkaClient.close();
    }
}
