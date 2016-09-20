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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class InternalTopicManager {

    private static final Logger log = LoggerFactory.getLogger(InternalTopicManager.class);

    // TODO: the following LogConfig dependency should be removed after KIP-4
    public static final String CLEANUP_POLICY_PROP = "cleanup.policy";
    private static final Set<String> CLEANUP_POLICIES = Utils.mkSet("compact", "delete");
    public static final String RETENTION_MS = "retention.ms";
    public static final Long WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);

    private final long windowChangeLogAdditionalRetention;

    private final int replicationFactor;
    private StreamsKafkaClient streamsKafkaClient;
    private StreamsConfig config;

    public static boolean isValidCleanupPolicy(final String cleanupPolicy) {
        if (cleanupPolicy == null) {
            return false;
        }
        final String[] policies = cleanupPolicy.toLowerCase(Locale.ROOT).split(",");
        for (String policy : policies) {
            if (!CLEANUP_POLICIES.contains(policy.trim())) {
                return false;
            }
        }
        return true;
    }

    public InternalTopicManager() {
        this.replicationFactor = 0;
        this.windowChangeLogAdditionalRetention = WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT;
    }

    public InternalTopicManager(StreamsConfig config, final int replicationFactor, long windowChangeLogAdditionalRetention) {
        this.config = config;
        this.streamsKafkaClient = new StreamsKafkaClient(config);
        this.replicationFactor = replicationFactor;
        this.windowChangeLogAdditionalRetention = windowChangeLogAdditionalRetention;
    }

    /**
     * If the topic exists do nothing (we don't change partititions or delete the existing topics).
     * Otherwise create the new topic.
     * @param topic
     * @param numPartitions
     */
    public void makeReady(InternalTopicConfig topic, int numPartitions) {

        if (!streamsKafkaClient.topicExists(topic.name())) {
            streamsKafkaClient.createTopic(topic, numPartitions, replicationFactor, windowChangeLogAdditionalRetention);
        } else {
            MetadataResponse.TopicMetadata topicMetadata = streamsKafkaClient.getTopicMetadata(topic.name());
            if (topicMetadata != null) {
                if (topicMetadata.partitionMetadata().size() != numPartitions) {
                    throw new StreamsException("Topic already exists but the number of partitions is not the same as the requested " + numPartitions + " partitions.");
                }
            } else {
                throw new StreamsException("Topic metadata is corrupted.");
            }

        }
    }
}
