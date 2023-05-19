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
package org.apache.kafka.connect.storage;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.TopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

public abstract class KafkaTopicBasedBackingStore {
    private static final Logger log = LoggerFactory.getLogger(KafkaTopicBasedBackingStore.class);

    Consumer<TopicAdmin> topicInitializer(String topic, NewTopic topicDescription, WorkerConfig config, Time time) {
        return admin -> {
            log.debug("Creating Connect internal topic for {}", getTopicPurpose());
            // Create the topic if it doesn't exist
            Set<String> newTopics = createTopics(topicDescription, admin, config, time);
            if (!newTopics.contains(topic)) {
                // It already existed, so check that the topic cleanup policy is compact only and not delete
                log.debug("Using admin client to check cleanup policy of '{}' topic is '{}'", topic, TopicConfig.CLEANUP_POLICY_COMPACT);
                admin.verifyTopicCleanupPolicyOnlyCompact(topic, getTopicConfig(), getTopicPurpose());
            }
        };
    }

    private Set<String> createTopics(NewTopic topicDescription, TopicAdmin admin, WorkerConfig config, Time time) {
        // get the prefixless default api timeout and retry backoff for topic creation retry configs
        AdminClientConfig adminClientConfig = new AdminClientConfig(config.originals());
        long timeoutMs = adminClientConfig.getInt(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
        long backOffMs = adminClientConfig.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG);
        return admin.createTopicsWithRetry(topicDescription, timeoutMs, backOffMs, time);
    }

    // visible for testing
    <K> KafkaBasedLog<K, byte[]> createKafkaBasedLog(String topic, Map<String, Object> producerProps,
                                                     Map<String, Object> consumerProps,
                                                     Callback<ConsumerRecord<K, byte[]>> consumedCallback,
                                                     final NewTopic topicDescription, Supplier<TopicAdmin> adminSupplier,
                                                     WorkerConfig config, Time time) {
        Consumer<TopicAdmin> createTopics = topicInitializer(topic, topicDescription, config, time);
        return new KafkaBasedLog<>(topic, producerProps, consumerProps, adminSupplier, consumedCallback, time, createTopics);
    }

    protected abstract String getTopicConfig();

    protected abstract String getTopicPurpose();
}
