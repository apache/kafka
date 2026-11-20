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
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.TopicAdmin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public abstract class KafkaTopicBasedBackingStore {
    private static final Logger log = LoggerFactory.getLogger(KafkaTopicBasedBackingStore.class);

    Consumer<TopicAdmin> topicInitializer(String topic, NewTopic topicDescription, WorkerConfig config, Time time) {
        return admin -> {
            if (config.internalTopicsCreationEnabled()) {
                log.debug("Creating Connect internal topic for {}", getTopicPurpose());
                createOrFindTopic(topic, topicDescription, admin, config, time);
            } else {
                log.debug("Skipping creation of Connect internal topic for {} because automatic topic creation is disabled", getTopicPurpose());
                Map<String, TopicDescription> existing = admin.describeTopics(topic);
                if (existing.isEmpty()) {
                    String msg = String.format("Topic '%s' specified via the '%s' property is missing." +
                                    " The config '%s' is set to '%s', so automatic creation of internal topics is disabled." +
                                    " Either enable automatic creation or create the topics manually.",
                            topic, getTopicConfig(), DistributedConfig.INTERNAL_TOPICS_AUTOMATIC_CREATION_ENABLE_CONFIG, config.internalTopicsCreationEnabled());
                    throw new ConnectException(msg);
                }

                verifyTopicConfig(topic, admin);
            }
        };
    }

    private void verifyTopicConfig(String topic, TopicAdmin admin) {
        // It already existed, so check that the topic cleanup policy is compact only and not delete
        log.debug("Using admin client to check cleanup policy of '{}' topic is '{}'", topic, TopicConfig.CLEANUP_POLICY_COMPACT);
        admin.verifyTopicCleanupPolicyOnlyCompact(topic, getTopicConfig(), getTopicPurpose());
    }

    private void createOrFindTopic(String topic, NewTopic topicDescription, TopicAdmin admin, WorkerConfig config, Time time) {
        // get the prefixless default api timeout and retry backoff for topic creation retry configs
        AdminClientConfig adminClientConfig = new AdminClientConfig(config.originals());
        long timeoutMs = adminClientConfig.getInt(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
        long backOffMs = adminClientConfig.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG);
        Timer timer = time.timer(timeoutMs);
        while (true) {
            TopicAdmin.TopicCreationResponse response =
                    admin.createTopicsWithRetry(topicDescription, timer.remainingMs(), backOffMs, time);
            if (response.isCreated(topic)) {
                return;
            }
            // The topic already exists, or it could not be created (e.g. the worker is not authorized to create it),
            // in which case check whether it exists
            if (response.isExisting(topic) || !admin.describeTopics(topic).isEmpty()) {
                verifyTopicConfig(topic, admin);
                return;
            }
            // The topic is missing. Don't continue without it, since it could then be auto-created by the broker
            // with the wrong configs. The reason it could not be created may be transient, so retry until timeout.
            timer.update();
            if (timer.isExpired()) {
                String msg = String.format("Topic '%s' supplied via the '%s' property does not exist and could not be created." +
                                " Either create the topic manually with '%s=%s', or allow the worker to create it.",
                        topic, getTopicConfig(), TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
                throw new ConnectException(msg);
            }
            log.info("Topic '{}' supplied via the '{}' property does not exist and could not be created, retrying, {}ms remaining",
                    topic, getTopicConfig(), timer.remainingMs());
            timer.sleep(backOffMs);
        }
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
