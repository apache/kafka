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
package org.apache.kafka.connect.util;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Utility to simplify creating and managing topics via the {@link org.apache.kafka.clients.admin.AdminClient}.
 */
public class TopicAdmin implements AutoCloseable {

    private static final String CLEANUP_POLICY_CONFIG = "cleanup.policy";
    private static final String CLEANUP_POLICY_COMPACT = "compact";

    private static final String MIN_INSYNC_REPLICAS_CONFIG = "min.insync.replicas";

    private static final String UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG = "unclean.leader.election.enable";

    /**
     * A builder of {@link NewTopic} instances.
     */
    public static class NewTopicBuilder {
        private String name;
        private int numPartitions;
        private short replicationFactor;
        private Map<String, String> configs = new HashMap<>();

        NewTopicBuilder(String name) {
            this.name = name;
        }

        /**
         * Specify the desired number of partitions for the topic.
         *
         * @param numPartitions the desired number of partitions; must be positive
         * @return this builder to allow methods to be chained; never null
         */
        public NewTopicBuilder partitions(int numPartitions) {
            this.numPartitions = numPartitions;
            return this;
        }

        /**
         * Specify the desired replication factor for the topic.
         *
         * @param replicationFactor the desired replication factor; must be positive
         * @return this builder to allow methods to be chained; never null
         */
        public NewTopicBuilder replicationFactor(short replicationFactor) {
            this.replicationFactor = replicationFactor;
            return this;
        }

        /**
         * Specify that the topic should be compacted.
         *
         * @return this builder to allow methods to be chained; never null
         */
        public NewTopicBuilder compacted() {
            this.configs.put(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT);
            return this;
        }

        /**
         * Specify the minimum number of in-sync replicas required for this topic.
         *
         * @param minInSyncReplicas the minimum number of in-sync replicas allowed for the topic; must be positive
         * @return this builder to allow methods to be chained; never null
         */
        public NewTopicBuilder minInSyncReplicas(short minInSyncReplicas) {
            this.configs.put(MIN_INSYNC_REPLICAS_CONFIG, Short.toString(minInSyncReplicas));
            return this;
        }

        /**
         * Specify whether the broker is allowed to elect a leader that was not an in-sync replica when no ISRs
         * are available.
         *
         * @param allow true if unclean leaders can be elected, or false if they are not allowed
         * @return this builder to allow methods to be chained; never null
         */
        public NewTopicBuilder uncleanLeaderElection(boolean allow) {
            this.configs.put(UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, Boolean.toString(allow));
            return this;
        }

        /**
         * Specify the configuration properties for the topic, overwriting any previously-set properties.
         *
         * @param configs the desired topic configuration properties, or null if all existing properties should be cleared
         * @return this builder to allow methods to be chained; never null
         */
        public NewTopicBuilder config(Map<String, Object> configs) {
            if (configs != null) {
                for (Map.Entry<String, Object> entry : configs.entrySet()) {
                    Object value = entry.getValue();
                    this.configs.put(entry.getKey(), value != null ? value.toString() : null);
                }
            } else {
                this.configs.clear();
            }
            return this;
        }

        /**
         * Build the {@link NewTopic} representation.
         *
         * @return the topic description; never null
         */
        public NewTopic build() {
            return new NewTopic(name, numPartitions, replicationFactor).configs(configs);
        }
    }

    /**
     * Obtain a {@link NewTopicBuilder builder} to define a {@link NewTopic}.
     * @param topicName the name of the topic
     * @return the {@link NewTopic} description of the topic; never null
     */
    public static NewTopicBuilder defineTopic(String topicName) {
        return new NewTopicBuilder(topicName);
    }

    private static final Logger log = LoggerFactory.getLogger(TopicAdmin.class);
    private final Map<String, Object> adminConfig;
    private final AdminClient admin;

    /**
     * Create a new topic admin component with the given configuration.
     *
     * @param adminConfig the configuration for the {@link AdminClient}
     */
    public TopicAdmin(Map<String, Object> adminConfig) {
        this(adminConfig, AdminClient.create(adminConfig));
    }

    // visible for testing
    TopicAdmin(Map<String, Object> adminConfig, AdminClient adminClient) {
        this.admin = adminClient;
        this.adminConfig = adminConfig != null ? adminConfig : Collections.<String, Object>emptyMap();
    }

    /**
     * Get a description of one or more topics.
     *
     * @param topicNames the names of the topics
     * @return the mutable map of existing topic descriptions keyed by topic names; null only if the broker cannot perform this operation
     * @throws ExecutionException   if an exception occurred during this operation
     * @throws InterruptedException if this thread was interrupted while waiting for the description
     */
    private Map<String, TopicDescription> describeTopics(Collection<String> topicNames) throws ExecutionException, InterruptedException {
        if (topicNames == null || topicNames.isEmpty()) return Collections.emptyMap();
        Map<String, KafkaFuture<TopicDescription>> results = admin.describeTopics(topicNames).results();
        Map<String, TopicDescription> descriptions = new HashMap<>();
        for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : results.entrySet()) {
            String topic = entry.getKey();
            try {
                descriptions.put(topic, entry.getValue().get());
            } catch (InterruptedException e) {
                throw e;
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof UnsupportedVersionException) {
                    // Broker is too old
                    return null;
                }
                if (cause instanceof UnknownTopicOrPartitionException || cause instanceof InvalidTopicException) {
                    continue;
                }
                throw e;
            }
        }
        return descriptions;
    }

    /**
     * Given the topic definition, check whether the topic exists and if not then attempt to create the
     * missing topic. The brokers will use its default topic options for anything not specified in the supplied definition.
     * This method returns the topic details for the newly-created or previously-existing topic.
     * <p>
     * This operation requires the broker to support clients performing several administrative operations. If the broker
     * does not support this capability, this method returns null.
     *
     * @param topic the specifications of the topic
     * @return the topic description of the newly-created or existing topic; null only
     * if the broker is too old to perform this operation
     * @throws ConnectException if an error occurs, the operation takes too long, or the thread is interrupted while
     *                          attempting to perform this operation
     */
    public TopicDescription createTopicIfMissing(NewTopic topic) {
        Map<String, TopicDescription> result = createTopicsIfMissing(topic);
        return result != null ? result.get(topic.name()) : null;
    }

    /**
     * Given the topic definitions, check whether the topics exists and for all those that do not attempt to create the
     * missing topics. The brokers will use its default topic options for anything not specified in the supplied definitions.
     * This method returns the topic details for all of the topic included in the supplied descriptions.
     * <p>
     * This operation requires the broker to support clients performing several administrative operations. If the broker
     * does not support this capability, this method returns null.
     *
     * @param topics the specifications of the topics
     * @return the topic description of the newly-created or existing topics, keyed by the topic names; null only
     * if the broker is too old to perform this operation
     * @throws ConnectException if an error occurs, the operation takes too long, or the thread is interrupted while
     *                          attempting to perform this operation
     */
    public Map<String, TopicDescription> createTopicsIfMissing(NewTopic... topics) {
        if (topics == null || topics.length == 0) return Collections.emptyMap();

        Map<String, NewTopic> topicsByName = new HashMap<>();
        for (NewTopic topic : topics) {
            if (topic != null) topicsByName.put(topic.name(), topic);
        }
        Set<String> requestedTopicNames = new HashSet<>(topicsByName.keySet());

        try {
            // Check for existing topics
            Map<String, TopicDescription> descriptions = describeTopics(requestedTopicNames);
            if (descriptions == null) {
                // The broker is too old to do this work, so return immediately
                log.debug("Unable to use Kafka admin client to read topic descriptions using the brokers at {}", this);
                return null;
            }

            for (String existingTopicName : descriptions.keySet()) {
                log.debug("Found existing topic '{}' on the brokers at {}", existingTopicName, this);
                topicsByName.remove(existingTopicName);
            }
            if (topicsByName.isEmpty()) {
                return descriptions;
            }

            // Attempt to create any missing topics
            CreateTopicsOptions args = new CreateTopicsOptions().validateOnly(false);
            Map<String, KafkaFuture<Void>> newResults = admin.createTopics(topicsByName.values(), args).results();

            // Iterate over each one so that we can handle individual failures like when some topics already exist
            for (Map.Entry<String, KafkaFuture<Void>> entry : newResults.entrySet()) {
                String topic = entry.getKey();
                try {
                    entry.getValue().get();
                    log.info("Created topic {} on brokers at {}", topicsByName.get(topic), this);
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof UnsupportedVersionException) {
                        // Broker is too old
                        log.info("Unable to use Kafka admin client to create topics using the brokers at {}", this);
                        return null;
                    }
                    if (e.getCause() instanceof TopicExistsException) {
                        // Must have been created by another client, so keep going
                        log.debug("Found existing topic '{}' on the brokers at {}", topic, this);
                        continue;
                    }
                    throw e;
                }
            }
            // Get descriptions for the topics we just tried to create, including any that did in fact exist
            Map<String, TopicDescription> newDescriptions = describeTopics(topicsByName.keySet());
            // And combine with the descriptions for the already-existing topics
            descriptions.putAll(newDescriptions);
            return descriptions;
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new ConnectException("Interrupted while attempting to create/find topic(s) '" + requestedTopicNames + "'", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnsupportedVersionException) {
                // Broker is too old
                log.debug("Unable to use Kafka admin client to create topic descriptions using the brokers at {}", this);
                return null;
            }
            if (cause instanceof TimeoutException) {
                // Timed out waiting for the operation to complete
                throw new ConnectException("Timed out while checking for or creating topic(s) '" + requestedTopicNames + "'." +
                        " This could indicate a connectivity issue, unavailable topic partitions, or if" +
                        " this is your first use of the topic it may have taken too long to create.", cause);
            }
            throw new ConnectException("Error while attempting to create/find topics '" + requestedTopicNames + "'", cause);
        }
    }

    @Override
    public void close() {
        admin.close();
    }

    @Override
    public String toString() {
        Object servers = adminConfig.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
        return servers != null ? servers.toString() : "<unknown>";
    }
}
