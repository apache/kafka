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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utility to simplify creating and managing topics via the {@link Admin}.
 */
public class TopicAdmin implements AutoCloseable {

    public static final TopicCreationResponse EMPTY_CREATION = new TopicCreationResponse(Collections.emptySet(), Collections.emptySet());

    public static class TopicCreationResponse {

        private final Set<String> created;
        private final Set<String> existing;

        public TopicCreationResponse(Set<String> createdTopicNames, Set<String> existingTopicNames) {
            this.created = Collections.unmodifiableSet(createdTopicNames);
            this.existing = Collections.unmodifiableSet(existingTopicNames);
        }

        public Set<String> createdTopics() {
            return created;
        }

        public Set<String> existingTopics() {
            return existing;
        }

        public boolean isCreated(String topicName) {
            return created.contains(topicName);
        }

        public boolean isExisting(String topicName) {
            return existing.contains(topicName);
        }

        public boolean isCreatedOrExisting(String topicName) {
            return isCreated(topicName) || isExisting(topicName);
        }

        public int createdTopicsCount() {
            return created.size();
        }

        public int existingTopicsCount() {
            return existing.size();
        }

        public int createdOrExistingTopicsCount() {
            return createdTopicsCount() + existingTopicsCount();
        }

        public boolean isEmpty() {
            return createdOrExistingTopicsCount() == 0;
        }

        @Override
        public String toString() {
            return "TopicCreationResponse{" + "created=" + created + ", existing=" + existing + '}';
        }
    }

    public static final int NO_PARTITIONS = -1;
    public static final short NO_REPLICATION_FACTOR = -1;

    private static final String CLEANUP_POLICY_CONFIG = TopicConfig.CLEANUP_POLICY_CONFIG;
    private static final String CLEANUP_POLICY_COMPACT = TopicConfig.CLEANUP_POLICY_COMPACT;
    private static final String MIN_INSYNC_REPLICAS_CONFIG = TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG;
    private static final String UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG = TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG;

    /**
     * A builder of {@link NewTopic} instances.
     */
    public static class NewTopicBuilder {
        private final String name;
        private int numPartitions = NO_PARTITIONS;
        private short replicationFactor = NO_REPLICATION_FACTOR;
        private final Map<String, String> configs = new HashMap<>();

        NewTopicBuilder(String name) {
            this.name = name;
        }

        /**
         * Specify the desired number of partitions for the topic.
         *
         * @param numPartitions the desired number of partitions; must be positive, or -1 to
         *                      signify using the broker's default
         * @return this builder to allow methods to be chained; never null
         */
        public NewTopicBuilder partitions(int numPartitions) {
            this.numPartitions = numPartitions;
            return this;
        }

        /**
         * Specify the topic's number of partition should be the broker configuration for
         * {@code num.partitions}.
         *
         * @return this builder to allow methods to be chained; never null
         */
        public NewTopicBuilder defaultPartitions() {
            this.numPartitions = NO_PARTITIONS;
            return this;
        }

        /**
         * Specify the desired replication factor for the topic.
         *
         * @param replicationFactor the desired replication factor; must be positive, or -1 to
         *                          signify using the broker's default
         * @return this builder to allow methods to be chained; never null
         */
        public NewTopicBuilder replicationFactor(short replicationFactor) {
            this.replicationFactor = replicationFactor;
            return this;
        }

        /**
         * Specify the replication factor for the topic should be the broker configurations for
         * {@code default.replication.factor}.
         *
         * @return this builder to allow methods to be chained; never null
         */
        public NewTopicBuilder defaultReplicationFactor() {
            this.replicationFactor = NO_REPLICATION_FACTOR;
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
            return new NewTopic(
                    name,
                    Optional.of(numPartitions),
                    Optional.of(replicationFactor)
            ).configs(configs);
        }
    }

    /**
     * Obtain a {@link NewTopicBuilder builder} to define a {@link NewTopic}.
     *
     * @param topicName the name of the topic
     * @return the {@link NewTopic} description of the topic; never null
     */
    public static NewTopicBuilder defineTopic(String topicName) {
        return new NewTopicBuilder(topicName);
    }

    private static final Logger log = LoggerFactory.getLogger(TopicAdmin.class);
    private final Map<String, Object> adminConfig;
    private final Admin admin;
    private final boolean logCreation;

    /**
     * Create a new topic admin component with the given configuration.
     *
     * @param adminConfig the configuration for the {@link Admin}
     */
    public TopicAdmin(Map<String, Object> adminConfig) {
        this(adminConfig, Admin.create(adminConfig));
    }

    // visible for testing
    TopicAdmin(Map<String, Object> adminConfig, Admin adminClient) {
        this(adminConfig, adminClient, true);
    }

    // visible for testing
    TopicAdmin(Map<String, Object> adminConfig, Admin adminClient, boolean logCreation) {
        this.admin = adminClient;
        this.adminConfig = adminConfig != null ? adminConfig : Collections.emptyMap();
        this.logCreation = logCreation;
    }

    /**
     * Get the {@link Admin} client used by this topic admin object.
     * @return the Kafka admin instance; never null
     */
    public Admin admin() {
        return admin;
    }

   /**
     * Attempt to create the topic described by the given definition, returning true if the topic was created or false
     * if the topic already existed.
     *
     * @param topic the specification of the topic
     * @return true if the topic was created or false if the topic already existed.
     * @throws ConnectException            if an error occurs, the operation takes too long, or the thread is interrupted while
     *                                     attempting to perform this operation
     * @throws UnsupportedVersionException if the broker does not support the necessary APIs to perform this request
     */
    public boolean createTopic(NewTopic topic) {
        if (topic == null) return false;
        Set<String> newTopicNames = createTopics(topic);
        return newTopicNames.contains(topic.name());
    }

    /**
     * Attempt to create the topics described by the given definitions, returning all of the names of those topics that
     * were created by this request. Any existing topics with the same name are unchanged, and the names of such topics
     * are excluded from the result.
     * <p>
     * If multiple topic definitions have the same topic name, the last one with that name will be used.
     * <p>
     * Apache Kafka added support for creating topics in 0.10.1.0, so this method works as expected with that and later versions.
     * With brokers older than 0.10.1.0, this method is unable to create topics and always returns an empty set.
     *
     * @param topics the specifications of the topics
     * @return the names of the topics that were created by this operation; never null but possibly empty
     * @throws ConnectException            if an error occurs, the operation takes too long, or the thread is interrupted while
     *                                     attempting to perform this operation
     */
    public Set<String> createTopics(NewTopic... topics) {
        return createOrFindTopics(topics).createdTopics();
    }

    /**
     * Attempt to find or create the topic described by the given definition, returning true if the topic was created or had
     * already existed, or false if the topic did not exist and could not be created.
     *
     * @param topic the specification of the topic
     * @return true if the topic was created or existed, or false if the topic could not already existed.
     * @throws ConnectException            if an error occurs, the operation takes too long, or the thread is interrupted while
     *                                     attempting to perform this operation
     * @throws UnsupportedVersionException if the broker does not support the necessary APIs to perform this request
     */
    public boolean createOrFindTopic(NewTopic topic) {
        if (topic == null) return false;
        return createOrFindTopics(topic).isCreatedOrExisting(topic.name());
    }

    /**
     * Attempt to create the topics described by the given definitions, returning all of the names of those topics that
     * were created by this request. Any existing topics with the same name are unchanged, and the names of such topics
     * are excluded from the result.
     * <p>
     * If multiple topic definitions have the same topic name, the last one with that name will be used.
     * <p>
     * Apache Kafka added support for creating topics in 0.10.1.0, so this method works as expected with that and later versions.
     * With brokers older than 0.10.1.0, this method is unable to create topics and always returns an empty set.
     *
     * @param topics the specifications of the topics
     * @return the {@link TopicCreationResponse} with the names of the newly created and existing topics;
     *         never null but possibly empty
     * @throws ConnectException if an error occurs, the operation takes too long, or the thread is interrupted while
     *                          attempting to perform this operation
     */
    public TopicCreationResponse createOrFindTopics(NewTopic... topics) {
        Map<String, NewTopic> topicsByName = new HashMap<>();
        if (topics != null) {
            for (NewTopic topic : topics) {
                if (topic != null) topicsByName.put(topic.name(), topic);
            }
        }
        if (topicsByName.isEmpty()) return EMPTY_CREATION;
        String bootstrapServers = bootstrapServers();
        String topicNameList = Utils.join(topicsByName.keySet(), "', '");

        // Attempt to create any missing topics
        CreateTopicsOptions args = new CreateTopicsOptions().validateOnly(false);
        Map<String, KafkaFuture<Void>> newResults = admin.createTopics(topicsByName.values(), args).values();

        // Iterate over each future so that we can handle individual failures like when some topics already exist
        Set<String> newlyCreatedTopicNames = new HashSet<>();
        Set<String> existingTopicNames = new HashSet<>();
        for (Map.Entry<String, KafkaFuture<Void>> entry : newResults.entrySet()) {
            String topic = entry.getKey();
            try {
                entry.getValue().get();
                if (logCreation) {
                    log.info("Created topic {} on brokers at {}", topicsByName.get(topic), bootstrapServers);
                }
                newlyCreatedTopicNames.add(topic);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof TopicExistsException) {
                    log.debug("Found existing topic '{}' on the brokers at {}", topic, bootstrapServers);
                    existingTopicNames.add(topic);
                    continue;
                }
                if (cause instanceof UnsupportedVersionException) {
                    log.debug("Unable to create topic(s) '{}' since the brokers at {} do not support the CreateTopics API." +
                            " Falling back to assume topic(s) exist or will be auto-created by the broker.",
                            topicNameList, bootstrapServers);
                    return EMPTY_CREATION;
                }
                if (cause instanceof ClusterAuthorizationException) {
                    log.debug("Not authorized to create topic(s) '{}' upon the brokers {}." +
                            " Falling back to assume topic(s) exist or will be auto-created by the broker.",
                            topicNameList, bootstrapServers);
                    return EMPTY_CREATION;
                }
                if (cause instanceof TopicAuthorizationException) {
                    log.debug("Not authorized to create topic(s) '{}' upon the brokers {}." +
                                    " Falling back to assume topic(s) exist or will be auto-created by the broker.",
                            topicNameList, bootstrapServers);
                    return EMPTY_CREATION;
                }
                if (cause instanceof InvalidConfigurationException) {
                    throw new ConnectException("Unable to create topic(s) '" + topicNameList + "': " + cause.getMessage(),
                            cause);
                }
                if (cause instanceof TimeoutException) {
                    // Timed out waiting for the operation to complete
                    throw new ConnectException("Timed out while checking for or creating topic(s) '" + topicNameList + "'." +
                            " This could indicate a connectivity issue, unavailable topic partitions, or if" +
                            " this is your first use of the topic it may have taken too long to create.", cause);
                }
                throw new ConnectException("Error while attempting to create/find topic(s) '" + topicNameList + "'", e);
            } catch (InterruptedException e) {
                Thread.interrupted();
                throw new ConnectException("Interrupted while attempting to create/find topic(s) '" + topicNameList + "'", e);
            }
        }
        return new TopicCreationResponse(newlyCreatedTopicNames, existingTopicNames);
    }

    /**
     * Attempt to fetch the descriptions of the given topics
     * Apache Kafka added support for describing topics in 0.10.0.0, so this method works as expected with that and later versions.
     * With brokers older than 0.10.0.0, this method is unable to describe topics and always returns an empty set.
     *
     * @param topics the topics to describe
     * @return a map of topic names to topic descriptions of the topics that were requested; never null but possibly empty
     * @throws RetriableException if a retriable error occurs, the operation takes too long, or the
     * thread is interrupted while attempting to perform this operation
     * @throws ConnectException if a non retriable error occurs
     */
    public Map<String, TopicDescription> describeTopics(String... topics) {
        if (topics == null) {
            return Collections.emptyMap();
        }
        String bootstrapServers = bootstrapServers();
        String topicNameList = String.join(", ", topics);

        Map<String, KafkaFuture<TopicDescription>> newResults =
                admin.describeTopics(Arrays.asList(topics), new DescribeTopicsOptions()).values();

        // Iterate over each future so that we can handle individual failures like when some topics don't exist
        Map<String, TopicDescription> existingTopics = new HashMap<>();
        newResults.forEach((topic, desc) -> {
            try {
                existingTopics.put(topic, desc.get());
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof UnknownTopicOrPartitionException) {
                    log.debug("Topic '{}' does not exist on the brokers at {}", topic, bootstrapServers);
                    return;
                }
                if (cause instanceof ClusterAuthorizationException || cause instanceof TopicAuthorizationException) {
                    String msg = String.format("Not authorized to describe topic(s) '%s' on the brokers %s",
                            topicNameList, bootstrapServers);
                    throw new ConnectException(msg, cause);
                }
                if (cause instanceof UnsupportedVersionException) {
                    String msg = String.format("Unable to describe topic(s) '%s' since the brokers "
                                    + "at %s do not support the DescribeTopics API.",
                            topicNameList, bootstrapServers);
                    throw new ConnectException(msg, cause);
                }
                if (cause instanceof TimeoutException) {
                    // Timed out waiting for the operation to complete
                    throw new RetriableException("Timed out while describing topics '" + topicNameList + "'", cause);
                }
                throw new ConnectException("Error while attempting to describe topics '" + topicNameList + "'", e);
            } catch (InterruptedException e) {
                Thread.interrupted();
                throw new RetriableException("Interrupted while attempting to describe topics '" + topicNameList + "'", e);
            }
        });
        return existingTopics;
    }

    /**
     * Verify the named topic uses only compaction for the cleanup policy.
     *
     * @param topic             the name of the topic
     * @param workerTopicConfig the name of the worker configuration that specifies the topic name
     * @return true if the admin client could be used to verify the topic setting, or false if
     *         the verification could not be performed, likely because the admin client principal
     *         did not have the required permissions or because the broker was older than 0.11.0.0
     * @throws ConfigException if the actual topic setting did not match the required setting
     */
    public boolean verifyTopicCleanupPolicyOnlyCompact(String topic, String workerTopicConfig,
            String topicPurpose) {
        Set<String> cleanupPolicies = topicCleanupPolicy(topic);
        if (cleanupPolicies.isEmpty()) {
            log.info("Unable to use admin client to verify the cleanup policy of '{}' "
                      + "topic is '{}', either because the broker is an older "
                      + "version or because the Kafka principal used for Connect "
                      + "internal topics does not have the required permission to "
                      + "describe topic configurations.", topic, TopicConfig.CLEANUP_POLICY_COMPACT);
            return false;
        }
        Set<String> expectedPolicies = Collections.singleton(TopicConfig.CLEANUP_POLICY_COMPACT);
        if (!cleanupPolicies.equals(expectedPolicies)) {
            String expectedPolicyStr = String.join(",", expectedPolicies);
            String cleanupPolicyStr = String.join(",", cleanupPolicies);
            String msg = String.format("Topic '%s' supplied via the '%s' property is required "
                    + "to have '%s=%s' to guarantee consistency and durability of "
                    + "%s, but found the topic currently has '%s=%s'. Continuing would likely "
                    + "result in eventually losing %s and problems restarting this Connect "
                    + "cluster in the future. Change the '%s' property in the "
                    + "Connect worker configurations to use a topic with '%s=%s'.",
                    topic, workerTopicConfig, TopicConfig.CLEANUP_POLICY_CONFIG, expectedPolicyStr,
                    topicPurpose, TopicConfig.CLEANUP_POLICY_CONFIG, cleanupPolicyStr, topicPurpose,
                    workerTopicConfig, TopicConfig.CLEANUP_POLICY_CONFIG, expectedPolicyStr);
            throw new ConfigException(msg);
        }
        return true;
    }

    /**
     * Get the cleanup policy for a topic.
     *
     * @param topic the name of the topic
     * @return the set of cleanup policies set for the topic; may be empty if the topic does not
     *         exist or the topic's cleanup policy could not be retrieved
     */
    public Set<String> topicCleanupPolicy(String topic) {
        Config topicConfig = describeTopicConfig(topic);
        if (topicConfig == null) {
            // The topic must not exist
            log.debug("Unable to find topic '{}' when getting cleanup policy", topic);
            return Collections.emptySet();
        }
        ConfigEntry entry = topicConfig.get(CLEANUP_POLICY_CONFIG);
        if (entry != null && entry.value() != null) {
            String policyStr = entry.value();
            log.debug("Found cleanup.policy={} for topic '{}'", policyStr, topic);
            return Arrays.stream(policyStr.split(","))
                         .map(String::trim)
                         .filter(s -> !s.isEmpty())
                         .map(String::toLowerCase)
                         .collect(Collectors.toSet());
        }
        // This is unexpected, as the topic config should include the cleanup.policy even if
        // the topic settings don't override the broker's log.cleanup.policy. But just to be safe.
        log.debug("Found no cleanup.policy for topic '{}'", topic);
        return Collections.emptySet();
    }

    /**
     * Attempt to fetch the topic configuration for the given topic.
     * Apache Kafka added support for describing topic configurations in 0.11.0.0, so this method
     * works as expected with that and later versions. With brokers older than 0.11.0.0, this method
     * is unable get the topic configurations and always returns a null value.
     *
     * <p>If the topic does not exist, a null value is returned.
     *
     * @param topic the name of the topic for which the topic configuration should be obtained
     * @return the topic configuration if the topic exists, or null if the topic did not exist
     * @throws RetriableException if a retriable error occurs, the operation takes too long, or the
     *         thread is interrupted while attempting to perform this operation
     * @throws ConnectException if a non retriable error occurs
     */
    public Config describeTopicConfig(String topic) {
        return describeTopicConfigs(topic).get(topic);
    }

    /**
     * Attempt to fetch the topic configurations for the given topics.
     * Apache Kafka added support for describing topic configurations in 0.11.0.0, so this method
     * works as expected with that and later versions. With brokers older than 0.11.0.0, this method
     * is unable get the topic configurations and always returns an empty set.
     *
     * <p>An entry with a null Config is placed into the resulting map for any topic that does
     * not exist on the brokers.
     *
     * @param topicNames the topics to obtain configurations
     * @return the map of topic configurations for each existing topic, or an empty map if none
     * of the topics exist
     * @throws RetriableException if a retriable error occurs, the operation takes too long, or the
     *         thread is interrupted while attempting to perform this operation
     * @throws ConnectException if a non retriable error occurs
     */
    public Map<String, Config> describeTopicConfigs(String... topicNames) {
        if (topicNames == null) {
            return Collections.emptyMap();
        }
        Collection<String> topics = Arrays.stream(topicNames)
                                          .filter(Objects::nonNull)
                                          .map(String::trim)
                                          .filter(s -> !s.isEmpty())
                                          .collect(Collectors.toList());
        if (topics.isEmpty()) {
            return Collections.emptyMap();
        }
        String bootstrapServers = bootstrapServers();
        String topicNameList = topics.stream().collect(Collectors.joining(", "));
        Collection<ConfigResource> resources = topics.stream()
                                                     .map(t -> new ConfigResource(ConfigResource.Type.TOPIC, t))
                                                     .collect(Collectors.toList());

        Map<ConfigResource, KafkaFuture<Config>> newResults = admin.describeConfigs(resources, new DescribeConfigsOptions()).values();

        // Iterate over each future so that we can handle individual failures like when some topics don't exist
        Map<String, Config> result = new HashMap<>();
        newResults.forEach((resource, configs) -> {
            String topic = resource.name();
            try {
                result.put(topic, configs.get());
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof UnknownTopicOrPartitionException) {
                    log.debug("Topic '{}' does not exist on the brokers at {}", topic, bootstrapServers);
                    result.put(topic, null);
                } else if (cause instanceof ClusterAuthorizationException || cause instanceof TopicAuthorizationException) {
                    log.debug("Not authorized to describe topic config for topic '{}' on brokers at {}", topic, bootstrapServers);
                } else if (cause instanceof UnsupportedVersionException) {
                    log.debug("API to describe topic config for topic '{}' is unsupported on brokers at {}", topic, bootstrapServers);
                } else if (cause instanceof TimeoutException) {
                    String msg = String.format("Timed out while waiting to describe topic config for topic '%s' on brokers at %s",
                            topic, bootstrapServers);
                    throw new RetriableException(msg, e);
                } else {
                    String msg = String.format("Error while attempting to describe topic config for topic '%s' on brokers at %s",
                            topic, bootstrapServers);
                    throw new ConnectException(msg, e);
                }
            } catch (InterruptedException e) {
                Thread.interrupted();
                String msg = String.format("Interrupted while attempting to describe topic configs '%s'", topicNameList);
                throw new RetriableException(msg, e);
            }
        });
        return result;
    }

    /**
     * Fetch the most recent offset for each of the supplied {@link TopicPartition} objects.
     *
     * @param partitions the topic partitions
     * @return the map of offset for each topic partition, or an empty map if the supplied partitions
     *         are null or empty
     * @throws UnsupportedVersionException if the admin client cannot read end offsets
     * @throws TimeoutException if the offset metadata could not be fetched before the amount of time allocated
     *         by {@code request.timeout.ms} expires, and this call can be retried
     * @throws LeaderNotAvailableException if the leader was not available and this call can be retried
     * @throws RetriableException if a retriable error occurs, or the thread is interrupted while attempting 
     *         to perform this operation
     * @throws ConnectException if a non retriable error occurs
     */
    public Map<TopicPartition, Long> endOffsets(Set<TopicPartition> partitions) {
        if (partitions == null || partitions.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<TopicPartition, OffsetSpec> offsetSpecMap = partitions.stream().collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest()));
        ListOffsetsResult resultFuture = admin.listOffsets(offsetSpecMap);
        // Get the individual result for each topic partition so we have better error messages
        Map<TopicPartition, Long> result = new HashMap<>();
        for (TopicPartition partition : partitions) {
            try {
                ListOffsetsResultInfo info = resultFuture.partitionResult(partition).get();
                result.put(partition, info.offset());
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                String topic = partition.topic();
                if (cause instanceof AuthorizationException) {
                    String msg = String.format("Not authorized to get the end offsets for topic '%s' on brokers at %s", topic, bootstrapServers());
                    throw new ConnectException(msg, e);
                } else if (cause instanceof UnsupportedVersionException) {
                    // Should theoretically never happen, because this method is the same as what the consumer uses and therefore
                    // should exist in the broker since before the admin client was added
                    String msg = String.format("API to get the get the end offsets for topic '%s' is unsupported on brokers at %s", topic, bootstrapServers());
                    throw new UnsupportedVersionException(msg, e);
                } else if (cause instanceof TimeoutException) {
                    String msg = String.format("Timed out while waiting to get end offsets for topic '%s' on brokers at %s", topic, bootstrapServers());
                    throw new TimeoutException(msg, e);
                } else if (cause instanceof LeaderNotAvailableException) {
                    String msg = String.format("Unable to get end offsets during leader election for topic '%s' on brokers at %s", topic, bootstrapServers());
                    throw new LeaderNotAvailableException(msg, e);
                } else if (cause instanceof org.apache.kafka.common.errors.RetriableException) {
                    throw (org.apache.kafka.common.errors.RetriableException) cause;
                } else {
                    String msg = String.format("Error while getting end offsets for topic '%s' on brokers at %s", topic, bootstrapServers());
                    throw new ConnectException(msg, e);
                }
            } catch (InterruptedException e) {
                Thread.interrupted();
                String msg = String.format("Interrupted while attempting to read end offsets for topic '%s' on brokers at %s", partition.topic(), bootstrapServers());
                throw new RetriableException(msg, e);
            }
        }
        return result;
    }

    @Override
    public void close() {
        admin.close();
    }

    public void close(Duration timeout) {
        admin.close(timeout);
    }

    private String bootstrapServers() {
        Object servers = adminConfig.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
        return servers != null ? servers.toString() : "<unknown>";
    }
}
