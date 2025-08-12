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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.internals.ClientUtils.QuietConsumerConfig;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;



public class InternalTopicManager {
    private static final String BUG_ERROR_MESSAGE = "This indicates a bug. " +
        "Please report at https://issues.apache.org/jira/projects/KAFKA/issues or to the dev-mailing list (https://kafka.apache.org/contact).";
    private static final String INTERRUPTED_ERROR_MESSAGE = "Thread got interrupted. " + BUG_ERROR_MESSAGE;

    private final Logger log;

    private final Time time;
    private final Admin adminClient;

    private final short replicationFactor;
    private final long windowChangeLogAdditionalRetention;
    private final long retryBackOffMs;
    private final long retryTimeoutMs;

    private final Map<String, String> defaultTopicConfigs = new HashMap<>();

    public InternalTopicManager(final Time time,
                                final Admin adminClient,
                                final StreamsConfig streamsConfig) {
        this.time = time;
        this.adminClient = adminClient;

        final LogContext logContext = new LogContext(String.format("stream-thread [%s] ", Thread.currentThread().getName()));
        log = logContext.logger(getClass());

        replicationFactor = streamsConfig.getInt(StreamsConfig.REPLICATION_FACTOR_CONFIG).shortValue();
        windowChangeLogAdditionalRetention = streamsConfig.getLong(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG);
        retryBackOffMs = streamsConfig.getLong(StreamsConfig.RETRY_BACKOFF_MS_CONFIG);
        final Map<String, Object> consumerConfig = streamsConfig.getMainConsumerConfigs("dummy", "dummy", -1);
        // need to add mandatory configs; otherwise `QuietConsumerConfig` throws
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        retryTimeoutMs = new QuietConsumerConfig(consumerConfig).getInt(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG) / 2L;

        log.debug("Configs:" + Utils.NL +
            "\t{} = {}" + Utils.NL +
            "\t{} = {}",
            StreamsConfig.REPLICATION_FACTOR_CONFIG, replicationFactor,
            StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, windowChangeLogAdditionalRetention);

        for (final Map.Entry<String, Object> entry : streamsConfig.originalsWithPrefix(StreamsConfig.TOPIC_PREFIX).entrySet()) {
            if (entry.getValue() != null) {
                defaultTopicConfigs.put(entry.getKey(), entry.getValue().toString());
            }
        }
    }

    static class ValidationResult {
        private final Set<String> missingTopics = new HashSet<>();
        private final Map<String, List<String>> misconfigurationsForTopics = new HashMap<>();

        public void addMissingTopic(final String topic) {
            missingTopics.add(topic);
        }

        public Set<String> missingTopics() {
            return Collections.unmodifiableSet(missingTopics);
        }

        public void addMisconfiguration(final String topic, final String message) {
            misconfigurationsForTopics.computeIfAbsent(topic, ignored -> new ArrayList<>())
                .add(message);
        }

        public Map<String, List<String>> misconfigurationsForTopics() {
            return Collections.unmodifiableMap(misconfigurationsForTopics);
        }
    }

    /**
     * Validates the internal topics passed.
     *
     * The validation of the internal topics verifies if the topics:
     * - are missing on the brokers
     * - have the expected number of partitions
     * - have configured a clean-up policy that avoids data loss
     *
     * @param topicConfigs internal topics to validate
     *
     * @return validation results that contains
     *         - the set of missing internal topics on the brokers
     *         - descriptions of misconfigurations per topic
     */
    public ValidationResult validate(final Map<String, InternalTopicConfig> topicConfigs) {
        log.info("Starting to validate internal topics {}.", topicConfigs.keySet());

        final long now = time.milliseconds();
        final long deadline = now + retryTimeoutMs;

        final ValidationResult validationResult = new ValidationResult();
        final Set<String> topicDescriptionsStillToValidate = new HashSet<>(topicConfigs.keySet());
        final Set<String> topicConfigsStillToValidate = new HashSet<>(topicConfigs.keySet());
        while (!topicDescriptionsStillToValidate.isEmpty() || !topicConfigsStillToValidate.isEmpty()) {
            Map<String, KafkaFuture<TopicDescription>> descriptionsForTopic = Collections.emptyMap();
            if (!topicDescriptionsStillToValidate.isEmpty()) {
                final DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topicDescriptionsStillToValidate);
                descriptionsForTopic = describeTopicsResult.topicNameValues();
            }
            Map<String, KafkaFuture<Config>> configsForTopic = Collections.emptyMap();
            if (!topicConfigsStillToValidate.isEmpty()) {
                final DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(
                    topicConfigsStillToValidate.stream()
                        .map(topic -> new ConfigResource(Type.TOPIC, topic))
                        .collect(Collectors.toSet())
                );
                configsForTopic = describeConfigsResult.values().entrySet().stream()
                    .collect(Collectors.toMap(entry -> entry.getKey().name(), Map.Entry::getValue));
            }

            while (!descriptionsForTopic.isEmpty() || !configsForTopic.isEmpty()) {
                if (!descriptionsForTopic.isEmpty()) {
                    doValidateTopic(
                        validationResult,
                        descriptionsForTopic,
                        topicConfigs,
                        topicDescriptionsStillToValidate,
                        (streamsSide, brokerSide) -> validatePartitionCount(validationResult, streamsSide, brokerSide)
                    );
                }
                if (!configsForTopic.isEmpty()) {
                    doValidateTopic(
                        validationResult,
                        configsForTopic,
                        topicConfigs,
                        topicConfigsStillToValidate,
                        (streamsSide, brokerSide) -> validateCleanupPolicy(validationResult, streamsSide, brokerSide)
                    );
                }

                maybeThrowTimeoutException(
                    Arrays.asList(topicDescriptionsStillToValidate, topicConfigsStillToValidate),
                    deadline,
                    String.format("Could not validate internal topics within %d milliseconds. " +
                        "This can happen if the Kafka cluster is temporarily not available.", retryTimeoutMs)
                );

                if (!descriptionsForTopic.isEmpty() || !configsForTopic.isEmpty()) {
                    Utils.sleep(100);
                }
            }

            maybeSleep(
                Arrays.asList(topicDescriptionsStillToValidate, topicConfigsStillToValidate),
                deadline,
                "validated"
            );
        }

        log.info("Completed validation of internal topics {}.", topicConfigs.keySet());
        return validationResult;
    }

    private <V> void doValidateTopic(final ValidationResult validationResult,
                                     final Map<String, KafkaFuture<V>> futuresForTopic,
                                     final Map<String, InternalTopicConfig> topicsConfigs,
                                     final Set<String> topicsStillToValidate,
                                     final BiConsumer<InternalTopicConfig, V> validator) {
        for (final String topicName : new HashSet<>(topicsStillToValidate)) {
            if (!futuresForTopic.containsKey(topicName)) {
                throw new IllegalStateException("Description results do not contain topics to validate. " + BUG_ERROR_MESSAGE);
            }
            final KafkaFuture<V> future = futuresForTopic.get(topicName);
            if (future.isDone()) {
                try {
                    final V brokerSideTopicConfig = future.get();
                    final InternalTopicConfig streamsSideTopicConfig = topicsConfigs.get(topicName);
                    validator.accept(streamsSideTopicConfig, brokerSideTopicConfig);
                    topicsStillToValidate.remove(topicName);
                } catch (final ExecutionException executionException) {
                    final Throwable cause = executionException.getCause();
                    if (cause instanceof UnknownTopicOrPartitionException) {
                        log.info("Internal topic {} is missing", topicName);
                        validationResult.addMissingTopic(topicName);
                        topicsStillToValidate.remove(topicName);
                    } else if (cause instanceof LeaderNotAvailableException) {
                        log.info("The leader of internal topic {} is not available.", topicName);
                    } else if (cause instanceof TimeoutException) {
                        log.info("Retrieving data for internal topic {} timed out.", topicName);
                    } else {
                        log.error("Unexpected error during internal topic validation: ", cause);
                        throw new StreamsException(
                            String.format("Could not validate internal topic %s for the following reason: ", topicName),
                            cause
                        );
                    }
                } catch (final InterruptedException interruptedException) {
                    throw new InterruptException(interruptedException);
                } finally {
                    futuresForTopic.remove(topicName);
                }
            }
        }
    }

    private void validatePartitionCount(final ValidationResult validationResult,
                                        final InternalTopicConfig topicConfig,
                                        final TopicDescription topicDescription) {
        final String topicName = topicConfig.name();
        final int requiredPartitionCount = topicConfig.numberOfPartitions()
            .orElseThrow(() -> new IllegalStateException("No partition count is specified for internal topic " +
                topicName + ". " + BUG_ERROR_MESSAGE));
        final int actualPartitionCount = topicDescription.partitions().size();
        if (actualPartitionCount != requiredPartitionCount) {
            validationResult.addMisconfiguration(
                topicName,
                "Internal topic " + topicName + " requires " + requiredPartitionCount + " partitions, " +
                "but the existing topic on the broker has " + actualPartitionCount + " partitions."
            );
        }
    }

    private void validateCleanupPolicy(final ValidationResult validationResult,
                                       final InternalTopicConfig topicConfig,
                                       final Config brokerSideTopicConfig) {
        if (topicConfig instanceof UnwindowedUnversionedChangelogTopicConfig) {
            validateCleanupPolicyForUnwindowedUnversionedChangelogs(validationResult, topicConfig, brokerSideTopicConfig);
        } else if (topicConfig instanceof WindowedChangelogTopicConfig) {
            validateCleanupPolicyForWindowedChangelogs(validationResult, topicConfig, brokerSideTopicConfig);
        } else if (topicConfig instanceof VersionedChangelogTopicConfig) {
            validateCleanupPolicyForVersionedChangelogs(validationResult, topicConfig, brokerSideTopicConfig);
        } else if (topicConfig instanceof RepartitionTopicConfig) {
            validateCleanupPolicyForRepartitionTopic(validationResult, topicConfig, brokerSideTopicConfig);
        } else {
            throw new IllegalStateException("Internal topic " + topicConfig.name() + " has unknown type.");
        }
    }

    private void validateCleanupPolicyForUnwindowedUnversionedChangelogs(final ValidationResult validationResult,
                                                                         final InternalTopicConfig topicConfig,
                                                                         final Config brokerSideTopicConfig) {
        final String topicName = topicConfig.name();
        final String cleanupPolicy = getBrokerSideConfigValue(brokerSideTopicConfig, TopicConfig.CLEANUP_POLICY_CONFIG, topicName);
        if (cleanupPolicy.contains(TopicConfig.CLEANUP_POLICY_DELETE)) {
            validationResult.addMisconfiguration(
                topicName,
                "Cleanup policy (" + TopicConfig.CLEANUP_POLICY_CONFIG + ") of existing internal topic "
                    + topicName + " should not contain \""
                + TopicConfig.CLEANUP_POLICY_DELETE + "\"."
            );
        }
    }

    private void validateCleanupPolicyForWindowedChangelogs(final ValidationResult validationResult,
                                                            final InternalTopicConfig topicConfig,
                                                            final Config brokerSideTopicConfig) {
        final String topicName = topicConfig.name();
        final String cleanupPolicy = getBrokerSideConfigValue(brokerSideTopicConfig, TopicConfig.CLEANUP_POLICY_CONFIG, topicName);
        if (cleanupPolicy.contains(TopicConfig.CLEANUP_POLICY_DELETE)) {
            final long brokerSideRetentionMs =
                Long.parseLong(getBrokerSideConfigValue(brokerSideTopicConfig, TopicConfig.RETENTION_MS_CONFIG, topicName));
            final Map<String, String> streamsSideConfig =
                topicConfig.properties(defaultTopicConfigs, windowChangeLogAdditionalRetention);
            final long streamsSideRetentionMs = Long.parseLong(streamsSideConfig.get(TopicConfig.RETENTION_MS_CONFIG));
            if (brokerSideRetentionMs < streamsSideRetentionMs) {
                validationResult.addMisconfiguration(
                    topicName,
                    "Retention time (" + TopicConfig.RETENTION_MS_CONFIG + ") of existing internal topic "
                        + topicName + " is " + brokerSideRetentionMs + " but should be " + streamsSideRetentionMs + " or larger."
                );
            }
            final String brokerSideRetentionBytes =
                getBrokerSideConfigValue(brokerSideTopicConfig, TopicConfig.RETENTION_BYTES_CONFIG, topicName);
            if (brokerSideRetentionBytes != null) {
                validationResult.addMisconfiguration(
                    topicName,
                    "Retention byte (" + TopicConfig.RETENTION_BYTES_CONFIG + ") of existing internal topic "
                        + topicName + " is set but it should be unset."
                );
            }
        }
    }

    private void validateCleanupPolicyForVersionedChangelogs(final ValidationResult validationResult,
                                                             final InternalTopicConfig topicConfig,
                                                             final Config brokerSideTopicConfig) {
        final String topicName = topicConfig.name();
        final String cleanupPolicy = getBrokerSideConfigValue(brokerSideTopicConfig, TopicConfig.CLEANUP_POLICY_CONFIG, topicName);

        if (cleanupPolicy.contains(TopicConfig.CLEANUP_POLICY_DELETE)) {
            validationResult.addMisconfiguration(
                topicName,
                "Cleanup policy (" + TopicConfig.CLEANUP_POLICY_CONFIG + ") of existing internal topic "
                    + topicName + " should not contain \""
                    + TopicConfig.CLEANUP_POLICY_DELETE + "\"."
            );
        }

        final long brokerSideCompactionLagMs =
            Long.parseLong(getBrokerSideConfigValue(brokerSideTopicConfig, TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, topicName));
        final Map<String, String> streamsSideConfig =
            topicConfig.properties(defaultTopicConfigs, windowChangeLogAdditionalRetention);
        final long streamsSideCompactionLagMs = Long.parseLong(streamsSideConfig.get(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG));
        if (brokerSideCompactionLagMs < streamsSideCompactionLagMs) {
            validationResult.addMisconfiguration(
                topicName,
                "Min compaction lag (" + TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG + ") of existing internal topic "
                    + topicName + " is " + brokerSideCompactionLagMs + " but should be " + streamsSideCompactionLagMs + " or larger."
            );
        }
    }

    private void validateCleanupPolicyForRepartitionTopic(final ValidationResult validationResult,
                                                          final InternalTopicConfig topicConfig,
                                                          final Config brokerSideTopicConfig) {
        final String topicName = topicConfig.name();
        final String cleanupPolicy = getBrokerSideConfigValue(brokerSideTopicConfig, TopicConfig.CLEANUP_POLICY_CONFIG, topicName);
        if (cleanupPolicy.contains(TopicConfig.CLEANUP_POLICY_COMPACT)) {
            validationResult.addMisconfiguration(
                topicName,
                "Cleanup policy (" + TopicConfig.CLEANUP_POLICY_CONFIG + ") of existing internal topic "
                    + topicName + " should not contain \"" + TopicConfig.CLEANUP_POLICY_COMPACT + "\"."
            );
        } else if (cleanupPolicy.contains(TopicConfig.CLEANUP_POLICY_DELETE)) {
            final long brokerSideRetentionMs =
                Long.parseLong(getBrokerSideConfigValue(brokerSideTopicConfig, TopicConfig.RETENTION_MS_CONFIG, topicName));
            if (brokerSideRetentionMs != -1) {
                validationResult.addMisconfiguration(
                    topicName,
                    "Retention time (" + TopicConfig.RETENTION_MS_CONFIG + ") of existing internal topic "
                        + topicName + " is " + brokerSideRetentionMs + " but should be -1."
                );
            }
            final String brokerSideRetentionBytes =
                getBrokerSideConfigValue(brokerSideTopicConfig, TopicConfig.RETENTION_BYTES_CONFIG, topicName);
            if (brokerSideRetentionBytes != null) {
                validationResult.addMisconfiguration(
                    topicName,
                    "Retention byte (" + TopicConfig.RETENTION_BYTES_CONFIG + ") of existing internal topic "
                        + topicName + " is set but it should be unset."
                );
            }
        }
    }

    private String getBrokerSideConfigValue(final Config brokerSideTopicConfig,
                                            final String configName,
                                            final String topicName) {
        final ConfigEntry brokerSideConfigEntry = brokerSideTopicConfig.get(configName);
        if (brokerSideConfigEntry == null) {
            throw new IllegalStateException("The config " + configName + " for topic " +
                topicName + " could not be " + "retrieved from the brokers. " + BUG_ERROR_MESSAGE);
        }
        return brokerSideConfigEntry.value();
    }

    public Map<String, List<TopicPartitionInfo>> getTopicPartitionInfo(final Set<String> topics) {
        log.debug("Starting to describe topics {} in partition assignor.", topics);

        long currentWallClockMs = time.milliseconds();
        final long deadlineMs = currentWallClockMs + retryTimeoutMs;

        final Set<String> topicsToDescribe = new HashSet<>(topics);
        final Map<String, List<TopicPartitionInfo>> topicPartitionInfo = new HashMap<>();

        while (!topicsToDescribe.isEmpty()) {
            final Map<String, List<TopicPartitionInfo>> existed = getTopicPartitionInfo(topicsToDescribe, null);
            topicPartitionInfo.putAll(existed);
            topicsToDescribe.removeAll(topicPartitionInfo.keySet());
            if (!topicsToDescribe.isEmpty()) {
                currentWallClockMs = time.milliseconds();

                if (currentWallClockMs >= deadlineMs) {
                    final String timeoutError = String.format(
                        "Could not create topics within %d milliseconds. " +
                            "This can happen if the Kafka cluster is temporarily not available.",
                        retryTimeoutMs);
                    log.error(timeoutError);
                    throw new TimeoutException(timeoutError);
                }
                log.info(
                    "Topics {} could not be describe fully. Will retry in {} milliseconds. Remaining time in milliseconds: {}",
                    topics,
                    retryBackOffMs,
                    deadlineMs - currentWallClockMs
                );
                Utils.sleep(retryBackOffMs);
            }
        }
        log.debug("Completed describing topics");
        return topicPartitionInfo;
    }

    /**
     * Prepares a set of given internal topics.
     *
     * If a topic does not exist creates a new topic.
     * If a topic with the correct number of partitions exists ignores it.
     * If a topic exists already but has different number of partitions we fail and throw exception requesting user to reset the app before restarting again.
     * @return the set of topics which had to be newly created
     */
    public Set<String> makeReady(final Map<String, InternalTopicConfig> topics) {
        // we will do the validation / topic-creation in a loop, until we have confirmed all topics
        // have existed with the expected number of partitions, or some create topic returns fatal errors.
        log.debug("Starting to validate internal topics {} in partition assignor.", topics);

        long currentWallClockMs = time.milliseconds();
        final long deadlineMs = currentWallClockMs + retryTimeoutMs;

        Set<String> topicsNotReady = new HashSet<>(topics.keySet());
        final Set<String> newlyCreatedTopics = new HashSet<>();

        while (!topicsNotReady.isEmpty()) {
            final Set<String> tempUnknownTopics = new HashSet<>();
            topicsNotReady = validateTopics(topicsNotReady, topics, tempUnknownTopics);
            newlyCreatedTopics.addAll(topicsNotReady);

            if (!topicsNotReady.isEmpty()) {
                final Set<NewTopic> newTopics = new HashSet<>();

                for (final String topicName : topicsNotReady) {
                    if (tempUnknownTopics.contains(topicName)) {
                        // for the tempUnknownTopics, don't create topic for them
                        // we'll check again later if remaining retries > 0
                        continue;
                    }
                    final InternalTopicConfig internalTopicConfig = Objects.requireNonNull(topics.get(topicName));
                    final Map<String, String> topicConfig = internalTopicConfig.properties(defaultTopicConfigs, windowChangeLogAdditionalRetention);

                    log.debug("Going to create topic {} with {} partitions and config {}.",
                        internalTopicConfig.name(),
                        internalTopicConfig.numberOfPartitions(),
                        topicConfig);

                    newTopics.add(
                        new NewTopic(
                            internalTopicConfig.name(),
                            internalTopicConfig.numberOfPartitions(),
                            Optional.of(replicationFactor))
                            .configs(topicConfig));
                }

                // it's possible that although some topics are not ready yet because they
                // are temporarily not available, not that they do not exist; in this case
                // the new topics to create may be empty and hence we can skip here
                if (!newTopics.isEmpty()) {
                    final CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);

                    for (final Map.Entry<String, KafkaFuture<Void>> createTopicResult : createTopicsResult.values().entrySet()) {
                        final String topicName = createTopicResult.getKey();
                        try {
                            createTopicResult.getValue().get();
                            topicsNotReady.remove(topicName);
                        } catch (final InterruptedException fatalException) {
                            // this should not happen; if it ever happens it indicate a bug
                            Thread.currentThread().interrupt();
                            log.error(INTERRUPTED_ERROR_MESSAGE, fatalException);
                            throw new IllegalStateException(INTERRUPTED_ERROR_MESSAGE, fatalException);
                        } catch (final ExecutionException executionException) {
                            final Throwable cause = executionException.getCause();
                            if (cause instanceof TopicExistsException) {
                                // This topic didn't exist earlier or its leader not known before; just retain it for next round of validation.
                                log.info(
                                        "Could not create topic {}. Topic is probably marked for deletion (number of partitions is unknown).\n"
                                                +
                                                "Will retry to create this topic in {} ms (to let broker finish async delete operation first).\n"
                                                +
                                                "Error message was: {}", topicName, retryBackOffMs,
                                        cause.toString());
                            } else {
                                log.error("Unexpected error during topic creation for {}.\n" +
                                        "Error message was: {}", topicName, cause.toString());

                                if (cause instanceof UnsupportedVersionException) {
                                    final String errorMessage = cause.getMessage();
                                    if (errorMessage != null &&
                                            errorMessage.startsWith("Creating topics with default partitions/replication factor are only supported in CreateTopicRequest version 4+")) {

                                        throw new StreamsException(String.format(
                                                "Could not create topic %s, because brokers don't support configuration replication.factor=-1."
                                                        + " You can change the replication.factor config or upgrade your brokers to version 2.4 or newer to avoid this error.",
                                                topicName)
                                        );
                                    }
                                } else if (cause instanceof TimeoutException) {
                                    log.error("Creating topic {} timed out.\n" +
                                            "Error message was: {}", topicName, cause.toString());
                                } else {
                                    throw new StreamsException(
                                            String.format("Could not create topic %s.", topicName),
                                            cause
                                    );
                                }
                            }
                        }
                    }
                }
            }

            if (!topicsNotReady.isEmpty()) {
                currentWallClockMs = time.milliseconds();

                if (currentWallClockMs >= deadlineMs) {
                    final String timeoutError = String.format("Could not create topics within %d milliseconds. " +
                        "This can happen if the Kafka cluster is temporarily not available.", retryTimeoutMs);
                    log.error(timeoutError);
                    throw new TimeoutException(timeoutError);
                }
                log.info(
                    "Topics {} could not be made ready. Will retry in {} milliseconds. Remaining time in milliseconds: {}",
                    topicsNotReady,
                    retryBackOffMs,
                    deadlineMs - currentWallClockMs
                );
                Utils.sleep(retryBackOffMs);
            }
        }
        log.debug("Completed validating internal topics and created {}", newlyCreatedTopics);

        return newlyCreatedTopics;
    }

    /**
     * Try to get the partition information for the given topics; return the partition info for topics that already exists.
     *
     * Topics that were not able to get its description will simply not be returned
     */
    // visible for testing
    protected Map<String, List<TopicPartitionInfo>> getTopicPartitionInfo(final Set<String> topics,
                                                                          final Set<String> tempUnknownTopics) {
        final DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topics);
        final Map<String, KafkaFuture<TopicDescription>> futures = describeTopicsResult.topicNameValues();

        final Map<String, List<TopicPartitionInfo>> topicPartitionInfo = new HashMap<>();
        for (final Map.Entry<String, KafkaFuture<TopicDescription>> topicFuture : futures.entrySet()) {
            final String topicName = topicFuture.getKey();
            try {
                final TopicDescription topicDescription = topicFuture.getValue().get();
                topicPartitionInfo.put(topicName, topicDescription.partitions());
            } catch (final InterruptedException fatalException) {
                // this should not happen; if it ever happens it indicate a bug
                Thread.currentThread().interrupt();
                log.error(INTERRUPTED_ERROR_MESSAGE, fatalException);
                throw new IllegalStateException(INTERRUPTED_ERROR_MESSAGE, fatalException);
            } catch (final ExecutionException couldNotDescribeTopicException) {
                final Throwable cause = couldNotDescribeTopicException.getCause();
                if (cause instanceof UnknownTopicOrPartitionException) {
                    // This topic didn't exist
                    log.debug("Topic {} is unknown or not found, hence not existed yet.\n" +
                        "Error message was: {}", topicName, cause.toString());
                } else if (cause instanceof LeaderNotAvailableException) {
                    if (tempUnknownTopics != null) {
                        tempUnknownTopics.add(topicName);
                    }
                    log.debug("The leader of topic {} is not available.\n" +
                        "Error message was: {}", topicName, cause.toString());
                } else if (cause instanceof TimeoutException) {
                    if (tempUnknownTopics != null) {
                        tempUnknownTopics.add(topicName);
                    }
                    log.debug("Describing topic {} (to get number of partitions) timed out.\n" +
                            "Error message was: {}", topicName, cause.toString());
                } else {
                    log.error("Unexpected error during topic description for {}.\n" +
                        "Error message was: {}", topicName, cause.toString());
                    throw new StreamsException(String.format("Could not create topic %s.", topicName), cause);
                }
            }
        }

        return topicPartitionInfo;
    }

    /**
     * Try to get the number of partitions for the given topics; return the number of partitions for topics that already exists.
     *
     * Topics that were not able to get its description will simply not be returned
     */
    // visible for testing
    protected Map<String, Integer> getNumPartitions(final Set<String> topics,
                                                    final Set<String> tempUnknownTopics) {
        log.debug("Trying to check if topics {} have been created with expected number of partitions.", topics);

        final Map<String, List<TopicPartitionInfo>> topicPartitionInfo = getTopicPartitionInfo(topics, tempUnknownTopics);
        return topicPartitionInfo.entrySet().stream().collect(Collectors.toMap(
            Entry::getKey, e -> e.getValue().size()));
    }

    /**
     * Check the existing topics to have correct number of partitions; and return the remaining topics that needs to be created
     */
    private Set<String> validateTopics(final Set<String> topicsToValidate,
                                       final Map<String, InternalTopicConfig> topicsMap,
                                       final Set<String> tempUnknownTopics) {
        if (!topicsMap.keySet().containsAll(topicsToValidate)) {
            throw new IllegalStateException("The topics map " + topicsMap.keySet() + " does not contain all the topics " +
                topicsToValidate + " trying to validate.");
        }

        final Map<String, Integer> existedTopicPartition = getNumPartitions(topicsToValidate, tempUnknownTopics);

        final Set<String> topicsToCreate = new HashSet<>();
        for (final String topicName : topicsToValidate) {
            final Optional<Integer> numberOfPartitions = topicsMap.get(topicName).numberOfPartitions();
            if (numberOfPartitions.isEmpty()) {
                log.error("Found undefined number of partitions for topic {}", topicName);
                throw new StreamsException("Topic " + topicName + " number of partitions not defined");
            }
            if (existedTopicPartition.containsKey(topicName)) {
                if (!existedTopicPartition.get(topicName).equals(numberOfPartitions.get())) {
                    final String errorMsg = String.format("Existing internal topic %s has invalid partitions: " +
                            "expected: %d; actual: %d. " +
                            "Use 'org.apache.kafka.tools.StreamsResetter' tool to clean up invalid topics before processing.",
                        topicName, numberOfPartitions.get(), existedTopicPartition.get(topicName));
                    log.error(errorMsg);
                    throw new StreamsException(errorMsg);
                }
            } else {
                topicsToCreate.add(topicName);
            }
        }

        return topicsToCreate;
    }

    /**
     * Sets up internal topics.
     *
     * Either the given topic are all created or the method fails with an exception.
     *
     * @param topicConfigs internal topics to setup
     */
    public void setup(final Map<String, InternalTopicConfig> topicConfigs) {
        log.info("Starting to setup internal topics {}.", topicConfigs.keySet());

        final long now = time.milliseconds();
        final long deadline = now + retryTimeoutMs;

        final Map<String, Map<String, String>> streamsSideTopicConfigs = topicConfigs.values().stream()
            .collect(Collectors.toMap(
                InternalTopicConfig::name,
                topicConfig -> topicConfig.properties(defaultTopicConfigs, windowChangeLogAdditionalRetention)
            ));
        final Set<String> createdTopics = new HashSet<>();
        final Set<String> topicStillToCreate = new HashSet<>(topicConfigs.keySet());
        while (!topicStillToCreate.isEmpty()) {
            final Set<NewTopic> newTopics = topicStillToCreate.stream()
                .map(topicName -> new NewTopic(
                        topicName,
                        topicConfigs.get(topicName).numberOfPartitions(),
                        Optional.of(replicationFactor)
                    ).configs(streamsSideTopicConfigs.get(topicName))
                ).collect(Collectors.toSet());

            log.info("Going to create internal topics: " + newTopics);
            final CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);

            processCreateTopicResults(createTopicsResult, topicStillToCreate, createdTopics, deadline);

            maybeSleep(Collections.singletonList(topicStillToCreate), deadline, "created");
        }

        log.info("Completed setup of internal topics {}.", topicConfigs.keySet());
    }

    private void processCreateTopicResults(final CreateTopicsResult createTopicsResult,
                                           final Set<String> topicStillToCreate,
                                           final Set<String> createdTopics,
                                           final long deadline) {
        final Map<String, Throwable> lastErrorsSeenForTopic = new HashMap<>();
        final Map<String, KafkaFuture<Void>> createResultForTopic = createTopicsResult.values();
        while (!createResultForTopic.isEmpty()) {
            for (final String topicName : new HashSet<>(topicStillToCreate)) {
                if (!createResultForTopic.containsKey(topicName)) {
                    cleanUpCreatedTopics(createdTopics);
                    throw new IllegalStateException("Create topic results do not contain internal topic " + topicName
                        + " to setup. " + BUG_ERROR_MESSAGE);
                }
                final KafkaFuture<Void> createResult = createResultForTopic.get(topicName);
                if (createResult.isDone()) {
                    try {
                        createResult.get();
                        createdTopics.add(topicName);
                        topicStillToCreate.remove(topicName);
                    } catch (final ExecutionException executionException) {
                        final Throwable cause = executionException.getCause();
                        if (cause instanceof TopicExistsException) {
                            lastErrorsSeenForTopic.put(topicName, cause);
                            log.info("Internal topic {} already exists. Topic is probably marked for deletion. " +
                                "Will retry to create this topic later (to let broker complete async delete operation first)",
                                topicName);
                        } else if (cause instanceof TimeoutException) {
                            lastErrorsSeenForTopic.put(topicName, cause);
                            log.info("Creating internal topic {} timed out.", topicName);
                        } else {
                            cleanUpCreatedTopics(createdTopics);
                            log.error("Unexpected error during creation of internal topic: ", cause);
                            throw new StreamsException(
                                String.format("Could not create internal topic %s for the following reason: ", topicName),
                                cause
                            );
                        }
                    } catch (final InterruptedException interruptedException) {
                        throw new InterruptException(interruptedException);
                    } finally {
                        createResultForTopic.remove(topicName);
                    }
                }
            }

            maybeThrowTimeoutExceptionDuringSetup(
                topicStillToCreate,
                createdTopics,
                lastErrorsSeenForTopic,
                deadline
            );

            if (!createResultForTopic.isEmpty()) {
                Utils.sleep(100);
            }
        }
    }

    private void cleanUpCreatedTopics(final Set<String> topicsToCleanUp) {
        log.info("Starting to clean up internal topics {}.", topicsToCleanUp);

        final long now = time.milliseconds();
        final long deadline = now + retryTimeoutMs;

        final Set<String> topicsStillToCleanup = new HashSet<>(topicsToCleanUp);
        while (!topicsStillToCleanup.isEmpty()) {
            log.info("Going to cleanup internal topics: " + topicsStillToCleanup);
            final DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topicsStillToCleanup);
            final Map<String, KafkaFuture<Void>> deleteResultForTopic = deleteTopicsResult.topicNameValues();
            while (!deleteResultForTopic.isEmpty()) {
                for (final String topicName : new HashSet<>(topicsStillToCleanup)) {
                    if (!deleteResultForTopic.containsKey(topicName)) {
                        throw new IllegalStateException("Delete topic results do not contain internal topic " + topicName
                            + " to clean up. " + BUG_ERROR_MESSAGE);
                    }
                    final KafkaFuture<Void> deleteResult = deleteResultForTopic.get(topicName);
                    if (deleteResult.isDone()) {
                        try {
                            deleteResult.get();
                            topicsStillToCleanup.remove(topicName);
                        } catch (final ExecutionException executionException) {
                            final Throwable cause = executionException.getCause();
                            if (cause instanceof UnknownTopicOrPartitionException) {
                                log.info("Internal topic {} to clean up is missing", topicName);
                            } else if (cause instanceof LeaderNotAvailableException) {
                                log.info("The leader of internal topic {} to clean up is not available.", topicName);
                            } else if (cause instanceof TimeoutException) {
                                log.info("Cleaning up internal topic {} timed out.", topicName);
                            } else {
                                log.error("Unexpected error during cleanup of internal topics: ", cause);
                                throw new StreamsException(
                                    String.format("Could not clean up internal topics %s, because during the cleanup " +
                                            "of topic %s the following error occurred: ",
                                        topicsStillToCleanup, topicName),
                                    cause
                                );
                            }
                        } catch (final InterruptedException interruptedException) {
                            throw new InterruptException(interruptedException);
                        } finally {
                            deleteResultForTopic.remove(topicName);
                        }
                    }
                }

                maybeThrowTimeoutException(
                    Collections.singletonList(topicsStillToCleanup),
                    deadline,
                    String.format("Could not cleanup internal topics within %d milliseconds. This can happen if the " +
                            "Kafka cluster is temporarily not available or the broker did not complete topic creation " +
                            "before the cleanup. The following internal topics could not be cleaned up: %s",
                        retryTimeoutMs, topicsStillToCleanup)
                );

                if (!deleteResultForTopic.isEmpty()) {
                    Utils.sleep(100);
                }
            }

            maybeSleep(
                Collections.singletonList(topicsStillToCleanup),
                deadline,
                "validated"
            );
        }

        log.info("Completed cleanup of internal topics {}.", topicsToCleanUp);
    }

    private void maybeThrowTimeoutException(final List<Set<String>> topicStillToProcess,
                                            final long deadline,
                                            final String errorMessage) {
        if (topicStillToProcess.stream().anyMatch(resultSet -> !resultSet.isEmpty())) {
            final long now = time.milliseconds();
            if (now >= deadline) {
                log.error(errorMessage);
                throw new TimeoutException(errorMessage);
            }
        }
    }

    private void maybeThrowTimeoutExceptionDuringSetup(final Set<String> topicStillToProcess,
                                                       final Set<String> createdTopics,
                                                       final Map<String, Throwable> lastErrorsSeenForTopic,
                                                       final long deadline) {
        if (topicStillToProcess.stream().anyMatch(resultSet -> !resultSet.isEmpty())) {
            final long now = time.milliseconds();
            if (now >= deadline) {
                cleanUpCreatedTopics(createdTopics);
                final String errorMessage = String.format("Could not create internal topics within %d milliseconds. This can happen if the " +
                    "Kafka cluster is temporarily not available or a topic is marked for deletion and the broker " +
                    "did not complete its deletion within the timeout. The last errors seen per topic are: %s",
                    retryTimeoutMs, lastErrorsSeenForTopic);
                log.error(errorMessage);
                throw new TimeoutException(errorMessage);
            }
        }
    }

    private void maybeSleep(final List<Set<String>> resultSetsStillToValidate,
                            final long deadline,
                            final String action) {
        if (resultSetsStillToValidate.stream().anyMatch(resultSet -> !resultSet.isEmpty())) {
            final long now = time.milliseconds();
            log.info(
                "Internal topics {} could not be {}. Will retry in {} milliseconds. Remaining time in milliseconds: {}",
                resultSetsStillToValidate.stream().flatMap(Collection::stream).collect(Collectors.toSet()),
                action,
                retryBackOffMs,
                deadline - now
            );
            Utils.sleep(retryBackOffMs);
        }
    }
}
