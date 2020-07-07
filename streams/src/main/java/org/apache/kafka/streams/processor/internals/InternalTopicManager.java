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
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.internals.ClientUtils.QuietAdminClientConfig;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class InternalTopicManager {
    private final static String INTERRUPTED_ERROR_MESSAGE = "Thread got interrupted. This indicates a bug. " +
        "Please report at https://issues.apache.org/jira/projects/KAFKA or dev-mailing list (https://kafka.apache.org/contact).";

    private final Logger log;
    private final long windowChangeLogAdditionalRetention;
    private final Map<String, String> defaultTopicConfigs = new HashMap<>();

    private final short replicationFactor;
    private final Admin adminClient;

    private final int retries;
    private final long retryBackOffMs;

    public InternalTopicManager(final Admin adminClient, final StreamsConfig streamsConfig) {
        this.adminClient = adminClient;

        final LogContext logContext = new LogContext(String.format("stream-thread [%s] ", Thread.currentThread().getName()));
        log = logContext.logger(getClass());

        replicationFactor = streamsConfig.getInt(StreamsConfig.REPLICATION_FACTOR_CONFIG).shortValue();
        windowChangeLogAdditionalRetention = streamsConfig.getLong(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG);
        final QuietAdminClientConfig adminConfigs = new QuietAdminClientConfig(streamsConfig);
        retries = adminConfigs.getInt(AdminClientConfig.RETRIES_CONFIG);
        retryBackOffMs = adminConfigs.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG);

        log.debug("Configs:" + Utils.NL +
            "\t{} = {}" + Utils.NL +
            "\t{} = {}" + Utils.NL +
            "\t{} = {}",
            AdminClientConfig.RETRIES_CONFIG, retries,
            StreamsConfig.REPLICATION_FACTOR_CONFIG, replicationFactor,
            StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, windowChangeLogAdditionalRetention);

        for (final Map.Entry<String, Object> entry : streamsConfig.originalsWithPrefix(StreamsConfig.TOPIC_PREFIX).entrySet()) {
            if (entry.getValue() != null) {
                defaultTopicConfigs.put(entry.getKey(), entry.getValue().toString());
            }
        }
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

        int remainingRetries = retries;
        Set<String> topicsNotReady = new HashSet<>(topics.keySet());
        final Set<String> newlyCreatedTopics = new HashSet<>();

        while (!topicsNotReady.isEmpty() && remainingRetries >= 0) {
            final Set<String> tempUnknownTopics = new HashSet<>();
            topicsNotReady = validateTopics(topicsNotReady, topics, tempUnknownTopics, remainingRetries);
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
                    final Map<String, String> topicConfig = internalTopicConfig.getProperties(defaultTopicConfigs, windowChangeLogAdditionalRetention);

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
                            log.info("Could not create topic {}. Topic is probably marked for deletion (number of partitions is unknown).\n" +
                                "Will retry to create this topic in {} ms (to let broker finish async delete operation first).\n" +
                                "Error message was: {}", topicName, retryBackOffMs, cause.toString());
                        } else {
                            log.error("Unexpected error during topic creation for {}.\n" +
                                "Error message was: {}", topicName, cause.toString());
                            throw new StreamsException(String.format("Could not create topic %s.", topicName), cause);
                        }
                    }
                }
            }


            if (!topicsNotReady.isEmpty()) {
                log.info("Topics {} can not be made ready with {} retries left", topicsNotReady, remainingRetries);

                Utils.sleep(retryBackOffMs);

                remainingRetries--;
            }
        }

        if (!topicsNotReady.isEmpty()) {
            final String timeoutAndRetryError = String.format("Could not create topics after %d retries. " +
                "This can happen if the Kafka cluster is temporary not available. " +
                "You can increase admin client config `retries` to be resilient against this error.", retries);
            log.error(timeoutAndRetryError);
            throw new StreamsException(timeoutAndRetryError);
        }
        log.debug("Completed validating internal topics and created {}", newlyCreatedTopics);

        return newlyCreatedTopics;
    }

    /**
     * Try to get the number of partitions for the given topics; return the number of partitions for topics that already exists.
     *
     * Topics that were not able to get its description will simply not be returned
     */
    // visible for testing
    protected Map<String, Integer> getNumPartitions(final Set<String> topics,
                                                    final Set<String> tempUnknownTopics,
                                                    final boolean hasRemainingRetries) {
        log.debug("Trying to check if topics {} have been created with expected number of partitions.", topics);

        final DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topics);
        final Map<String, KafkaFuture<TopicDescription>> futures = describeTopicsResult.values();

        final Map<String, Integer> existedTopicPartition = new HashMap<>();
        for (final Map.Entry<String, KafkaFuture<TopicDescription>> topicFuture : futures.entrySet()) {
            final String topicName = topicFuture.getKey();
            try {
                final TopicDescription topicDescription = topicFuture.getValue().get();
                existedTopicPartition.put(topicName, topicDescription.partitions().size());
            } catch (final InterruptedException fatalException) {
                // this should not happen; if it ever happens it indicate a bug
                Thread.currentThread().interrupt();
                log.error(INTERRUPTED_ERROR_MESSAGE, fatalException);
                throw new IllegalStateException(INTERRUPTED_ERROR_MESSAGE, fatalException);
            } catch (final ExecutionException couldNotDescribeTopicException) {
                final Throwable cause = couldNotDescribeTopicException.getCause();
                if (cause instanceof UnknownTopicOrPartitionException) {
                    // This topic didn't exist, proceed to try to create it
                    log.debug("Topic {} is unknown or not found, hence not existed yet.\n" +
                        "Error message was: {}", topicName, cause.toString());
                } else if (cause instanceof LeaderNotAvailableException) {
                    tempUnknownTopics.add(topicName);
                    if (!hasRemainingRetries) {
                        // run out of retries, throw exception directly
                        throw new StreamsException(
                            String.format("The leader of the Topic %s is not available.", topicName), cause);
                    }
                    log.info("The leader of the Topic {} is not available.\n" +
                        "Error message was: {}", topicName, cause.toString());
                } else {
                    log.error("Unexpected error during topic description for {}.\n" +
                        "Error message was: {}", topicName, cause.toString());
                    throw new StreamsException(String.format("Could not create topic %s.", topicName), cause);
                }
            }
        }

        return existedTopicPartition;
    }

    /**
     * Check the existing topics to have correct number of partitions; and return the remaining topics that needs to be created
     */
    private Set<String> validateTopics(final Set<String> topicsToValidate,
                                       final Map<String, InternalTopicConfig> topicsMap,
                                       final Set<String> tempUnknownTopics,
                                       final int remainingRetries) {
        if (!topicsMap.keySet().containsAll(topicsToValidate)) {
            throw new IllegalStateException("The topics map " + topicsMap.keySet() + " does not contain all the topics " +
                topicsToValidate + " trying to validate.");
        }

        final Map<String, Integer> existedTopicPartition =
            getNumPartitions(topicsToValidate, tempUnknownTopics, remainingRetries > 0);

        final Set<String> topicsToCreate = new HashSet<>();
        for (final String topicName : topicsToValidate) {
            final Optional<Integer> numberOfPartitions = topicsMap.get(topicName).numberOfPartitions();
            if (!numberOfPartitions.isPresent()) {
                log.error("Found undefined number of partitions for topic {}", topicName);
                throw new StreamsException("Topic " + topicName + " number of partitions not defined");
            }
            if (existedTopicPartition.containsKey(topicName)) {
                if (!existedTopicPartition.get(topicName).equals(numberOfPartitions.get())) {
                    final String errorMsg = String.format("Existing internal topic %s has invalid partitions: " +
                            "expected: %d; actual: %d. " +
                            "Use 'kafka.tools.StreamsResetter' tool to clean up invalid topics before processing.",
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
}
