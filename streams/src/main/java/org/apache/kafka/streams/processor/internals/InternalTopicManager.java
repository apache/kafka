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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class InternalTopicManager {
    private final static String INTERRUPTED_ERROR_MESSAGE = "Thread got interrupted. This indicates a bug. " +
        "Please report at https://issues.apache.org/jira/projects/KAFKA or dev-mailing list (https://kafka.apache.org/contact).";

    private final Logger log;
    private final long windowChangeLogAdditionalRetention;
    private final Map<String, String> defaultTopicConfigs = new HashMap<>();

    private final short replicationFactor;
    private final AdminClient adminClient;

    private final int retries;

    public InternalTopicManager(final AdminClient adminClient,
                                final StreamsConfig streamsConfig) {
        this.adminClient = adminClient;

        LogContext logContext = new LogContext(String.format("stream-thread [%s] ", Thread.currentThread().getName()));
        log = logContext.logger(getClass());

        replicationFactor = streamsConfig.getInt(StreamsConfig.REPLICATION_FACTOR_CONFIG).shortValue();
        windowChangeLogAdditionalRetention = streamsConfig.getLong(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG);
        retries = new AdminClientConfig(streamsConfig.getAdminConfigs("dummy")).getInt(AdminClientConfig.RETRIES_CONFIG);

        log.debug("Configs:" + Utils.NL,
            "\t{} = {}" + Utils.NL,
            "\t{} = {}" + Utils.NL,
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
     */
    public void makeReady(final Map<String, InternalTopicConfig> topics) {
        final Map<String, Integer> existingTopicPartitions = getNumPartitions(topics.keySet());
        final Set<InternalTopicConfig> topicsToBeCreated = validateTopicPartitions(topics.values(), existingTopicPartitions);
        if (topicsToBeCreated.size() > 0) {
            final Set<NewTopic> newTopics = new HashSet<>();

            for (final InternalTopicConfig internalTopicConfig : topicsToBeCreated) {
                final Map<String, String> topicConfig = internalTopicConfig.getProperties(defaultTopicConfigs, windowChangeLogAdditionalRetention);

                log.debug("Going to create topic {} with {} partitions and config {}.",
                        internalTopicConfig.name(),
                        internalTopicConfig.numberOfPartitions(),
                        topicConfig);

                newTopics.add(
                    new NewTopic(
                        internalTopicConfig.name(),
                        internalTopicConfig.numberOfPartitions(),
                        replicationFactor)
                    .configs(topicConfig));
            }

            // TODO: KAFKA-6928. should not need retries in the outer caller as it will be retried internally in admin client
            int remainingRetries = retries;
            boolean retry;
            do {
                retry = false;

                final CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);

                final Set<String> createTopicNames = new HashSet<>();
                for (final Map.Entry<String, KafkaFuture<Void>> createTopicResult : createTopicsResult.values().entrySet()) {
                    try {
                        createTopicResult.getValue().get();
                        createTopicNames.add(createTopicResult.getKey());
                    } catch (final ExecutionException couldNotCreateTopic) {
                        final Throwable cause = couldNotCreateTopic.getCause();
                        final String topicName = createTopicResult.getKey();

                        if (cause instanceof TimeoutException) {
                            retry = true;
                            log.debug("Could not get number of partitions for topic {} due to timeout. " +
                                "Will try again (remaining retries {}).", topicName, remainingRetries - 1);
                        } else if (cause instanceof TopicExistsException) {
                            createTopicNames.add(createTopicResult.getKey());
                            log.info("Topic {} exist already: {}",
                                topicName,
                                couldNotCreateTopic.toString());
                        } else {
                            throw new StreamsException(String.format("Could not create topic %s.", topicName),
                                couldNotCreateTopic);
                        }
                    } catch (final InterruptedException fatalException) {
                        Thread.currentThread().interrupt();
                        log.error(INTERRUPTED_ERROR_MESSAGE, fatalException);
                        throw new IllegalStateException(INTERRUPTED_ERROR_MESSAGE, fatalException);
                    }
                }

                if (retry) {
                    final Iterator<NewTopic> it = newTopics.iterator();
                    while (it.hasNext()) {
                        if (createTopicNames.contains(it.next().name())) {
                            it.remove();
                        }
                    }

                    continue;
                }

                return;
            } while (remainingRetries-- > 0);

            final String timeoutAndRetryError = "Could not create topics. " +
                "This can happen if the Kafka cluster is temporary not available. " +
                "You can increase admin client config `retries` to be resilient against this error.";
            log.error(timeoutAndRetryError);
            throw new StreamsException(timeoutAndRetryError);
        }
    }

    /**
     * Get the number of partitions for the given topics
     */
    // visible for testing
    protected Map<String, Integer> getNumPartitions(final Set<String> topics) {
        log.debug("Trying to check if topics {} have been created with expected number of partitions.", topics);

        // TODO: KAFKA-6928. should not need retries in the outer caller as it will be retried internally in admin client
        int remainingRetries = retries;
        boolean retry;
        do {
            retry = false;

            final DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topics);
            final Map<String, KafkaFuture<TopicDescription>> futures = describeTopicsResult.values();

            final Map<String, Integer> existingNumberOfPartitionsPerTopic = new HashMap<>();
            for (final Map.Entry<String, KafkaFuture<TopicDescription>> topicFuture : futures.entrySet()) {
                try {
                    final TopicDescription topicDescription = topicFuture.getValue().get();
                    existingNumberOfPartitionsPerTopic.put(
                        topicFuture.getKey(),
                        topicDescription.partitions().size());
                } catch (final InterruptedException fatalException) {
                    Thread.currentThread().interrupt();
                    log.error(INTERRUPTED_ERROR_MESSAGE, fatalException);
                    throw new IllegalStateException(INTERRUPTED_ERROR_MESSAGE, fatalException);
                } catch (final ExecutionException couldNotDescribeTopicException) {
                    final Throwable cause = couldNotDescribeTopicException.getCause();
                    if (cause instanceof TimeoutException) {
                        retry = true;
                        log.debug("Could not get number of partitions for topic {} due to timeout. " +
                            "Will try again (remaining retries {}).", topicFuture.getKey(), remainingRetries - 1);
                    } else {
                        final String error = "Could not get number of partitions for topic {} due to {}";
                        log.debug(error, topicFuture.getKey(), cause.toString());
                    }
                }
            }

            if (retry) {
                topics.removeAll(existingNumberOfPartitionsPerTopic.keySet());
                continue;
            }

            return existingNumberOfPartitionsPerTopic;
        } while (remainingRetries-- > 0);

        return Collections.emptyMap();
    }

    /**
     * Check the existing topics to have correct number of partitions; and return the non existing topics to be created
     */
    private Set<InternalTopicConfig> validateTopicPartitions(final Collection<InternalTopicConfig> topicsPartitionsMap,
                                                             final Map<String, Integer> existingTopicNamesPartitions) {
        final Set<InternalTopicConfig> topicsToBeCreated = new HashSet<>();
        for (final InternalTopicConfig topic : topicsPartitionsMap) {
            final int numberOfPartitions = topic.numberOfPartitions();
            if (existingTopicNamesPartitions.containsKey(topic.name())) {
                if (!existingTopicNamesPartitions.get(topic.name()).equals(numberOfPartitions)) {
                    final String errorMsg = String.format("Existing internal topic %s has invalid partitions: " +
                            "expected: %d; actual: %d. " +
                            "Use 'kafka.tools.StreamsResetter' tool to clean up invalid topics before processing.",
                        topic.name(), numberOfPartitions, existingTopicNamesPartitions.get(topic.name()));
                    log.error(errorMsg);
                    throw new StreamsException(errorMsg);
                }
            } else {
                topicsToBeCreated.add(topic);
            }
        }

        return topicsToBeCreated;
    }

}
