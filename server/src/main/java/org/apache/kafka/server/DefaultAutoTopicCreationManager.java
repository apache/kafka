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

package org.apache.kafka.server;

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfigCollection;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.share.ShareCoordinatorConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.server.config.AbstractKafkaConfig;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.quota.ControllerMutationQuota;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class DefaultAutoTopicCreationManager implements AutoTopicCreationManager {

    private static final int DEFAULT_TOPIC_ERROR_CACHE_CAPACITY = 1000;
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultAutoTopicCreationManager.class);

    private final AbstractKafkaConfig config;
    private final TopicCreator topicCreator;
    private final Supplier<Map<String, String>> groupCoordinatorConfigsSupplier;
    private final Supplier<Map<String, String>> shareCoordinatorConfigsSupplier;
    private final Supplier<Map<String, String>> transactionTopicConfigsSupplier;
    private final Time time;
    private final Set<String> inflightTopics = ConcurrentHashMap.newKeySet();
    private final ExpiringErrorCache topicCreationErrorCache;

    public DefaultAutoTopicCreationManager(
            AbstractKafkaConfig config,
            Supplier<Map<String, String>> groupCoordinatorConfigsSupplier,
            Supplier<Map<String, String>> transactionTopicConfigsSupplier,
            Supplier<Map<String, String>> shareCoordinatorConfigsSupplier,
            TopicCreator topicCreator,
            Time time
    ) {
        this(
            config,
            groupCoordinatorConfigsSupplier,
            transactionTopicConfigsSupplier,
            shareCoordinatorConfigsSupplier,
            topicCreator,
            time,
            // Hardcoded default capacity; can be overridden in tests via constructor param
            DEFAULT_TOPIC_ERROR_CACHE_CAPACITY
        );
    }

    // VisibleForTesting
    DefaultAutoTopicCreationManager(
            AbstractKafkaConfig config,
            Supplier<Map<String, String>> groupCoordinatorConfigsSupplier,
            Supplier<Map<String, String>> transactionTopicConfigsSupplier,
            Supplier<Map<String, String>> shareCoordinatorConfigsSupplier,
            TopicCreator topicCreator,
            Time time,
            int topicErrorCacheCapacity
    ) {
        this.config = config;
        this.groupCoordinatorConfigsSupplier = groupCoordinatorConfigsSupplier;
        this.shareCoordinatorConfigsSupplier = shareCoordinatorConfigsSupplier;
        this.transactionTopicConfigsSupplier = transactionTopicConfigsSupplier;
        this.time = time;
        this.topicCreator = topicCreator;
        this.topicCreationErrorCache = new ExpiringErrorCache(topicErrorCacheCapacity, time);
    }

    @Override
    public List<MetadataResponseTopic> createTopics(
            Set<String> topics,
            ControllerMutationQuota controllerMutationQuota,
            RequestContext metadataRequestContext
    ) {
        var creatableTopics = new HashMap<String, CreatableTopic>();
        var uncreatableTopicResponses = new ArrayList<MetadataResponseTopic>();
        topics.forEach(topic -> {
            // Attempt basic topic validation before sending any requests to the controller.
            if (!isValidTopicName(topic)) {
                uncreatableTopicResponses.add(new MetadataResponseTopic()
                    .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code())
                    .setName(topic)
                    .setIsInternal(Topic.isInternal(topic)));
            } else if (!inflightTopics.add(topic)) {
                uncreatableTopicResponses.add(new MetadataResponseTopic()
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setName(topic)
                    .setIsInternal(Topic.isInternal(topic)));
            } else {
                creatableTopics.put(topic, creatableTopic(topic));
            }
        });
        var creatableTopicResponses = creatableTopics.isEmpty() ?
                List.<MetadataResponseTopic>of() : sendCreateTopicRequest(creatableTopics, metadataRequestContext);
        return Stream.concat(uncreatableTopicResponses.stream(), creatableTopicResponses.stream())
                .toList();
    }

    @Override
    public void createStreamsInternalTopics(
            Map<String, CreatableTopic> topics,
            RequestContext requestContext,
            long timeoutMs
    ) {
        if (topics.isEmpty()) {
            return;
        }

        var currentTimeMs = time.milliseconds();

        // Filter out topics that are:
        // 1. Already in error cache (back-off period)
        // 2. Already in-flight (concurrent request)
        var topicsToCreate = new HashMap<String, CreatableTopic>();
        topics.forEach((topicName, creatableTopic) -> {
            if (!topicCreationErrorCache.hasError(topicName, currentTimeMs)
                    && inflightTopics.add(topicName)) {
                topicsToCreate.put(topicName, creatableTopic);
            }
        });

        if (!topicsToCreate.isEmpty()) {
            sendCreateTopicRequestWithErrorCaching(topicsToCreate, requestContext, timeoutMs);
        }
    }

    @Override
    public Map<String, String> getStreamsInternalTopicCreationErrors(Set<String> topicNames, long currentTimeMs) {
        return topicCreationErrorCache.getErrorsForTopics(topicNames, currentTimeMs);
    }

    private List<MetadataResponseTopic> sendCreateTopicRequest(
            Map<String, CreatableTopic> creatableTopics,
            RequestContext requestContext
    ) {
        var createTopicsRequest = makeCreateTopicsRequestBuilder(creatableTopics);

        var responseFuture = Optional.ofNullable(requestContext)
                .map(context -> topicCreator.createTopicWithPrincipal(context, createTopicsRequest))
                .orElseGet(() -> topicCreator.createTopicWithoutPrincipal(createTopicsRequest));

        responseFuture.whenComplete((response, throwable) -> {
            clearInflightRequests(creatableTopics);
            if (throwable != null) {
                logError(creatableTopics, throwable);
            } else if (response != null) {
                response.data().topics().forEach(topicResult -> {
                    var error = Errors.forCode(topicResult.errorCode());
                    if (error != Errors.NONE) {
                        LOGGER.warn("Auto topic creation failed for {} with error '{}': {}.",
                                topicResult.name(), error.name(), topicResult.errorMessage());
                    }
                });
            } else {
                LOGGER.warn("CreateTopicsResponse future completed with null response and no exception");
            }
        });

        var creatableTopicResponses = creatableTopics.keySet().stream()
                .map(topic -> new MetadataResponseTopic()
                        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                        .setName(topic)
                        .setIsInternal(Topic.isInternal(topic)))
                .toList();

        LOGGER.info("Sent auto-creation request for {} to the active controller.", creatableTopics.keySet());
        return creatableTopicResponses;
    }

    private void clearInflightRequests(Map<String, CreatableTopic> creatableTopics) {
        creatableTopics.keySet().forEach(inflightTopics::remove);
        LOGGER.debug("Cleared inflight topic creation state for {}.", creatableTopics);
    }

    private CreateTopicsRequest.Builder makeCreateTopicsRequestBuilder(Map<String, CreatableTopic> creatableTopics) {
        var topicsToCreate = new CreateTopicsRequestData.CreatableTopicCollection(creatableTopics.size());
        topicsToCreate.addAll(creatableTopics.values());
        return new CreateTopicsRequest.Builder(
                new CreateTopicsRequestData()
                        .setTimeoutMs(config.requestTimeoutMs())
                        .setTopics(topicsToCreate)
        );
    }

    private static void logError(Map<String, CreatableTopic> creatableTopics, Throwable throwable) {
        if (throwable instanceof TimeoutException) {
            LOGGER.debug("Auto topic creation timed out for {}.", creatableTopics.keySet());
        } else if (throwable instanceof AuthenticationException) {
            LOGGER.warn("Auto topic creation failed for {} with authentication exception.", creatableTopics.keySet());
        } else if (throwable instanceof UnsupportedVersionException) {
            LOGGER.warn("Auto topic creation failed for {} with invalid version exception.", creatableTopics.keySet());
        } else {
            LOGGER.warn("Auto topic creation failed for {} with exception.", creatableTopics.keySet(), throwable);
        }
    }

    private CreatableTopic creatableTopic(String topic) {
        return switch (topic) {
            case Topic.GROUP_METADATA_TOPIC_NAME -> {
                var groupCoordinatorConfig = new GroupCoordinatorConfig(config);
                yield new CreatableTopic()
                    .setName(topic)
                    .setNumPartitions(groupCoordinatorConfig.offsetsTopicPartitions())
                    .setReplicationFactor(groupCoordinatorConfig.offsetsTopicReplicationFactor())
                    .setConfigs(convertToTopicConfigCollections(groupCoordinatorConfigsSupplier.get()));
            }
            case Topic.TRANSACTION_STATE_TOPIC_NAME -> {
                var transactionLogConfig = new TransactionLogConfig(config);
                yield new CreatableTopic()
                    .setName(topic)
                    .setNumPartitions(transactionLogConfig.transactionTopicPartitions())
                    .setReplicationFactor(transactionLogConfig.transactionTopicReplicationFactor())
                    .setConfigs(convertToTopicConfigCollections(transactionTopicConfigsSupplier.get()));
            }
            case Topic.SHARE_GROUP_STATE_TOPIC_NAME -> {
                var shareCoordinatorConfig = new ShareCoordinatorConfig(config);
                yield new CreatableTopic()
                    .setName(topic)
                    .setNumPartitions(shareCoordinatorConfig.shareCoordinatorStateTopicNumPartitions())
                    .setReplicationFactor(shareCoordinatorConfig.shareCoordinatorStateTopicReplicationFactor())
                    .setConfigs(convertToTopicConfigCollections(shareCoordinatorConfigsSupplier.get()));
            }
            default -> {
                int numPartitions = config.originals().containsKey(ServerLogConfigs.NUM_PARTITIONS_CONFIG)
                        ? config.numPartitions()
                        : CreateTopicsRequest.NO_NUM_PARTITIONS;
                short replicationFactor = config.originals().containsKey(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG)
                        ? (short) config.defaultReplicationFactor()
                        : CreateTopicsRequest.NO_REPLICATION_FACTOR;
                yield new CreatableTopic()
                        .setName(topic)
                        .setNumPartitions(numPartitions)
                        .setReplicationFactor(replicationFactor);
            }
        };
    }

    private static CreatableTopicConfigCollection convertToTopicConfigCollections(Map<String, String> config) {
        return new CreatableTopicConfigCollection(
                config.entrySet().stream()
                        .map(entry -> new CreatableTopicConfig()
                                .setName(entry.getKey())
                                .setValue(entry.getValue()))
                        .toList()
        );
    }

    private static boolean isValidTopicName(String topic) {
        try {
            Topic.validate(topic);
            return true;
        } catch (InvalidTopicException e) {
            return false;
        }
    }

    private void sendCreateTopicRequestWithErrorCaching(
            Map<String, CreatableTopic> creatableTopics,
            RequestContext requestContext,
            long timeoutMs
    ) {
        var createTopicsRequest = makeCreateTopicsRequestBuilder(creatableTopics);

        var responseFuture = topicCreator.createTopicWithPrincipal(requestContext, createTopicsRequest);

        responseFuture.whenComplete((response, throwable) -> {
            clearInflightRequests(creatableTopics);
            if (throwable != null) {
                logError(creatableTopics, throwable);
                var errorMessage = throwable.getMessage() != null ? throwable.getMessage() : throwable.toString();
                cacheTopicCreationErrors(creatableTopics.keySet(), errorMessage, timeoutMs);
            } else if (response != null) {
                LOGGER.debug("Auto topic creation completed for {} with response {}.", creatableTopics.keySet(), response);
                cacheTopicCreationErrorsFromResponse(response, timeoutMs);
            } else {
                var ex = new IllegalStateException("CreateTopicsResponse future completed with null response and no exception");
                LOGGER.error("Auto topic creation failed for {} due to unexpected future completion state.", creatableTopics.keySet(), ex);
                cacheTopicCreationErrors(creatableTopics.keySet(), ex.getMessage(), timeoutMs);
            }
        });
    }

    private void cacheTopicCreationErrors(Set<String> topicNames, String errorMessage, long ttlMs) {
        for (String topicName : topicNames) {
            topicCreationErrorCache.put(topicName, errorMessage, ttlMs);
        }
    }

    private void cacheTopicCreationErrorsFromResponse(CreateTopicsResponse response, long ttlMs) {
        response.data().topics().forEach(topicResult -> {
            if (topicResult.errorCode() != Errors.NONE.code()) {
                var errorMessage = Optional.ofNullable(topicResult.errorMessage())
                        .filter(s -> !s.isEmpty())
                        .orElse(Errors.forCode(topicResult.errorCode()).message());
                topicCreationErrorCache.put(topicResult.name(), errorMessage, ttlMs);
                LOGGER.debug("Cached topic creation error for {}: {}", topicResult.name(), errorMessage);
            }
        });
    }

    @Override
    public void close() {
        topicCreationErrorCache.clear();
    }
}
