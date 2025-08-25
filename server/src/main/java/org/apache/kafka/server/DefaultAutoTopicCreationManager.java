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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfigCollection;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.share.ShareCoordinatorConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.config.AbstractKafkaConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class DefaultAutoTopicCreationManager implements AutoTopicCreationManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultAutoTopicCreationManager.class);

    private final AbstractKafkaConfig config;
    private final NodeToControllerChannelManager channelManager;
    private final Supplier<Properties> groupCoordinator;
    private final Supplier<Properties> shareCoordinator;
    private final Supplier<Properties> transactionTopicConfigsSupplier;
    private final Set<String> inflightTopics = ConcurrentHashMap.newKeySet();

    public DefaultAutoTopicCreationManager(
            AbstractKafkaConfig config,
            NodeToControllerChannelManager channelManager,
            Supplier<Properties> groupCoordinatorConfigsSupplier,
            Supplier<Properties> transactionTopicConfigsSupplier,
            Supplier<Properties> shareCoordinatorConfigsSupplier
    ) {
        this.config = config;
        this.channelManager = channelManager;
        this.groupCoordinator = groupCoordinatorConfigsSupplier;
        this.shareCoordinator = shareCoordinatorConfigsSupplier;
        this.transactionTopicConfigsSupplier = transactionTopicConfigsSupplier;
    }

    @Override
    public List<MetadataResponseTopic> createTopics(Set<String> topics, Optional<RequestContext> metadataRequestContext) {
        var creatableTopics = new HashMap<String, CreatableTopic>();
        var uncreatableTopicResponses = new ArrayList<MetadataResponseTopic>();
        topics.forEach(topic -> {
            // Attempt basic topic validation before sending any requests to the controller.
            Optional<Errors> validationError;
            if (!isValidTopicName(topic)) {
                validationError = Optional.of(Errors.INVALID_TOPIC_EXCEPTION);
            } else if (!inflightTopics.add(topic)) {
                validationError = Optional.of(Errors.UNKNOWN_TOPIC_OR_PARTITION);
            } else {
                validationError = Optional.empty();
            }

            if (validationError.isPresent()) {
                uncreatableTopicResponses.add(new MetadataResponseTopic()
                        .setErrorCode(validationError.get().code())
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
            RequestContext requestContext
    ) {
        if (topics.isEmpty()) {
            return;
        }
        topics.values().forEach(creatableTopic -> {
            if (creatableTopic.numPartitions() == -1) {
                creatableTopic.setNumPartitions(config.numPartitions());
            }
            if (creatableTopic.replicationFactor() == -1) {
                creatableTopic.setReplicationFactor((short) config.defaultReplicationFactor());
            }
        });
        sendCreateTopicRequest(topics, Optional.of(requestContext));
    }

    private List<MetadataResponseTopic> sendCreateTopicRequest(
            Map<String, CreatableTopic> creatableTopics,
            Optional<RequestContext> requestContext
    ) {
        var topicsToCreate = new CreateTopicsRequestData.CreatableTopicCollection(creatableTopics.size());
        topicsToCreate.addAll(creatableTopics.values());
        var createTopicsRequest = new CreateTopicsRequest.Builder(
                new CreateTopicsRequestData()
                        .setTimeoutMs(config.requestTimeoutMs())
                        .setTopics(topicsToCreate)
        );

        var requestCompletionHandler = new ControllerRequestCompletionHandler() {
            @Override
            public void onTimeout() {
                clearInflightRequests(creatableTopics);
                LOGGER.debug("Auto topic creation timed out for {}.", creatableTopics.keySet());
            }

            @Override
            public void onComplete(ClientResponse response) {
                clearInflightRequests(creatableTopics);
                if (response.authenticationException() != null) {
                    LOGGER.warn("Auto topic creation failed for {} with authentication exception.", creatableTopics.keySet());
                } else if (response.versionMismatch() != null) {
                    LOGGER.warn("Auto topic creation failed for {} with invalid version exception.", creatableTopics.keySet());
                } else {
                    if (response.hasResponse()) {
                        if (response.responseBody() instanceof CreateTopicsResponse createTopicsResponse) {
                            createTopicsResponse.data().topics().forEach(topicResult -> {
                                var error = Errors.forCode(topicResult.errorCode());
                                if (error != Errors.NONE) {
                                    LOGGER.warn("Auto topic creation failed for {} with error '{}': {}.", topicResult.name(), error.name(), topicResult.errorMessage());
                                }
                            });
                        } else {
                            LOGGER.warn("Auto topic creation request received unexpected response type: {}.", response.responseBody().getClass().getSimpleName());
                        }
                    }
                    LOGGER.debug("Auto topic creation completed for {} with response {}.", creatableTopics.keySet(), response.responseBody());
                }
            }
        };

        var request = requestContext.<AbstractRequest.Builder<? extends AbstractRequest>>map(context -> {
            short requestVersion = channelManager.controllerApiVersions()
                    .map(nodeApiVersions -> nodeApiVersions.latestUsableVersion(ApiKeys.CREATE_TOPICS))
                    // We will rely on the Metadata request to be retried in the case
                    // that the latest version is not usable by the controller.
                    .orElseGet(ApiKeys.CREATE_TOPICS::latestVersion);

            // Borrow client information such as client id and correlation id from the original request,
            // in order to correlate the create request with the original metadata request.
            var requestHeader = new RequestHeader(
                    ApiKeys.CREATE_TOPICS,
                    requestVersion,
                    context.clientId(),
                    context.correlationId()
            );

            return ForwardingManagerUtils.buildEnvelopeRequest(context, createTopicsRequest.build(requestVersion).serializeWithHeader(requestHeader));
        }).orElse(createTopicsRequest);

        channelManager.sendRequest(request, requestCompletionHandler);

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

    private CreatableTopic creatableTopic(String topic) {
        return switch (topic) {
            case Topic.GROUP_METADATA_TOPIC_NAME -> {
                var groupCoordinatorConfig = new GroupCoordinatorConfig(config);
                yield new CreatableTopic()
                    .setName(topic)
                    .setNumPartitions(groupCoordinatorConfig.offsetsTopicPartitions())
                    .setReplicationFactor(groupCoordinatorConfig.offsetsTopicReplicationFactor())
                    .setConfigs(convertToTopicConfigCollections(groupCoordinator.get()));
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
                    .setConfigs(convertToTopicConfigCollections(shareCoordinator.get()));
            }
            default -> new CreatableTopic()
                    .setName(topic)
                    .setNumPartitions(config.numPartitions())
                    .setReplicationFactor((short) config.defaultReplicationFactor());
        };
    }

    private static CreatableTopicConfigCollection convertToTopicConfigCollections(Properties config) {
        return new CreatableTopicConfigCollection(
                config.entrySet().stream()
                        .map(entry -> new CreatableTopicConfig()
                                .setName(entry.getKey().toString())
                                .setValue(entry.getValue().toString()))
                        .toList()
                        .iterator()
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
}
