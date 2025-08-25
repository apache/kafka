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
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfigCollection;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.EnvelopeRequest;
import org.apache.kafka.common.requests.EnvelopeResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.share.ShareCoordinatorConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.config.AbstractKafkaConfig;
import org.apache.kafka.server.config.KRaftConfigs;
import org.apache.kafka.server.config.ServerConfigs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.common.internals.Topic.GROUP_METADATA_TOPIC_NAME;
import static org.apache.kafka.common.internals.Topic.SHARE_GROUP_STATE_TOPIC_NAME;
import static org.apache.kafka.common.internals.Topic.TRANSACTION_STATE_TOPIC_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AutoTopicCreationManagerTest {
    @SuppressWarnings("unchecked")
    private static final Class<AbstractRequest.Builder<? extends AbstractRequest>> ABSTRACT_REQUEST_BUILDER_CLASS =
            (Class<AbstractRequest.Builder<? extends AbstractRequest>>) (Class<?>) AbstractRequest.Builder.class;

    private final int requestTimeout = 100;
    private AbstractKafkaConfig config;
    private final MetadataCache metadataCache = Mockito.mock(MetadataCache.class);
    private final NodeToControllerChannelManager brokerToController = Mockito.mock(NodeToControllerChannelManager.class);
    private AutoTopicCreationManager autoTopicCreationManager;

    private final int internalTopicPartitions = 2;
    private final short internalTopicReplicationFactor = 2;

    @BeforeEach
    public void setup() {
        var props = new Properties();
        props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "1");
        props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker");
        props.setProperty(ServerConfigs.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeout));

        props.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, String.valueOf(internalTopicPartitions));
        props.setProperty(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, String.valueOf(internalTopicPartitions));
        props.setProperty(ShareCoordinatorConfig.STATE_TOPIC_REPLICATION_FACTOR_CONFIG, String.valueOf(internalTopicPartitions));

        props.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, String.valueOf(internalTopicReplicationFactor));
        props.setProperty(TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, String.valueOf(internalTopicReplicationFactor));
        props.setProperty(ShareCoordinatorConfig.STATE_TOPIC_NUM_PARTITIONS_CONFIG, String.valueOf(internalTopicReplicationFactor));

        config = new AbstractKafkaConfig(AbstractKafkaConfig.CONFIG_DEF, props, Map.of(), false) { };
        var aliveBrokers = List.of(new Node(0, "host0", 0), new Node(1, "host1", 1));
        Mockito.when(metadataCache.getAliveBrokerNodes(any(ListenerName.class))).thenReturn(aliveBrokers);
    }

    @Test
    public void testCreateOffsetTopic() {
        testCreateTopic(GROUP_METADATA_TOPIC_NAME, true, internalTopicPartitions, internalTopicReplicationFactor);
    }

    @Test
    public void testCreateTxnTopic() {
        testCreateTopic(TRANSACTION_STATE_TOPIC_NAME, true, internalTopicPartitions, internalTopicReplicationFactor);
    }

    @Test
    public void testCreateShareStateTopic() {
        testCreateTopic(SHARE_GROUP_STATE_TOPIC_NAME, true, internalTopicPartitions, internalTopicReplicationFactor);
    }

    @Test
    public void testCreateNonInternalTopic() {
        testCreateTopic("topic", false, 1, (short) 1);
    }

    private void testCreateTopic(
            String topicName,
            Boolean isInternal,
            int numPartitions,
            short replicationFactor
    ) {
        autoTopicCreationManager = new DefaultAutoTopicCreationManager(
                config,
                brokerToController,
                Properties::new,
                Properties::new,
                Properties::new);

        var topicsCollection = new CreateTopicsRequestData.CreatableTopicCollection();
        topicsCollection.add(getNewTopic(topicName, numPartitions, replicationFactor));
        var requestBody = new CreateTopicsRequest.Builder(
                new CreateTopicsRequestData()
                        .setTopics(topicsCollection)
                        .setTimeoutMs(requestTimeout));

        // Calling twice with the same topic will only trigger one forwarding.
        createTopicAndVerifyResult(Errors.UNKNOWN_TOPIC_OR_PARTITION, topicName, isInternal);
        createTopicAndVerifyResult(Errors.UNKNOWN_TOPIC_OR_PARTITION, topicName, isInternal);

        verify(brokerToController).sendRequest(
                ArgumentMatchers.eq(requestBody),
                any(ControllerRequestCompletionHandler.class));
    }

    @Test
    public void testTopicCreationWithMetadataContextPassPrincipal() throws UnknownHostException {
        var topicName = "topic";
        var userPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user");
        var serializeIsCalled = new AtomicBoolean(false);
        var principalSerde = new KafkaPrincipalSerde() {
            @Override
            public byte[] serialize(KafkaPrincipal principal) {
                    assertEquals(principal, userPrincipal);
                    serializeIsCalled.set(true);
                    return Utils.utf8(principal.toString());
            }

            @Override
            public KafkaPrincipal deserialize(byte[] bytes) {
                return SecurityUtils.parseKafkaPrincipal(Utils.utf8(bytes));
            }
        };

        var requestContext = initializeRequestContext(userPrincipal, Optional.of(principalSerde));

        autoTopicCreationManager.createTopics(Set.of(topicName), Optional.of(requestContext));

        assertTrue(serializeIsCalled.get());

        var argumentCaptor = ArgumentCaptor.forClass(ABSTRACT_REQUEST_BUILDER_CLASS);
        verify(brokerToController).sendRequest(
                argumentCaptor.capture(),
                any(ControllerRequestCompletionHandler.class));
        var capturedRequest = ((EnvelopeRequest.Builder) argumentCaptor.getValue()).build(ApiKeys.ENVELOPE.latestVersion());
        assertEquals(userPrincipal, SecurityUtils.parseKafkaPrincipal(Utils.utf8(capturedRequest.requestPrincipal())));
    }

    @Test
    public void testTopicCreationWithMetadataContextWhenPrincipalSerdeNotDefined() throws UnknownHostException {
        var topicName = "topic";
        var requestContext = initializeRequestContext(KafkaPrincipal.ANONYMOUS, Optional.empty());

        // Throw upon undefined principal serde when building the forward request
        assertThrows(IllegalArgumentException.class, () -> autoTopicCreationManager.createTopics(
                Set.of(topicName), Optional.of(requestContext)));
    }

    @Test
    public void testTopicCreationWithMetadataContextNoRetryUponUnsupportedVersion() throws UnknownHostException {
        var topicName = "topic";
        var principalSerde = new KafkaPrincipalSerde() {
            @Override
            public byte[] serialize(KafkaPrincipal principal) {
                return Utils.utf8(principal.toString());
            }

            @Override
            public KafkaPrincipal deserialize(byte[] bytes) {
                return SecurityUtils.parseKafkaPrincipal(Utils.utf8(bytes));
            }
        };

        var requestContext = initializeRequestContext(KafkaPrincipal.ANONYMOUS, Optional.of(principalSerde));
        autoTopicCreationManager.createTopics(Set.of(topicName), Optional.of(requestContext));
        autoTopicCreationManager.createTopics(Set.of(topicName), Optional.of(requestContext));

        // Should only trigger once
        var argumentCaptor = ArgumentCaptor.forClass(ControllerRequestCompletionHandler.class);
        verify(brokerToController).sendRequest(any(), argumentCaptor.capture());

        // Complete with unsupported version will not trigger a retry, but cleanup the inflight topics instead
        var header = new RequestHeader(ApiKeys.ENVELOPE, (short) 0, "client", 1);
        var response = new EnvelopeResponse(ByteBuffer.allocate(0), Errors.UNSUPPORTED_VERSION);
        var clientResponse = new ClientResponse(header, null, null,
                0, 0, false, null, null, response);
        argumentCaptor.getValue().onComplete(clientResponse);
        verify(brokerToController, times(1)).sendRequest(
                any(ABSTRACT_REQUEST_BUILDER_CLASS),
                argumentCaptor.capture());

        // Could do the send again as inflight topics are cleared.
        autoTopicCreationManager.createTopics(Set.of(topicName), Optional.of(requestContext));
        verify(brokerToController, times(2)).sendRequest(
                any(ABSTRACT_REQUEST_BUILDER_CLASS),
                argumentCaptor.capture());
    }

    @Test
    public void testCreateStreamsInternalTopics() throws UnknownHostException {
        var topicConfig = new CreatableTopicConfigCollection();
        topicConfig.add(new CreatableTopicConfig().setName("cleanup.policy").setValue("compact"));

        var topics = new LinkedHashMap<String, CreatableTopic>();
        topics.put("stream-topic-1", new CreatableTopic()
                .setName("stream-topic-1")
                .setNumPartitions(3)
                .setReplicationFactor((short) 2)
                .setConfigs(topicConfig));
        topics.put("stream-topic-2", new CreatableTopic()
                .setName("stream-topic-2")
                .setNumPartitions(1)
                .setReplicationFactor((short) 1));
        var requestContext = initializeRequestContextWithUserPrincipal();

        autoTopicCreationManager = new DefaultAutoTopicCreationManager(
                config,
                brokerToController,
                Properties::new,
                Properties::new,
                Properties::new);

        autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext);

        var argumentCaptor = ArgumentCaptor.forClass(ABSTRACT_REQUEST_BUILDER_CLASS);
        verify(brokerToController).sendRequest(
                argumentCaptor.capture(),
                any(ControllerRequestCompletionHandler.class));

        var requestHeader = new RequestHeader(ApiKeys.CREATE_TOPICS, ApiKeys.CREATE_TOPICS.latestVersion(), "clientId", 0);
        var capturedRequest = ((EnvelopeRequest.Builder) argumentCaptor.getValue()).build(ApiKeys.ENVELOPE.latestVersion());
        var topicsCollection = new CreateTopicsRequestData.CreatableTopicCollection();
        topicsCollection.add(getNewTopic("stream-topic-1", 3, (short) 2).setConfigs(topicConfig));
        topicsCollection.add(getNewTopic("stream-topic-2", 1, (short) 1));
        var requestBody = new CreateTopicsRequest.Builder(
                new CreateTopicsRequestData()
                        .setTopics(topicsCollection)
                        .setTimeoutMs(requestTimeout))
                .build(ApiKeys.CREATE_TOPICS.latestVersion());

        var forwardedRequestBuffer = capturedRequest.requestData().duplicate();
        assertEquals(requestHeader, RequestHeader.parse(forwardedRequestBuffer));
        assertEquals(requestBody.data(), CreateTopicsRequest.parse(new ByteBufferAccessor(forwardedRequestBuffer),
                ApiKeys.CREATE_TOPICS.latestVersion()).data());
    }

    @Test
    public void testCreateStreamsInternalTopicsWithEmptyTopics() throws UnknownHostException {
        var topics = Map.<String, CreatableTopic>of();
        var requestContext = initializeRequestContextWithUserPrincipal();

        autoTopicCreationManager = new DefaultAutoTopicCreationManager(
                config,
                brokerToController,
                Properties::new,
                Properties::new,
                Properties::new);

        autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext);

        verify(brokerToController, never()).sendRequest(
                any(ABSTRACT_REQUEST_BUILDER_CLASS),
                any(ControllerRequestCompletionHandler.class));
    }

    @Test
    public void testCreateStreamsInternalTopicsWithDefaultConfig() throws UnknownHostException {
        var topics = Map.of(
                "stream-topic-1",
                new CreatableTopic()
                        .setName("stream-topic-1")
                        .setNumPartitions(-1)
                        .setReplicationFactor((short) -1));
        var requestContext = initializeRequestContextWithUserPrincipal();

        autoTopicCreationManager = new DefaultAutoTopicCreationManager(
                config,
                brokerToController,
                Properties::new,
                Properties::new,
                Properties::new);

        autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext);

        var argumentCaptor = ArgumentCaptor.forClass(ABSTRACT_REQUEST_BUILDER_CLASS);
        verify(brokerToController).sendRequest(
                argumentCaptor.capture(),
                any(ControllerRequestCompletionHandler.class));

        var capturedRequest = ((EnvelopeRequest.Builder) argumentCaptor.getValue()).build(ApiKeys.ENVELOPE.latestVersion());

        var requestHeader = new RequestHeader(ApiKeys.CREATE_TOPICS, ApiKeys.CREATE_TOPICS.latestVersion(), "clientId", 0);
        var topicsCollection = new CreateTopicsRequestData.CreatableTopicCollection();
        topicsCollection.add(getNewTopic("stream-topic-1", config.numPartitions(), (short) config.defaultReplicationFactor()));
        var requestBody = new CreateTopicsRequest.Builder(
                new CreateTopicsRequestData()
                        .setTopics(topicsCollection)
                        .setTimeoutMs(requestTimeout))
                .build(ApiKeys.CREATE_TOPICS.latestVersion());
        var forwardedRequestBuffer = capturedRequest.requestData().duplicate();
        assertEquals(requestHeader, RequestHeader.parse(forwardedRequestBuffer));
        assertEquals(requestBody.data(), CreateTopicsRequest.parse(new ByteBufferAccessor(forwardedRequestBuffer),
                ApiKeys.CREATE_TOPICS.latestVersion()).data());
    }

    @Test
    public void testCreateStreamsInternalTopicsPassesPrincipal() throws UnknownHostException {
        var topics = Map.of(
                "stream-topic-1",
                new CreatableTopic()
                        .setName("stream-topic-1")
                        .setNumPartitions(-1)
                        .setReplicationFactor((short) -1));
        var requestContext = initializeRequestContextWithUserPrincipal();

        autoTopicCreationManager = new DefaultAutoTopicCreationManager(
                config,
                brokerToController,
                Properties::new,
                Properties::new,
                Properties::new);

        autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext);

        var argumentCaptor = ArgumentCaptor.forClass(ABSTRACT_REQUEST_BUILDER_CLASS);

        verify(brokerToController).sendRequest(
                argumentCaptor.capture(),
                any(ControllerRequestCompletionHandler.class));
        var capturedRequest = ((EnvelopeRequest.Builder) argumentCaptor.getValue())
                .build(ApiKeys.ENVELOPE.latestVersion());
        assertEquals(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user"),
                SecurityUtils.parseKafkaPrincipal(Utils.utf8(capturedRequest.requestPrincipal())));
    }

    private RequestContext initializeRequestContextWithUserPrincipal() throws UnknownHostException {
        var userPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user");
        var principalSerde = new KafkaPrincipalSerde() {
            @Override
            public byte[] serialize(KafkaPrincipal principal) {
                return Utils.utf8(principal.toString());
            }
            @Override
            public KafkaPrincipal deserialize(byte[] bytes) {
                return SecurityUtils.parseKafkaPrincipal(Utils.utf8(bytes));
            }
        };
        return initializeRequestContext(userPrincipal, Optional.of(principalSerde));
    }

    private RequestContext initializeRequestContext(
            KafkaPrincipal kafkaPrincipal,
            Optional<KafkaPrincipalSerde> principalSerde
    ) throws UnknownHostException {

        autoTopicCreationManager = new DefaultAutoTopicCreationManager(
                config,
                brokerToController,
                Properties::new,
                Properties::new,
                Properties::new);

        var createTopicApiVersion = new ApiVersionsResponseData.ApiVersion()
                .setApiKey(ApiKeys.CREATE_TOPICS.id)
                .setMinVersion(ApiKeys.CREATE_TOPICS.oldestVersion())
                .setMaxVersion(ApiKeys.CREATE_TOPICS.latestVersion());
        Mockito.when(brokerToController.controllerApiVersions())
                .thenReturn(Optional.of(NodeApiVersions.create(List.of(createTopicApiVersion))));

        var requestHeader = new RequestHeader(ApiKeys.METADATA, ApiKeys.METADATA.latestVersion(),
                "clientId", 0);
        return new RequestContext(requestHeader, "1", InetAddress.getLocalHost(), Optional.empty(),
                kafkaPrincipal, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
                SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false, principalSerde);
    }

    private void createTopicAndVerifyResult(
            Errors error,
            String topicName,
            boolean isInternal
    ) {
        var topicResponses = autoTopicCreationManager.createTopics(Set.of(topicName), Optional.empty());

        var expectedResponses = List.of(new MetadataResponseTopic()
                .setErrorCode(error.code())
                .setIsInternal(isInternal)
                .setName(topicName));

        assertEquals(expectedResponses, topicResponses);
    }

    private static CreatableTopic getNewTopic(
            String topicName,
            int numPartitions,
            short replicationFactor
    ) {
        return new CreatableTopic()
                .setName(topicName)
                .setNumPartitions(numPartitions)
                .setReplicationFactor(replicationFactor);
    }
}
