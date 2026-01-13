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

package kafka.server;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.EnvelopeRequest;
import org.apache.kafka.common.requests.EnvelopeResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.NodeToControllerChannelManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KRaftTopicCreatorTest {

    private static final int REQUEST_TIMEOUT = 100;

    private NodeToControllerChannelManager brokerToController;
    private KRaftTopicCreator kraftTopicCreator;

    @BeforeEach
    public void setup() {
        brokerToController = mock(NodeToControllerChannelManager.class);

        ApiVersionsResponseData.ApiVersion createTopicApiVersion = new ApiVersionsResponseData.ApiVersion()
            .setApiKey(ApiKeys.CREATE_TOPICS.id)
            .setMinVersion(ApiKeys.CREATE_TOPICS.oldestVersion())
            .setMaxVersion(ApiKeys.CREATE_TOPICS.latestVersion());

        when(brokerToController.controllerApiVersions())
            .thenReturn(Optional.of(NodeApiVersions.create(Collections.singleton(createTopicApiVersion))));

        kraftTopicCreator = new KRaftTopicCreator(brokerToController);
    }

    @Test
    public void testCreateTopicWithMetadataContextPassPrincipal() throws Exception {
        String topicName = "topic";
        KafkaPrincipal userPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user");
        AtomicBoolean serializeIsCalled = new AtomicBoolean(false);

        KafkaPrincipalSerde principalSerde = new KafkaPrincipalSerde() {
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

        RequestContext requestContext = initializeRequestContext(userPrincipal, Optional.of(principalSerde));
        CreateTopicsRequest.Builder createTopicsRequest = createCreateTopicsRequestBuilder(topicName);

        kraftTopicCreator.createTopicWithPrincipal(requestContext, createTopicsRequest);

        assertTrue(serializeIsCalled.get());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<AbstractRequest.Builder<? extends AbstractRequest>> argumentCaptor =
            (ArgumentCaptor<AbstractRequest.Builder<? extends AbstractRequest>>) (ArgumentCaptor<?>) ArgumentCaptor.forClass(AbstractRequest.Builder.class);
        verify(brokerToController).sendRequest(
            argumentCaptor.capture(),
            any(ControllerRequestCompletionHandler.class));

        EnvelopeRequest capturedRequest = (EnvelopeRequest) argumentCaptor.getValue()
            .build(ApiKeys.ENVELOPE.latestVersion());
        assertEquals(userPrincipal, SecurityUtils.parseKafkaPrincipal(Utils.utf8(capturedRequest.requestPrincipal())));
    }

    @Test
    public void testCreateTopicWithMetadataContextWhenPrincipalSerdeNotDefined() {
        String topicName = "topic";
        RequestContext requestContext = initializeRequestContext(KafkaPrincipal.ANONYMOUS, Optional.empty());
        CreateTopicsRequest.Builder createTopicsRequest = createCreateTopicsRequestBuilder(topicName);

        assertThrows(IllegalArgumentException.class,
            () -> kraftTopicCreator.createTopicWithPrincipal(requestContext, createTopicsRequest));
    }

    @Test
    public void testCreateTopicWithoutRequestContext() {
        String topicName = "topic";
        CreateTopicsRequest.Builder createTopicsRequest = createCreateTopicsRequestBuilder(topicName);

        kraftTopicCreator.createTopicWithoutPrincipal(createTopicsRequest);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<AbstractRequest.Builder<? extends AbstractRequest>> argumentCaptor =
            (ArgumentCaptor<AbstractRequest.Builder<? extends AbstractRequest>>) (ArgumentCaptor<?>) ArgumentCaptor.forClass(AbstractRequest.Builder.class);
        verify(brokerToController).sendRequest(
            argumentCaptor.capture(),
            any(ControllerRequestCompletionHandler.class));

        AbstractRequest.Builder<?> capturedRequest = argumentCaptor.getValue();
        assertInstanceOf(CreateTopicsRequest.Builder.class, capturedRequest,
            "Should send CreateTopicsRequest.Builder when no request context provided");
    }

    @Test
    public void testEnvelopeResponseSuccessfulParsing() throws Exception {
        String topicName = "test-topic";
        RequestContext requestContext = initializeRequestContextWithUserPrincipal();
        CreateTopicsRequest.Builder createTopicsRequest = createCreateTopicsRequestBuilder(topicName);

        CompletableFuture<CreateTopicsResponse> responseFuture =
            kraftTopicCreator.createTopicWithPrincipal(requestContext, createTopicsRequest);

        ArgumentCaptor<ControllerRequestCompletionHandler> argumentCaptor =
            ArgumentCaptor.forClass(ControllerRequestCompletionHandler.class);
        verify(brokerToController).sendRequest(
            any(),
            argumentCaptor.capture());

        CreateTopicsResponseData createTopicsResponseData = new CreateTopicsResponseData();
        CreateTopicsResponseData.CreatableTopicResult topicResult =
            new CreateTopicsResponseData.CreatableTopicResult()
                .setName(topicName)
                .setErrorCode(Errors.NONE.code())
                .setNumPartitions(1)
                .setReplicationFactor((short) 1);
        createTopicsResponseData.topics().add(topicResult);

        CreateTopicsResponse createTopicsResponse = new CreateTopicsResponse(createTopicsResponseData);
        short requestVersion = ApiKeys.CREATE_TOPICS.latestVersion();
        int correlationId = requestContext.correlationId();
        String clientId = requestContext.clientId();

        ResponseHeader responseHeader = new ResponseHeader(
            correlationId,
            ApiKeys.CREATE_TOPICS.responseHeaderVersion(requestVersion)
        );
        ByteBuffer serializedResponse = RequestUtils.serialize(
            responseHeader.data(),
            responseHeader.headerVersion(),
            createTopicsResponse.data(),
            requestVersion
        );

        EnvelopeResponse envelopeResponse = new EnvelopeResponse(serializedResponse, Errors.NONE);
        RequestHeader requestHeader = new RequestHeader(ApiKeys.ENVELOPE, (short) 0, clientId, correlationId);
        ClientResponse clientResponse = new ClientResponse(
            requestHeader, null, null, 0, 0, false, null, null, envelopeResponse
        );

        argumentCaptor.getValue().onComplete(clientResponse);

        CreateTopicsResponse result = responseFuture.get();
        assertEquals(1, result.data().topics().size());
        assertEquals(topicName, result.data().topics().iterator().next().name());
        assertEquals(Errors.NONE.code(), result.data().topics().iterator().next().errorCode());
    }

    @Test
    public void testEnvelopeResponseWithEnvelopeError() {
        String topicName = "test-topic";
        RequestContext requestContext = initializeRequestContextWithUserPrincipal();
        CreateTopicsRequest.Builder createTopicsRequest = createCreateTopicsRequestBuilder(topicName);

        CompletableFuture<CreateTopicsResponse> responseFuture =
            kraftTopicCreator.createTopicWithPrincipal(requestContext, createTopicsRequest);

        ArgumentCaptor<ControllerRequestCompletionHandler> argumentCaptor =
            ArgumentCaptor.forClass(ControllerRequestCompletionHandler.class);
        verify(brokerToController).sendRequest(
            any(),
            argumentCaptor.capture());

        EnvelopeResponse envelopeResponse = new EnvelopeResponse(ByteBuffer.allocate(0), Errors.UNSUPPORTED_VERSION);
        RequestHeader requestHeader = new RequestHeader(
            ApiKeys.ENVELOPE, (short) 0, requestContext.clientId(), requestContext.correlationId()
        );
        ClientResponse clientResponse = new ClientResponse(
            requestHeader, null, null, 0, 0, false, null, null, envelopeResponse
        );

        argumentCaptor.getValue().onComplete(clientResponse);

        assertThrows(ExecutionException.class, responseFuture::get);
        assertTrue(responseFuture.isCompletedExceptionally());
    }

    @Test
    public void testEnvelopeResponseParsingException() {
        String topicName = "test-topic";
        RequestContext requestContext = initializeRequestContextWithUserPrincipal();
        CreateTopicsRequest.Builder createTopicsRequest = createCreateTopicsRequestBuilder(topicName);

        CompletableFuture<CreateTopicsResponse> responseFuture =
            kraftTopicCreator.createTopicWithPrincipal(requestContext, createTopicsRequest);

        ArgumentCaptor<ControllerRequestCompletionHandler> argumentCaptor =
            ArgumentCaptor.forClass(ControllerRequestCompletionHandler.class);
        verify(brokerToController).sendRequest(
            any(),
            argumentCaptor.capture());

        ByteBuffer malformedData = ByteBuffer.wrap("invalid response data".getBytes());
        EnvelopeResponse envelopeResponse = new EnvelopeResponse(malformedData, Errors.NONE);
        RequestHeader requestHeader = new RequestHeader(
            ApiKeys.ENVELOPE, (short) 0, requestContext.clientId(), requestContext.correlationId()
        );
        ClientResponse clientResponse = new ClientResponse(
            requestHeader, null, null, 0, 0, false, null, null, envelopeResponse
        );

        argumentCaptor.getValue().onComplete(clientResponse);
        assertTrue(responseFuture.isCompletedExceptionally());
        ExecutionException exception = assertThrows(ExecutionException.class, responseFuture::get);
        assertInstanceOf(RuntimeException.class, exception.getCause());
    }

    @Test
    public void testEnvelopeResponseWithTopicErrors() throws Exception {
        String topicName1 = "test-topic-1";
        String topicName2 = "test-topic-2";
        RequestContext requestContext = initializeRequestContextWithUserPrincipal();

        CreateTopicsRequestData.CreatableTopicCollection topicsCollection =
            new CreateTopicsRequestData.CreatableTopicCollection();
        topicsCollection.add(
            new CreateTopicsRequestData.CreatableTopic()
                .setName(topicName1)
                .setNumPartitions(1)
                .setReplicationFactor((short) 1)
        );
        topicsCollection.add(
            new CreateTopicsRequestData.CreatableTopic()
                .setName(topicName2)
                .setNumPartitions(1)
                .setReplicationFactor((short) 1)
        );
        CreateTopicsRequest.Builder createTopicsRequest = new CreateTopicsRequest.Builder(
            new CreateTopicsRequestData()
                .setTopics(topicsCollection)
                .setTimeoutMs(REQUEST_TIMEOUT)
        );

        CompletableFuture<CreateTopicsResponse> responseFuture =
            kraftTopicCreator.createTopicWithPrincipal(requestContext, createTopicsRequest);

        ArgumentCaptor<ControllerRequestCompletionHandler> argumentCaptor =
            ArgumentCaptor.forClass(ControllerRequestCompletionHandler.class);
        verify(brokerToController).sendRequest(
            any(),
            argumentCaptor.capture());

        CreateTopicsResponseData createTopicsResponseData = new CreateTopicsResponseData();

        CreateTopicsResponseData.CreatableTopicResult successResult =
            new CreateTopicsResponseData.CreatableTopicResult()
                .setName(topicName1)
                .setErrorCode(Errors.NONE.code())
                .setNumPartitions(1)
                .setReplicationFactor((short) 1);
        createTopicsResponseData.topics().add(successResult);

        CreateTopicsResponseData.CreatableTopicResult errorResult =
            new CreateTopicsResponseData.CreatableTopicResult()
                .setName(topicName2)
                .setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code())
                .setErrorMessage("Topic already exists");
        createTopicsResponseData.topics().add(errorResult);

        CreateTopicsResponse createTopicsResponse = new CreateTopicsResponse(createTopicsResponseData);
        short requestVersion = ApiKeys.CREATE_TOPICS.latestVersion();
        int correlationId = requestContext.correlationId();
        String clientId = requestContext.clientId();

        ResponseHeader responseHeader = new ResponseHeader(
            correlationId,
            ApiKeys.CREATE_TOPICS.responseHeaderVersion(requestVersion)
        );
        ByteBuffer serializedResponse = RequestUtils.serialize(
            responseHeader.data(),
            responseHeader.headerVersion(),
            createTopicsResponse.data(),
            requestVersion
        );

        EnvelopeResponse envelopeResponse = new EnvelopeResponse(serializedResponse, Errors.NONE);
        RequestHeader requestHeader = new RequestHeader(ApiKeys.ENVELOPE, (short) 0, clientId, correlationId);
        ClientResponse clientResponse = new ClientResponse(
            requestHeader, null, null, 0, 0, false, null, null, envelopeResponse
        );

        argumentCaptor.getValue().onComplete(clientResponse);

        CreateTopicsResponse result = responseFuture.get();
        assertEquals(2, result.data().topics().size());
        Map<String, CreateTopicsResponseData.CreatableTopicResult> results = result.data().topics().stream()
            .collect(Collectors.toMap(
                CreateTopicsResponseData.CreatableTopicResult::name,
                t -> t
            ));
        assertEquals(Errors.NONE.code(), results.get(topicName1).errorCode());
        assertEquals(Errors.TOPIC_ALREADY_EXISTS.code(), results.get(topicName2).errorCode());
        assertEquals("Topic already exists", results.get(topicName2).errorMessage());
    }

    @Test
    public void testTimeoutException() {
        String topicName = "test-topic";
        RequestContext requestContext = initializeRequestContextWithUserPrincipal();
        CreateTopicsRequest.Builder createTopicsRequest = createCreateTopicsRequestBuilder(topicName);

        CompletableFuture<CreateTopicsResponse> responseFuture =
            kraftTopicCreator.createTopicWithPrincipal(requestContext, createTopicsRequest);

        ArgumentCaptor<ControllerRequestCompletionHandler> argumentCaptor =
            ArgumentCaptor.forClass(ControllerRequestCompletionHandler.class);
        verify(brokerToController).sendRequest(
            any(),
            argumentCaptor.capture());

        argumentCaptor.getValue().onTimeout();

        ExecutionException exception = assertThrows(ExecutionException.class, responseFuture::get);
        assertInstanceOf(TimeoutException.class, exception.getCause());
        assertTrue(responseFuture.isCompletedExceptionally());
    }

    @Test
    public void testAuthenticationException() {
        String topicName = "test-topic";
        RequestContext requestContext = initializeRequestContextWithUserPrincipal();
        CreateTopicsRequest.Builder createTopicsRequest = createCreateTopicsRequestBuilder(topicName);

        CompletableFuture<CreateTopicsResponse> responseFuture =
            kraftTopicCreator.createTopicWithPrincipal(requestContext, createTopicsRequest);

        ArgumentCaptor<ControllerRequestCompletionHandler> argumentCaptor =
            ArgumentCaptor.forClass(ControllerRequestCompletionHandler.class);
        verify(brokerToController).sendRequest(
            any(),
            argumentCaptor.capture());

        RequestHeader requestHeader = new RequestHeader(
            ApiKeys.ENVELOPE, (short) 0, requestContext.clientId(), requestContext.correlationId()
        );
        AuthenticationException authException = new AuthenticationException("Authentication failed");
        ClientResponse clientResponse = new ClientResponse(
            requestHeader, null, null, 0, 0, false, null, authException, null
        );

        argumentCaptor.getValue().onComplete(clientResponse);

        ExecutionException exception = assertThrows(ExecutionException.class, responseFuture::get);
        assertInstanceOf(AuthenticationException.class, exception.getCause());
        assertTrue(responseFuture.isCompletedExceptionally());
    }

    @Test
    public void testVersionMismatchException() {
        String topicName = "test-topic";
        RequestContext requestContext = initializeRequestContextWithUserPrincipal();
        CreateTopicsRequest.Builder createTopicsRequest = createCreateTopicsRequestBuilder(topicName);

        CompletableFuture<CreateTopicsResponse> responseFuture =
            kraftTopicCreator.createTopicWithPrincipal(requestContext, createTopicsRequest);

        ArgumentCaptor<ControllerRequestCompletionHandler> argumentCaptor =
            ArgumentCaptor.forClass(ControllerRequestCompletionHandler.class);
        verify(brokerToController).sendRequest(
            any(),
            argumentCaptor.capture());

        RequestHeader requestHeader = new RequestHeader(
            ApiKeys.ENVELOPE, (short) 0, requestContext.clientId(), requestContext.correlationId()
        );
        UnsupportedVersionException versionMismatch = new UnsupportedVersionException("Version mismatch");
        ClientResponse clientResponse = new ClientResponse(
            requestHeader, null, null, 0, 0, false, versionMismatch, null, null
        );

        argumentCaptor.getValue().onComplete(clientResponse);

        ExecutionException exception = assertThrows(ExecutionException.class, responseFuture::get);
        assertInstanceOf(UnsupportedVersionException.class, exception.getCause());
        assertTrue(responseFuture.isCompletedExceptionally());
    }

    @Test
    public void testDirectCreateTopicsResponse() throws Exception {
        String topicName = "test-topic";
        CreateTopicsRequest.Builder createTopicsRequest = createCreateTopicsRequestBuilder(topicName);

        CompletableFuture<CreateTopicsResponse> responseFuture =
            kraftTopicCreator.createTopicWithoutPrincipal(createTopicsRequest);

        ArgumentCaptor<ControllerRequestCompletionHandler> argumentCaptor =
            ArgumentCaptor.forClass(ControllerRequestCompletionHandler.class);
        verify(brokerToController).sendRequest(
            any(),
            argumentCaptor.capture());

        CreateTopicsResponseData createTopicsResponseData = new CreateTopicsResponseData();
        CreateTopicsResponseData.CreatableTopicResult topicResult =
            new CreateTopicsResponseData.CreatableTopicResult()
                .setName(topicName)
                .setErrorCode(Errors.NONE.code())
                .setNumPartitions(1)
                .setReplicationFactor((short) 1);
        createTopicsResponseData.topics().add(topicResult);

        CreateTopicsResponse createTopicsResponse = new CreateTopicsResponse(createTopicsResponseData);
        RequestHeader requestHeader = new RequestHeader(ApiKeys.CREATE_TOPICS, (short) 0, "client", 1);
        ClientResponse clientResponse = new ClientResponse(
            requestHeader, null, null, 0, 0, false, null, null, createTopicsResponse
        );

        argumentCaptor.getValue().onComplete(clientResponse);

        CreateTopicsResponse result = responseFuture.get();
        assertEquals(1, result.data().topics().size());
        assertEquals(topicName, result.data().topics().iterator().next().name());
    }

    @Test
    public void testUnexpectedResponseType() {
        String topicName = "test-topic";
        CreateTopicsRequest.Builder createTopicsRequest = createCreateTopicsRequestBuilder(topicName);

        CompletableFuture<CreateTopicsResponse> responseFuture =
            kraftTopicCreator.createTopicWithoutPrincipal(createTopicsRequest);

        ArgumentCaptor<ControllerRequestCompletionHandler> argumentCaptor =
            ArgumentCaptor.forClass(ControllerRequestCompletionHandler.class);
        verify(brokerToController).sendRequest(
            any(),
            argumentCaptor.capture());

        MetadataResponse unexpectedResponse = new MetadataResponse(
            new MetadataResponseData(),
            ApiKeys.METADATA.latestVersion()
        );
        RequestHeader requestHeader = new RequestHeader(ApiKeys.CREATE_TOPICS, (short) 0, "client", 1);
        ClientResponse clientResponse = new ClientResponse(
            requestHeader, null, null, 0, 0, false, null, null, unexpectedResponse
        );

        argumentCaptor.getValue().onComplete(clientResponse);

        ExecutionException exception = assertThrows(ExecutionException.class, responseFuture::get);
        assertInstanceOf(IllegalStateException.class, exception.getCause());
        assertTrue(responseFuture.isCompletedExceptionally());
    }

    private RequestContext initializeRequestContextWithUserPrincipal() {
        KafkaPrincipal userPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user");
        KafkaPrincipalSerde principalSerde = new KafkaPrincipalSerde() {
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
    ) {
        try {
            RequestHeader requestHeader = new RequestHeader(
                ApiKeys.METADATA,
                ApiKeys.METADATA.latestVersion(),
                "clientId",
                0
            );
            return new RequestContext(
                requestHeader,
                "1",
                InetAddress.getLocalHost(),
                Optional.empty(),
                kafkaPrincipal,
                ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
                SecurityProtocol.PLAINTEXT,
                ClientInformation.EMPTY,
                false,
                principalSerde
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CreateTopicsRequest.Builder createCreateTopicsRequestBuilder(String topicName) {
        CreateTopicsRequestData.CreatableTopicCollection topicsCollection =
            new CreateTopicsRequestData.CreatableTopicCollection();
        topicsCollection.add(
            new CreateTopicsRequestData.CreatableTopic()
                .setName(topicName)
                .setNumPartitions(1)
                .setReplicationFactor((short) 1)
        );
        return new CreateTopicsRequest.Builder(
            new CreateTopicsRequestData()
                .setTopics(topicsCollection)
                .setTimeoutMs(REQUEST_TIMEOUT)
        );
    }
}
