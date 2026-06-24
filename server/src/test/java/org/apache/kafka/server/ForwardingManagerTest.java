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

import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.AlterConfigsResponseData;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AlterConfigsRequest;
import org.apache.kafka.common.requests.AlterConfigsResponse;
import org.apache.kafka.common.requests.EnvelopeRequest;
import org.apache.kafka.common.requests.EnvelopeResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.network.Request;
import org.apache.kafka.network.metrics.RequestChannelMetrics;
import org.apache.kafka.server.util.MockTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ForwardingManagerTest {

    private final MockTime time = new MockTime();
    private final DefaultKafkaPrincipalBuilder principalBuilder = new DefaultKafkaPrincipalBuilder(null, null);
    @SuppressWarnings("unchecked")
    private final Supplier<ControllerInformation> controllerNodeProvider = Mockito.mock(Supplier.class);
    private final int requestCorrelationId = 27;
    private final KafkaPrincipal clientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "client");
    private final HeaderAndBuffer headerAndBuffer = buildRequest(testAlterConfigRequest(), requestCorrelationId);

    private MockClient client;
    private MockNodeToControllerChannelManager brokerToController;
    private ForwardingManagerImpl forwardingManager;
    private KafkaMetric queueTimeMsP999;
    private KafkaMetric queueLength;
    private KafkaMetric remoteTimeMsP999;
    private Metrics metrics;
    private Request request;

    @BeforeEach
    public void setUp() throws UnknownHostException {
        client = new MockClient(time);
        metrics = new Metrics();
        brokerToController = new MockNodeToControllerChannelManager(client, time, controllerNodeProvider, controllerApiVersions());
        forwardingManager = new ForwardingManagerImpl(brokerToController, metrics);
        queueTimeMsP999 = metrics.metrics().get(forwardingManager.forwardingManagerMetrics().queueTimeMsHist().latencyP999Name());
        queueLength = metrics.metrics().get(forwardingManager.forwardingManagerMetrics().queueLengthName());
        remoteTimeMsP999 = metrics.metrics().get(forwardingManager.forwardingManagerMetrics().remoteTimeMsHist().latencyP999Name());
        request = buildRequest(headerAndBuffer.header, headerAndBuffer.buffer, clientPrincipal);
    }

    @AfterEach
    public void tearDown() {
        client.close();
        metrics.close();
    }

    @Test
    public void testResponseCorrelationIdMismatch() {
        AlterConfigsResponse responseBody = new AlterConfigsResponse(new AlterConfigsResponseData());
        ByteBuffer responseBuffer = RequestTestUtils.serializeResponseWithHeader(responseBody, headerAndBuffer.header.apiVersion(),
                requestCorrelationId + 1);

        Mockito.when(controllerNodeProvider.get()).thenReturn(controllerInfo());
        MockClient.RequestMatcher isEnvelopeRequest = req -> req instanceof EnvelopeRequest;
        client.prepareResponse(isEnvelopeRequest, new EnvelopeResponse(responseBuffer, Errors.NONE));

        AtomicReference<Optional<AbstractResponse>> responseOpt = new AtomicReference<>();
        forwardingManager.forwardRequest(request, responseOpt::set);
        brokerToController.poll();
        assertTrue(Optional.ofNullable(responseOpt.get()).isPresent());

        AbstractResponse response = responseOpt.get().get();
        assertEquals(Map.of(Errors.UNKNOWN_SERVER_ERROR, 1), response.errorCounts());
    }

    @Test
    public void testUnsupportedVersions() {
        AlterConfigsResponse responseBody = new AlterConfigsResponse(new AlterConfigsResponseData());
        ByteBuffer responseBuffer = RequestTestUtils.serializeResponseWithHeader(responseBody,
                headerAndBuffer.header.apiVersion(), requestCorrelationId);

        Mockito.when(controllerNodeProvider.get()).thenReturn(controllerInfo());
        MockClient.RequestMatcher isEnvelopeRequest = req -> req instanceof EnvelopeRequest;
        client.prepareResponse(isEnvelopeRequest, new EnvelopeResponse(responseBuffer, Errors.UNSUPPORTED_VERSION));

        AtomicReference<Optional<AbstractResponse>> responseOpt = new AtomicReference<>();
        forwardingManager.forwardRequest(request, responseOpt::set);
        brokerToController.poll();
        assertEquals(Optional.empty(), responseOpt.get());
    }

    @Test
    public void testForwardingTimeoutWaitingForControllerDiscovery() {
        Mockito.when(controllerNodeProvider.get()).thenReturn(emptyControllerInfo());

        AtomicReference<AbstractResponse> response = new AtomicReference<>();
        forwardingManager.forwardRequest(request, res -> res.ifPresent(response::set));
        brokerToController.poll();
        assertNull(response.get());

        // The controller is not discovered before reaching the retry timeout.
        // The request should fail with a timeout error.
        time.sleep(brokerToController.getTimeoutMs());
        brokerToController.poll();
        assertNotNull(response.get());

        AlterConfigsResponse alterConfigResponse = (AlterConfigsResponse) response.get();
        assertEquals(Map.of(Errors.REQUEST_TIMED_OUT, 1), alterConfigResponse.errorCounts());
    }

    @Test
    public void testForwardingTimeoutAfterRetry() {
        Mockito.when(controllerNodeProvider.get()).thenReturn(controllerInfo());

        AtomicReference<AbstractResponse> response = new AtomicReference<>();
        forwardingManager.forwardRequest(request, res -> res.ifPresent(response::set));
        brokerToController.poll();
        assertNull(response.get());

        // After reaching the retry timeout, we get a disconnect. Instead of retrying,
        // we should fail the request with a timeout error.
        time.sleep(brokerToController.getTimeoutMs());
        client.respond(testAlterConfigRequest().getErrorResponse(0, Errors.UNKNOWN_SERVER_ERROR.exception()), true);
        brokerToController.poll();
        brokerToController.poll();
        assertNotNull(response.get());

        AlterConfigsResponse alterConfigResponse = (AlterConfigsResponse) response.get();
        assertEquals(Map.of(Errors.REQUEST_TIMED_OUT, 1), alterConfigResponse.errorCounts());
    }

    @Test
    public void testUnsupportedVersionFromNetworkClient() {
        Mockito.when(controllerNodeProvider.get()).thenReturn(controllerInfo());

        MockClient.RequestMatcher isEnvelopeRequest = req -> req instanceof EnvelopeRequest;
        client.prepareUnsupportedVersionResponse(isEnvelopeRequest);

        AtomicReference<AbstractResponse> response = new AtomicReference<>();
        forwardingManager.forwardRequest(request, res -> res.ifPresent(response::set));
        brokerToController.poll();
        assertNotNull(response.get());

        AlterConfigsResponse alterConfigResponse = (AlterConfigsResponse) response.get();
        assertEquals(Map.of(Errors.UNKNOWN_SERVER_ERROR, 1), alterConfigResponse.errorCounts());
    }

    @Test
    public void testFailedAuthentication() {
        Mockito.when(controllerNodeProvider.get()).thenReturn(controllerInfo());

        client.createPendingAuthenticationError(controllerInfo().node().get(), 50);

        AtomicReference<AbstractResponse> response = new AtomicReference<>();
        forwardingManager.forwardRequest(request, res -> res.ifPresent(response::set));
        brokerToController.poll();
        assertNotNull(response.get());

        AlterConfigsResponse alterConfigResponse = (AlterConfigsResponse) response.get();
        assertEquals(Map.of(Errors.UNKNOWN_SERVER_ERROR, 1), alterConfigResponse.errorCounts());
    }

    @Test
    public void testForwardingManagerMetricsOnComplete() {
        AlterConfigsResponse responseBody = new AlterConfigsResponse(new AlterConfigsResponseData());
        ByteBuffer responseBuffer = RequestTestUtils.serializeResponseWithHeader(responseBody,
                headerAndBuffer.header.apiVersion(), requestCorrelationId);

        Mockito.when(controllerNodeProvider.get()).thenReturn(controllerInfo());
        MockClient.RequestMatcher isEnvelopeRequest = req -> req instanceof EnvelopeRequest;
        client.prepareResponse(isEnvelopeRequest, new EnvelopeResponse(responseBuffer, Errors.UNSUPPORTED_VERSION));

        AtomicReference<Optional<AbstractResponse>> responseOpt = new AtomicReference<>();
        forwardingManager.forwardRequest(request, responseOpt::set);
        assertEquals(1, (Integer) queueLength.metricValue());

        brokerToController.poll();
        client.poll(10000, time.milliseconds());
        assertEquals(0, (Integer) queueLength.metricValue());
        assertNotEquals(Double.NaN, (Double) queueTimeMsP999.metricValue());
        assertNotEquals(Double.NaN, (Double) remoteTimeMsP999.metricValue());
    }

    @Test
    public void testForwardingManagerMetricsOnTimeout() {
        Mockito.when(controllerNodeProvider.get()).thenReturn(controllerInfo());

        AtomicReference<AbstractResponse> response = new AtomicReference<>();
        forwardingManager.forwardRequest(request, res -> res.ifPresent(response::set));
        assertEquals(1, (Integer) queueLength.metricValue());

        time.sleep(brokerToController.getTimeoutMs());
        brokerToController.poll();
        assertEquals(0, (Integer) queueLength.metricValue());
        assertEquals(brokerToController.getTimeoutMs() * 0.999, (Double) queueTimeMsP999.metricValue());
        assertEquals(Double.NaN, (Double) remoteTimeMsP999.metricValue());
    }

    record HeaderAndBuffer(RequestHeader header, ByteBuffer buffer) { }

    private HeaderAndBuffer buildRequest(AbstractRequest body, int correlationId) {
        RequestHeader header = new RequestHeader(
                body.apiKey(),
                body.version(),
                "clientId",
                correlationId
        );
        ByteBuffer buffer = body.serializeWithHeader(header);
        // Fast-forward buffer to start of the request as `Request` expects
        RequestHeader.parse(buffer);
        return new HeaderAndBuffer(header, buffer);
    }

    private Request buildRequest(
            RequestHeader requestHeader,
            ByteBuffer requestBuffer,
            KafkaPrincipal principal
    ) throws UnknownHostException {
        RequestContext requestContext = new RequestContext(
                requestHeader,
                "1",
                InetAddress.getLocalHost(),
                Optional.empty(),
                principal,
                new ListenerName("client"),
                SecurityProtocol.SASL_PLAINTEXT,
                ClientInformation.EMPTY,
                false,
                Optional.of(principalBuilder)
        );

        return new Request(1, requestContext, time.nanoseconds(), MemoryPool.NONE, requestBuffer, new RequestChannelMetrics(ApiMessageType.ListenerType.CONTROLLER));
    }

    private AlterConfigsRequest testAlterConfigRequest() {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, "foo");
        List<AlterConfigsRequest.ConfigEntry> configs = List.of(
                new AlterConfigsRequest.ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1"));
        return new AlterConfigsRequest.Builder(
                Map.of(configResource, new AlterConfigsRequest.Config(configs)),
                false)
                .build();
    }

    private static NodeApiVersions controllerApiVersions() {
        // The Envelope API is not yet included in the standard set of APIs
        ApiVersionsResponseData.ApiVersion envelopeApiVersion = new ApiVersionsResponseData.ApiVersion()
                .setApiKey(ApiKeys.ENVELOPE.id)
                .setMinVersion(ApiKeys.ENVELOPE.oldestVersion())
                .setMaxVersion(ApiKeys.ENVELOPE.latestVersion());
        return NodeApiVersions.create(List.of(envelopeApiVersion));
    }

    private ControllerInformation controllerInfo() {
        return new ControllerInformation(Optional.of(new Node(0, "host", 1234)), new ListenerName(""), SecurityProtocol.PLAINTEXT, "");
    }

    private ControllerInformation emptyControllerInfo() {
        return new ControllerInformation(Optional.empty(), new ListenerName(""), SecurityProtocol.PLAINTEXT, "");
    }

}
