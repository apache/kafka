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
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.RebootstrapRequiredException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsRequestData;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.PushTelemetryRequestData;
import org.apache.kafka.common.message.PushTelemetryResponseData;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsRequest;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.common.requests.PushTelemetryResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator;
import org.apache.kafka.common.telemetry.internals.ClientTelemetrySender;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.DelayedReceive;
import org.apache.kafka.test.MockSelector;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.common.protocol.ApiKeys.PRODUCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NetworkClientTest {

    protected final int defaultRequestTimeoutMs = 1000;
    protected final MockTime time = new MockTime();
    protected final MockSelector selector = new MockSelector(time);
    protected final Node node = TestUtils.singletonCluster().nodes().iterator().next();
    protected final long reconnectBackoffMsTest = 10 * 1000;
    protected final long reconnectBackoffMaxMsTest = 10 * 10000;
    protected final long connectionSetupTimeoutMsTest = 5 * 1000;
    protected final long connectionSetupTimeoutMaxMsTest = 127 * 1000;
    private final int reconnectBackoffExpBase = ClusterConnectionStates.RECONNECT_BACKOFF_EXP_BASE;
    private final double reconnectBackoffJitter = ClusterConnectionStates.RECONNECT_BACKOFF_JITTER;
    private final TestMetadataUpdater metadataUpdater = new TestMetadataUpdater(Collections.singletonList(node));
    private final NetworkClient client = createNetworkClient(reconnectBackoffMaxMsTest);
    private final NetworkClient clientWithNoExponentialBackoff = createNetworkClient(reconnectBackoffMsTest);
    private final NetworkClient clientWithStaticNodes = createNetworkClientWithStaticNodes();
    private final NetworkClient clientWithNoVersionDiscovery = createNetworkClientWithNoVersionDiscovery();

    private static ArrayList<InetAddress> initialAddresses;
    private static ArrayList<InetAddress> newAddresses;

    static {
        try {
            initialAddresses = new ArrayList<>(Arrays.asList(
                    InetAddress.getByName("10.200.20.100"),
                    InetAddress.getByName("10.200.20.101"),
                    InetAddress.getByName("10.200.20.102")
            ));
            newAddresses = new ArrayList<>(Arrays.asList(
                    InetAddress.getByName("10.200.20.103"),
                    InetAddress.getByName("10.200.20.104"),
                    InetAddress.getByName("10.200.20.105")
            ));
        } catch (UnknownHostException e) {
            fail("Attempted to create an invalid InetAddress, this should not happen");
        }
    }

    private NetworkClient createNetworkClient(long reconnectBackoffMaxMs) {
        return new NetworkClient(selector, metadataUpdater, "mock", Integer.MAX_VALUE,
                reconnectBackoffMsTest, reconnectBackoffMaxMs, 64 * 1024, 64 * 1024,
                defaultRequestTimeoutMs, connectionSetupTimeoutMsTest, connectionSetupTimeoutMaxMsTest, time, true, new ApiVersions(), new LogContext(),
                MetadataRecoveryStrategy.NONE);
    }

    private NetworkClient createNetworkClientWithMaxInFlightRequestsPerConnection(
            int maxInFlightRequestsPerConnection, long reconnectBackoffMaxMs) {
        return new NetworkClient(selector, metadataUpdater, "mock", maxInFlightRequestsPerConnection,
                reconnectBackoffMsTest, reconnectBackoffMaxMs, 64 * 1024, 64 * 1024,
                defaultRequestTimeoutMs, connectionSetupTimeoutMsTest, connectionSetupTimeoutMaxMsTest, time, true, new ApiVersions(), new LogContext(),
                MetadataRecoveryStrategy.NONE);
    }

    private NetworkClient createNetworkClientWithMultipleNodes(long reconnectBackoffMaxMs, long connectionSetupTimeoutMsTest, int nodeNumber) {
        List<Node> nodes = TestUtils.clusterWith(nodeNumber).nodes();
        TestMetadataUpdater metadataUpdater = new TestMetadataUpdater(nodes);
        return new NetworkClient(selector, metadataUpdater, "mock", Integer.MAX_VALUE,
                reconnectBackoffMsTest, reconnectBackoffMaxMs, 64 * 1024, 64 * 1024,
                defaultRequestTimeoutMs, connectionSetupTimeoutMsTest, connectionSetupTimeoutMaxMsTest, time, true, new ApiVersions(), new LogContext(),
                MetadataRecoveryStrategy.NONE);
    }

    private NetworkClient createNetworkClientWithStaticNodes() {
        return new NetworkClient(selector, metadataUpdater,
                "mock-static", Integer.MAX_VALUE, 0, 0, 64 * 1024, 64 * 1024, defaultRequestTimeoutMs,
                connectionSetupTimeoutMsTest, connectionSetupTimeoutMaxMsTest, time, true, new ApiVersions(), new LogContext(),
                MetadataRecoveryStrategy.NONE);
    }

    private NetworkClient createNetworkClientWithNoVersionDiscovery(Metadata metadata) {
        return new NetworkClient(selector, metadata, "mock", Integer.MAX_VALUE,
                reconnectBackoffMsTest, 0, 64 * 1024, 64 * 1024,
                defaultRequestTimeoutMs, connectionSetupTimeoutMsTest, connectionSetupTimeoutMaxMsTest, time, false, new ApiVersions(), new LogContext(),
                MetadataRecoveryStrategy.NONE);
    }

    private NetworkClient createNetworkClientWithNoVersionDiscovery() {
        return new NetworkClient(selector, metadataUpdater, "mock", Integer.MAX_VALUE,
                reconnectBackoffMsTest, reconnectBackoffMaxMsTest,
                64 * 1024, 64 * 1024, defaultRequestTimeoutMs,
                connectionSetupTimeoutMsTest, connectionSetupTimeoutMaxMsTest, time, false, new ApiVersions(), new LogContext(),
                MetadataRecoveryStrategy.NONE);
    }

    @BeforeEach
    public void setup() {
        selector.reset();
    }

    @Test
    public void testSendToUnreadyNode() {
        MetadataRequest.Builder builder = new MetadataRequest.Builder(Collections.singletonList("test"), true);
        long now = time.milliseconds();
        ClientRequest request = client.newClientRequest("5", builder, now, false);
        assertThrows(IllegalStateException.class, () -> client.send(request, now));
    }

    @Test
    public void testSimpleRequestResponse() {
        checkSimpleRequestResponse(client);
    }

    @Test
    public void testSimpleRequestResponseWithStaticNodes() {
        checkSimpleRequestResponse(clientWithStaticNodes);
    }

    @Test
    public void testSimpleRequestResponseWithNoBrokerDiscovery() {
        checkSimpleRequestResponse(clientWithNoVersionDiscovery);
    }

    @Test
    public void testDnsLookupFailure() {
        /* Fail cleanly when the node has a bad hostname */
        assertFalse(client.ready(new Node(1234, "badhost", 1234), time.milliseconds()));
    }

    @Test
    public void testClose() {
        client.ready(node, time.milliseconds());
        awaitReady(client, node);
        client.poll(1, time.milliseconds());
        assertTrue(client.isReady(node, time.milliseconds()), "The client should be ready");

        ProduceRequest.Builder builder = ProduceRequest.forCurrentMagic(new ProduceRequestData()
                .setTopicData(new ProduceRequestData.TopicProduceDataCollection())
                .setAcks((short) 1)
                .setTimeoutMs(1000));
        ClientRequest request = client.newClientRequest(node.idString(), builder, time.milliseconds(), true);
        client.send(request, time.milliseconds());
        assertEquals(1, client.inFlightRequestCount(node.idString()),
            "There should be 1 in-flight request after send");
        assertTrue(client.hasInFlightRequests(node.idString()));
        assertTrue(client.hasInFlightRequests());

        client.close(node.idString());
        assertEquals(0, client.inFlightRequestCount(node.idString()), "There should be no in-flight request after close");
        assertFalse(client.hasInFlightRequests(node.idString()));
        assertFalse(client.hasInFlightRequests());
        assertFalse(client.isReady(node, 0), "Connection should not be ready after close");
    }

    @Test
    public void testUnsupportedVersionDuringInternalMetadataRequest() {
        List<String> topics = Collections.singletonList("topic_1");

        // disabling auto topic creation for versions less than 4 is not supported
        MetadataRequest.Builder builder = new MetadataRequest.Builder(topics, false, (short) 3);
        client.sendInternalMetadataRequest(builder, node.idString(), time.milliseconds());
        assertEquals(UnsupportedVersionException.class, metadataUpdater.getAndClearFailure().getClass());
    }

    @Test
    public void testRebootstrap() {
        long rebootstrapTriggerMs = 1000;
        AtomicInteger rebootstrapCount = new AtomicInteger();
        Metadata metadata = new Metadata(50, 50, 5000, new LogContext(), new ClusterResourceListeners()) {
            @Override
            public synchronized void rebootstrap() {
                super.rebootstrap();
                rebootstrapCount.incrementAndGet();
            }
        };

        NetworkClient client = new NetworkClient(selector, metadata, "mock", Integer.MAX_VALUE,
                reconnectBackoffMsTest, 0, 64 * 1024, 64 * 1024,
                defaultRequestTimeoutMs, connectionSetupTimeoutMsTest, connectionSetupTimeoutMaxMsTest, time, false, new ApiVersions(), new LogContext(),
                rebootstrapTriggerMs,
                MetadataRecoveryStrategy.REBOOTSTRAP);
        MetadataUpdater metadataUpdater = TestUtils.fieldValue(client, NetworkClient.class, "metadataUpdater");
        metadata.bootstrap(Collections.singletonList(new InetSocketAddress("localhost", 9999)));

        metadata.requestUpdate(true);
        client.poll(0, time.milliseconds());
        time.sleep(rebootstrapTriggerMs + 1);
        client.poll(0, time.milliseconds());
        assertEquals(1, rebootstrapCount.get());
        time.sleep(1);
        client.poll(0, time.milliseconds());
        assertEquals(1, rebootstrapCount.get());

        metadata.requestUpdate(true);
        client.poll(0, time.milliseconds());
        assertEquals(1, rebootstrapCount.get());
        metadataUpdater.handleFailedRequest(time.milliseconds(), Optional.of(new KafkaException()));
        client.poll(0, time.milliseconds());
        assertEquals(1, rebootstrapCount.get());
        time.sleep(rebootstrapTriggerMs);
        client.poll(0, time.milliseconds());
        assertEquals(2, rebootstrapCount.get());

        metadata.requestUpdate(true);
        client.poll(0, time.milliseconds());
        assertEquals(2, rebootstrapCount.get());

        MetadataRequest.Builder builder = new MetadataRequest.Builder(Collections.emptyList(), true);
        ClientRequest request = client.newClientRequest(node.idString(), builder, time.milliseconds(), true);
        MetadataResponse rebootstrapResponse = (MetadataResponse) builder.build().getErrorResponse(0, new RebootstrapRequiredException("rebootstrap"));
        metadataUpdater.handleSuccessfulResponse(request.makeHeader(builder.latestAllowedVersion()), time.milliseconds(), rebootstrapResponse);
        assertEquals(2, rebootstrapCount.get());
        time.sleep(50);
        client.poll(0, time.milliseconds());
        assertEquals(3, rebootstrapCount.get());
    }

    @Test
    public void testInflightRequestsDuringRebootstrap() {
        long refreshBackoffMs = 50;
        long rebootstrapTriggerMs = 1000;
        int defaultRequestTimeoutMs = 5000;
        AtomicInteger rebootstrapCount = new AtomicInteger();
        Metadata metadata = new Metadata(refreshBackoffMs, refreshBackoffMs, 5000, new LogContext(), new ClusterResourceListeners())  {
            @Override
            public synchronized void rebootstrap() {
                super.rebootstrap();
                rebootstrapCount.incrementAndGet();
            }
        };
        metadata.bootstrap(Collections.singletonList(new InetSocketAddress("localhost", 9999)));
        NetworkClient client = new NetworkClient(selector, metadata, "mock", Integer.MAX_VALUE,
                reconnectBackoffMsTest, 0, 64 * 1024, 64 * 1024,
                defaultRequestTimeoutMs, connectionSetupTimeoutMsTest, connectionSetupTimeoutMaxMsTest, time, false, new ApiVersions(), new LogContext(),
                rebootstrapTriggerMs, MetadataRecoveryStrategy.REBOOTSTRAP);

        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith(2, Collections.emptyMap());
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, time.milliseconds());
        List<Node> nodes = metadata.fetch().nodes();
        nodes.forEach(node -> {
            client.ready(node, time.milliseconds());
            awaitReady(client, node);
        });

        // Queue a request
        sendEmptyProduceRequest(client, nodes.get(0).idString());
        List<ClientResponse> responses = client.poll(0, time.milliseconds());
        assertEquals(0, responses.size());
        assertEquals(1, client.inFlightRequestCount());

        // Trigger rebootstrap
        metadata.requestUpdate(true);
        time.sleep(refreshBackoffMs);
        responses = client.poll(0, time.milliseconds());
        assertEquals(0, responses.size());
        assertEquals(2, client.inFlightRequestCount());
        time.sleep(rebootstrapTriggerMs + 1);
        responses = client.poll(0, time.milliseconds());

        // Verify that inflight produce request was aborted with disconnection
        assertEquals(1, responses.size());
        assertEquals(PRODUCE, responses.get(0).requestHeader().apiKey());
        assertTrue(responses.get(0).wasDisconnected());
        assertEquals(0, client.inFlightRequestCount());
        assertEquals(Collections.emptySet(), nodes.stream().filter(node -> !client.connectionFailed(node)).collect(Collectors.toSet()));
    }

    private void checkSimpleRequestResponse(NetworkClient networkClient) {
        awaitReady(networkClient, node); // has to be before creating any request, as it may send ApiVersionsRequest and its response is mocked with correlation id 0
        short requestVersion = PRODUCE.latestVersion();
        ProduceRequest.Builder builder = new ProduceRequest.Builder(
                requestVersion,
                requestVersion,
                new ProduceRequestData()
                    .setAcks((short) 1)
                    .setTimeoutMs(1000));
        TestCallbackHandler handler = new TestCallbackHandler();
        ClientRequest request = networkClient.newClientRequest(node.idString(), builder, time.milliseconds(),
            true, defaultRequestTimeoutMs, handler);
        networkClient.send(request, time.milliseconds());
        networkClient.poll(1, time.milliseconds());
        assertEquals(1, networkClient.inFlightRequestCount());
        ProduceResponse produceResponse = new ProduceResponse(new ProduceResponseData());
        ByteBuffer buffer = RequestTestUtils.serializeResponseWithHeader(produceResponse, requestVersion, request.correlationId());
        selector.completeReceive(new NetworkReceive(node.idString(), buffer));
        List<ClientResponse> responses = networkClient.poll(1, time.milliseconds());
        assertEquals(1, responses.size());
        assertTrue(handler.executed, "The handler should have executed.");
        assertTrue(handler.response.hasResponse(), "Should have a response body.");
        assertEquals(request.correlationId(), handler.response.requestHeader().correlationId(),
            "Should be correlated to the original request");
    }

    private void delayedApiVersionsResponse(int correlationId, short version, ApiVersionsResponse response) {
        ByteBuffer buffer = RequestTestUtils.serializeResponseWithHeader(response, version, correlationId);
        selector.delayedReceive(new DelayedReceive(node.idString(), new NetworkReceive(node.idString(), buffer)));
    }

    private void setExpectedApiVersionsResponse(ApiVersionsResponse response) {
        short apiVersionsResponseVersion = response.apiVersion(ApiKeys.API_VERSIONS.id).maxVersion();
        delayedApiVersionsResponse(0, apiVersionsResponseVersion, response);
    }

    private void awaitReady(NetworkClient client, Node node) {
        if (client.discoverBrokerVersions()) {
            setExpectedApiVersionsResponse(TestUtils.defaultApiVersionsResponse(
                ApiMessageType.ListenerType.ZK_BROKER));
        }
        while (!client.ready(node, time.milliseconds()))
            client.poll(1, time.milliseconds());
        selector.clear();
    }

    @Test
    public void testInvalidApiVersionsRequest() {
        // initiate the connection
        client.ready(node, time.milliseconds());

        // handle the connection, send the ApiVersionsRequest
        client.poll(0, time.milliseconds());

        // check that the ApiVersionsRequest has been initiated
        assertTrue(client.hasInFlightRequests(node.idString()));

        // prepare response
        delayedApiVersionsResponse(0, ApiKeys.API_VERSIONS.latestVersion(),
            new ApiVersionsResponse(
                new ApiVersionsResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setThrottleTimeMs(0)
            ));

        // handle completed receives
        client.poll(0, time.milliseconds());

        // the ApiVersionsRequest is gone
        assertFalse(client.hasInFlightRequests(node.idString()));

        // various assertions
        assertFalse(client.isReady(node, time.milliseconds()));
    }

    @Test
    public void testApiVersionsRequest() {
        // initiate the connection
        client.ready(node, time.milliseconds());

        // handle the connection, send the ApiVersionsRequest
        client.poll(0, time.milliseconds());

        // check that the ApiVersionsRequest has been initiated
        assertTrue(client.hasInFlightRequests(node.idString()));

        // prepare response
        delayedApiVersionsResponse(0, ApiKeys.API_VERSIONS.latestVersion(), defaultApiVersionsResponse());

        // handle completed receives
        client.poll(0, time.milliseconds());

        // the ApiVersionsRequest is gone
        assertFalse(client.hasInFlightRequests(node.idString()));

        // various assertions
        assertTrue(client.isReady(node, time.milliseconds()));
    }

    @Test
    public void testUnsupportedApiVersionsRequestWithVersionProvidedByTheBroker() {
        // initiate the connection
        client.ready(node, time.milliseconds());

        // handle the connection, initiate first ApiVersionsRequest
        client.poll(0, time.milliseconds());

        // ApiVersionsRequest is in flight but not sent yet
        assertTrue(client.hasInFlightRequests(node.idString()));

        // completes initiated sends
        client.poll(0, time.milliseconds());
        assertEquals(1, selector.completedSends().size());

        ByteBuffer buffer = selector.completedSendBuffers().get(0).buffer();
        RequestHeader header = parseHeader(buffer);
        assertEquals(ApiKeys.API_VERSIONS, header.apiKey());
        assertEquals(4, header.apiVersion());

        // prepare response
        ApiVersionCollection apiKeys = new ApiVersionCollection();
        apiKeys.add(new ApiVersion()
            .setApiKey(ApiKeys.API_VERSIONS.id)
            .setMinVersion((short) 0)
            .setMaxVersion((short) 2));
        delayedApiVersionsResponse(0, (short) 0,
            new ApiVersionsResponse(
                new ApiVersionsResponseData()
                    .setErrorCode(Errors.UNSUPPORTED_VERSION.code())
                    .setApiKeys(apiKeys)
            ));

        // handle ApiVersionResponse, initiate second ApiVersionRequest
        client.poll(0, time.milliseconds());

        // ApiVersionsRequest is in flight but not sent yet
        assertTrue(client.hasInFlightRequests(node.idString()));

        // ApiVersionsResponse has been received
        assertEquals(1, selector.completedReceives().size());

        // clean up the buffers
        selector.completedSends().clear();
        selector.completedSendBuffers().clear();
        selector.completedReceives().clear();

        // completes initiated sends
        client.poll(0, time.milliseconds());

        // ApiVersionsRequest has been sent
        assertEquals(1, selector.completedSends().size());

        buffer = selector.completedSendBuffers().get(0).buffer();
        header = parseHeader(buffer);
        assertEquals(ApiKeys.API_VERSIONS, header.apiKey());
        assertEquals(2, header.apiVersion());

        // prepare response
        delayedApiVersionsResponse(1, (short) 0, defaultApiVersionsResponse());

        // handle completed receives
        client.poll(0, time.milliseconds());

        // the ApiVersionsRequest is gone
        assertFalse(client.hasInFlightRequests(node.idString()));
        assertEquals(1, selector.completedReceives().size());

        // the client is ready
        assertTrue(client.isReady(node, time.milliseconds()));
    }

    @Test
    public void testUnsupportedApiVersionsRequestWithoutVersionProvidedByTheBroker() {
        // initiate the connection
        client.ready(node, time.milliseconds());

        // handle the connection, initiate first ApiVersionsRequest
        client.poll(0, time.milliseconds());

        // ApiVersionsRequest is in flight but not sent yet
        assertTrue(client.hasInFlightRequests(node.idString()));

        // completes initiated sends
        client.poll(0, time.milliseconds());
        assertEquals(1, selector.completedSends().size());

        ByteBuffer buffer = selector.completedSendBuffers().get(0).buffer();
        RequestHeader header = parseHeader(buffer);
        assertEquals(ApiKeys.API_VERSIONS, header.apiKey());
        assertEquals(4, header.apiVersion());

        // prepare response
        delayedApiVersionsResponse(0, (short) 0,
            new ApiVersionsResponse(
                new ApiVersionsResponseData()
                    .setErrorCode(Errors.UNSUPPORTED_VERSION.code())
            ));

        // handle ApiVersionResponse, initiate second ApiVersionRequest
        client.poll(0, time.milliseconds());

        // ApiVersionsRequest is in flight but not sent yet
        assertTrue(client.hasInFlightRequests(node.idString()));

        // ApiVersionsResponse has been received
        assertEquals(1, selector.completedReceives().size());

        // clean up the buffers
        selector.completedSends().clear();
        selector.completedSendBuffers().clear();
        selector.completedReceives().clear();

        // completes initiated sends
        client.poll(0, time.milliseconds());

        // ApiVersionsRequest has been sent
        assertEquals(1, selector.completedSends().size());

        buffer = selector.completedSendBuffers().get(0).buffer();
        header = parseHeader(buffer);
        assertEquals(ApiKeys.API_VERSIONS, header.apiKey());
        assertEquals(0, header.apiVersion());

        // prepare response
        delayedApiVersionsResponse(1, (short) 0, defaultApiVersionsResponse());

        // handle completed receives
        client.poll(0, time.milliseconds());

        // the ApiVersionsRequest is gone
        assertFalse(client.hasInFlightRequests(node.idString()));
        assertEquals(1, selector.completedReceives().size());

        // the client is ready
        assertTrue(client.isReady(node, time.milliseconds()));
    }

    @Test
    public void testRequestTimeout() {
        testRequestTimeout(defaultRequestTimeoutMs + 5000);
    }

    @Test
    public void testDefaultRequestTimeout() {
        testRequestTimeout(defaultRequestTimeoutMs);
    }

    /**
     * This is a helper method that will execute two produce calls. The first call is expected to work and the
     * second produce call is intentionally made to emulate a request timeout. In the case that a timeout occurs
     * during a request, we want to ensure that we {@link Metadata#requestUpdate(boolean) request a metadata update} so that
     * on a subsequent invocation of {@link NetworkClient#poll(long, long) poll}, the metadata request will be sent.
     *
     * <p/>
     *
     * The {@link MetadataUpdater} has a specific method to handle
     * {@link NetworkClient.DefaultMetadataUpdater#handleServerDisconnect(long, String, Optional) server disconnects}
     * which is where we {@link Metadata#requestUpdate(boolean) request a metadata update}. This test helper method ensures
     * that is invoked by checking {@link Metadata#updateRequested()} after the simulated timeout.
     *
     * @param requestTimeoutMs Timeout in ms
     */
    private void testRequestTimeout(int requestTimeoutMs) {
        Metadata metadata = new Metadata(50, 50, 5000, new LogContext(), new ClusterResourceListeners());
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith(2, Collections.emptyMap());
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, time.milliseconds());

        NetworkClient client = createNetworkClientWithNoVersionDiscovery(metadata);

        // Send first produce without any timeout.
        ClientResponse clientResponse = produce(client, requestTimeoutMs, false);
        assertEquals(node.idString(), clientResponse.destination());
        assertFalse(clientResponse.wasDisconnected(), "Expected response to succeed and not disconnect");
        assertFalse(clientResponse.wasTimedOut(), "Expected response to succeed and not time out");
        assertFalse(metadata.updateRequested(), "Expected NetworkClient to not need to update metadata");

        // Send second request, but emulate a timeout.
        clientResponse = produce(client, requestTimeoutMs, true);
        assertEquals(node.idString(), clientResponse.destination());
        assertTrue(clientResponse.wasDisconnected(), "Expected response to fail due to disconnection");
        assertTrue(clientResponse.wasTimedOut(), "Expected response to fail due to timeout");
        assertTrue(metadata.updateRequested(), "Expected NetworkClient to have called requestUpdate on metadata on timeout");
    }

    private ClientResponse produce(NetworkClient client, int requestTimeoutMs, boolean shouldEmulateTimeout) {
        awaitReady(client, node); // has to be before creating any request, as it may send ApiVersionsRequest and its response is mocked with correlation id 0
        ProduceRequest.Builder builder = ProduceRequest.forCurrentMagic(new ProduceRequestData()
                .setTopicData(new ProduceRequestData.TopicProduceDataCollection())
                .setAcks((short) 1)
                .setTimeoutMs(1000));
        TestCallbackHandler handler = new TestCallbackHandler();
        ClientRequest request = client.newClientRequest(node.idString(), builder, time.milliseconds(), true,
                requestTimeoutMs, handler);
        client.send(request, time.milliseconds());

        if (shouldEmulateTimeout) {
            // For a delay of slightly more than our timeout threshold to emulate the request timing out.
            time.sleep(requestTimeoutMs + 1);
        } else {
            ProduceResponse produceResponse = new ProduceResponse(new ProduceResponseData());
            ByteBuffer buffer = RequestTestUtils.serializeResponseWithHeader(produceResponse, PRODUCE.latestVersion(), request.correlationId());
            selector.completeReceive(new NetworkReceive(node.idString(), buffer));
        }

        List<ClientResponse> responses = client.poll(0, time.milliseconds());
        assertEquals(1, responses.size());
        return responses.get(0);
    }

    @Test
    public void testConnectionSetupTimeout() {
        // Use two nodes to ensure that the logic iterate over a set of more than one
        // element. ConcurrentModificationException is not triggered otherwise.
        final Cluster cluster = TestUtils.clusterWith(2);
        final Node node0 = cluster.nodeById(0);
        final Node node1 = cluster.nodeById(1);

        client.ready(node0, time.milliseconds());
        selector.serverConnectionBlocked(node0.idString());

        client.ready(node1, time.milliseconds());
        selector.serverConnectionBlocked(node1.idString());

        client.poll(0, time.milliseconds());
        assertFalse(client.connectionFailed(node),
            "The connections should not fail before the socket connection setup timeout elapsed");

        time.sleep((long) (connectionSetupTimeoutMsTest * 1.2) + 1);
        client.poll(0, time.milliseconds());
        assertTrue(client.connectionFailed(node),
            "Expected the connections to fail due to the socket connection setup timeout");
    }

    @Test
    public void testConnectionTimeoutAfterThrottling() {
        awaitReady(client, node);
        short requestVersion = PRODUCE.latestVersion();
        int timeoutMs = 1000;
        ProduceRequest.Builder builder = new ProduceRequest.Builder(
            requestVersion,
            requestVersion,
            new ProduceRequestData()
                .setAcks((short) 1)
                .setTimeoutMs(timeoutMs));
        TestCallbackHandler handler = new TestCallbackHandler();
        ClientRequest r1 = client.newClientRequest(node.idString(), builder, time.milliseconds(), true,
                defaultRequestTimeoutMs, handler);

        client.send(r1, time.milliseconds());
        client.poll(0, time.milliseconds());

        // Throttle long enough to ensure other inFlight requests timeout.
        ProduceResponse pr = new ProduceResponse(new ProduceResponseData().setThrottleTimeMs(timeoutMs));
        ByteBuffer buffer = RequestTestUtils.serializeResponseWithHeader(pr, requestVersion, r1.correlationId());
        selector.delayedReceive(new DelayedReceive(node.idString(), new NetworkReceive(node.idString(), buffer)));
        ClientRequest r2 = client.newClientRequest(node.idString(), builder, time.milliseconds(), true,
                defaultRequestTimeoutMs, handler);
        client.send(r2, time.milliseconds());
        time.sleep(timeoutMs);
        client.poll(0, time.milliseconds());

        assertEquals(1, client.inFlightRequestCount(node.idString()));
        assertFalse(client.connectionFailed(node), "Connection should not have failed due to the extra time spent throttling.");
    }

    @Test
    public void testConnectionThrottling() {
        // Instrument the test to return a response with a 100ms throttle delay.
        awaitReady(client, node);
        short requestVersion = PRODUCE.latestVersion();
        ProduceRequest.Builder builder = new ProduceRequest.Builder(
            requestVersion,
            requestVersion,
            new ProduceRequestData()
                .setAcks((short) 1)
                .setTimeoutMs(1000));
        TestCallbackHandler handler = new TestCallbackHandler();
        ClientRequest request = client.newClientRequest(node.idString(), builder, time.milliseconds(), true,
                defaultRequestTimeoutMs, handler);
        client.send(request, time.milliseconds());
        client.poll(1, time.milliseconds());
        int throttleTime = 100;
        ProduceResponse produceResponse = new ProduceResponse(new ProduceResponseData().setThrottleTimeMs(throttleTime));
        ByteBuffer buffer = RequestTestUtils.serializeResponseWithHeader(produceResponse, requestVersion, request.correlationId());
        selector.completeReceive(new NetworkReceive(node.idString(), buffer));
        client.poll(1, time.milliseconds());

        // The connection is not ready due to throttling.
        assertFalse(client.ready(node, time.milliseconds()));
        assertEquals(100, client.throttleDelayMs(node, time.milliseconds()));

        // After 50ms, the connection is not ready yet.
        time.sleep(50);
        assertFalse(client.ready(node, time.milliseconds()));
        assertEquals(50, client.throttleDelayMs(node, time.milliseconds()));

        // After another 50ms, the throttling is done and the connection becomes ready again.
        time.sleep(50);
        assertTrue(client.ready(node, time.milliseconds()));
        assertEquals(0, client.throttleDelayMs(node, time.milliseconds()));
    }

    // Creates expected ApiVersionsResponse from the specified node, where the max protocol version for the specified
    // key is set to the specified version.
    private ApiVersionsResponse createExpectedApiVersionsResponse(ApiKeys key, short maxVersion) {
        ApiVersionCollection versionList = new ApiVersionCollection();
        for (ApiKeys apiKey : ApiKeys.values()) {
            if (apiKey == key) {
                versionList.add(new ApiVersion()
                    .setApiKey(apiKey.id)
                    .setMinVersion((short) 0)
                    .setMaxVersion(maxVersion));
            } else versionList.add(ApiVersionsResponse.toApiVersion(apiKey));
        }
        return new ApiVersionsResponse(new ApiVersionsResponseData()
            .setErrorCode(Errors.NONE.code())
            .setThrottleTimeMs(0)
            .setApiKeys(versionList));
    }

    @Test
    public void testThrottlingNotEnabledForConnectionToOlderBroker() {
        // Instrument the test so that the max protocol version for PRODUCE returned from the node is 5 and thus
        // client-side throttling is not enabled. Also, return a response with a 100ms throttle delay.
        setExpectedApiVersionsResponse(createExpectedApiVersionsResponse(PRODUCE, (short) 5));
        while (!client.ready(node, time.milliseconds()))
            client.poll(1, time.milliseconds());
        selector.clear();

        int correlationId = sendEmptyProduceRequest();
        client.poll(1, time.milliseconds());

        sendThrottledProduceResponse(correlationId, 100, (short) 5);
        client.poll(1, time.milliseconds());

        // Since client-side throttling is disabled, the connection is ready even though the response indicated a
        // throttle delay.
        assertTrue(client.ready(node, time.milliseconds()));
        assertEquals(0, client.throttleDelayMs(node, time.milliseconds()));
    }

    private int sendEmptyProduceRequest() {
        return sendEmptyProduceRequest(client, node.idString());
    }

    private int sendEmptyProduceRequest(NetworkClient client, String nodeId) {
        ProduceRequest.Builder builder = ProduceRequest.forCurrentMagic(new ProduceRequestData()
                .setTopicData(new ProduceRequestData.TopicProduceDataCollection())
                .setAcks((short) 1)
                .setTimeoutMs(1000));
        TestCallbackHandler handler = new TestCallbackHandler();
        ClientRequest request = client.newClientRequest(nodeId, builder, time.milliseconds(), true,
                defaultRequestTimeoutMs, handler);
        client.send(request, time.milliseconds());
        return request.correlationId();
    }

    private void sendResponse(AbstractResponse response, short version, int correlationId) {
        ByteBuffer buffer = RequestTestUtils.serializeResponseWithHeader(response, version, correlationId);
        selector.completeReceive(new NetworkReceive(node.idString(), buffer));
    }

    private void sendThrottledProduceResponse(int correlationId, int throttleMs, short version) {
        ProduceResponse response = new ProduceResponse(new ProduceResponseData().setThrottleTimeMs(throttleMs));
        sendResponse(response, version, correlationId);
    }

    @Test
    public void testLeastLoadedNode() {
        client.ready(node, time.milliseconds());
        assertFalse(client.isReady(node, time.milliseconds()));
        LeastLoadedNode leastLoadedNode = client.leastLoadedNode(time.milliseconds());
        assertEquals(node, leastLoadedNode.node());
        assertTrue(leastLoadedNode.hasNodeAvailableOrConnectionReady());

        awaitReady(client, node);
        client.poll(1, time.milliseconds());
        assertTrue(client.isReady(node, time.milliseconds()), "The client should be ready");

        // leastloadednode should be our single node
        leastLoadedNode = client.leastLoadedNode(time.milliseconds());
        assertTrue(leastLoadedNode.hasNodeAvailableOrConnectionReady());
        Node leastNode = leastLoadedNode.node();
        assertEquals(leastNode.id(), node.id(), "There should be one leastloadednode");

        // sleep for longer than reconnect backoff
        time.sleep(reconnectBackoffMsTest);

        // CLOSE node
        selector.serverDisconnect(node.idString());

        client.poll(1, time.milliseconds());
        assertFalse(client.ready(node, time.milliseconds()), "After we forced the disconnection the client is no longer ready.");
        leastLoadedNode = client.leastLoadedNode(time.milliseconds());
        assertFalse(leastLoadedNode.hasNodeAvailableOrConnectionReady());
        assertNull(leastLoadedNode.node(), "There should be NO leastloadednode");
    }

    @Test
    public void testHasNodeAvailableOrConnectionReady() {
        NetworkClient client = createNetworkClientWithMaxInFlightRequestsPerConnection(1, reconnectBackoffMaxMsTest);
        awaitReady(client, node);

        long now = time.milliseconds();
        LeastLoadedNode leastLoadedNode = client.leastLoadedNode(now);
        assertEquals(node, leastLoadedNode.node());
        assertTrue(leastLoadedNode.hasNodeAvailableOrConnectionReady());

        MetadataRequest.Builder builder = new MetadataRequest.Builder(Collections.emptyList(), true);
        ClientRequest request = client.newClientRequest(node.idString(), builder, now, true);
        client.send(request, now);
        client.poll(defaultRequestTimeoutMs, now);

        leastLoadedNode = client.leastLoadedNode(now);
        assertNull(leastLoadedNode.node());
        assertTrue(leastLoadedNode.hasNodeAvailableOrConnectionReady());
    }

    @Test
    public void testLeastLoadedNodeProvideDisconnectedNodesPrioritizedByLastConnectionTimestamp() {
        int nodeNumber = 3;
        NetworkClient client = createNetworkClientWithMultipleNodes(0, connectionSetupTimeoutMsTest, nodeNumber);

        Set<Node> providedNodeIds = new HashSet<>();
        for (int i = 0; i < nodeNumber * 10; i++) {
            Node node = client.leastLoadedNode(time.milliseconds()).node();
            assertNotNull(node, "Should provide a node");
            providedNodeIds.add(node);
            client.ready(node, time.milliseconds());
            client.disconnect(node.idString());
            time.sleep(connectionSetupTimeoutMsTest + 1);
            client.poll(0, time.milliseconds());
            // Define a round as nodeNumber of nodes have been provided
            // In each round every node should be provided exactly once
            if ((i + 1) % nodeNumber == 0) {
                assertEquals(nodeNumber, providedNodeIds.size(), "All the nodes should be provided");
                providedNodeIds.clear();
            }
        }
    }

    @Test
    public void testAuthenticationFailureWithInFlightMetadataRequest() {
        int refreshBackoffMs = 50;

        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith(2, Collections.emptyMap());
        Metadata metadata = new Metadata(refreshBackoffMs, refreshBackoffMs, 5000, new LogContext(), new ClusterResourceListeners());
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, time.milliseconds());

        Cluster cluster = metadata.fetch();
        Node node1 = cluster.nodes().get(0);
        Node node2 = cluster.nodes().get(1);

        NetworkClient client = createNetworkClientWithNoVersionDiscovery(metadata);

        awaitReady(client, node1);

        metadata.requestUpdate(true);
        time.sleep(refreshBackoffMs);

        client.poll(0, time.milliseconds());

        Optional<Node> nodeWithPendingMetadataOpt = cluster.nodes().stream()
                .filter(node -> client.hasInFlightRequests(node.idString()))
                .findFirst();
        assertEquals(Optional.of(node1), nodeWithPendingMetadataOpt);

        assertFalse(client.ready(node2, time.milliseconds()));
        selector.serverAuthenticationFailed(node2.idString());
        client.poll(0, time.milliseconds());
        assertNotNull(client.authenticationException(node2));

        ByteBuffer requestBuffer = selector.completedSendBuffers().get(0).buffer();
        RequestHeader header = parseHeader(requestBuffer);
        assertEquals(ApiKeys.METADATA, header.apiKey());

        ByteBuffer responseBuffer = RequestTestUtils.serializeResponseWithHeader(metadataResponse, header.apiVersion(), header.correlationId());
        selector.delayedReceive(new DelayedReceive(node1.idString(), new NetworkReceive(node1.idString(), responseBuffer)));

        int initialUpdateVersion = metadata.updateVersion();
        client.poll(0, time.milliseconds());
        assertEquals(initialUpdateVersion + 1, metadata.updateVersion());
    }

    @Test
    public void testLeastLoadedNodeConsidersThrottledConnections() {
        client.ready(node, time.milliseconds());
        awaitReady(client, node);
        client.poll(1, time.milliseconds());
        assertTrue(client.isReady(node, time.milliseconds()), "The client should be ready");

        int correlationId = sendEmptyProduceRequest();
        client.poll(1, time.milliseconds());

        sendThrottledProduceResponse(correlationId, 100, PRODUCE.latestVersion());
        client.poll(1, time.milliseconds());

        // leastloadednode should return null since the node is throttled
        assertNull(client.leastLoadedNode(time.milliseconds()).node());
    }

    @Test
    public void testConnectionDelayWithNoExponentialBackoff() {
        long now = time.milliseconds();
        long delay = clientWithNoExponentialBackoff.connectionDelay(node, now);

        assertEquals(0, delay);
    }

    @Test
    public void testConnectionDelayConnectedWithNoExponentialBackoff() {
        awaitReady(clientWithNoExponentialBackoff, node);

        long now = time.milliseconds();
        long delay = clientWithNoExponentialBackoff.connectionDelay(node, now);

        assertEquals(Long.MAX_VALUE, delay);
    }

    @Test
    public void testConnectionDelayDisconnectedWithNoExponentialBackoff() {
        awaitReady(clientWithNoExponentialBackoff, node);

        selector.serverDisconnect(node.idString());
        clientWithNoExponentialBackoff.poll(defaultRequestTimeoutMs, time.milliseconds());
        long delay = clientWithNoExponentialBackoff.connectionDelay(node, time.milliseconds());

        assertEquals(reconnectBackoffMsTest, delay);

        // Sleep until there is no connection delay
        time.sleep(delay);
        assertEquals(0, clientWithNoExponentialBackoff.connectionDelay(node, time.milliseconds()));

        // Start connecting and disconnect before the connection is established
        client.ready(node, time.milliseconds());
        selector.serverDisconnect(node.idString());
        client.poll(defaultRequestTimeoutMs, time.milliseconds());

        // Second attempt should have the same behaviour as exponential backoff is disabled
        assertEquals(reconnectBackoffMsTest, delay);
    }

    @Test
    public void testConnectionDelay() {
        long now = time.milliseconds();
        long delay = client.connectionDelay(node, now);

        assertEquals(0, delay);
    }

    @Test
    public void testConnectionDelayConnected() {
        awaitReady(client, node);

        long now = time.milliseconds();
        long delay = client.connectionDelay(node, now);

        assertEquals(Long.MAX_VALUE, delay);
    }

    @Test
    public void testConnectionDelayDisconnected() {
        awaitReady(client, node);

        // First disconnection
        selector.serverDisconnect(node.idString());
        client.poll(defaultRequestTimeoutMs, time.milliseconds());
        long delay = client.connectionDelay(node, time.milliseconds());
        long expectedDelay = reconnectBackoffMsTest;
        double jitter = 0.3;
        assertEquals(expectedDelay, delay, expectedDelay * jitter);

        // Sleep until there is no connection delay
        time.sleep(delay);
        assertEquals(0, client.connectionDelay(node, time.milliseconds()));

        // Start connecting and disconnect before the connection is established
        client.ready(node, time.milliseconds());
        selector.serverDisconnect(node.idString());
        client.poll(defaultRequestTimeoutMs, time.milliseconds());

        // Second attempt should take twice as long with twice the jitter
        expectedDelay = Math.round(delay * 2);
        delay = client.connectionDelay(node, time.milliseconds());
        jitter = 0.6;
        assertEquals(expectedDelay, delay, expectedDelay * jitter);
    }

    @Test
    public void testDisconnectDuringUserMetadataRequest() {
        // this test ensures that the default metadata updater does not intercept a user-initiated
        // metadata request when the remote node disconnects with the request in-flight.
        awaitReady(client, node);

        MetadataRequest.Builder builder = new MetadataRequest.Builder(Collections.emptyList(), true);
        long now = time.milliseconds();
        ClientRequest request = client.newClientRequest(node.idString(), builder, now, true);
        client.send(request, now);
        client.poll(defaultRequestTimeoutMs, now);
        assertEquals(1, client.inFlightRequestCount(node.idString()));
        assertTrue(client.hasInFlightRequests(node.idString()));
        assertTrue(client.hasInFlightRequests());

        selector.close(node.idString());
        List<ClientResponse> responses = client.poll(defaultRequestTimeoutMs, time.milliseconds());
        assertEquals(1, responses.size());
        assertTrue(responses.iterator().next().wasDisconnected());
    }

    @Test
    public void testServerDisconnectAfterInternalApiVersionRequest() throws Exception {
        final long numIterations = 5;
        double reconnectBackoffMaxExp = Math.log(reconnectBackoffMaxMsTest / (double) Math.max(reconnectBackoffMsTest, 1))
            / Math.log(reconnectBackoffExpBase);
        for (int i = 0; i < numIterations; i++) {
            selector.clear();
            awaitInFlightApiVersionRequest();
            selector.serverDisconnect(node.idString());

            // The failed ApiVersion request should not be forwarded to upper layers
            List<ClientResponse> responses = client.poll(0, time.milliseconds());
            assertFalse(client.hasInFlightRequests(node.idString()));
            assertTrue(responses.isEmpty());

            long expectedBackoff = Math.round(Math.pow(reconnectBackoffExpBase, Math.min(i, reconnectBackoffMaxExp))
                * reconnectBackoffMsTest);
            long delay = client.connectionDelay(node, time.milliseconds());
            assertEquals(expectedBackoff, delay, reconnectBackoffJitter * expectedBackoff);
            if (i == numIterations - 1) {
                break;
            }
            time.sleep(delay + 1);
        }
    }

    @Test
    public void testClientDisconnectAfterInternalApiVersionRequest() throws Exception {
        awaitInFlightApiVersionRequest();
        client.disconnect(node.idString());
        assertFalse(client.hasInFlightRequests(node.idString()));

        // The failed ApiVersion request should not be forwarded to upper layers
        List<ClientResponse> responses = client.poll(0, time.milliseconds());
        assertTrue(responses.isEmpty());
    }

    @Test
    public void testDisconnectWithMultipleInFlights() {
        NetworkClient client = this.clientWithNoVersionDiscovery;
        awaitReady(client, node);
        assertTrue(client.isReady(node, time.milliseconds()),
            "Expected NetworkClient to be ready to send to node " + node.idString());

        MetadataRequest.Builder builder = new MetadataRequest.Builder(Collections.emptyList(), true);
        long now = time.milliseconds();

        final List<ClientResponse> callbackResponses = new ArrayList<>();
        RequestCompletionHandler callback = callbackResponses::add;

        ClientRequest request1 = client.newClientRequest(node.idString(), builder, now, true, defaultRequestTimeoutMs, callback);
        client.send(request1, now);
        client.poll(0, now);

        ClientRequest request2 = client.newClientRequest(node.idString(), builder, now, true, defaultRequestTimeoutMs, callback);
        client.send(request2, now);
        client.poll(0, now);

        assertNotEquals(request1.correlationId(), request2.correlationId());

        assertEquals(2, client.inFlightRequestCount());
        assertEquals(2, client.inFlightRequestCount(node.idString()));

        client.disconnect(node.idString());

        List<ClientResponse> responses = client.poll(0, time.milliseconds());
        assertEquals(2, responses.size());
        assertEquals(responses, callbackResponses);
        assertEquals(0, client.inFlightRequestCount());
        assertEquals(0, client.inFlightRequestCount(node.idString()));

        // Ensure that the responses are returned in the order they were sent
        ClientResponse response1 = responses.get(0);
        assertTrue(response1.wasDisconnected());
        assertEquals(request1.correlationId(), response1.requestHeader().correlationId());

        ClientResponse response2 = responses.get(1);
        assertTrue(response2.wasDisconnected());
        assertEquals(request2.correlationId(), response2.requestHeader().correlationId());
    }

    @Test
    public void testCallDisconnect() {
        awaitReady(client, node);
        assertTrue(client.isReady(node, time.milliseconds()),
            "Expected NetworkClient to be ready to send to node " + node.idString());
        assertFalse(client.connectionFailed(node),
            "Did not expect connection to node " + node.idString() + " to be failed");
        client.disconnect(node.idString());
        assertFalse(client.isReady(node, time.milliseconds()),
            "Expected node " + node.idString() + " to be disconnected.");
        assertTrue(client.connectionFailed(node),
            "Expected connection to node " + node.idString() + " to be failed after disconnect");
        assertFalse(client.canConnect(node, time.milliseconds()));

        // ensure disconnect does not reset backoff period if already disconnected
        time.sleep(reconnectBackoffMaxMsTest);
        assertTrue(client.canConnect(node, time.milliseconds()));
        client.disconnect(node.idString());
        assertTrue(client.canConnect(node, time.milliseconds()));
    }

    @Test
    public void testCorrelationId() {
        int count = 100;
        Set<Integer> ids = IntStream.range(0, count)
            .mapToObj(i -> client.nextCorrelationId())
            .collect(Collectors.toSet());
        assertEquals(count, ids.size());
        ids.forEach(id -> assertTrue(id < SaslClientAuthenticator.MIN_RESERVED_CORRELATION_ID));
    }

    @Test
    public void testReconnectAfterAddressChange() {
        AddressChangeHostResolver mockHostResolver = new AddressChangeHostResolver(
                initialAddresses.toArray(new InetAddress[0]), newAddresses.toArray(new InetAddress[0]));
        AtomicInteger initialAddressConns = new AtomicInteger();
        AtomicInteger newAddressConns = new AtomicInteger();
        MockSelector selector = new MockSelector(this.time, inetSocketAddress -> {
            InetAddress inetAddress = inetSocketAddress.getAddress();
            if (initialAddresses.contains(inetAddress)) {
                initialAddressConns.incrementAndGet();
            } else if (newAddresses.contains(inetAddress)) {
                newAddressConns.incrementAndGet();
            }
            return (mockHostResolver.useNewAddresses() && newAddresses.contains(inetAddress)) ||
                   (!mockHostResolver.useNewAddresses() && initialAddresses.contains(inetAddress));
        });

        ClientTelemetrySender mockClientTelemetrySender = mock(ClientTelemetrySender.class);
        when(mockClientTelemetrySender.timeToNextUpdate(anyLong())).thenReturn(0L);

        NetworkClient client = new NetworkClient(metadataUpdater, null, selector, "mock", Integer.MAX_VALUE,
                reconnectBackoffMsTest, reconnectBackoffMaxMsTest, 64 * 1024, 64 * 1024,
                defaultRequestTimeoutMs, connectionSetupTimeoutMsTest, connectionSetupTimeoutMaxMsTest,
                time, false, new ApiVersions(), null, new LogContext(), mockHostResolver, mockClientTelemetrySender,
                Long.MAX_VALUE, MetadataRecoveryStrategy.NONE);

        // Connect to one the initial addresses, then change the addresses and disconnect
        client.ready(node, time.milliseconds());
        time.sleep(connectionSetupTimeoutMaxMsTest);
        client.poll(0, time.milliseconds());
        assertTrue(client.isReady(node, time.milliseconds()));
        // First poll should try to update the node but couldn't because node remains in connecting state
        // i.e. connection handling is completed after telemetry update.
        assertNull(client.telemetryConnectedNode());

        client.poll(0, time.milliseconds());
        assertEquals(node, client.telemetryConnectedNode());

        mockHostResolver.changeAddresses();
        selector.serverDisconnect(node.idString());
        client.poll(0, time.milliseconds());
        assertFalse(client.isReady(node, time.milliseconds()));
        assertNull(client.telemetryConnectedNode());

        time.sleep(reconnectBackoffMaxMsTest);
        client.ready(node, time.milliseconds());
        time.sleep(connectionSetupTimeoutMaxMsTest);
        client.poll(0, time.milliseconds());
        assertTrue(client.isReady(node, time.milliseconds()));
        assertNull(client.telemetryConnectedNode());

        client.poll(0, time.milliseconds());
        assertEquals(node, client.telemetryConnectedNode());

        // We should have tried to connect to one initial address and one new address, and resolved DNS twice
        assertEquals(1, initialAddressConns.get());
        assertEquals(1, newAddressConns.get());
        assertEquals(2, mockHostResolver.resolutionCount());
        verify(mockClientTelemetrySender, times(5)).timeToNextUpdate(anyLong());
    }

    @Test
    public void testFailedConnectionToFirstAddress() {
        AddressChangeHostResolver mockHostResolver = new AddressChangeHostResolver(
                initialAddresses.toArray(new InetAddress[0]), newAddresses.toArray(new InetAddress[0]));
        AtomicInteger initialAddressConns = new AtomicInteger();
        AtomicInteger newAddressConns = new AtomicInteger();
        MockSelector selector = new MockSelector(this.time, inetSocketAddress -> {
            InetAddress inetAddress = inetSocketAddress.getAddress();
            if (initialAddresses.contains(inetAddress)) {
                initialAddressConns.incrementAndGet();
            } else if (newAddresses.contains(inetAddress)) {
                newAddressConns.incrementAndGet();
            }
            // Refuse first connection attempt
            return initialAddressConns.get() > 1;
        });

        ClientTelemetrySender mockClientTelemetrySender = mock(ClientTelemetrySender.class);
        when(mockClientTelemetrySender.timeToNextUpdate(anyLong())).thenReturn(0L);

        NetworkClient client = new NetworkClient(metadataUpdater, null, selector, "mock", Integer.MAX_VALUE,
                reconnectBackoffMsTest, reconnectBackoffMaxMsTest, 64 * 1024, 64 * 1024,
                defaultRequestTimeoutMs, connectionSetupTimeoutMsTest, connectionSetupTimeoutMaxMsTest,
                time, false, new ApiVersions(), null, new LogContext(), mockHostResolver, mockClientTelemetrySender,
                Long.MAX_VALUE, MetadataRecoveryStrategy.NONE);

        // First connection attempt should fail
        client.ready(node, time.milliseconds());
        time.sleep(connectionSetupTimeoutMaxMsTest);
        client.poll(0, time.milliseconds());
        assertFalse(client.isReady(node, time.milliseconds()));
        assertNull(client.telemetryConnectedNode());

        // Second connection attempt should succeed
        time.sleep(reconnectBackoffMaxMsTest);
        client.ready(node, time.milliseconds());
        time.sleep(connectionSetupTimeoutMaxMsTest);
        client.poll(0, time.milliseconds());
        assertTrue(client.isReady(node, time.milliseconds()));
        assertNull(client.telemetryConnectedNode());

        // Next client poll after handling connection setup should update telemetry node.
        client.poll(0, time.milliseconds());
        assertEquals(node, client.telemetryConnectedNode());

        // We should have tried to connect to two of the initial addresses, none of the new address, and should
        // only have resolved DNS once
        assertEquals(2, initialAddressConns.get());
        assertEquals(0, newAddressConns.get());
        assertEquals(1, mockHostResolver.resolutionCount());
        verify(mockClientTelemetrySender, times(3)).timeToNextUpdate(anyLong());
    }

    @Test
    public void testFailedConnectionToFirstAddressAfterReconnect() {
        AddressChangeHostResolver mockHostResolver = new AddressChangeHostResolver(
                initialAddresses.toArray(new InetAddress[0]), newAddresses.toArray(new InetAddress[0]));
        AtomicInteger initialAddressConns = new AtomicInteger();
        AtomicInteger newAddressConns = new AtomicInteger();
        MockSelector selector = new MockSelector(this.time, inetSocketAddress -> {
            InetAddress inetAddress = inetSocketAddress.getAddress();
            if (initialAddresses.contains(inetAddress)) {
                initialAddressConns.incrementAndGet();
            } else if (newAddresses.contains(inetAddress)) {
                newAddressConns.incrementAndGet();
            }
            // Refuse first connection attempt to the new addresses
            return initialAddresses.contains(inetAddress) || newAddressConns.get() > 1;
        });

        ClientTelemetrySender mockClientTelemetrySender = mock(ClientTelemetrySender.class);
        when(mockClientTelemetrySender.timeToNextUpdate(anyLong())).thenReturn(0L);

        NetworkClient client = new NetworkClient(metadataUpdater, null, selector, "mock", Integer.MAX_VALUE,
                reconnectBackoffMsTest, reconnectBackoffMaxMsTest, 64 * 1024, 64 * 1024,
                defaultRequestTimeoutMs, connectionSetupTimeoutMsTest, connectionSetupTimeoutMaxMsTest,
                time, false, new ApiVersions(), null, new LogContext(), mockHostResolver, mockClientTelemetrySender,
                Long.MAX_VALUE, MetadataRecoveryStrategy.NONE);

        // Connect to one the initial addresses, then change the addresses and disconnect
        client.ready(node, time.milliseconds());
        time.sleep(connectionSetupTimeoutMaxMsTest);
        client.poll(0, time.milliseconds());
        assertTrue(client.isReady(node, time.milliseconds()));
        assertNull(client.telemetryConnectedNode());
        // Next client poll after handling connection setup should update telemetry node.
        client.poll(0, time.milliseconds());
        assertEquals(node, client.telemetryConnectedNode());

        mockHostResolver.changeAddresses();
        selector.serverDisconnect(node.idString());
        client.poll(0, time.milliseconds());
        assertFalse(client.isReady(node, time.milliseconds()));
        assertNull(client.telemetryConnectedNode());

        // First connection attempt to new addresses should fail
        time.sleep(reconnectBackoffMaxMsTest);
        client.ready(node, time.milliseconds());
        time.sleep(connectionSetupTimeoutMaxMsTest);
        client.poll(0, time.milliseconds());
        assertFalse(client.isReady(node, time.milliseconds()));
        assertNull(client.telemetryConnectedNode());

        // Second connection attempt to new addresses should succeed
        time.sleep(reconnectBackoffMaxMsTest);
        client.ready(node, time.milliseconds());
        time.sleep(connectionSetupTimeoutMaxMsTest);
        client.poll(0, time.milliseconds());
        assertTrue(client.isReady(node, time.milliseconds()));
        assertNull(client.telemetryConnectedNode());

        // Next client poll after handling connection setup should update telemetry node.
        client.poll(0, time.milliseconds());
        assertEquals(node, client.telemetryConnectedNode());

        // We should have tried to connect to one of the initial addresses and two of the new addresses (the first one
        // failed), and resolved DNS twice, once for each set of addresses
        assertEquals(1, initialAddressConns.get());
        assertEquals(2, newAddressConns.get());
        assertEquals(2, mockHostResolver.resolutionCount());
        verify(mockClientTelemetrySender, times(6)).timeToNextUpdate(anyLong());
    }

    @Test
    public void testCloseConnectingNode() {
        Cluster cluster = TestUtils.clusterWith(2);
        Node node0 = cluster.nodeById(0);
        Node node1 = cluster.nodeById(1);
        client.ready(node0, time.milliseconds());
        selector.serverConnectionBlocked(node0.idString());
        client.poll(1, time.milliseconds());
        client.close(node0.idString());

        // Poll without any connections should return without exceptions
        client.poll(0, time.milliseconds());
        assertFalse(NetworkClientUtils.isReady(client, node0, time.milliseconds()));
        assertFalse(NetworkClientUtils.isReady(client, node1, time.milliseconds()));

        // Connection to new node should work
        client.ready(node1, time.milliseconds());
        ByteBuffer buffer = RequestTestUtils.serializeResponseWithHeader(defaultApiVersionsResponse(), ApiKeys.API_VERSIONS.latestVersion(), 0);
        selector.delayedReceive(new DelayedReceive(node1.idString(), new NetworkReceive(node1.idString(), buffer)));
        while (!client.ready(node1, time.milliseconds()))
            client.poll(1, time.milliseconds());
        assertTrue(client.isReady(node1, time.milliseconds()));
        selector.clear();

        // New connection to node closed earlier should work
        client.ready(node0, time.milliseconds());
        buffer = RequestTestUtils.serializeResponseWithHeader(defaultApiVersionsResponse(), ApiKeys.API_VERSIONS.latestVersion(), 1);
        selector.delayedReceive(new DelayedReceive(node0.idString(), new NetworkReceive(node0.idString(), buffer)));
        while (!client.ready(node0, time.milliseconds()))
            client.poll(1, time.milliseconds());
        assertTrue(client.isReady(node0, time.milliseconds()));
    }

    @Test
    public void testConnectionDoesNotRemainStuckInCheckingApiVersionsStateIfChannelNeverBecomesReady() {
        final Cluster cluster = TestUtils.clusterWith(1);
        final Node node = cluster.nodeById(0);

        // Channel is ready by default so we mark it as not ready.
        client.ready(node, time.milliseconds());
        selector.channelNotReady(node.idString());

        // Channel should not be ready.
        client.poll(0, time.milliseconds());
        assertFalse(NetworkClientUtils.isReady(client, node, time.milliseconds()));

        // Connection should time out if the channel does not become ready within
        // the connection setup timeout. This ensures that the client does not remain
        // stuck in the CHECKING_API_VERSIONS state.
        time.sleep((long) (connectionSetupTimeoutMsTest * 1.2) + 1);
        client.poll(0, time.milliseconds());
        assertTrue(client.connectionFailed(node));
    }

    @Test
    public void testTelemetryRequest() {
        ClientTelemetrySender mockClientTelemetrySender = mock(ClientTelemetrySender.class);
        when(mockClientTelemetrySender.timeToNextUpdate(anyLong())).thenReturn(0L);

        NetworkClient client = new NetworkClient(metadataUpdater, null, selector, "mock", Integer.MAX_VALUE,
            reconnectBackoffMsTest, reconnectBackoffMaxMsTest, 64 * 1024, 64 * 1024,
            defaultRequestTimeoutMs, connectionSetupTimeoutMsTest, connectionSetupTimeoutMaxMsTest,
            time, true, new ApiVersions(), null, new LogContext(), new DefaultHostResolver(), mockClientTelemetrySender,
            Long.MAX_VALUE, MetadataRecoveryStrategy.NONE);

        // Send the ApiVersionsRequest
        client.ready(node, time.milliseconds());
        client.poll(0, time.milliseconds());
        assertNull(client.telemetryConnectedNode());
        assertTrue(client.hasInFlightRequests(node.idString()));
        delayedApiVersionsResponse(0, ApiKeys.API_VERSIONS.latestVersion(), TestUtils.defaultApiVersionsResponse(
            ApiMessageType.ListenerType.BROKER));
        // handle ApiVersionsResponse
        client.poll(0, time.milliseconds());
        // the ApiVersionsRequest is gone
        assertFalse(client.hasInFlightRequests(node.idString()));
        selector.clear();

        GetTelemetrySubscriptionsRequest.Builder getRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true);
        when(mockClientTelemetrySender.createRequest()).thenReturn(Optional.of(getRequest));

        GetTelemetrySubscriptionsResponse getResponse = new GetTelemetrySubscriptionsResponse(new GetTelemetrySubscriptionsResponseData());
        ByteBuffer buffer = RequestTestUtils.serializeResponseWithHeader(getResponse, ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS.latestVersion(), 1);
        selector.completeReceive(new NetworkReceive(node.idString(), buffer));

        // Initiate poll to send GetTelemetrySubscriptions request
        client.poll(0, time.milliseconds());
        assertTrue(client.isReady(node, time.milliseconds()));
        assertEquals(node, client.telemetryConnectedNode());
        verify(mockClientTelemetrySender, times(1)).handleResponse(any(GetTelemetrySubscriptionsResponse.class));
        selector.clear();

        PushTelemetryRequest.Builder pushRequest = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData(), true);
        when(mockClientTelemetrySender.createRequest()).thenReturn(Optional.of(pushRequest));

        PushTelemetryResponse pushResponse = new PushTelemetryResponse(new PushTelemetryResponseData());
        ByteBuffer pushBuffer = RequestTestUtils.serializeResponseWithHeader(pushResponse, ApiKeys.PUSH_TELEMETRY.latestVersion(), 2);
        selector.completeReceive(new NetworkReceive(node.idString(), pushBuffer));

        // Initiate poll to send PushTelemetry request
        client.poll(0, time.milliseconds());
        assertTrue(client.isReady(node, time.milliseconds()));
        assertEquals(node, client.telemetryConnectedNode());
        verify(mockClientTelemetrySender, times(1)).handleResponse(any(PushTelemetryResponse.class));
        verify(mockClientTelemetrySender, times(4)).timeToNextUpdate(anyLong());
        verify(mockClientTelemetrySender, times(2)).createRequest();
    }

    private RequestHeader parseHeader(ByteBuffer buffer) {
        buffer.getInt(); // skip size
        return RequestHeader.parse(buffer.slice());
    }

    private void awaitInFlightApiVersionRequest() throws Exception {
        client.ready(node, time.milliseconds());
        TestUtils.waitForCondition(() -> {
            client.poll(0, time.milliseconds());
            return client.hasInFlightRequests(node.idString());
        }, 1000, "");
        assertFalse(client.isReady(node, time.milliseconds()));
    }

    private ApiVersionsResponse defaultApiVersionsResponse() {
        return TestUtils.defaultApiVersionsResponse(ApiMessageType.ListenerType.ZK_BROKER);
    }

    private static class TestCallbackHandler implements RequestCompletionHandler {
        public boolean executed = false;
        public ClientResponse response;

        public void onComplete(ClientResponse response) {
            this.executed = true;
            this.response = response;
        }
    }

    // ManualMetadataUpdater with ability to keep track of failures
    private static class TestMetadataUpdater extends ManualMetadataUpdater {
        KafkaException failure;

        public TestMetadataUpdater(List<Node> nodes) {
            super(nodes);
        }

        @Override
        public void handleServerDisconnect(long now, String destinationId, Optional<AuthenticationException> maybeAuthException) {
            maybeAuthException.ifPresent(exception ->
                failure = exception
            );
            super.handleServerDisconnect(now, destinationId, maybeAuthException);
        }

        @Override
        public void handleFailedRequest(long now, Optional<KafkaException> maybeFatalException) {
            maybeFatalException.ifPresent(exception ->
                failure = exception
            );
        }

        public KafkaException getAndClearFailure() {
            KafkaException failure = this.failure;
            this.failure = null;
            return failure;
        }
    }
}
