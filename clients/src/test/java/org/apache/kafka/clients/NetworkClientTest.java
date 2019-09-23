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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.CommonFields;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.DelayedReceive;
import org.apache.kafka.test.MockSelector;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NetworkClientTest {

    protected final int defaultRequestTimeoutMs = 1000;
    protected final MockTime time = new MockTime();
    protected final MockSelector selector = new MockSelector(time);
    protected final Node node = TestUtils.singletonCluster().nodes().iterator().next();
    protected final long reconnectBackoffMsTest = 10 * 1000;
    protected final long reconnectBackoffMaxMsTest = 10 * 10000;

    private final TestMetadataUpdater metadataUpdater = new TestMetadataUpdater(Collections.singletonList(node));
    private final NetworkClient client = createNetworkClient(reconnectBackoffMaxMsTest);
    private final NetworkClient clientWithNoExponentialBackoff = createNetworkClient(reconnectBackoffMsTest);
    private final NetworkClient clientWithStaticNodes = createNetworkClientWithStaticNodes();
    private final NetworkClient clientWithNoVersionDiscovery = createNetworkClientWithNoVersionDiscovery();

    private NetworkClient createNetworkClient(long reconnectBackoffMaxMs) {
        return new NetworkClient(selector, metadataUpdater, "mock", Integer.MAX_VALUE,
                reconnectBackoffMsTest, reconnectBackoffMaxMs, 64 * 1024, 64 * 1024,
                defaultRequestTimeoutMs, ClientDnsLookup.DEFAULT, time, true, new ApiVersions(), new LogContext());
    }

    private NetworkClient createNetworkClientWithStaticNodes() {
        return new NetworkClient(selector, metadataUpdater,
                "mock-static", Integer.MAX_VALUE, 0, 0, 64 * 1024, 64 * 1024, defaultRequestTimeoutMs,
                ClientDnsLookup.DEFAULT, time, true, new ApiVersions(), new LogContext());
    }

    private NetworkClient createNetworkClientWithNoVersionDiscovery() {
        return new NetworkClient(selector, metadataUpdater, "mock", Integer.MAX_VALUE,
                reconnectBackoffMsTest, reconnectBackoffMaxMsTest,
                64 * 1024, 64 * 1024, defaultRequestTimeoutMs,
                ClientDnsLookup.DEFAULT, time, false, new ApiVersions(), new LogContext());
    }

    @Before
    public void setup() {
        selector.reset();
    }

    @Test(expected = IllegalStateException.class)
    public void testSendToUnreadyNode() {
        MetadataRequest.Builder builder = new MetadataRequest.Builder(Arrays.asList("test"), true);
        long now = time.milliseconds();
        ClientRequest request = client.newClientRequest("5", builder, now, false);
        client.send(request, now);
        client.poll(1, time.milliseconds());
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
        assertTrue("The client should be ready", client.isReady(node, time.milliseconds()));

        ProduceRequest.Builder builder = ProduceRequest.Builder.forCurrentMagic((short) 1, 1000,
                Collections.<TopicPartition, MemoryRecords>emptyMap());
        ClientRequest request = client.newClientRequest(node.idString(), builder, time.milliseconds(), true);
        client.send(request, time.milliseconds());
        assertEquals("There should be 1 in-flight request after send", 1,
                client.inFlightRequestCount(node.idString()));
        assertTrue(client.hasInFlightRequests(node.idString()));
        assertTrue(client.hasInFlightRequests());

        client.close(node.idString());
        assertEquals("There should be no in-flight request after close", 0, client.inFlightRequestCount(node.idString()));
        assertFalse(client.hasInFlightRequests(node.idString()));
        assertFalse(client.hasInFlightRequests());
        assertFalse("Connection should not be ready after close", client.isReady(node, 0));
    }

    @Test
    public void testUnsupportedVersionDuringInternalMetadataRequest() {
        List<String> topics = Arrays.asList("topic_1");

        // disabling auto topic creation for versions less than 4 is not supported
        MetadataRequest.Builder builder = new MetadataRequest.Builder(topics, false, (short) 3);
        client.sendInternalMetadataRequest(builder, node.idString(), time.milliseconds());
        assertEquals(UnsupportedVersionException.class, metadataUpdater.getAndClearFailure().getClass());
    }

    private void checkSimpleRequestResponse(NetworkClient networkClient) {
        awaitReady(networkClient, node); // has to be before creating any request, as it may send ApiVersionsRequest and its response is mocked with correlation id 0
        ProduceRequest.Builder builder = ProduceRequest.Builder.forCurrentMagic((short) 1, 1000,
                        Collections.emptyMap());
        TestCallbackHandler handler = new TestCallbackHandler();
        ClientRequest request = networkClient.newClientRequest(
                node.idString(), builder, time.milliseconds(), true, defaultRequestTimeoutMs, handler);
        networkClient.send(request, time.milliseconds());
        networkClient.poll(1, time.milliseconds());
        assertEquals(1, networkClient.inFlightRequestCount());
        ResponseHeader respHeader = new ResponseHeader(request.correlationId());
        Struct resp = new Struct(ApiKeys.PRODUCE.responseSchema(ApiKeys.PRODUCE.latestVersion()));
        resp.set("responses", new Object[0]);
        Struct responseHeaderStruct = respHeader.toStruct();
        int size = responseHeaderStruct.sizeOf() + resp.sizeOf();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        responseHeaderStruct.writeTo(buffer);
        resp.writeTo(buffer);
        buffer.flip();
        selector.completeReceive(new NetworkReceive(node.idString(), buffer));
        List<ClientResponse> responses = networkClient.poll(1, time.milliseconds());
        assertEquals(1, responses.size());
        assertTrue("The handler should have executed.", handler.executed);
        assertTrue("Should have a response body.", handler.response.hasResponse());
        assertEquals("Should be correlated to the original request",
                request.correlationId(), handler.response.requestHeader().correlationId());
    }

    private void setExpectedApiVersionsResponse(ApiVersionsResponse response) {
        short apiVersionsResponseVersion = response.apiVersion(ApiKeys.API_VERSIONS.id).maxVersion;
        ByteBuffer buffer = response.serialize(apiVersionsResponseVersion, new ResponseHeader(0));
        selector.delayedReceive(new DelayedReceive(node.idString(), new NetworkReceive(node.idString(), buffer)));
    }

    private void awaitReady(NetworkClient client, Node node) {
        if (client.discoverBrokerVersions()) {
            setExpectedApiVersionsResponse(ApiVersionsResponse.defaultApiVersionsResponse());
        }
        while (!client.ready(node, time.milliseconds()))
            client.poll(1, time.milliseconds());
        selector.clear();
    }

    @Test
    public void testRequestTimeout() {
        awaitReady(client, node); // has to be before creating any request, as it may send ApiVersionsRequest and its response is mocked with correlation id 0
        ProduceRequest.Builder builder = ProduceRequest.Builder.forCurrentMagic((short) 1,
                1000, Collections.emptyMap());
        TestCallbackHandler handler = new TestCallbackHandler();
        int requestTimeoutMs = defaultRequestTimeoutMs + 5000;
        ClientRequest request = client.newClientRequest(node.idString(), builder, time.milliseconds(), true,
                requestTimeoutMs, handler);
        assertEquals(requestTimeoutMs, request.requestTimeoutMs());
        testRequestTimeout(request);
    }

    @Test
    public void testDefaultRequestTimeout() {
        awaitReady(client, node); // has to be before creating any request, as it may send ApiVersionsRequest and its response is mocked with correlation id 0
        ProduceRequest.Builder builder = ProduceRequest.Builder.forCurrentMagic((short) 1,
                1000, Collections.emptyMap());
        ClientRequest request = client.newClientRequest(node.idString(), builder, time.milliseconds(), true);
        assertEquals(defaultRequestTimeoutMs, request.requestTimeoutMs());
        testRequestTimeout(request);
    }

    private void testRequestTimeout(ClientRequest request) {
        client.send(request, time.milliseconds());

        time.sleep(request.requestTimeoutMs() + 1);
        List<ClientResponse> responses = client.poll(0, time.milliseconds());

        assertEquals(1, responses.size());
        ClientResponse clientResponse = responses.get(0);
        assertEquals(node.idString(), clientResponse.destination());
        assertTrue("Expected response to fail due to disconnection", clientResponse.wasDisconnected());
    }

    @Test
    public void testConnectionThrottling() {
        // Instrument the test to return a response with a 100ms throttle delay.
        awaitReady(client, node);
        ProduceRequest.Builder builder = ProduceRequest.Builder.forCurrentMagic((short) 1, 1000,
            Collections.emptyMap());
        TestCallbackHandler handler = new TestCallbackHandler();
        ClientRequest request = client.newClientRequest(node.idString(), builder, time.milliseconds(), true,
                defaultRequestTimeoutMs, handler);
        client.send(request, time.milliseconds());
        client.poll(1, time.milliseconds());
        ResponseHeader respHeader = new ResponseHeader(request.correlationId());
        Struct resp = new Struct(ApiKeys.PRODUCE.responseSchema(ApiKeys.PRODUCE.latestVersion()));
        resp.set("responses", new Object[0]);
        resp.set(CommonFields.THROTTLE_TIME_MS, 100);
        Struct responseHeaderStruct = respHeader.toStruct();
        int size = responseHeaderStruct.sizeOf() + resp.sizeOf();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        responseHeaderStruct.writeTo(buffer);
        resp.writeTo(buffer);
        buffer.flip();
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
        List<ApiVersionsResponse.ApiVersion> versionList = new ArrayList<>();
        for (ApiKeys apiKey : ApiKeys.values()) {
            if (apiKey == key) {
                versionList.add(new ApiVersionsResponse.ApiVersion(apiKey, (short) 0, maxVersion));
            } else {
                versionList.add(new ApiVersionsResponse.ApiVersion(apiKey));
            }
        }
        return new ApiVersionsResponse(0, Errors.NONE, versionList);
    }

    @Test
    public void testThrottlingNotEnabledForConnectionToOlderBroker() {
        // Instrument the test so that the max protocol version for PRODUCE returned from the node is 5 and thus
        // client-side throttling is not enabled. Also, return a response with a 100ms throttle delay.
        setExpectedApiVersionsResponse(createExpectedApiVersionsResponse(ApiKeys.PRODUCE, (short) 5));
        while (!client.ready(node, time.milliseconds()))
            client.poll(1, time.milliseconds());
        selector.clear();

        int correlationId = sendEmptyProduceRequest();
        client.poll(1, time.milliseconds());

        sendThrottledProduceResponse(correlationId, 100);
        client.poll(1, time.milliseconds());

        // Since client-side throttling is disabled, the connection is ready even though the response indicated a
        // throttle delay.
        assertTrue(client.ready(node, time.milliseconds()));
        assertEquals(0, client.throttleDelayMs(node, time.milliseconds()));
    }

    private int sendEmptyProduceRequest() {
        ProduceRequest.Builder builder = ProduceRequest.Builder.forCurrentMagic((short) 1, 1000,
                Collections.emptyMap());
        TestCallbackHandler handler = new TestCallbackHandler();
        ClientRequest request = client.newClientRequest(node.idString(), builder, time.milliseconds(), true,
                defaultRequestTimeoutMs, handler);
        client.send(request, time.milliseconds());
        return request.correlationId();
    }


    private void sendResponse(int correlationId, Struct response) {
        ResponseHeader respHeader = new ResponseHeader(correlationId);
        Struct responseHeaderStruct = respHeader.toStruct();
        int size = responseHeaderStruct.sizeOf() + response.sizeOf();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        responseHeaderStruct.writeTo(buffer);
        response.writeTo(buffer);
        buffer.flip();
        selector.completeReceive(new NetworkReceive(node.idString(), buffer));
    }

    private void sendThrottledProduceResponse(int correlationId, int throttleMs) {
        Struct resp = new Struct(ApiKeys.PRODUCE.responseSchema(ApiKeys.PRODUCE.latestVersion()));
        resp.set("responses", new Object[0]);
        resp.set(CommonFields.THROTTLE_TIME_MS, throttleMs);
        sendResponse(correlationId, resp);
    }

    @Test
    public void testLeastLoadedNode() {
        client.ready(node, time.milliseconds());
        assertFalse(client.isReady(node, time.milliseconds()));
        assertEquals(node, client.leastLoadedNode(time.milliseconds()));

        awaitReady(client, node);
        client.poll(1, time.milliseconds());
        assertTrue("The client should be ready", client.isReady(node, time.milliseconds()));

        // leastloadednode should be our single node
        Node leastNode = client.leastLoadedNode(time.milliseconds());
        assertEquals("There should be one leastloadednode", leastNode.id(), node.id());

        // sleep for longer than reconnect backoff
        time.sleep(reconnectBackoffMsTest);

        // CLOSE node
        selector.serverDisconnect(node.idString());

        client.poll(1, time.milliseconds());
        assertFalse("After we forced the disconnection the client is no longer ready.", client.ready(node, time.milliseconds()));
        leastNode = client.leastLoadedNode(time.milliseconds());
        assertNull("There should be NO leastloadednode", leastNode);
    }

    @Test
    public void testLeastLoadedNodeConsidersThrottledConnections() {
        client.ready(node, time.milliseconds());
        awaitReady(client, node);
        client.poll(1, time.milliseconds());
        assertTrue("The client should be ready", client.isReady(node, time.milliseconds()));

        int correlationId = sendEmptyProduceRequest();
        client.poll(1, time.milliseconds());

        sendThrottledProduceResponse(correlationId, 100);
        client.poll(1, time.milliseconds());

        // leastloadednode should return null since the node is throttled
        assertNull(client.leastLoadedNode(time.milliseconds()));
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

        MetadataRequest.Builder builder = new MetadataRequest.Builder(Collections.<String>emptyList(), true);
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
        awaitInFlightApiVersionRequest();
        selector.serverDisconnect(node.idString());

        // The failed ApiVersion request should not be forwarded to upper layers
        List<ClientResponse> responses = client.poll(0, time.milliseconds());
        assertFalse(client.hasInFlightRequests(node.idString()));
        assertTrue(responses.isEmpty());
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
        assertTrue("Expected NetworkClient to be ready to send to node " + node.idString(),
                client.isReady(node, time.milliseconds()));

        MetadataRequest.Builder builder = new MetadataRequest.Builder(Collections.<String>emptyList(), true);
        long now = time.milliseconds();

        final List<ClientResponse> callbackResponses = new ArrayList<>();
        RequestCompletionHandler callback = new RequestCompletionHandler() {
            @Override
            public void onComplete(ClientResponse response) {
                callbackResponses.add(response);
            }
        };

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
    public void testCallDisconnect() throws Exception {
        awaitReady(client, node);
        assertTrue("Expected NetworkClient to be ready to send to node " + node.idString(),
            client.isReady(node, time.milliseconds()));
        assertFalse("Did not expect connection to node " + node.idString() + " to be failed",
            client.connectionFailed(node));
        client.disconnect(node.idString());
        assertFalse("Expected node " + node.idString() + " to be disconnected.",
            client.isReady(node, time.milliseconds()));
        assertTrue("Expected connection to node " + node.idString() + " to be failed after disconnect",
            client.connectionFailed(node));
        assertFalse(client.canConnect(node, time.milliseconds()));

        // ensure disconnect does not reset blackout period if already disconnected
        time.sleep(reconnectBackoffMaxMsTest);
        assertTrue(client.canConnect(node, time.milliseconds()));
        client.disconnect(node.idString());
        assertTrue(client.canConnect(node, time.milliseconds()));
    }

    private void awaitInFlightApiVersionRequest() throws Exception {
        client.ready(node, time.milliseconds());
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                client.poll(0, time.milliseconds());
                return client.hasInFlightRequests(node.idString());
            }
        }, 1000, "");
        assertFalse(client.isReady(node, time.milliseconds()));
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
        public void handleFatalException(KafkaException exception) {
            failure = exception;
            super.handleFatalException(exception);
        }

        public KafkaException getAndClearFailure() {
            KafkaException failure = this.failure;
            this.failure = null;
            return failure;
        }
    }
}
