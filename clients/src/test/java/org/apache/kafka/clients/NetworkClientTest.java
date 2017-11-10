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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.protocol.ApiKeys;
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
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NetworkClientTest {

    protected final int requestTimeoutMs = 1000;
    protected final MockTime time = new MockTime();
    protected final MockSelector selector = new MockSelector(time);
    protected final Metadata metadata = new Metadata(0, Long.MAX_VALUE, true);
    protected final int nodeId = 1;
    protected final Cluster cluster = TestUtils.singletonCluster("test", nodeId);
    protected final Node node = cluster.nodes().get(0);
    protected final long reconnectBackoffMsTest = 10 * 1000;
    protected final long reconnectBackoffMaxMsTest = 10 * 10000;

    private final NetworkClient client = createNetworkClient(reconnectBackoffMaxMsTest);
    private final NetworkClient clientWithNoExponentialBackoff = createNetworkClient(reconnectBackoffMsTest);
    private final NetworkClient clientWithStaticNodes = createNetworkClientWithStaticNodes();
    private final NetworkClient clientWithNoVersionDiscovery = createNetworkClientWithNoVersionDiscovery();

    private NetworkClient createNetworkClient(long reconnectBackoffMaxMs) {
        return new NetworkClient(selector, metadata, "mock", Integer.MAX_VALUE,
                reconnectBackoffMsTest, reconnectBackoffMaxMs, 64 * 1024, 64 * 1024,
                requestTimeoutMs, time, true, new ApiVersions(), new LogContext());
    }

    private NetworkClient createNetworkClientWithStaticNodes() {
        return new NetworkClient(selector, new ManualMetadataUpdater(Arrays.asList(node)),
                "mock-static", Integer.MAX_VALUE, 0, 0, 64 * 1024, 64 * 1024, requestTimeoutMs,
                time, true, new ApiVersions(), new LogContext());
    }

    private NetworkClient createNetworkClientWithNoVersionDiscovery() {
        return new NetworkClient(selector, metadata, "mock", Integer.MAX_VALUE,
                reconnectBackoffMsTest, reconnectBackoffMaxMsTest,
                64 * 1024, 64 * 1024, requestTimeoutMs, time, false, new ApiVersions(), new LogContext());
    }

    @Before
    public void setup() {
        metadata.update(cluster, Collections.<String>emptySet(), time.milliseconds());
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

    private void checkSimpleRequestResponse(NetworkClient networkClient) {
        awaitReady(networkClient, node); // has to be before creating any request, as it may send ApiVersionsRequest and its response is mocked with correlation id 0
        ProduceRequest.Builder builder = ProduceRequest.Builder.forCurrentMagic((short) 1, 1000,
                        Collections.<TopicPartition, MemoryRecords>emptyMap());
        TestCallbackHandler handler = new TestCallbackHandler();
        ClientRequest request = networkClient.newClientRequest(
                node.idString(), builder, time.milliseconds(), true, handler);
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

    private void maybeSetExpectedApiVersionsResponse() {
        ApiVersionsResponse response = ApiVersionsResponse.defaultApiVersionsResponse();
        short apiVersionsResponseVersion = response.apiVersion(ApiKeys.API_VERSIONS.id).maxVersion;
        ByteBuffer buffer = response.serialize(apiVersionsResponseVersion, new ResponseHeader(0));
        selector.delayedReceive(new DelayedReceive(node.idString(), new NetworkReceive(node.idString(), buffer)));
    }

    private void awaitReady(NetworkClient client, Node node) {
        if (client.discoverBrokerVersions()) {
            maybeSetExpectedApiVersionsResponse();
        }
        while (!client.ready(node, time.milliseconds()))
            client.poll(1, time.milliseconds());
        selector.clear();
    }

    @Test
    public void testRequestTimeout() {
        awaitReady(client, node); // has to be before creating any request, as it may send ApiVersionsRequest and its response is mocked with correlation id 0
        ProduceRequest.Builder builder = ProduceRequest.Builder.forCurrentMagic((short) 1,
                1000, Collections.<TopicPartition, MemoryRecords>emptyMap());
        TestCallbackHandler handler = new TestCallbackHandler();
        long now = time.milliseconds();
        ClientRequest request = client.newClientRequest(
                node.idString(), builder, now, true, handler);
        client.send(request, now);
        // sleeping to make sure that the time since last send is greater than requestTimeOut
        time.sleep(3000);
        client.poll(3000, time.milliseconds());
        assertEquals(1, selector.disconnected().size());
        assertTrue("Node not found in disconnected map", selector.disconnected().containsKey(node.idString()));
    }

    @Test
    public void testLeastLoadedNode() {
        client.ready(node, time.milliseconds());
        awaitReady(client, node);
        client.poll(1, time.milliseconds());
        assertTrue("The client should be ready", client.isReady(node, time.milliseconds()));
        
        // leastloadednode should be our single node
        Node leastNode = client.leastLoadedNode(time.milliseconds());
        assertEquals("There should be one leastloadednode", leastNode.id(), node.id());
        
        // sleep for longer than reconnect backoff
        time.sleep(reconnectBackoffMsTest);
        
        // CLOSE node 
        selector.close(node.idString());
        
        client.poll(1, time.milliseconds());
        assertFalse("After we forced the disconnection the client is no longer ready.", client.ready(node, time.milliseconds()));
        leastNode = client.leastLoadedNode(time.milliseconds());
        assertEquals("There should be NO leastloadednode", leastNode, null);
        
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

        selector.close(node.idString());
        clientWithNoExponentialBackoff.poll(requestTimeoutMs, time.milliseconds());
        long delay = clientWithNoExponentialBackoff.connectionDelay(node, time.milliseconds());

        assertEquals(reconnectBackoffMsTest, delay);

        // Sleep until there is no connection delay
        time.sleep(delay);
        assertEquals(0, clientWithNoExponentialBackoff.connectionDelay(node, time.milliseconds()));

        // Start connecting and disconnect before the connection is established
        client.ready(node, time.milliseconds());
        selector.close(node.idString());
        client.poll(requestTimeoutMs, time.milliseconds());

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
        selector.close(node.idString());
        client.poll(requestTimeoutMs, time.milliseconds());
        long delay = client.connectionDelay(node, time.milliseconds());
        long expectedDelay = reconnectBackoffMsTest;
        double jitter = 0.2;
        assertEquals(expectedDelay, delay, expectedDelay * jitter);

        // Sleep until there is no connection delay
        time.sleep(delay);
        assertEquals(0, client.connectionDelay(node, time.milliseconds()));

        // Start connecting and disconnect before the connection is established
        client.ready(node, time.milliseconds());
        selector.close(node.idString());
        client.poll(requestTimeoutMs, time.milliseconds());

        // Second attempt should take twice as long with twice the jitter
        expectedDelay = Math.round(delay * 2);
        delay = client.connectionDelay(node, time.milliseconds());
        jitter = 0.4;
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
        client.poll(requestTimeoutMs, now);
        assertEquals(1, client.inFlightRequestCount(node.idString()));
        assertTrue(client.hasInFlightRequests(node.idString()));
        assertTrue(client.hasInFlightRequests());

        selector.close(node.idString());
        List<ClientResponse> responses = client.poll(requestTimeoutMs, time.milliseconds());
        assertEquals(1, responses.size());
        assertTrue(responses.iterator().next().wasDisconnected());
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

    private static class TestCallbackHandler implements RequestCompletionHandler {
        public boolean executed = false;
        public ClientResponse response;

        public void onComplete(ClientResponse response) {
            this.executed = true;
            this.response = response;
        }
    }
}
