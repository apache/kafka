/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.MockSelector;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class NetworkClientTest {

    private final int requestTimeoutMs = 1000;
    private MockTime time = new MockTime();
    private MockSelector selector = new MockSelector(time);
    private Metadata metadata = new Metadata(0, Long.MAX_VALUE);
    private int nodeId = 1;
    private Cluster cluster = TestUtils.singletonCluster("test", nodeId);
    private Node node = cluster.nodes().get(0);
    private long reconnectBackoffMsTest = 10 * 1000;
    private NetworkClient client = new NetworkClient(selector, metadata, "mock", Integer.MAX_VALUE, reconnectBackoffMsTest, 
            64 * 1024, 64 * 1024, requestTimeoutMs, time);
    
    private NetworkClient clientWithStaticNodes = new NetworkClient(selector, new ManualMetadataUpdater(Arrays.asList(node)),
            "mock-static", Integer.MAX_VALUE, 0, 64 * 1024, 64 * 1024, requestTimeoutMs, time);

    @Before
    public void setup() {
        metadata.update(cluster, time.milliseconds());
    }

    @Test(expected = IllegalStateException.class)
    public void testSendToUnreadyNode() {
        RequestSend send = new RequestSend("5",
                                           client.nextRequestHeader(ApiKeys.METADATA),
                                           new MetadataRequest(Arrays.asList("test")).toStruct());
        ClientRequest request = new ClientRequest(time.milliseconds(), false, send, null);
        client.send(request, time.milliseconds());
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
    public void testClose() {
        client.ready(node, time.milliseconds());
        awaitReady(client, node);
        client.poll(1, time.milliseconds());
        assertTrue("The client should be ready", client.isReady(node, time.milliseconds()));

        ProduceRequest produceRequest = new ProduceRequest((short) 1, 1000, Collections.<TopicPartition, ByteBuffer>emptyMap());
        RequestHeader reqHeader = client.nextRequestHeader(ApiKeys.PRODUCE);
        RequestSend send = new RequestSend(node.idString(), reqHeader, produceRequest.toStruct());
        ClientRequest request = new ClientRequest(time.milliseconds(), true, send, null);
        client.send(request, time.milliseconds());
        assertEquals("There should be 1 in-flight request after send", 1, client.inFlightRequestCount(node.idString()));

        client.close(node.idString());
        assertEquals("There should be no in-flight request after close", 0, client.inFlightRequestCount(node.idString()));
        assertFalse("Connection should not be ready after close", client.isReady(node, 0));
    }

    private void checkSimpleRequestResponse(NetworkClient networkClient) {
        ProduceRequest produceRequest = new ProduceRequest((short) 1, 1000, Collections.<TopicPartition, ByteBuffer>emptyMap());
        RequestHeader reqHeader = networkClient.nextRequestHeader(ApiKeys.PRODUCE);
        RequestSend send = new RequestSend(node.idString(), reqHeader, produceRequest.toStruct());
        TestCallbackHandler handler = new TestCallbackHandler();
        ClientRequest request = new ClientRequest(time.milliseconds(), true, send, handler);
        awaitReady(networkClient, node);
        networkClient.send(request, time.milliseconds());
        networkClient.poll(1, time.milliseconds());
        assertEquals(1, networkClient.inFlightRequestCount());
        ResponseHeader respHeader = new ResponseHeader(reqHeader.correlationId());
        Struct resp = new Struct(ProtoUtils.currentResponseSchema(ApiKeys.PRODUCE.id));
        resp.set("responses", new Object[0]);
        int size = respHeader.sizeOf() + resp.sizeOf();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        respHeader.writeTo(buffer);
        resp.writeTo(buffer);
        buffer.flip();
        selector.completeReceive(new NetworkReceive(node.idString(), buffer));
        List<ClientResponse> responses = networkClient.poll(1, time.milliseconds());
        assertEquals(1, responses.size());
        assertTrue("The handler should have executed.", handler.executed);
        assertTrue("Should have a response body.", handler.response.hasResponse());
        assertEquals("Should be correlated to the original request", request, handler.response.request());
    }

    private void awaitReady(NetworkClient client, Node node) {
        while (!client.ready(node, time.milliseconds()))
            client.poll(1, time.milliseconds());
    }

    @Test
    public void testRequestTimeout() {
        ProduceRequest produceRequest = new ProduceRequest((short) 1, 1000, Collections.<TopicPartition, ByteBuffer>emptyMap());
        RequestHeader reqHeader = client.nextRequestHeader(ApiKeys.PRODUCE);
        RequestSend send = new RequestSend(node.idString(), reqHeader, produceRequest.toStruct());
        TestCallbackHandler handler = new TestCallbackHandler();
        ClientRequest request = new ClientRequest(time.milliseconds(), true, send, handler);
        awaitReady(client, node);
        long now = time.milliseconds();
        client.send(request, now);
        // sleeping to make sure that the time since last send is greater than requestTimeOut
        time.sleep(3000);
        client.poll(3000, time.milliseconds());
        String disconnectedNode = selector.disconnected().get(0);
        assertEquals(node.idString(), disconnectedNode);
    }

    @Test
    public void testLeastLoadedNode() {
        Node leastNode = null;
        client.ready(node, time.milliseconds());
        awaitReady(client, node);
        client.poll(1, time.milliseconds());
        assertTrue("The client should be ready", client.isReady(node, time.milliseconds()));
        
        // leastloadednode should be our single node
        leastNode = client.leastLoadedNode(time.milliseconds());
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

        selector.close(node.idString());
        client.poll(requestTimeoutMs, time.milliseconds());
        long delay = client.connectionDelay(node, time.milliseconds());

        assertEquals(reconnectBackoffMsTest, delay);
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
