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

    private MockTime time = new MockTime();
    private MockSelector selector = new MockSelector(time);
    private Metadata metadata = new Metadata(0, Long.MAX_VALUE);
    private int nodeId = 1;
    private Cluster cluster = TestUtils.singletonCluster("test", nodeId);
    private Node node = cluster.nodes().get(0);
    private NetworkClient client = new NetworkClient(selector, metadata, "mock", Integer.MAX_VALUE, 0, 64 * 1024, 64 * 1024);

    @Before
    public void setup() {
        metadata.update(cluster, time.milliseconds());
    }

    @Test
    public void testReadyAndDisconnect() {
        assertFalse("Client begins unready as it has no connection.", client.ready(node, time.milliseconds()));
        assertEquals("The connection is established as a side-effect of the readiness check", 1, selector.connected().size());
        client.poll(1, time.milliseconds());
        selector.clear();
        assertTrue("Now the client is ready", client.ready(node, time.milliseconds()));
        selector.disconnect(node.id());
        client.poll(1, time.milliseconds());
        selector.clear();
        assertFalse("After we forced the disconnection the client is no longer ready.", client.ready(node, time.milliseconds()));
        assertTrue("Metadata should get updated.", metadata.timeToNextUpdate(time.milliseconds()) == 0);
    }

    @Test(expected = IllegalStateException.class)
    public void testSendToUnreadyNode() {
        RequestSend send = new RequestSend(5,
                                           client.nextRequestHeader(ApiKeys.METADATA),
                                           new MetadataRequest(Arrays.asList("test")).toStruct());
        ClientRequest request = new ClientRequest(time.milliseconds(), false, send, null);
        client.send(request);
        client.poll(1, time.milliseconds());
    }

    @Test
    public void testSimpleRequestResponse() {
        ProduceRequest produceRequest = new ProduceRequest((short) 1, 1000, Collections.<TopicPartition, ByteBuffer>emptyMap());
        RequestHeader reqHeader = client.nextRequestHeader(ApiKeys.PRODUCE);
        RequestSend send = new RequestSend(node.id(), reqHeader, produceRequest.toStruct());
        TestCallbackHandler handler = new TestCallbackHandler();
        ClientRequest request = new ClientRequest(time.milliseconds(), true, send, handler);
        awaitReady(client, node);
        client.send(request);
        client.poll(1, time.milliseconds());
        assertEquals(1, client.inFlightRequestCount());
        ResponseHeader respHeader = new ResponseHeader(reqHeader.correlationId());
        Struct resp = new Struct(ProtoUtils.currentResponseSchema(ApiKeys.PRODUCE.id));
        resp.set("responses", new Object[0]);
        int size = respHeader.sizeOf() + resp.sizeOf();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        respHeader.writeTo(buffer);
        resp.writeTo(buffer);
        buffer.flip();
        selector.completeReceive(new NetworkReceive(node.id(), buffer));
        List<ClientResponse> responses = client.poll(1, time.milliseconds());
        assertEquals(1, responses.size());
        assertTrue("The handler should have executed.", handler.executed);
        assertTrue("Should have a response body.", handler.response.hasResponse());
        assertEquals("Should be correlated to the original request", request, handler.response.request());
    }

    private void awaitReady(NetworkClient client, Node node) {
        while (!client.ready(node, time.milliseconds()))
            client.poll(1, time.milliseconds());
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
