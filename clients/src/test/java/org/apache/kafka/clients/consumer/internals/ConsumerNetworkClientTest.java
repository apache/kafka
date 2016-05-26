/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConsumerNetworkClientTest {

    private String topicName = "test";
    private MockTime time = new MockTime();
    private MockClient client = new MockClient(time);
    private Cluster cluster = TestUtils.singletonCluster(topicName, 1);
    private Node node = cluster.nodes().get(0);
    private Metadata metadata = new Metadata(0, Long.MAX_VALUE);
    private ConsumerNetworkClient consumerClient = new ConsumerNetworkClient(client, metadata, time, 100, 1000);

    @Test
    public void send() {
        client.prepareResponse(heartbeatResponse(Errors.NONE.code()));
        RequestFuture<ClientResponse> future = consumerClient.send(node, ApiKeys.METADATA, heartbeatRequest());
        assertEquals(1, consumerClient.pendingRequestCount());
        assertEquals(1, consumerClient.pendingRequestCount(node));
        assertFalse(future.isDone());

        consumerClient.poll(future);
        assertTrue(future.isDone());
        assertTrue(future.succeeded());

        ClientResponse clientResponse = future.value();
        HeartbeatResponse response = new HeartbeatResponse(clientResponse.responseBody());
        assertEquals(Errors.NONE.code(), response.errorCode());
    }

    @Test
    public void multiSend() {
        client.prepareResponse(heartbeatResponse(Errors.NONE.code()));
        client.prepareResponse(heartbeatResponse(Errors.NONE.code()));
        RequestFuture<ClientResponse> future1 = consumerClient.send(node, ApiKeys.METADATA, heartbeatRequest());
        RequestFuture<ClientResponse> future2 = consumerClient.send(node, ApiKeys.METADATA, heartbeatRequest());
        assertEquals(2, consumerClient.pendingRequestCount());
        assertEquals(2, consumerClient.pendingRequestCount(node));

        consumerClient.awaitPendingRequests(node);
        assertTrue(future1.succeeded());
        assertTrue(future2.succeeded());
    }

    @Test
    public void schedule() {
        TestDelayedTask task = new TestDelayedTask();
        consumerClient.schedule(task, time.milliseconds());
        consumerClient.poll(0);
        assertEquals(1, task.executions);

        consumerClient.schedule(task, time.milliseconds() + 100);
        consumerClient.poll(0);
        assertEquals(1, task.executions);

        time.sleep(100);
        consumerClient.poll(0);
        assertEquals(2, task.executions);
    }

    @Test
    public void wakeup() {
        RequestFuture<ClientResponse> future = consumerClient.send(node, ApiKeys.METADATA, heartbeatRequest());
        consumerClient.wakeup();
        try {
            consumerClient.poll(0);
            fail();
        } catch (WakeupException e) {
        }

        client.respond(heartbeatResponse(Errors.NONE.code()));
        consumerClient.poll(future);
        assertTrue(future.isDone());
    }

    @Test
    public void sendExpiry() throws InterruptedException {
        long unsentExpiryMs = 10;
        final AtomicBoolean isReady = new AtomicBoolean();
        final AtomicBoolean disconnected = new AtomicBoolean();
        client = new MockClient(time) {
            @Override
            public boolean ready(Node node, long now) {
                if (isReady.get())
                    return super.ready(node, now);
                else
                    return false;
            }
            @Override
            public boolean connectionFailed(Node node) {
                return disconnected.get();
            }
        };
        // Queue first send, sleep long enough for this to expire and then queue second send
        consumerClient = new ConsumerNetworkClient(client, metadata, time, 100, unsentExpiryMs);
        RequestFuture<ClientResponse> future1 = consumerClient.send(node, ApiKeys.METADATA, heartbeatRequest());
        assertEquals(1, consumerClient.pendingRequestCount());
        assertEquals(1, consumerClient.pendingRequestCount(node));
        assertFalse(future1.isDone());

        time.sleep(unsentExpiryMs + 1);
        RequestFuture<ClientResponse> future2 = consumerClient.send(node, ApiKeys.METADATA, heartbeatRequest());
        assertEquals(2, consumerClient.pendingRequestCount());
        assertEquals(2, consumerClient.pendingRequestCount(node));
        assertFalse(future2.isDone());

        // First send should have expired and second send still pending
        consumerClient.poll(0);
        assertTrue(future1.isDone());
        assertFalse(future1.succeeded());
        assertEquals(1, consumerClient.pendingRequestCount());
        assertEquals(1, consumerClient.pendingRequestCount(node));
        assertFalse(future2.isDone());

        // Enable send, the un-expired send should succeed on poll
        isReady.set(true);
        client.prepareResponse(heartbeatResponse(Errors.NONE.code()));
        consumerClient.poll(future2);
        ClientResponse clientResponse = future2.value();
        HeartbeatResponse response = new HeartbeatResponse(clientResponse.responseBody());
        assertEquals(Errors.NONE.code(), response.errorCode());

        // Disable ready flag to delay send and queue another send. Disconnection should remove pending send
        isReady.set(false);
        RequestFuture<ClientResponse> future3 = consumerClient.send(node, ApiKeys.METADATA, heartbeatRequest());
        assertEquals(1, consumerClient.pendingRequestCount());
        assertEquals(1, consumerClient.pendingRequestCount(node));
        disconnected.set(true);
        consumerClient.poll(0);
        assertTrue(future3.isDone());
        assertFalse(future3.succeeded());
        assertEquals(0, consumerClient.pendingRequestCount());
        assertEquals(0, consumerClient.pendingRequestCount(node));
    }

    private HeartbeatRequest heartbeatRequest() {
        return new HeartbeatRequest("group", 1, "memberId");
    }

    private Struct heartbeatResponse(short error) {
        HeartbeatResponse response = new HeartbeatResponse(error);
        return response.toStruct();
    }

    private static class TestDelayedTask implements DelayedTask {
        int executions = 0;
        @Override
        public void run(long now) {
            executions++;
        }
    }

}
