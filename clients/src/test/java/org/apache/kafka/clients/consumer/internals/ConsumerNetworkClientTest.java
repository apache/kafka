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
    private Metadata metadata = new Metadata(0, Long.MAX_VALUE, cluster, 0);
    private ConsumerNetworkClient consumerClient = new ConsumerNetworkClient(client, metadata, time, 100);

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
