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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.network.ByteBufferSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AlterConfigsRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.Resource;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class InFlightRequestsTest {

    private final int maxInFlightRequestsPerConnection = 20;
    private final InFlightRequests inFlightRequests = new InFlightRequests(maxInFlightRequestsPerConnection);
    private final List<NetworkClient.InFlightRequest> requests = new ArrayList<>();
    private final Map<String, Integer> nodeWiseRequestCounter = new HashMap<>();
    private Integer nodeIdCounter = 0;

    @Before
    public void setUp() throws Exception {
        for (nodeIdCounter = 0; nodeIdCounter < 2; nodeIdCounter++) {
            String nodeId = nodeIdCounter.toString();
            pushRequests(nodeId, 5);
        }
    }

    @Test
    public void testAdd() {
        String nodeId = (nodeIdCounter++).toString();
        int totalRequest = 10;
        pushRequests(nodeId, totalRequest);
        assertEquals(totalRequest, inFlightRequests.count(nodeId));
    }

    @Test
    public void testCompleteNext() {
        NetworkClient.InFlightRequest oldestRequest = requests.remove(0);
        decrementCounter(oldestRequest.destination);
        assertEquals(oldestRequest, inFlightRequests.completeNext(oldestRequest.destination));
    }

    @Test(expected = IllegalStateException.class)
    public void testCompleteNextWithInvalidNode() {
        inFlightRequests.completeNext("");
    }


    @Test
    public void testLastSent() {
        NetworkClient.InFlightRequest lastSentRequest = requests.get(requests.size() - 1);
        assertEquals(lastSentRequest, inFlightRequests.lastSent(lastSentRequest.destination));
    }

    @Test(expected = IllegalStateException.class)
    public void testLastSentWithInvalidNode() {
        inFlightRequests.lastSent("");
    }

    @Test
    public void testCompleteLastSent() {
        NetworkClient.InFlightRequest lastSentRequest = requests.remove(requests.size() - 1);
        decrementCounter(lastSentRequest.destination);
        assertEquals(lastSentRequest, inFlightRequests.completeLastSent(lastSentRequest.destination));
    }

    @Test(expected = IllegalStateException.class)
    public void testCompleteLastSentWithInvalidNode() {
        inFlightRequests.completeLastSent("");
    }

    @Test
    public void testCanSendMore() {
        String nodeId = (nodeIdCounter++).toString();
        assertTrue("On Node addition, it should be able to handle the incoming InFlight requests",
                inFlightRequests.canSendMore(nodeId));
        pushRequests(nodeId, maxInFlightRequestsPerConnection);
        assertFalse("Once the max inFlightRequests per node reached, it should reject the requests",
                inFlightRequests.canSendMore(nodeId));
    }

    @Test
    public void testCount() {
        assertEquals(requests.size(), inFlightRequests.count());
    }

    @Test
    public void testIsEmpty() {
        assertEquals(inFlightRequests.count() == 0, inFlightRequests.isEmpty());
    }

    @Test
    public void testCountInSingleNode() {
        String nodeId = (nodeIdCounter++).toString();
        int totalRequest = 10;
        pushRequests(nodeId, totalRequest);
        assertEquals(totalRequest, inFlightRequests.count(nodeId));
    }

    @Test
    public void testIsEmptyInSingleNode()  {
        String nodeId = (nodeIdCounter++).toString();
        assertTrue("New node InFlight requests should be empty", inFlightRequests.isEmpty(nodeId));

        nodeId = (nodeIdCounter++).toString();
        int totalRequest = 10;
        pushRequests(nodeId, totalRequest);

        assertEquals(totalRequest, inFlightRequests.count(nodeId));
        inFlightRequests.clearAll(nodeId);
        assertTrue("Once the InFlight requests are cleared, it should be empty", inFlightRequests.isEmpty(nodeId));
    }

    @Test
    public void testClearAll() {
        String nodeId = (nodeIdCounter++).toString();
        int totalRequest = 10;
        pushRequests(nodeId, totalRequest);

        assertEquals(totalRequest, inFlightRequests.count(nodeId));
        inFlightRequests.clearAll(nodeId);
        assertEquals(0, inFlightRequests.count(nodeId));
    }

    @Test
    public void testGetNodesWithTimedOutRequests() {
        MockTime time = new MockTime(100);
        List<String> nodesWithTimedOutRequests = inFlightRequests.getNodesWithTimedOutRequests(time.milliseconds(), 10);
        assertEquals(nodeWiseRequestCounter.size(), nodesWithTimedOutRequests.size());
    }

    private void pushRequests(String nodeIdString, int totalRequest) {
        for (int request = 0; request < totalRequest; request++) {
            NetworkClient.InFlightRequest inFlightRequest = createInFlightRequest(nodeIdString);
            requests.add(inFlightRequest);
            incrementCounter(nodeIdString);
            inFlightRequests.add(inFlightRequest);
        }
    }

    private NetworkClient.InFlightRequest createInFlightRequest(String nodeIdString) {
        RequestHeader header = new RequestHeader((short) 0, (short) 0, "client", 1);
        Time time = new MockTime();
        RequestCompletionHandler handler = new RequestCompletionHandler() {
            @Override
            public void onComplete(ClientResponse response) {
                // no op
            }
        };
        AbstractRequest abstractRequest = new AlterConfigsRequest((short) 0, Collections.<Resource, AlterConfigsRequest.Config>emptyMap(), false);
        Node node = new Node(Integer.parseInt(nodeIdString), "localhost", 9092);
        Send send = new ByteBufferSend(node.idString());
        return new NetworkClient.InFlightRequest(header, time.milliseconds(), node.idString(), handler, true,
                true, abstractRequest, send, time.milliseconds());
    }

    private void incrementCounter(String nodeId) {
        Integer counter = nodeWiseRequestCounter.get(nodeId);
        if (counter == null) {
            counter = 0;
        }
        nodeWiseRequestCounter.put(nodeId, counter + 1);
    }

    private void decrementCounter(String nodeId) {
        Integer counter = nodeWiseRequestCounter.get(nodeId);
        if (counter - 1 == 0) {
            nodeWiseRequestCounter.remove(nodeId);
        } else {
            nodeWiseRequestCounter.put(nodeId, counter - 1);
        }
    }
}