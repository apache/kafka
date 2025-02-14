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
package org.apache.kafka.raft;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequestManagerTest {
    private final MockTime time = new MockTime();
    private final int requestTimeoutMs = 30000;
    private final int retryBackoffMs = 100;
    private final Random random = new Random(1);

    @Test
    public void testResetAllConnections() {
        Node node1 = new Node(1, "mock-host-1", 4321);
        Node node2 = new Node(2, "mock-host-2", 4321);

        RequestManager cache = new RequestManager(
            makeBootstrapList(3),
            retryBackoffMs,
            requestTimeoutMs,
            random
        );

        // One host has an inflight request
        cache.onRequestSent(node1, 1, time.milliseconds());
        assertFalse(cache.isReady(node1, time.milliseconds()));

        // Another is backing off
        cache.onRequestSent(node2, 2, time.milliseconds());
        cache.onResponseResult(node2, 2, false, time.milliseconds());
        assertFalse(cache.isReady(node2, time.milliseconds()));

        cache.resetAll();

        // Now both should be ready
        assertTrue(cache.isReady(node1, time.milliseconds()));
        assertTrue(cache.isReady(node2, time.milliseconds()));
    }

    @Test
    public void testBackoffAfterFailure() {
        Node node = new Node(1, "mock-host-1", 4321);

        RequestManager cache = new RequestManager(
            makeBootstrapList(3),
            retryBackoffMs,
            requestTimeoutMs,
            random
        );

        assertTrue(cache.isReady(node, time.milliseconds()));

        long correlationId = 1;
        cache.onRequestSent(node, correlationId, time.milliseconds());
        assertFalse(cache.isReady(node, time.milliseconds()));

        cache.onResponseResult(node, correlationId, false, time.milliseconds());
        assertFalse(cache.isReady(node, time.milliseconds()));

        time.sleep(retryBackoffMs);
        assertTrue(cache.isReady(node, time.milliseconds()));
    }

    @Test
    public void testSuccessfulResponse() {
        Node node = new Node(1, "mock-host-1", 4321);

        RequestManager cache = new RequestManager(
            makeBootstrapList(3),
            retryBackoffMs,
            requestTimeoutMs,
            random
        );

        long correlationId = 1;
        cache.onRequestSent(node, correlationId, time.milliseconds());
        assertFalse(cache.isReady(node, time.milliseconds()));
        cache.onResponseResult(node, correlationId, true, time.milliseconds());
        assertTrue(cache.isReady(node, time.milliseconds()));
    }

    @Test
    public void testIgnoreUnexpectedResponse() {
        Node node = new Node(1, "mock-host-1", 4321);

        RequestManager cache = new RequestManager(
            makeBootstrapList(3),
            retryBackoffMs,
            requestTimeoutMs,
            random
        );

        long correlationId = 1;
        cache.onRequestSent(node, correlationId, time.milliseconds());
        assertFalse(cache.isReady(node, time.milliseconds()));
        cache.onResponseResult(node, correlationId + 1, true, time.milliseconds());
        assertFalse(cache.isReady(node, time.milliseconds()));
    }

    @Test
    public void testRequestTimeout() {
        Node node = new Node(1, "mock-host-1", 4321);

        RequestManager cache = new RequestManager(
            makeBootstrapList(3),
            retryBackoffMs,
            requestTimeoutMs,
            random
        );

        long correlationId = 1;
        cache.onRequestSent(node, correlationId, time.milliseconds());
        assertFalse(cache.isReady(node, time.milliseconds()));

        time.sleep(requestTimeoutMs - 1);
        assertFalse(cache.isReady(node, time.milliseconds()));

        time.sleep(1);
        assertTrue(cache.isReady(node, time.milliseconds()));
    }

    @Test
    public void testRequestToBootstrapList() {
        List<Node> bootstrapList = makeBootstrapList(2);
        RequestManager cache = new RequestManager(
            bootstrapList,
            retryBackoffMs,
            requestTimeoutMs,
            random
        );

        // Find a ready node with the starting state
        Node bootstrapNode1 = cache.findReadyBootstrapServer(time.milliseconds()).get();
        assertTrue(
            bootstrapList.contains(bootstrapNode1),
            String.format("%s is not in %s", bootstrapNode1, bootstrapList)
        );
        assertEquals(0, cache.backoffBeforeAvailableBootstrapServer(time.milliseconds()));

        // Send a request and check the cache state
        cache.onRequestSent(bootstrapNode1, 1, time.milliseconds());
        assertEquals(
            Optional.empty(),
            cache.findReadyBootstrapServer(time.milliseconds())
        );
        assertEquals(requestTimeoutMs, cache.backoffBeforeAvailableBootstrapServer(time.milliseconds()));

        // Fail the request
        time.sleep(100);
        cache.onResponseResult(bootstrapNode1, 1, false, time.milliseconds());
        Node bootstrapNode2 = cache.findReadyBootstrapServer(time.milliseconds()).get();
        assertNotEquals(bootstrapNode1, bootstrapNode2);
        assertEquals(0, cache.backoffBeforeAvailableBootstrapServer(time.milliseconds()));

        // Send a request to the second node and check the state
        cache.onRequestSent(bootstrapNode2, 2, time.milliseconds());
        assertEquals(
            Optional.empty(),
            cache.findReadyBootstrapServer(time.milliseconds())
        );
        assertEquals(requestTimeoutMs, cache.backoffBeforeAvailableBootstrapServer(time.milliseconds()));


        // Fail the second request before the request timeout
        time.sleep(retryBackoffMs - 1);
        cache.onResponseResult(bootstrapNode2, 2, false, time.milliseconds());
        assertEquals(
            Optional.empty(),
            cache.findReadyBootstrapServer(time.milliseconds())
        );
        assertEquals(1, cache.backoffBeforeAvailableBootstrapServer(time.milliseconds()));

        // Timeout the first backoff and show that that node is ready
        time.sleep(1);
        Node bootstrapNode3 = cache.findReadyBootstrapServer(time.milliseconds()).get();
        assertEquals(bootstrapNode1, bootstrapNode3);
        assertEquals(0, cache.backoffBeforeAvailableBootstrapServer(time.milliseconds()));
    }

    @Test
    public void testFindReadyWithInflightRequest() {
        Node otherNode = new Node(1, "other-node", 1234);
        List<Node> bootstrapList = makeBootstrapList(3);
        RequestManager cache = new RequestManager(
            bootstrapList,
            retryBackoffMs,
            requestTimeoutMs,
            random
        );

        // Send request to a node that is not in the bootstrap list
        cache.onRequestSent(otherNode, 1, time.milliseconds());
        assertEquals(Optional.empty(), cache.findReadyBootstrapServer(time.milliseconds()));
    }

    @Test
    public void testFindReadyWithRequestTimedout() {
        Node otherNode = new Node(1, "other-node", 1234);
        List<Node> bootstrapList = makeBootstrapList(3);
        RequestManager cache = new RequestManager(
            bootstrapList,
            retryBackoffMs,
            requestTimeoutMs,
            random
        );

        // Send request to a node that is not in the bootstrap list
        cache.onRequestSent(otherNode, 1, time.milliseconds());
        assertTrue(cache.isResponseExpected(otherNode, 1));
        assertEquals(Optional.empty(), cache.findReadyBootstrapServer(time.milliseconds()));

        // Timeout the request
        time.sleep(requestTimeoutMs);
        Node bootstrapNode = cache.findReadyBootstrapServer(time.milliseconds()).get();
        assertTrue(bootstrapList.contains(bootstrapNode));
        assertFalse(cache.isResponseExpected(otherNode, 1));
    }

    @Test
    public void testAnyInflightRequestWithAnyRequest() {
        Node otherNode = new Node(1, "other-node", 1234);
        List<Node> bootstrapList = makeBootstrapList(3);
        RequestManager cache = new RequestManager(
            bootstrapList,
            retryBackoffMs,
            requestTimeoutMs,
            random
        );

        assertFalse(cache.hasAnyInflightRequest(time.milliseconds()));

        // Send a request and check state
        cache.onRequestSent(otherNode, 11, time.milliseconds());
        assertTrue(cache.hasAnyInflightRequest(time.milliseconds()));

        // Wait until the request times out
        time.sleep(requestTimeoutMs);
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds()));

        // Send another request and fail it
        cache.onRequestSent(otherNode, 12, time.milliseconds());
        cache.onResponseResult(otherNode, 12, false, time.milliseconds());
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds()));

        // Send another request and mark it successful
        cache.onRequestSent(otherNode, 12, time.milliseconds());
        cache.onResponseResult(otherNode, 12, true, time.milliseconds());
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds()));
    }

    private List<Node> makeBootstrapList(int numberOfNodes) {
        return IntStream.iterate(-2, id -> id - 1)
            .limit(numberOfNodes)
            .mapToObj(id -> new Node(id, String.format("mock-boot-host%d", id), 1234))
            .toList();
    }
}
