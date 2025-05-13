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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
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
    private final ApiKeys fetch = ApiKeys.FETCH;
    private final ApiKeys fetchSnapshot = ApiKeys.FETCH_SNAPSHOT;
    private final ApiKeys updateVoter = ApiKeys.UPDATE_RAFT_VOTER;

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

        // One host has inflight requests
        cache.onRequestSent(node1, 1, time.milliseconds(), fetch);
        assertFalse(cache.isReady(node1, time.milliseconds(), fetch));
        cache.onRequestSent(node1, 1, time.milliseconds(), updateVoter);
        assertFalse(cache.isReady(node1, time.milliseconds(), updateVoter));

        // Another is backing off
        cache.onRequestSent(node2, 2, time.milliseconds(), fetch);
        cache.onResponseResult(node2, 2, false, time.milliseconds(), fetch);
        assertFalse(cache.isReady(node2, time.milliseconds(), fetch));
        cache.onRequestSent(node2, 2, time.milliseconds(), updateVoter);
        cache.onResponseResult(node2, 2, false, time.milliseconds(), updateVoter);
        assertFalse(cache.isReady(node2, time.milliseconds(), updateVoter));

        cache.resetAll();

        // Now both should be ready
        assertTrue(cache.isReady(node1, time.milliseconds(), fetch));
        assertTrue(cache.isReady(node1, time.milliseconds(), updateVoter));
        assertTrue(cache.isReady(node2, time.milliseconds(), fetch));
        assertTrue(cache.isReady(node2, time.milliseconds(), updateVoter));
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

        assertTrue(cache.isReady(node, time.milliseconds(), fetch));

        long correlationId = 1;
        cache.onRequestSent(node, correlationId, time.milliseconds(), fetch);
        assertFalse(cache.isReady(node, time.milliseconds(), fetch));

        cache.onResponseResult(node, correlationId, false, time.milliseconds(), fetch);
        assertFalse(cache.isReady(node, time.milliseconds(), fetch));

        time.sleep(retryBackoffMs);
        assertTrue(cache.isReady(node, time.milliseconds(), fetch));
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
        cache.onRequestSent(node, correlationId, time.milliseconds(), fetch);
        assertFalse(cache.isReady(node, time.milliseconds(), fetch));
        cache.onResponseResult(node, correlationId, true, time.milliseconds(), fetch);
        assertTrue(cache.isReady(node, time.milliseconds(), fetch));
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
        cache.onRequestSent(node, correlationId, time.milliseconds(), fetch);
        assertFalse(cache.isReady(node, time.milliseconds(), fetch));
        cache.onResponseResult(node, correlationId + 1, true, time.milliseconds(), fetch);
        assertFalse(cache.isReady(node, time.milliseconds(), fetch));
    }

    @Test
    public void testIgnoreOtherRequestTypeResponses() {
        Node node = new Node(1, "mock-host-1", 4321);

        RequestManager cache = new RequestManager(
            makeBootstrapList(3),
            retryBackoffMs,
            requestTimeoutMs,
            random
        );
        long correlationId = 1;

        // completing a request of a different type should not affect the state of other requests
        assertTrue(cache.isReady(node, time.milliseconds(), updateVoter));
        assertTrue(cache.isReady(node, time.milliseconds(), fetch));

        cache.onRequestSent(node, correlationId, time.milliseconds(), fetch);
        assertTrue(cache.isReady(node, time.milliseconds(), updateVoter));
        assertFalse(cache.isReady(node, time.milliseconds(), fetch));

        cache.onRequestSent(node, correlationId, time.milliseconds(), updateVoter);
        assertFalse(cache.isReady(node, time.milliseconds(), updateVoter));
        assertFalse(cache.isReady(node, time.milliseconds(), fetch));

        cache.onResponseResult(node, correlationId, true, time.milliseconds(), updateVoter);
        assertTrue(cache.isReady(node, time.milliseconds(), updateVoter));
        assertFalse(cache.isReady(node, time.milliseconds(), fetch));

        cache.onResponseResult(node, correlationId, true, time.milliseconds(), fetch);
        assertTrue(cache.isReady(node, time.milliseconds(), updateVoter));
        assertTrue(cache.isReady(node, time.milliseconds(), fetch));
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
        cache.onRequestSent(node, correlationId, time.milliseconds(), fetch);
        assertFalse(cache.isReady(node, time.milliseconds(), fetch));

        time.sleep(requestTimeoutMs - 1);
        assertFalse(cache.isReady(node, time.milliseconds(), fetch));

        time.sleep(1);
        assertTrue(cache.isReady(node, time.milliseconds(), fetch));
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
        Node bootstrapNode1 = assertReadyBootstrapServer(cache, bootstrapList);

        // Send a request and check the cache state
        cache.onRequestSent(bootstrapNode1, 1, time.milliseconds(), fetch);
        assertNotReadyBootstrapServer(cache);

        // Fail the request
        time.sleep(100);
        cache.onResponseResult(bootstrapNode1, 1, false, time.milliseconds(), fetch);
        Node bootstrapNode2 = cache.findReadyBootstrapServer(time.milliseconds()).get();
        assertNotEquals(bootstrapNode1, bootstrapNode2);
        assertEquals(0, cache.backoffBeforeAvailableBootstrapServer(time.milliseconds()));

        // Send a request to the second node and check the state
        cache.onRequestSent(bootstrapNode2, 2, time.milliseconds(), fetch);
        assertNotReadyBootstrapServer(cache);

        // Fail the second request before the request timeout
        time.sleep(retryBackoffMs - 1);
        cache.onResponseResult(bootstrapNode2, 2, false, time.milliseconds(), fetch);
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
    public void testRequestToBootstrapListMultipleRequests() {
        List<Node> bootstrapList = makeBootstrapList(2);
        RequestManager cache = new RequestManager(
            bootstrapList,
            retryBackoffMs,
            requestTimeoutMs,
            random
        );

        // Pending fetch snapshot requests should also result in a returned backoff
        Node bootstrapNode = assertReadyBootstrapServer(cache, bootstrapList);

        // Send a request and check the cache state
        cache.onRequestSent(bootstrapNode, 1, time.milliseconds(), fetchSnapshot);
        assertNotReadyBootstrapServer(cache);

        // Other pending requests should not affect readiness of bootstrap servers
        cache.onRequestSent(bootstrapNode, 1, time.milliseconds(), updateVoter);
        assertEquals(requestTimeoutMs, cache.backoffBeforeAvailableBootstrapServer(time.milliseconds()));

        // Complete the fetch snapshot request and show that node is ready
        cache.onResponseResult(bootstrapNode, 1, true, time.milliseconds(), fetchSnapshot);
        assertTrue(cache.findReadyBootstrapServer(time.milliseconds()).isPresent());
        assertEquals(0, cache.backoffBeforeAvailableBootstrapServer(time.milliseconds()));
    }

    private Node assertReadyBootstrapServer(RequestManager cache, List<Node> bootstrapList) {
        Node bootstrapNode = cache.findReadyBootstrapServer(time.milliseconds()).get();
        assertTrue(
            bootstrapList.contains(bootstrapNode),
            String.format("%s is not in %s", bootstrapNode, bootstrapList)
        );
        assertEquals(0, cache.backoffBeforeAvailableBootstrapServer(time.milliseconds()));
        return bootstrapNode;
    }

    private void assertNotReadyBootstrapServer(RequestManager cache) {
        assertEquals(
            Optional.empty(),
            cache.findReadyBootstrapServer(time.milliseconds())
        );
        assertEquals(requestTimeoutMs, cache.backoffBeforeAvailableBootstrapServer(time.milliseconds()));
    }

    @Test
    public void testFindReadyWithInflightFetchRequest() {
        Node otherNode = new Node(1, "other-node", 1234);
        List<Node> bootstrapList = makeBootstrapList(3);
        RequestManager cache = new RequestManager(
            bootstrapList,
            retryBackoffMs,
            requestTimeoutMs,
            random
        );

        // Send request to a node that is not in the bootstrap list
        completeFetchAndCheckReadiness(cache, otherNode, fetch);

        // A pending fetch snapshot request does not return a ready node
        completeFetchAndCheckReadiness(cache, otherNode, fetchSnapshot);
    }

    private void completeFetchAndCheckReadiness(
        RequestManager cache,
        Node destination,
        ApiKeys apiKey
    ) {
        cache.onRequestSent(destination, 1, time.milliseconds(), apiKey);
        assertEquals(Optional.empty(), cache.findReadyBootstrapServer(time.milliseconds()));
        cache.onResponseResult(destination, 1, true, time.milliseconds(), apiKey);
        assertTrue(cache.findReadyBootstrapServer(time.milliseconds()).isPresent());
    }

    @Test
    public void testFindReadyWithOtherInflightRequest() {
        Node otherNode = new Node(1, "other-node", 1234);
        List<Node> bootstrapList = makeBootstrapList(3);
        RequestManager cache = new RequestManager(
            bootstrapList,
            retryBackoffMs,
            requestTimeoutMs,
            random
        );

        // Other pending request types do not affect readiness of bootstrap servers
        cache.onRequestSent(otherNode, 1, time.milliseconds(), updateVoter);
        assertTrue(cache.findReadyBootstrapServer(time.milliseconds()).isPresent());
    }

    @Test
    public void testFindReadyWithRequestTimedOut() {
        Node otherNode = new Node(1, "other-node", 1234);
        List<Node> bootstrapList = makeBootstrapList(3);
        RequestManager cache = new RequestManager(
            bootstrapList,
            retryBackoffMs,
            requestTimeoutMs,
            random
        );

        // Send request to a node that is not in the bootstrap list
        cache.onRequestSent(otherNode, 1, time.milliseconds(), fetch);
        assertTrue(cache.isResponseExpected(otherNode, 1, fetch));
        assertEquals(Optional.empty(), cache.findReadyBootstrapServer(time.milliseconds()));

        // Timeout the request
        time.sleep(requestTimeoutMs);
        Node bootstrapNode = cache.findReadyBootstrapServer(time.milliseconds()).get();
        assertTrue(bootstrapList.contains(bootstrapNode));
        assertFalse(cache.isResponseExpected(otherNode, 1, fetch));
    }

    @Test
    public void testAnyInflightRequestOneRequest() {
        Node otherNode = new Node(1, "other-node", 1234);
        List<Node> bootstrapList = makeBootstrapList(3);
        RequestManager cache = new RequestManager(
            bootstrapList,
            retryBackoffMs,
            requestTimeoutMs,
            random
        );

        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetch));

        // Send a request and check state
        cache.onRequestSent(otherNode, 11, time.milliseconds(), fetch);
        assertTrue(cache.hasAnyInflightRequest(time.milliseconds(), fetch));

        // Wait until the request times out
        time.sleep(requestTimeoutMs);
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetch));

        // Send another request and fail it
        cache.onRequestSent(otherNode, 12, time.milliseconds(), fetch);
        cache.onResponseResult(otherNode, 12, false, time.milliseconds(), fetch);
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetch));

        // Send another request and mark it successful
        cache.onRequestSent(otherNode, 12, time.milliseconds(), fetch);
        cache.onResponseResult(otherNode, 12, true, time.milliseconds(), fetch);
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetch));
    }

    @Test
    public void testAnyInflightRequestMultipleRequests() {
        Node otherNode = new Node(1, "other-node", 1234);
        List<Node> bootstrapList = makeBootstrapList(3);
        RequestManager cache = new RequestManager(
            bootstrapList,
            retryBackoffMs,
            requestTimeoutMs,
            random
        );

        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetch));
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), updateVoter));
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetchSnapshot));

        // Send a request and check state
        cache.onRequestSent(otherNode, 11, time.milliseconds(), fetch);
        assertTrue(cache.hasAnyInflightRequest(time.milliseconds(), fetch));
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), updateVoter));
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetchSnapshot));

        // Send the other request and check state
        cache.onRequestSent(otherNode, 11, time.milliseconds(), updateVoter);
        assertTrue(cache.hasAnyInflightRequest(time.milliseconds(), fetch));
        assertTrue(cache.hasAnyInflightRequest(time.milliseconds(), updateVoter));
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetchSnapshot));

        // Wait until the request times out
        time.sleep(requestTimeoutMs);
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetch));
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), updateVoter));
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetchSnapshot));

        // Results should not affect the connection state of other request types
        cache.onRequestSent(otherNode, 12, time.milliseconds(), updateVoter);

        // Send another request and fail it
        cache.onRequestSent(otherNode, 12, time.milliseconds(), fetch);
        cache.onResponseResult(otherNode, 12, false, time.milliseconds(), fetch);
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetch));
        assertTrue(cache.hasAnyInflightRequest(time.milliseconds(), updateVoter));

        // Send another request and mark it successful
        cache.onRequestSent(otherNode, 12, time.milliseconds(), fetchSnapshot);
        cache.onResponseResult(otherNode, 12, true, time.milliseconds(), fetchSnapshot);
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetchSnapshot));
        assertTrue(cache.hasAnyInflightRequest(time.milliseconds(), updateVoter));
    }

    private List<Node> makeBootstrapList(int numberOfNodes) {
        return IntStream.iterate(-2, id -> id - 1)
            .limit(numberOfNodes)
            .mapToObj(id -> new Node(id, String.format("mock-boot-host%d", id), 1234))
            .collect(Collectors.toList());
    }
}
