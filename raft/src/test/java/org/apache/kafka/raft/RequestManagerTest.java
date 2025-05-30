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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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

    @ParameterizedTest
    @EnumSource(value = ApiKeys.class, names = {"FETCH", "FETCH_SNAPSHOT"})
    public void testRequestToBootstrapList(ApiKeys apiKey) {
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
        cache.onRequestSent(bootstrapNode1, 1, time.milliseconds(), apiKey);
        assertNotReadyBootstrapServerOnSend(cache);

        // Fail the request. BootstrapNode1 begins backing off, meaning the other bootstrap
        // node is ready to serve a fetch request
        cache.onResponseResult(bootstrapNode1, 1, false, time.milliseconds(), apiKey);
        Node bootstrapNode2 = assertReadyBootstrapServer(cache, bootstrapList);
        assertNotEquals(bootstrapNode1, bootstrapNode2);

        // Send a request to the second node and check the state
        cache.onRequestSent(bootstrapNode2, 2, time.milliseconds(), apiKey);
        assertNotReadyBootstrapServerOnSend(cache);

        // Fail the second request before the bootstrapNode1's backoff is complete
        time.sleep(retryBackoffMs - 1);
        cache.onResponseResult(bootstrapNode2, 2, false, time.milliseconds(), apiKey);
        assertEquals(
            Optional.empty(),
            cache.findReadyBootstrapServer(time.milliseconds())
        );
        // This is the remaining backoff time for bootstrapNode1 to become available
        assertEquals(1, cache.backoffBeforeAvailableBootstrapServer(time.milliseconds()));

        // Timeout the first backoff and show that that node is ready
        time.sleep(1);
        Node bootstrapNode3 = assertReadyBootstrapServer(cache, bootstrapList);
        assertEquals(bootstrapNode1, bootstrapNode3);
    }

    @ParameterizedTest
    @EnumSource(value = ApiKeys.class, names = {"FETCH", "FETCH_SNAPSHOT"})
    public void testRequestToBootstrapListMultipleRequestTypes(ApiKeys apiKey) {
        List<Node> bootstrapList = makeBootstrapList(2);
        RequestManager cache = new RequestManager(
            bootstrapList,
            retryBackoffMs,
            requestTimeoutMs,
            random
        );

        // Other requests should not affect readiness of bootstrap servers for fetching
        Node bootstrapNode = assertReadyBootstrapServer(cache, bootstrapList);
        cache.onRequestSent(bootstrapNode, 1, time.milliseconds(), updateVoter);
        assertReadyBootstrapServer(cache, bootstrapList);

        // Send a request and check the cache state
        cache.onRequestSent(bootstrapNode, 1, time.milliseconds(), apiKey);
        assertNotReadyBootstrapServerOnSend(cache);

        // Other requests should not affect readiness of bootstrap servers
        cache.onResponseResult(bootstrapNode, 1, true, time.milliseconds(), updateVoter);
        assertNotReadyBootstrapServerOnSend(cache);
        cache.onRequestSent(bootstrapNode, 2, time.milliseconds(), updateVoter);
        assertNotReadyBootstrapServerOnSend(cache);

        // Complete the fetch or fetch snapshot request and show that node is ready
        cache.onResponseResult(bootstrapNode, 1, true, time.milliseconds(), apiKey);
        assertReadyBootstrapServer(cache, bootstrapList);

        // Other requests should not affect readiness of bootstrap servers
        cache.onResponseResult(bootstrapNode, 2, false, time.milliseconds(), updateVoter);
        assertReadyBootstrapServer(cache, bootstrapList);
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

    private void assertNotReadyBootstrapServerOnSend(RequestManager cache) {
        assertEquals(
            Optional.empty(),
            cache.findReadyBootstrapServer(time.milliseconds())
        );
        assertEquals(requestTimeoutMs, cache.backoffBeforeAvailableBootstrapServer(time.milliseconds()));
    }

    @ParameterizedTest
    @EnumSource(value = ApiKeys.class, names = {"FETCH", "FETCH_SNAPSHOT"})
    public void testFindReadyWithInflightFetchToNonBootstrapNode(ApiKeys apiKey) {
        Node otherNode = new Node(1, "other-node", 1234);
        List<Node> bootstrapList = makeBootstrapList(3);
        RequestManager cache = new RequestManager(
            bootstrapList,
            retryBackoffMs,
            requestTimeoutMs,
            random
        );

        cache.onRequestSent(otherNode, 1, time.milliseconds(), apiKey);
        assertNotReadyBootstrapServerOnSend(cache);
        cache.onResponseResult(otherNode, 1, true, time.milliseconds(), apiKey);
        assertReadyBootstrapServer(cache, bootstrapList);
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
    public void testAnyInflightRequestWithMultipleRequestTypes() {
        Node otherNode = new Node(1, "other-node", 1234);
        List<Node> bootstrapList = makeBootstrapList(3);
        RequestManager cache = new RequestManager(
            bootstrapList,
            retryBackoffMs,
            requestTimeoutMs,
            random
        );

        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetch));
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetchSnapshot));
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), updateVoter));

        // Send a request and check state
        cache.onRequestSent(otherNode, 11, time.milliseconds(), fetch);
        assertTrue(cache.hasAnyInflightRequest(time.milliseconds(), fetch));
        assertTrue(cache.hasAnyInflightRequest(time.milliseconds(), fetchSnapshot));
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), updateVoter));

        // Send the other request and check state
        cache.onRequestSent(otherNode, 11, time.milliseconds(), updateVoter);
        assertTrue(cache.hasAnyInflightRequest(time.milliseconds(), fetch));
        assertTrue(cache.hasAnyInflightRequest(time.milliseconds(), fetchSnapshot));
        assertTrue(cache.hasAnyInflightRequest(time.milliseconds(), updateVoter));

        // Wait until the request times out
        time.sleep(requestTimeoutMs);
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetch));
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetchSnapshot));
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), updateVoter));

        // Results should not affect the connection state of other request types
        cache.onRequestSent(otherNode, 12, time.milliseconds(), updateVoter);

        // Send another request and fail it
        cache.onRequestSent(otherNode, 12, time.milliseconds(), fetch);
        cache.onResponseResult(otherNode, 12, false, time.milliseconds(), fetch);
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetch));
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetchSnapshot));
        assertTrue(cache.hasAnyInflightRequest(time.milliseconds(), updateVoter));

        // Send fetch snapshot request, it should be treated the same as fetch
        cache.onRequestSent(otherNode, 12, time.milliseconds(), fetchSnapshot);
        assertTrue(cache.hasAnyInflightRequest(time.milliseconds(), fetch));
        assertTrue(cache.hasAnyInflightRequest(time.milliseconds(), fetchSnapshot));
        assertTrue(cache.hasAnyInflightRequest(time.milliseconds(), updateVoter));

        cache.onResponseResult(otherNode, 12, true, time.milliseconds(), fetchSnapshot);
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetch));
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetchSnapshot));
        assertTrue(cache.hasAnyInflightRequest(time.milliseconds(), updateVoter));
    }

    @ParameterizedTest
    @EnumSource(value = ApiKeys.class, names = {"FETCH", "FETCH_SNAPSHOT"})
    public void testAnyInflightRequestWithFetchOrFetchSnapshot(ApiKeys apiKey) {
        Node otherNode = new Node(1, "other-node", 1234);
        List<Node> bootstrapList = makeBootstrapList(3);
        RequestManager cache = new RequestManager(
            bootstrapList,
            retryBackoffMs,
            requestTimeoutMs,
            random
        );

        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetch));
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetchSnapshot));

        // Send a request and check state
        cache.onRequestSent(otherNode, 11, time.milliseconds(), apiKey);
        assertTrue(cache.hasAnyInflightRequest(time.milliseconds(), fetch));
        assertTrue(cache.hasAnyInflightRequest(time.milliseconds(), fetchSnapshot));

        // Wait until the request times out
        time.sleep(requestTimeoutMs);
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetch));
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetchSnapshot));

        // Send another request and fail it
        cache.onRequestSent(otherNode, 12, time.milliseconds(), apiKey);
        cache.onResponseResult(otherNode, 12, false, time.milliseconds(), apiKey);
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetch));
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetchSnapshot));

        // Send another request and mark it successful
        cache.onRequestSent(otherNode, 12, time.milliseconds(), apiKey);
        cache.onResponseResult(otherNode, 12, true, time.milliseconds(), apiKey);
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetch));
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), fetchSnapshot));
    }

    @ParameterizedTest
    @EnumSource(
        value = ApiKeys.class,
        names = {
            "VOTE",
            "BEGIN_QUORUM_EPOCH",
            "END_QUORUM_EPOCH",
            "API_VERSIONS",
            "UPDATE_RAFT_VOTER"
        })
    public void testAnyInflightRequestWithOtherKRaftRequests(ApiKeys apiKey) {
        Node otherNode = new Node(1, "other-node", 1234);
        List<Node> bootstrapList = makeBootstrapList(3);
        RequestManager cache = new RequestManager(
            bootstrapList,
            retryBackoffMs,
            requestTimeoutMs,
            random
        );

        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), apiKey));

        // Send a request and check state
        cache.onRequestSent(otherNode, 11, time.milliseconds(), apiKey);
        assertTrue(cache.hasAnyInflightRequest(time.milliseconds(), apiKey));

        // Wait until the request times out
        time.sleep(requestTimeoutMs);
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), apiKey));

        // Send another request and fail it
        cache.onRequestSent(otherNode, 12, time.milliseconds(), apiKey);
        cache.onResponseResult(otherNode, 12, false, time.milliseconds(), apiKey);
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), apiKey));

        // Send another request and mark it successful
        cache.onRequestSent(otherNode, 12, time.milliseconds(), apiKey);
        cache.onResponseResult(otherNode, 12, true, time.milliseconds(), apiKey);
        assertFalse(cache.hasAnyInflightRequest(time.milliseconds(), apiKey));
    }

    private List<Node> makeBootstrapList(int numberOfNodes) {
        return IntStream.iterate(-2, id -> id - 1)
            .limit(numberOfNodes)
            .mapToObj(id -> new Node(id, String.format("mock-boot-host%d", id), 1234))
            .collect(Collectors.toList());
    }
}
