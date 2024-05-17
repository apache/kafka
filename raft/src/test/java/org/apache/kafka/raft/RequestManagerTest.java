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
import java.util.stream.Collectors;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequestManagerTest {
    private final MockTime time = new MockTime();
    private final int requestTimeoutMs = 30000;
    private final int retryBackoffMs = 100;
    private final Random random = new Random(1);

    @Test
    public void testResetAllConnections() {
        List<Node> nodes = makeNodeList(IntStream.of(1, 2, 3));
        Node node1 = nodes.get(0);
        Node node2 = nodes.get(1);

        RequestManager cache = new RequestManager(
            nodes,
            retryBackoffMs,
            requestTimeoutMs,
            random);

        // One host has an inflight request
        cache.onRequestSent(node1, 1, time.milliseconds());
        assertFalse(cache.isReady(node1, time.milliseconds()));

        // Another is backing off
        cache.onRequestSent(node2, 2, time.milliseconds());
        cache.onResponseError(node2, 2, time.milliseconds());
        assertFalse(cache.isReady(node2, time.milliseconds()));

        cache.resetAll();

        // Now both should be ready
        assertTrue(cache.isReady(node1, time.milliseconds()));
        assertTrue(cache.isReady(node2, time.milliseconds()));
    }

    @Test
    public void testBackoffAfterFailure() {
        List<Node> nodes = makeNodeList(IntStream.of(1, 2, 3));
        Node node1 = nodes.get(0);

        RequestManager cache = new RequestManager(
            nodes,
            retryBackoffMs,
            requestTimeoutMs,
            random);

        assertTrue(cache.isReady(node1, time.milliseconds()));

        long correlationId = 1;
        cache.onRequestSent(node1, correlationId, time.milliseconds());
        assertFalse(cache.isReady(node1, time.milliseconds()));

        cache.onResponseError(node1, correlationId, time.milliseconds());
        assertFalse(cache.isReady(node1, time.milliseconds()));

        time.sleep(retryBackoffMs);
        assertTrue(cache.isReady(node1, time.milliseconds()));
    }

    @Test
    public void testSuccessfulResponse() {
        List<Node> nodes = makeNodeList(IntStream.of(1, 2, 3));
        Node node1 = nodes.get(0);

        RequestManager cache = new RequestManager(
            nodes,
            retryBackoffMs,
            requestTimeoutMs,
            random);

        long correlationId = 1;
        cache.onRequestSent(node1, correlationId, time.milliseconds());
        assertFalse(cache.isReady(node1, time.milliseconds()));
        cache.onResponseReceived(node1, correlationId);
        assertTrue(cache.isReady(node1, time.milliseconds()));
    }

    @Test
    public void testIgnoreUnexpectedResponse() {
        List<Node> nodes = makeNodeList(IntStream.of(1, 2, 3));
        Node node1 = nodes.get(0);

        RequestManager cache = new RequestManager(
            nodes,
            retryBackoffMs,
            requestTimeoutMs,
            random);

        long correlationId = 1;
        cache.onRequestSent(node1, correlationId, time.milliseconds());
        assertFalse(cache.isReady(node1, time.milliseconds()));
        cache.onResponseReceived(node1, correlationId + 1);
        assertFalse(cache.isReady(node1, time.milliseconds()));
    }

    @Test
    public void testRequestTimeout() {
        List<Node> nodes = makeNodeList(IntStream.of(1, 2, 3));
        Node node1 = nodes.get(0);

        RequestManager cache = new RequestManager(
            nodes,
            retryBackoffMs,
            requestTimeoutMs,
            random);

        long correlationId = 1;
        cache.onRequestSent(node1, correlationId, time.milliseconds());
        assertFalse(cache.isReady(node1, time.milliseconds()));

        time.sleep(requestTimeoutMs - 1);
        assertFalse(cache.isReady(node1, time.milliseconds()));

        time.sleep(1);
        assertTrue(cache.isReady(node1, time.milliseconds()));
    }

    private List<Node> makeNodeList(IntStream nodes) {
        return nodes
            .mapToObj(id -> new Node(id, String.format("mock-host-%d", id), 1234))
            .collect(Collectors.toList());
    }
}
