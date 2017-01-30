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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * A mock network client for use testing code
 */
public class MockClient implements KafkaClient {
    public static final RequestMatcher ALWAYS_TRUE = new RequestMatcher() {
        @Override
        public boolean matches(AbstractRequest body) {
            return true;
        }
    };

    private class FutureResponse {
        public final AbstractResponse responseBody;
        public final boolean disconnected;
        public final RequestMatcher requestMatcher;
        public Node node;

        public FutureResponse(AbstractResponse responseBody, boolean disconnected, RequestMatcher requestMatcher, Node node) {
            this.responseBody = responseBody;
            this.disconnected = disconnected;
            this.requestMatcher = requestMatcher;
            this.node = node;
        }

    }

    private final Time time;
    private final Metadata metadata;
    private int correlation = 0;
    private Node node = null;
    private final Set<String> ready = new HashSet<>();
    private final Map<Node, Long> blackedOut = new HashMap<>();
    // Use concurrent queue for requests so that requests may be queried from a different thread
    private final Queue<ClientRequest> requests = new ConcurrentLinkedDeque<>();
    // Use concurrent queue for responses so that responses may be updated during poll() from a different thread.
    private final Queue<ClientResponse> responses = new ConcurrentLinkedDeque<>();
    private final Queue<FutureResponse> futureResponses = new ArrayDeque<>();
    private final Queue<Cluster> metadataUpdates = new ArrayDeque<>();
    private volatile NodeApiVersions nodeApiVersions = NodeApiVersions.create();

    public MockClient(Time time) {
        this.time = time;
        this.metadata = null;
    }

    public MockClient(Time time, Metadata metadata) {
        this.time = time;
        this.metadata = metadata;
    }

    @Override
    public boolean isReady(Node node, long now) {
        return ready.contains(node.idString());
    }

    @Override
    public boolean ready(Node node, long now) {
        if (isBlackedOut(node))
            return false;
        ready.add(node.idString());
        return true;
    }

    @Override
    public long connectionDelay(Node node, long now) {
        return 0;
    }

    public void blackout(Node node, long duration) {
        blackedOut.put(node, time.milliseconds() + duration);
    }

    private boolean isBlackedOut(Node node) {
        if (blackedOut.containsKey(node)) {
            long expiration = blackedOut.get(node);
            if (time.milliseconds() > expiration) {
                blackedOut.remove(node);
                return false;
            } else {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean connectionFailed(Node node) {
        return isBlackedOut(node);
    }

    public void disconnect(String node) {
        long now = time.milliseconds();
        Iterator<ClientRequest> iter = requests.iterator();
        while (iter.hasNext()) {
            ClientRequest request = iter.next();
            if (request.destination().equals(node)) {
                responses.add(new ClientResponse(request.makeHeader(), request.callback(), request.destination(),
                        request.createdTimeMs(), now, true, null, null));
                iter.remove();
            }
        }
        ready.remove(node);
    }

    @Override
    public void send(ClientRequest request, long now) {
        Iterator<FutureResponse> iterator = futureResponses.iterator();
        while (iterator.hasNext()) {
            FutureResponse futureResp = iterator.next();
            if (futureResp.node != null && !request.destination().equals(futureResp.node.idString()))
                continue;
            request.requestBuilder().setVersion(nodeApiVersions.usableVersion(
                    request.requestBuilder().apiKey()));
            AbstractRequest abstractRequest = request.requestBuilder().build();
            if (!futureResp.requestMatcher.matches(abstractRequest))
                throw new IllegalStateException("Next in line response did not match expected request");

            ClientResponse resp = new ClientResponse(request.makeHeader(), request.callback(), request.destination(),
                    request.createdTimeMs(), time.milliseconds(), futureResp.disconnected, null, futureResp.responseBody);
            responses.add(resp);
            iterator.remove();
            return;
        }

        this.requests.add(request);
    }

    @Override
    public List<ClientResponse> poll(long timeoutMs, long now) {
        List<ClientResponse> copy = new ArrayList<>(this.responses);

        if (metadata != null && metadata.updateRequested()) {
            Cluster cluster = metadataUpdates.poll();
            if (cluster == null)
                metadata.update(metadata.fetch(), time.milliseconds());
            else
                metadata.update(cluster, time.milliseconds());
        }

        while (!this.responses.isEmpty()) {
            ClientResponse response = this.responses.poll();
            response.onComplete();
        }

        return copy;
    }

    public Queue<ClientRequest> requests() {
        return this.requests;
    }

    public void respond(AbstractResponse response) {
        respond(response, false);
    }

    public void respond(AbstractResponse response, boolean disconnected) {
        ClientRequest request = requests.remove();
        responses.add(new ClientResponse(request.makeHeader(), request.callback(), request.destination(),
                request.createdTimeMs(), time.milliseconds(), disconnected, null, response));
    }

    public void respondFrom(AbstractResponse response, Node node) {
        respondFrom(response, node, false);
    }

    public void respondFrom(AbstractResponse response, Node node, boolean disconnected) {
        Iterator<ClientRequest> iterator = requests.iterator();
        while (iterator.hasNext()) {
            ClientRequest request = iterator.next();
            if (request.destination().equals(node.idString())) {
                iterator.remove();
                responses.add(new ClientResponse(request.makeHeader(), request.callback(), request.destination(),
                        request.createdTimeMs(), time.milliseconds(), disconnected, null, response));
                return;
            }
        }
        throw new IllegalArgumentException("No requests available to node " + node);
    }

    public void prepareResponse(AbstractResponse response) {
        prepareResponse(ALWAYS_TRUE, response, false);
    }

    public void prepareResponseFrom(AbstractResponse response, Node node) {
        prepareResponseFrom(ALWAYS_TRUE, response, node, false);
    }

    /**
     * Prepare a response for a request matching the provided matcher. If the matcher does not
     * match, {@link KafkaClient#send(ClientRequest, long)} will throw IllegalStateException
     * @param matcher The matcher to apply
     * @param response The response body
     */
    public void prepareResponse(RequestMatcher matcher, AbstractResponse response) {
        prepareResponse(matcher, response, false);
    }

    public void prepareResponseFrom(RequestMatcher matcher, AbstractResponse response, Node node) {
        prepareResponseFrom(matcher, response, node, false);
    }

    public void prepareResponse(AbstractResponse response, boolean disconnected) {
        prepareResponse(ALWAYS_TRUE, response, disconnected);
    }

    public void prepareResponseFrom(AbstractResponse response, Node node, boolean disconnected) {
        prepareResponseFrom(ALWAYS_TRUE, response, node, disconnected);
    }

    /**
     * Prepare a response for a request matching the provided matcher. If the matcher does not
     * match, {@link KafkaClient#send(ClientRequest, long)} will throw IllegalStateException
     * @param matcher The matcher to apply
     * @param response The response body
     * @param disconnected Whether the request was disconnected
     */
    public void prepareResponse(RequestMatcher matcher, AbstractResponse response, boolean disconnected) {
        prepareResponseFrom(matcher, response, null, disconnected);
    }

    public void prepareResponseFrom(RequestMatcher matcher, AbstractResponse response, Node node, boolean disconnected) {
        futureResponses.add(new FutureResponse(response, disconnected, matcher, node));
    }

    public void waitForRequests(final int minRequests, long maxWaitMs) throws InterruptedException {
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return requests.size() >= minRequests;
            }
        }, maxWaitMs, "Expected requests have not been sent");
    }

    public void reset() {
        ready.clear();
        blackedOut.clear();
        requests.clear();
        responses.clear();
        futureResponses.clear();
        metadataUpdates.clear();
    }

    public void prepareMetadataUpdate(Cluster cluster) {
        metadataUpdates.add(cluster);
    }

    public void setNode(Node node) {
        this.node = node;
    }

    @Override
    public int inFlightRequestCount() {
        return requests.size();
    }

    @Override
    public int inFlightRequestCount(String node) {
        return requests.size();
    }

    @Override
    public ClientRequest newClientRequest(String nodeId, AbstractRequest.Builder<?> requestBuilder, long createdTimeMs,
                                          boolean expectResponse) {
        return newClientRequest(nodeId, requestBuilder, createdTimeMs, expectResponse, null);
    }

    @Override
    public ClientRequest newClientRequest(String nodeId, AbstractRequest.Builder<?> requestBuilder, long createdTimeMs,
                                          boolean expectResponse, RequestCompletionHandler callback) {
        return new ClientRequest(nodeId, requestBuilder, 0, "mockClientId", createdTimeMs,
                expectResponse, callback);
    }

    @Override
    public void wakeup() {
    }

    @Override
    public void close() {
    }

    @Override
    public void close(String node) {
        ready.remove(node);
    }

    @Override
    public Node leastLoadedNode(long now) {
        return this.node;
    }

    /**
     * The RequestMatcher provides a way to match a particular request to a response prepared
     * through {@link #prepareResponse(RequestMatcher, AbstractResponse)}. Basically this allows testers
     * to inspect the request body for the type of the request or for specific fields that should be set,
     * and to fail the test if it doesn't match.
     */
    public interface RequestMatcher {
        boolean matches(AbstractRequest body);
    }

    public void setNodeApiVersions(NodeApiVersions nodeApiVersions) {
        this.nodeApiVersions = nodeApiVersions;
    }
}
