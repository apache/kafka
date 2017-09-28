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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
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

    private static class FutureResponse {
        private final Node node;
        private final RequestMatcher requestMatcher;
        private final AbstractResponse responseBody;
        private final boolean disconnected;
        private final boolean isUnsupportedRequest;

        public FutureResponse(Node node,
                              RequestMatcher requestMatcher,
                              AbstractResponse responseBody,
                              boolean disconnected,
                              boolean isUnsupportedRequest) {
            this.node = node;
            this.requestMatcher = requestMatcher;
            this.responseBody = responseBody;
            this.disconnected = disconnected;
            this.isUnsupportedRequest = isUnsupportedRequest;
        }

    }

    private final Time time;
    private final Metadata metadata;
    private Set<String> unavailableTopics;
    private Cluster cluster;
    private Node node = null;
    private final Set<String> ready = new HashSet<>();
    private final Map<Node, Long> blackedOut = new HashMap<>();
    // Use concurrent queue for requests so that requests may be queried from a different thread
    private final Queue<ClientRequest> requests = new ConcurrentLinkedDeque<>();
    // Use concurrent queue for responses so that responses may be updated during poll() from a different thread.
    private final Queue<ClientResponse> responses = new ConcurrentLinkedDeque<>();
    private final Queue<FutureResponse> futureResponses = new ArrayDeque<>();
    private final Queue<MetadataUpdate> metadataUpdates = new ArrayDeque<>();
    private volatile NodeApiVersions nodeApiVersions = NodeApiVersions.create();

    public MockClient(Time time) {
        this(time, null);
    }

    public MockClient(Time time, Metadata metadata) {
        this.time = time;
        this.metadata = metadata;
        this.unavailableTopics = Collections.emptySet();
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

    @Override
    public AuthenticationException authenticationException(Node node) {
        return null;
    }

    @Override
    public void disconnect(String node) {
        long now = time.milliseconds();
        Iterator<ClientRequest> iter = requests.iterator();
        while (iter.hasNext()) {
            ClientRequest request = iter.next();
            if (request.destination().equals(node)) {
                short version = request.requestBuilder().latestAllowedVersion();
                responses.add(new ClientResponse(request.makeHeader(version), request.callback(), request.destination(),
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

            AbstractRequest.Builder<?> builder = request.requestBuilder();
            short version = nodeApiVersions.latestUsableVersion(request.apiKey(), builder.oldestAllowedVersion(),
                    builder.latestAllowedVersion());
            AbstractRequest abstractRequest = request.requestBuilder().build(version);
            if (!futureResp.requestMatcher.matches(abstractRequest))
                throw new IllegalStateException("Request matcher did not match next-in-line request " + abstractRequest);

            UnsupportedVersionException unsupportedVersionException = null;
            if (futureResp.isUnsupportedRequest)
                unsupportedVersionException = new UnsupportedVersionException("Api " +
                        request.apiKey() + " with version " + version);

            ClientResponse resp = new ClientResponse(request.makeHeader(version), request.callback(), request.destination(),
                    request.createdTimeMs(), time.milliseconds(), futureResp.disconnected,
                    unsupportedVersionException, futureResp.responseBody);
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
            MetadataUpdate metadataUpdate = metadataUpdates.poll();
            if (cluster != null)
                metadata.update(cluster, this.unavailableTopics, time.milliseconds());
            if (metadataUpdate == null)
                metadata.update(metadata.fetch(), this.unavailableTopics, time.milliseconds());
            else {
                this.unavailableTopics = metadataUpdate.unavailableTopics;
                metadata.update(metadataUpdate.cluster, metadataUpdate.unavailableTopics, time.milliseconds());
            }
        }

        ClientResponse response;
        while ((response = this.responses.poll()) != null) {
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

    public void respond(RequestMatcher matcher, AbstractResponse response) {
        ClientRequest nextRequest = requests.peek();
        if (nextRequest == null)
            throw new IllegalStateException("No current requests queued");

        AbstractRequest request = nextRequest.requestBuilder().build();
        if (!matcher.matches(request))
            throw new IllegalStateException("Request matcher did not match next-in-line request " + request);

        respond(response);
    }

    // Utility method to enable out of order responses
    public void respondToRequest(ClientRequest clientRequest, AbstractResponse response) {
        AbstractRequest request = clientRequest.requestBuilder().build();
        requests.remove(clientRequest);
        short version = clientRequest.requestBuilder().latestAllowedVersion();
        responses.add(new ClientResponse(clientRequest.makeHeader(version), clientRequest.callback(), clientRequest.destination(),
                clientRequest.createdTimeMs(), time.milliseconds(), false, null, response));
    }


    public void respond(AbstractResponse response, boolean disconnected) {
        ClientRequest request = requests.remove();
        short version = request.requestBuilder().latestAllowedVersion();
        responses.add(new ClientResponse(request.makeHeader(version), request.callback(), request.destination(),
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
                short version = request.requestBuilder().latestAllowedVersion();
                responses.add(new ClientResponse(request.makeHeader(version), request.callback(), request.destination(),
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
        prepareResponseFrom(ALWAYS_TRUE, response, node, false, false);
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
        prepareResponseFrom(matcher, response, node, false, false);
    }

    public void prepareResponse(AbstractResponse response, boolean disconnected) {
        prepareResponse(ALWAYS_TRUE, response, disconnected);
    }

    public void prepareResponseFrom(AbstractResponse response, Node node, boolean disconnected) {
        prepareResponseFrom(ALWAYS_TRUE, response, node, disconnected, false);
    }

    /**
     * Prepare a response for a request matching the provided matcher. If the matcher does not
     * match, {@link KafkaClient#send(ClientRequest, long)} will throw IllegalStateException.
     * @param matcher The request matcher to apply
     * @param response The response body
     * @param disconnected Whether the request was disconnected
     */
    public void prepareResponse(RequestMatcher matcher, AbstractResponse response, boolean disconnected) {
        prepareResponseFrom(matcher, response, null, disconnected, false);
    }

    /**
     * Raise an unsupported version error on the next request if it matches the given matcher.
     * If the matcher does not match, {@link KafkaClient#send(ClientRequest, long)} will throw IllegalStateException.
     * @param matcher The request matcher to apply
     */
    public void prepareUnsupportedVersionResponse(RequestMatcher matcher) {
        prepareResponseFrom(matcher, null, null, false, true);
    }

    private void prepareResponseFrom(RequestMatcher matcher,
                                     AbstractResponse response,
                                     Node node,
                                     boolean disconnected,
                                     boolean isUnsupportedVersion) {
        futureResponses.add(new FutureResponse(node, matcher, response, disconnected, isUnsupportedVersion));
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

    public void prepareMetadataUpdate(Cluster cluster, Set<String> unavailableTopics) {
        metadataUpdates.add(new MetadataUpdate(cluster, unavailableTopics));
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public void cluster(Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public int inFlightRequestCount() {
        return requests.size();
    }

    @Override
    public boolean hasInFlightRequests() {
        return !requests.isEmpty();
    }

    @Override
    public int inFlightRequestCount(String node) {
        int result = 0;
        for (ClientRequest req : requests) {
            if (req.destination().equals(node))
                ++result;
        }
        return result;
    }

    @Override
    public boolean hasInFlightRequests(String node) {
        return inFlightRequestCount(node) > 0;
    }

    @Override
    public boolean hasReadyNodes() {
        return !ready.isEmpty();
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

    private static class MetadataUpdate {
        final Cluster cluster;
        final Set<String> unavailableTopics;
        MetadataUpdate(Cluster cluster, Set<String> unavailableTopics) {
            this.cluster = cluster;
            this.unavailableTopics = unavailableTopics;
        }
    }
}
