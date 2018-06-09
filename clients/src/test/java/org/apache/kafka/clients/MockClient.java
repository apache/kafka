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
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;

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

    private int correlation;
    private final Time time;
    private final Metadata metadata;
    private Set<String> unavailableTopics;
    private Cluster cluster;
    private Node node = null;
    private final Set<String> ready = new HashSet<>();

    // Nodes awaiting reconnect backoff, will not be chosen by leastLoadedNode
    private final TransientSet<Node> blackedOut;
    // Nodes which will always fail to connect, but can be chosen by leastLoadedNode
    private final TransientSet<Node> unreachable;

    private final Map<Node, Long> pendingAuthenticationErrors = new HashMap<>();
    private final Map<Node, AuthenticationException> authenticationErrors = new HashMap<>();
    // Use concurrent queue for requests so that requests may be queried from a different thread
    private final Queue<ClientRequest> requests = new ConcurrentLinkedDeque<>();
    // Use concurrent queue for responses so that responses may be updated during poll() from a different thread.
    private final Queue<ClientResponse> responses = new ConcurrentLinkedDeque<>();
    private final Queue<FutureResponse> futureResponses = new ConcurrentLinkedDeque<>();
    private final Queue<MetadataUpdate> metadataUpdates = new ConcurrentLinkedDeque<>();
    private volatile NodeApiVersions nodeApiVersions = NodeApiVersions.create();
    private volatile int numBlockingWakeups = 0;

    public MockClient(Time time) {
        this(time, null);
    }

    public MockClient(Time time, Metadata metadata) {
        this.time = time;
        this.metadata = metadata;
        this.unavailableTopics = Collections.emptySet();
        this.blackedOut = new TransientSet<>(time);
        this.unreachable = new TransientSet<>(time);
    }

    @Override
    public boolean isReady(Node node, long now) {
        return ready.contains(node.idString());
    }

    @Override
    public boolean ready(Node node, long now) {
        if (blackedOut.contains(node, now))
            return false;

        if (unreachable.contains(node, now)) {
            blackout(node, 100);
            return false;
        }

        ready.add(node.idString());
        return true;
    }

    @Override
    public long connectionDelay(Node node, long now) {
        return blackedOut.expirationDelayMs(node, now);
    }

    @Override
    public long pollDelayMs(Node node, long now) {
        return connectionDelay(node, now);
    }

    public void blackout(Node node, long durationMs) {
        blackedOut.add(node, durationMs);
    }

    public void setUnreachable(Node node, long durationMs) {
        disconnect(node.idString());
        unreachable.add(node, durationMs);
    }

    public void authenticationFailed(Node node, long blackoutMs) {
        pendingAuthenticationErrors.remove(node);
        authenticationErrors.put(node, (AuthenticationException) Errors.SASL_AUTHENTICATION_FAILED.exception());
        disconnect(node.idString());
        blackout(node, blackoutMs);
    }

    public void createPendingAuthenticationError(Node node, long blackoutMs) {
        pendingAuthenticationErrors.put(node, blackoutMs);
    }

    @Override
    public boolean connectionFailed(Node node) {
        return blackedOut.contains(node);
    }

    @Override
    public AuthenticationException authenticationException(Node node) {
        return authenticationErrors.get(node);
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
                        request.createdTimeMs(), now, true, null, null, null));
                iter.remove();
            }
        }
        ready.remove(node);
    }

    @Override
    public void send(ClientRequest request, long now) {
        // Check if the request is directed to a node with a pending authentication error.
        for (Iterator<Map.Entry<Node, Long>> authErrorIter =
             pendingAuthenticationErrors.entrySet().iterator(); authErrorIter.hasNext(); ) {
            Map.Entry<Node, Long> entry = authErrorIter.next();
            Node node = entry.getKey();
            long blackoutMs = entry.getValue();
            if (node.idString().equals(request.destination())) {
                authErrorIter.remove();
                // Set up a disconnected ClientResponse and create an authentication error
                // for the affected node.
                authenticationFailed(node, blackoutMs);
                AbstractRequest.Builder<?> builder = request.requestBuilder();
                short version = nodeApiVersions.latestUsableVersion(request.apiKey(), builder.oldestAllowedVersion(),
                    builder.latestAllowedVersion());
                ClientResponse resp = new ClientResponse(request.makeHeader(version), request.callback(), request.destination(),
                    request.createdTimeMs(), time.milliseconds(), true, null,
                        new AuthenticationException("Authentication failed"), null);
                responses.add(resp);
                return;
            }
        }
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
                    unsupportedVersionException, null, futureResp.responseBody);
            responses.add(resp);
            iterator.remove();
            return;
        }

        this.requests.add(request);
    }

    /**
     * Simulate a blocking poll in order to test wakeup behavior.
     *
     * @param numBlockingWakeups The number of polls which will block until woken up
     */
    public synchronized void enableBlockingUntilWakeup(int numBlockingWakeups) {
        this.numBlockingWakeups = numBlockingWakeups;
    }

    @Override
    public synchronized void wakeup() {
        if (numBlockingWakeups > 0) {
            numBlockingWakeups--;
            notify();
        }
    }

    private synchronized void maybeAwaitWakeup() {
        try {
            int remainingBlockingWakeups = numBlockingWakeups;
            if (remainingBlockingWakeups <= 0)
                return;

            while (numBlockingWakeups == remainingBlockingWakeups)
                wait();
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    @Override
    public List<ClientResponse> poll(long timeoutMs, long now) {
        maybeAwaitWakeup();

        List<ClientResponse> copy = new ArrayList<>(this.responses);

        if (metadata != null && metadata.updateRequested()) {
            MetadataUpdate metadataUpdate = metadataUpdates.poll();
            if (cluster != null)
                metadata.update(cluster, this.unavailableTopics, time.milliseconds());
            if (metadataUpdate == null)
                metadata.update(metadata.fetch(), this.unavailableTopics, time.milliseconds());
            else {
                if (metadataUpdate.expectMatchRefreshTopics
                    && !metadata.topics().equals(metadataUpdate.cluster.topics())) {
                    throw new IllegalStateException("The metadata topics does not match expectation. "
                                                        + "Expected topics: " + metadataUpdate.cluster.topics()
                                                        + ", asked topics: " + metadata.topics());
                }
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
                clientRequest.createdTimeMs(), time.milliseconds(), false, null, null, response));
    }


    public void respond(AbstractResponse response, boolean disconnected) {
        ClientRequest request = requests.remove();
        short version = request.requestBuilder().latestAllowedVersion();
        responses.add(new ClientResponse(request.makeHeader(version), request.callback(), request.destination(),
                request.createdTimeMs(), time.milliseconds(), disconnected, null, null, response));
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
                        request.createdTimeMs(), time.milliseconds(), disconnected, null, null, response));
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
        unreachable.clear();
        requests.clear();
        responses.clear();
        futureResponses.clear();
        metadataUpdates.clear();
        authenticationErrors.clear();
    }

    public boolean hasPendingMetadataUpdates() {
        return !metadataUpdates.isEmpty();
    }

    public int numAwaitingResponses() {
        return futureResponses.size();
    }

    public void prepareMetadataUpdate(Cluster cluster, Set<String> unavailableTopics) {
        metadataUpdates.add(new MetadataUpdate(cluster, unavailableTopics, false));
    }

    public void prepareMetadataUpdate(Cluster cluster,
                                      Set<String> unavailableTopics,
                                      boolean expectMatchMetadataTopics) {
        metadataUpdates.add(new MetadataUpdate(cluster, unavailableTopics, expectMatchMetadataTopics));
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

    public boolean hasPendingResponses() {
        return !responses.isEmpty();
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
    public boolean hasReadyNodes(long now) {
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
        return new ClientRequest(nodeId, requestBuilder, correlation++, "mockClientId", createdTimeMs,
                expectResponse, callback);
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
        // Consistent with NetworkClient, we do not return nodes awaiting reconnect backoff
        if (blackedOut.contains(node, now))
            return null;
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
        final boolean expectMatchRefreshTopics;
        MetadataUpdate(Cluster cluster, Set<String> unavailableTopics, boolean expectMatchRefreshTopics) {
            this.cluster = cluster;
            this.unavailableTopics = unavailableTopics;
            this.expectMatchRefreshTopics = expectMatchRefreshTopics;
        }
    }

    private static class TransientSet<T> {
        // The elements in the set mapped to their expiration timestamps
        private final Map<T, Long> elements = new HashMap<>();
        private final Time time;

        private TransientSet(Time time) {
            this.time = time;
        }

        boolean contains(T element) {
            return contains(element, time.milliseconds());
        }

        boolean contains(T element, long now) {
            return expirationDelayMs(element, now) > 0;
        }

        void add(T element, long durationMs) {
            elements.put(element, time.milliseconds() + durationMs);
        }

        long expirationDelayMs(T element, long now) {
            Long expirationTimeMs = elements.get(element);
            if (expirationTimeMs == null) {
                return 0;
            } else if (now > expirationTimeMs) {
                elements.remove(element);
                return 0;
            } else {
                return expirationTimeMs - now;
            }
        }

        void clear() {
            elements.clear();
        }

    }

}
