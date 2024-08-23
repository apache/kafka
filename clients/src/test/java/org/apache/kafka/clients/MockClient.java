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
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.Time;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * A mock network client for use testing code
 */
public class MockClient implements KafkaClient {
    public static final RequestMatcher ALWAYS_TRUE = body -> true;

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
    private Runnable wakeupHook;
    private final Time time;
    private final MockMetadataUpdater metadataUpdater;
    private final Map<String, ConnectionState> connections = new HashMap<>();
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
    private volatile boolean active = true;
    private volatile CompletableFuture<String> disconnectFuture;
    private volatile Consumer<Node> readyCallback;

    public MockClient(Time time) {
        this(time, new NoOpMetadataUpdater());
    }

    public MockClient(Time time, Metadata metadata) {
        this(time, new DefaultMockMetadataUpdater(metadata));
    }

    public MockClient(Time time, MockMetadataUpdater metadataUpdater) {
        this.time = time;
        this.metadataUpdater = metadataUpdater;
    }

    public MockClient(Time time, List<Node> staticNodes) {
        this(time, new StaticMetadataUpdater(staticNodes));
    }

    public boolean isConnected(String idString) {
        return connectionState(idString).state == ConnectionState.State.CONNECTED;
    }

    private ConnectionState connectionState(String idString) {
        ConnectionState connectionState = connections.get(idString);
        if (connectionState == null) {
            connectionState = new ConnectionState();
            connections.put(idString, connectionState);
        }
        return connectionState;
    }

    @Override
    public boolean isReady(Node node, long now) {
        return connectionState(node.idString()).isReady(now);
    }

    @Override
    public boolean ready(Node node, long now) {
        if (readyCallback != null) {
            readyCallback.accept(node);
        }
        return connectionState(node.idString()).ready(now);
    }

    @Override
    public long connectionDelay(Node node, long now) {
        return connectionState(node.idString()).connectionDelay(now);
    }

    @Override
    public long pollDelayMs(Node node, long now) {
        return connectionDelay(node, now);
    }

    public void backoff(Node node, long durationMs) {
        connectionState(node.idString()).backoff(time.milliseconds() + durationMs);
    }

    public void setUnreachable(Node node, long durationMs) {
        disconnect(node.idString());
        connectionState(node.idString()).setUnreachable(time.milliseconds() + durationMs);
    }

    public void throttle(Node node, long durationMs) {
        connectionState(node.idString()).throttle(time.milliseconds() + durationMs);
    }

    public void delayReady(Node node, long durationMs) {
        connectionState(node.idString()).setReadyDelayed(time.milliseconds() + durationMs);
    }

    public void authenticationFailed(Node node, long backoffMs) {
        pendingAuthenticationErrors.remove(node);
        authenticationErrors.put(node, (AuthenticationException) Errors.SASL_AUTHENTICATION_FAILED.exception());
        disconnect(node.idString());
        backoff(node, backoffMs);
    }

    public void createPendingAuthenticationError(Node node, long backoffMs) {
        pendingAuthenticationErrors.put(node, backoffMs);
    }

    @Override
    public boolean connectionFailed(Node node) {
        return connectionState(node.idString()).isBackingOff(time.milliseconds());
    }

    @Override
    public AuthenticationException authenticationException(Node node) {
        return authenticationErrors.get(node);
    }

    public void setReadyCallback(Consumer<Node> onReadyCall) {
        this.readyCallback = onReadyCall;
    }

    public void setDisconnectFuture(CompletableFuture<String> disconnectFuture) {
        this.disconnectFuture = disconnectFuture;
    }

    @Override
    public void disconnect(String node) {
        disconnect(node, false);
    }

    public void disconnect(String node, boolean allowLateResponses) {
        long now = time.milliseconds();
        Iterator<ClientRequest> iter = requests.iterator();
        while (iter.hasNext()) {
            ClientRequest request = iter.next();
            if (request.destination().equals(node)) {
                short version = request.requestBuilder().latestAllowedVersion();
                responses.add(new ClientResponse(request.makeHeader(version), request.callback(), request.destination(),
                        request.createdTimeMs(), now, true, null, null, null));
                if (!allowLateResponses)
                    iter.remove();
            }
        }
        CompletableFuture<String> curDisconnectFuture = disconnectFuture;
        if (curDisconnectFuture != null) {
            curDisconnectFuture.complete(node);
        }
        connectionState(node).disconnect();
    }

    @Override
    public void send(ClientRequest request, long now) {
        if (!connectionState(request.destination()).isReady(now))
            throw new IllegalStateException("Cannot send " + request + " since the destination is not ready");

        // Check if the request is directed to a node with a pending authentication error.
        for (Iterator<Map.Entry<Node, Long>> authErrorIter =
             pendingAuthenticationErrors.entrySet().iterator(); authErrorIter.hasNext(); ) {
            Map.Entry<Node, Long> entry = authErrorIter.next();
            Node node = entry.getKey();
            long backoffMs = entry.getValue();
            if (node.idString().equals(request.destination())) {
                authErrorIter.remove();
                // Set up a disconnected ClientResponse and create an authentication error
                // for the affected node.
                authenticationFailed(node, backoffMs);
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

            try {
                short version = nodeApiVersions.latestUsableVersion(request.apiKey(), builder.oldestAllowedVersion(),
                        builder.latestAllowedVersion());


                AbstractRequest abstractRequest = request.requestBuilder().build(version);
                if (!futureResp.requestMatcher.matches(abstractRequest))
                    throw new IllegalStateException("Request matcher did not match next-in-line request "
                            + abstractRequest + " with prepared response " + futureResp.responseBody);

                UnsupportedVersionException unsupportedVersionException = null;
                if (futureResp.isUnsupportedRequest) {
                    unsupportedVersionException = new UnsupportedVersionException(
                            "Api " + request.apiKey() + " with version " + version);
                }

                ClientResponse resp = new ClientResponse(request.makeHeader(version), request.callback(), request.destination(),
                        request.createdTimeMs(), time.milliseconds(), futureResp.disconnected,
                        unsupportedVersionException, null, futureResp.responseBody);
                responses.add(resp);
            } catch (UnsupportedVersionException unsupportedVersionException) {
                ClientResponse resp = new ClientResponse(request.makeHeader(builder.latestAllowedVersion()), request.callback(), request.destination(),
                        request.createdTimeMs(), time.milliseconds(), false, unsupportedVersionException, null, null);
                responses.add(resp);
            }
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
        if (wakeupHook != null) {
            wakeupHook.run();
        }
    }

    private synchronized void maybeAwaitWakeup() {
        try {
            int remainingBlockingWakeups = numBlockingWakeups;
            if (remainingBlockingWakeups <= 0)
                return;

            TestUtils.waitForCondition(() -> {
                if (numBlockingWakeups == remainingBlockingWakeups)
                    MockClient.this.wait(500);
                return numBlockingWakeups < remainingBlockingWakeups;
            }, 5000, "Failed to receive expected wakeup");
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    @Override
    public List<ClientResponse> poll(long timeoutMs, long now) {
        maybeAwaitWakeup();
        checkTimeoutOfPendingRequests(now);

        // We skip metadata updates if all nodes are currently blacked out
        if (metadataUpdater.isUpdateNeeded() && leastLoadedNode(now).node() != null) {
            MetadataUpdate metadataUpdate = metadataUpdates.poll();
            if (metadataUpdate != null) {
                metadataUpdater.update(time, metadataUpdate);
            } else {
                metadataUpdater.updateWithCurrentMetadata(time);
            }
        }

        List<ClientResponse> copy = new ArrayList<>();
        ClientResponse response;
        while ((response = this.responses.poll()) != null) {
            response.onComplete();
            copy.add(response);
        }

        return copy;
    }

    private long elapsedTimeMs(long currentTimeMs, long startTimeMs) {
        return Math.max(0, currentTimeMs - startTimeMs);
    }

    private void checkTimeoutOfPendingRequests(long nowMs) {
        ClientRequest request = requests.peek();
        while (request != null && elapsedTimeMs(nowMs, request.createdTimeMs()) >= request.requestTimeoutMs()) {
            disconnect(request.destination());
            requests.poll();
            request = requests.peek();
        }
    }

    public Queue<ClientRequest> requests() {
        return this.requests;
    }

    public Queue<ClientResponse> responses() {
        return this.responses;
    }

    public Queue<FutureResponse> futureResponses() {
        return this.futureResponses;
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
        requests.remove(clientRequest);
        short version = clientRequest.requestBuilder().latestAllowedVersion();
        responses.add(new ClientResponse(clientRequest.makeHeader(version), clientRequest.callback(), clientRequest.destination(),
                clientRequest.createdTimeMs(), time.milliseconds(), false, null, null, response));
    }


    public void respond(AbstractResponse response, boolean disconnected) {
        if (requests.isEmpty())
            throw new IllegalStateException("No requests pending for inbound response " + response);
        ClientRequest request = requests.poll();
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

    public void prepareResponseFrom(RequestMatcher matcher, AbstractResponse response, Node node, boolean disconnected) {
        prepareResponseFrom(matcher, response, node, disconnected, false);
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
        TestUtils.waitForCondition(
                () -> requests.size() >= minRequests,
                maxWaitMs,
                "Expected requests have not been sent");
    }

    public void reset() {
        connections.clear();
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

    public void prepareMetadataUpdate(MetadataResponse updateResponse) {
        prepareMetadataUpdate(updateResponse, false);
    }

    public void prepareMetadataUpdate(MetadataResponse updateResponse,
                                      boolean expectMatchMetadataTopics) {
        metadataUpdates.add(new MetadataUpdate(updateResponse, expectMatchMetadataTopics));
    }

    public void updateMetadata(MetadataResponse updateResponse) {
        metadataUpdater.update(time, new MetadataUpdate(updateResponse, false));
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
        return !responses.isEmpty() || !futureResponses.isEmpty();
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
        return connections.values().stream().anyMatch(cxn -> cxn.isReady(now));
    }

    @Override
    public ClientRequest newClientRequest(String nodeId, AbstractRequest.Builder<?> requestBuilder, long createdTimeMs,
                                          boolean expectResponse) {
        return newClientRequest(nodeId, requestBuilder, createdTimeMs, expectResponse, 5000, null);
    }

    @Override
    public ClientRequest newClientRequest(String nodeId,
                                          AbstractRequest.Builder<?> requestBuilder,
                                          long createdTimeMs,
                                          boolean expectResponse,
                                          int requestTimeoutMs,
                                          RequestCompletionHandler callback) {
        return new ClientRequest(nodeId, requestBuilder, correlation++, "mockClientId", createdTimeMs,
                expectResponse, requestTimeoutMs, callback);
    }

    @Override
    public void initiateClose() {
        close();
    }

    @Override
    public boolean active() {
        return active;
    }

    @Override
    public void close() {
        active = false;
        metadataUpdater.close();
    }

    @Override
    public void close(String node) {
        connections.remove(node);
    }

    @Override
    public LeastLoadedNode leastLoadedNode(long now) {
        // Consistent with NetworkClient, we do not return nodes awaiting reconnect backoff
        for (Node node : metadataUpdater.fetchNodes()) {
            if (!connectionState(node.idString()).isBackingOff(now))
                return new LeastLoadedNode(node, true);
        }
        return new LeastLoadedNode(null, false);
    }

    public void setWakeupHook(Runnable wakeupHook) {
        this.wakeupHook = wakeupHook;
    }

    /**
     * The RequestMatcher provides a way to match a particular request to a response prepared
     * through {@link #prepareResponse(RequestMatcher, AbstractResponse)}. Basically this allows testers
     * to inspect the request body for the type of the request or for specific fields that should be set,
     * and to fail the test if it doesn't match.
     */
    @FunctionalInterface
    public interface RequestMatcher {
        boolean matches(AbstractRequest body);
    }

    public void setNodeApiVersions(NodeApiVersions nodeApiVersions) {
        this.nodeApiVersions = nodeApiVersions;
    }

    public static class MetadataUpdate {
        final MetadataResponse updateResponse;
        final boolean expectMatchRefreshTopics;

        MetadataUpdate(MetadataResponse updateResponse, boolean expectMatchRefreshTopics) {
            this.updateResponse = updateResponse;
            this.expectMatchRefreshTopics = expectMatchRefreshTopics;
        }

        private Set<String> topics() {
            return updateResponse.topicMetadata().stream()
                    .map(MetadataResponse.TopicMetadata::topic)
                    .collect(Collectors.toSet());
        }
    }

    /**
     * This is a dumbed down version of {@link MetadataUpdater} which is used to facilitate
     * metadata tracking primarily in order to serve {@link KafkaClient#leastLoadedNode(long)}
     * and bookkeeping through {@link Metadata}. The extensibility allows AdminClient, which does
     * not rely on {@link Metadata} to do its own thing.
     */
    public interface MockMetadataUpdater {
        List<Node> fetchNodes();

        boolean isUpdateNeeded();

        void update(Time time, MetadataUpdate update);

        default void updateWithCurrentMetadata(Time time) {}

        default void close() {}
    }

    private static class NoOpMetadataUpdater implements MockMetadataUpdater {
        @Override
        public List<Node> fetchNodes() {
            return Collections.emptyList();
        }

        @Override
        public boolean isUpdateNeeded() {
            return false;
        }

        @Override
        public void update(Time time, MetadataUpdate update) {
            throw new UnsupportedOperationException();
        }
    }

    private static class StaticMetadataUpdater extends NoOpMetadataUpdater {
        private final List<Node> nodes;
        public StaticMetadataUpdater(List<Node> nodes) {
            this.nodes = nodes;
        }

        @Override
        public List<Node> fetchNodes() {
            return nodes;
        }

    }

    private static class DefaultMockMetadataUpdater implements MockMetadataUpdater {
        private final Metadata metadata;
        private MetadataUpdate lastUpdate;

        public DefaultMockMetadataUpdater(Metadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public List<Node> fetchNodes() {
            return metadata.fetch().nodes();
        }

        @Override
        public boolean isUpdateNeeded() {
            return metadata.updateRequested();
        }

        @Override
        public void updateWithCurrentMetadata(Time time) {
            if (lastUpdate == null)
                throw new IllegalStateException("No previous metadata update to use");
            update(time, lastUpdate);
        }

        private void maybeCheckExpectedTopics(MetadataUpdate update, MetadataRequest.Builder builder) {
            if (update.expectMatchRefreshTopics) {
                if (builder.isAllTopics())
                    throw new IllegalStateException("The metadata topics does not match expectation. "
                            + "Expected topics: " + update.topics()
                            + ", asked topics: ALL");

                Set<String> requestedTopics = new HashSet<>(builder.topics());
                if (!requestedTopics.equals(update.topics())) {
                    throw new IllegalStateException("The metadata topics does not match expectation. "
                            + "Expected topics: " + update.topics()
                            + ", asked topics: " + requestedTopics);
                }
            }
        }

        @Override
        public void update(Time time, MetadataUpdate update) {
            MetadataRequest.Builder builder = metadata.newMetadataRequestBuilder();
            maybeCheckExpectedTopics(update, builder);
            metadata.updateWithCurrentRequestVersion(update.updateResponse, false, time.milliseconds());
            this.lastUpdate = update;
        }

        @Override
        public void close() {
            metadata.close();
        }
    }

    private static class ConnectionState {
        enum State { CONNECTING, CONNECTED, DISCONNECTED }

        private long throttledUntilMs = 0L;
        private long readyDelayedUntilMs = 0L;
        private long backingOffUntilMs = 0L;
        private long unreachableUntilMs = 0L;
        private State state = State.DISCONNECTED;

        void backoff(long untilMs) {
            backingOffUntilMs = untilMs;
        }

        void throttle(long untilMs) {
            throttledUntilMs = untilMs;
        }

        void setUnreachable(long untilMs) {
            unreachableUntilMs = untilMs;
        }

        void setReadyDelayed(long untilMs) {
            readyDelayedUntilMs = untilMs;
        }

        boolean isReady(long now) {
            return state == State.CONNECTED && notThrottled(now);
        }

        boolean isReadyDelayed(long now) {
            return now < readyDelayedUntilMs;
        }

        boolean notThrottled(long now) {
            return now > throttledUntilMs;
        }

        boolean isBackingOff(long now) {
            return now < backingOffUntilMs;
        }

        boolean isUnreachable(long now) {
            return now < unreachableUntilMs;
        }

        void disconnect() {
            state = State.DISCONNECTED;
        }

        long connectionDelay(long now) {
            if (state != State.DISCONNECTED)
                return Long.MAX_VALUE;

            if (backingOffUntilMs > now)
                return backingOffUntilMs - now;

            return 0;
        }

        boolean ready(long now) {
            switch (state) {
                case CONNECTED:
                    return notThrottled(now);

                case CONNECTING:
                    if (isReadyDelayed(now))
                        return false;
                    state = State.CONNECTED;
                    return ready(now);

                case DISCONNECTED:
                    if (isBackingOff(now)) {
                        return false;
                    } else if (isUnreachable(now)) {
                        backingOffUntilMs = now + 100;
                        return false;
                    }

                    state = State.CONNECTING;
                    return ready(now);

                default:
                    throw new IllegalArgumentException("Invalid state: " + state);
            }
        }

    }

}
