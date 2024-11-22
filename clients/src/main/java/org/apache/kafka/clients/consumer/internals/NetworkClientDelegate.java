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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.telemetry.internals.ClientTelemetrySender;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;

/**
 * A wrapper around the {@link org.apache.kafka.clients.NetworkClient} to handle network poll and send operations.
 */
public class NetworkClientDelegate implements AutoCloseable {

    private final KafkaClient client;
    private final BackgroundEventHandler backgroundEventHandler;
    private final Metadata metadata;
    private final Time time;
    private final Logger log;
    private final int requestTimeoutMs;
    private final Queue<UnsentRequest> unsentRequests;
    private final long retryBackoffMs;

    public NetworkClientDelegate(
            final Time time,
            final ConsumerConfig config,
            final LogContext logContext,
            final KafkaClient client,
            final Metadata metadata,
            final BackgroundEventHandler backgroundEventHandler) {
        this.time = time;
        this.client = client;
        this.metadata = metadata;
        this.backgroundEventHandler = backgroundEventHandler;
        this.log = logContext.logger(getClass());
        this.unsentRequests = new ArrayDeque<>();
        this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
    }

    // Visible for testing
    Queue<UnsentRequest> unsentRequests() {
        return unsentRequests;
    }

    public int inflightRequestCount() {
        return client.inFlightRequestCount();
    }

    /**
     * Check if the node is disconnected and unavailable for immediate reconnection (i.e. if it is in
     * reconnect backoff window following the disconnect).
     *
     * @param node {@link Node} to check for availability
     * @see NetworkClientUtils#isUnavailable(KafkaClient, Node, Time)
     */
    public boolean isUnavailable(Node node) {
        return NetworkClientUtils.isUnavailable(client, node, time);
    }

    /**
     * Checks for an authentication error on a given node and throws the exception if it exists.
     *
     * @param node {@link Node} to check for a previous {@link AuthenticationException}; if found it is thrown
     * @see NetworkClientUtils#maybeThrowAuthFailure(KafkaClient, Node)
     */
    public void maybeThrowAuthFailure(Node node) {
        NetworkClientUtils.maybeThrowAuthFailure(client, node);
    }

    /**
     * Initiate a connection if currently possible. This is only really useful for resetting
     * the failed status of a socket.
     *
     * @param node The node to connect to
     */
    public void tryConnect(Node node) {
        NetworkClientUtils.tryConnect(client, node, time);
    }

    /**
     * Returns the responses of the sent requests. This method will try to send the unsent requests, poll for responses,
     * and check the disconnected nodes.
     *
     * @param timeoutMs     timeout time
     * @param currentTimeMs current time
     */
    public void poll(final long timeoutMs, final long currentTimeMs) {
        trySend(currentTimeMs);

        long pollTimeoutMs = timeoutMs;
        if (!unsentRequests.isEmpty()) {
            pollTimeoutMs = Math.min(retryBackoffMs, pollTimeoutMs);
        }
        this.client.poll(pollTimeoutMs, currentTimeMs);
        maybePropagateMetadataError();
        checkDisconnects(currentTimeMs);
    }

    private void maybePropagateMetadataError() {
        try {
            metadata.maybeThrowAnyException();
        } catch (Exception e) {
            backgroundEventHandler.add(new ErrorEvent(e));
        }
    }

    /**
     * Return true if there is at least one in-flight request or unsent request.
     */
    public boolean hasAnyPendingRequests() {
        return client.hasInFlightRequests() || !unsentRequests.isEmpty();
    }

    /**
     * Tries to send the requests in the unsentRequest queue. If the request doesn't have an assigned node, it will
     * find the leastLoadedOne, and will be retried in the next {@code poll()}. If the request is expired, a
     * {@link TimeoutException} will be thrown.
     */
    private void trySend(final long currentTimeMs) {
        Iterator<UnsentRequest> iterator = unsentRequests.iterator();
        while (iterator.hasNext()) {
            UnsentRequest unsent = iterator.next();
            unsent.timer.update(currentTimeMs);
            if (unsent.timer.isExpired()) {
                iterator.remove();
                unsent.handler.onFailure(currentTimeMs, new TimeoutException(
                    "Failed to send request after " + unsent.timer.timeoutMs() + " ms."));
                continue;
            }

            if (!doSend(unsent, currentTimeMs)) {
                // continue to retry until timeout.
                continue;
            }
            iterator.remove();
        }
    }

    boolean doSend(final UnsentRequest r, final long currentTimeMs) {
        Node node = r.node.orElse(client.leastLoadedNode(currentTimeMs).node());
        if (node == null || nodeUnavailable(node)) {
            log.debug("No broker available to send the request: {}. Retrying.", r);
            return false;
        }
        ClientRequest request = makeClientRequest(r, node, currentTimeMs);
        if (!client.ready(node, currentTimeMs)) {
            // enqueue the request again if the node isn't ready yet. The request will be handled in the next iteration
            // of the event loop
            log.debug("Node is not ready, handle the request in the next event loop: node={}, request={}", node, r);
            return false;
        }
        client.send(request, currentTimeMs);
        return true;
    }

    protected void checkDisconnects(final long currentTimeMs) {
        // Check the connection of the unsent request. Disconnect the disconnected node if it is unable to be connected.
        Iterator<UnsentRequest> iter = unsentRequests.iterator();
        while (iter.hasNext()) {
            UnsentRequest u = iter.next();
            if (u.node.isPresent() && client.connectionFailed(u.node.get())) {
                iter.remove();
                AuthenticationException authenticationException = client.authenticationException(u.node.get());
                u.handler.onFailure(currentTimeMs, authenticationException);
            }
        }
    }

    private ClientRequest makeClientRequest(
        final UnsentRequest unsent,
        final Node node,
        final long currentTimeMs
    ) {
        return client.newClientRequest(
            node.idString(),
            unsent.requestBuilder,
            currentTimeMs,
            true,
            (int) unsent.timer.remainingMs(),
            unsent.handler
        );
    }

    public Node leastLoadedNode() {
        return this.client.leastLoadedNode(time.milliseconds()).node();
    }

    public void wakeup() {
        client.wakeup();
    }

    /**
     * Check if the code is disconnected and unavailable for immediate reconnection (i.e. if it is in reconnect
     * backoff window following the disconnect).
     */
    public boolean nodeUnavailable(final Node node) {
        return client.connectionFailed(node) && client.connectionDelay(node, time.milliseconds()) > 0;
    }

    public void close() throws IOException {
        this.client.close();
    }

    public long addAll(PollResult pollResult) {
        Objects.requireNonNull(pollResult);
        addAll(pollResult.unsentRequests);
        return pollResult.timeUntilNextPollMs;
    }

    public void addAll(final List<UnsentRequest> requests) {
        Objects.requireNonNull(requests);
        if (!requests.isEmpty()) {
            requests.forEach(this::add);
        }
    }

    public void add(final UnsentRequest r) {
        Objects.requireNonNull(r);
        r.setTimer(this.time, this.requestTimeoutMs);
        unsentRequests.add(r);
    }

    public static class PollResult {
        public static final long WAIT_FOREVER = Long.MAX_VALUE;
        public static final PollResult EMPTY = new PollResult(WAIT_FOREVER);
        public final long timeUntilNextPollMs;
        public final List<UnsentRequest> unsentRequests;

        public PollResult(final long timeUntilNextPollMs, final List<UnsentRequest> unsentRequests) {
            this.timeUntilNextPollMs = timeUntilNextPollMs;
            this.unsentRequests = Collections.unmodifiableList(unsentRequests);
        }

        public PollResult(final List<UnsentRequest> unsentRequests) {
            this(WAIT_FOREVER, unsentRequests);
        }

        public PollResult(final UnsentRequest unsentRequest) {
            this(Collections.singletonList(unsentRequest));
        }

        public PollResult(final long timeUntilNextPollMs) {
            this(timeUntilNextPollMs, Collections.emptyList());
        }
    }

    public static class UnsentRequest {
        private final AbstractRequest.Builder<?> requestBuilder;
        private final FutureCompletionHandler handler;
        private final Optional<Node> node; // empty if random node can be chosen

        private Timer timer;

        public UnsentRequest(final AbstractRequest.Builder<?> requestBuilder,
                             final Optional<Node> node) {
            Objects.requireNonNull(requestBuilder);
            this.requestBuilder = requestBuilder;
            this.node = node;
            this.handler = new FutureCompletionHandler();
        }

        void setTimer(final Time time, final long requestTimeoutMs) {
            this.timer = time.timer(requestTimeoutMs);
        }

        Timer timer() {
            return timer;
        }

        CompletableFuture<ClientResponse> future() {
            return handler.future;
        }

        FutureCompletionHandler handler() {
            return handler;
        }

        UnsentRequest whenComplete(BiConsumer<ClientResponse, Throwable> callback) {
            handler.future().whenComplete(callback);
            return this;
        }

        AbstractRequest.Builder<?> requestBuilder() {
            return requestBuilder;
        }

        Optional<Node> node() {
            return node;
        }

        @Override
        public String toString() {
            String remainingMs;

            if (timer != null) {
                timer.update();
                remainingMs = String.valueOf(timer.remainingMs());
            } else {
                remainingMs = "<not set>";
            }

            return "UnsentRequest{" +
                    "requestBuilder=" + requestBuilder +
                    ", handler=" + handler +
                    ", node=" + node +
                    ", remainingMs=" + remainingMs +
                    '}';
        }
    }

    public static class FutureCompletionHandler implements RequestCompletionHandler {

        private long responseCompletionTimeMs;
        private final CompletableFuture<ClientResponse> future;

        FutureCompletionHandler() {
            future = new CompletableFuture<>();
        }

        public void onFailure(final long currentTimeMs, final RuntimeException e) {
            this.responseCompletionTimeMs = currentTimeMs;
            if (e != null) {
                this.future.completeExceptionally(e);
            } else {
                this.future.completeExceptionally(DisconnectException.INSTANCE);
            }
        }

        public long completionTimeMs() {
            return responseCompletionTimeMs;
        }

        @Override
        public void onComplete(final ClientResponse response) {
            long completionTimeMs = response.receivedTimeMs();
            if (response.authenticationException() != null) {
                onFailure(completionTimeMs, response.authenticationException());
            } else if (response.wasDisconnected()) {
                onFailure(completionTimeMs, DisconnectException.INSTANCE);
            } else if (response.versionMismatch() != null) {
                onFailure(completionTimeMs, response.versionMismatch());
            } else {
                responseCompletionTimeMs = completionTimeMs;
                this.future.complete(response);
            }
        }

        public CompletableFuture<ClientResponse> future() {
            return future;
        }
    }

    /**
     * Creates a {@link Supplier} for deferred creation during invocation by
     * {@link ConsumerNetworkThread}.
     */
    public static Supplier<NetworkClientDelegate> supplier(final Time time,
                                                           final LogContext logContext,
                                                           final ConsumerMetadata metadata,
                                                           final ConsumerConfig config,
                                                           final ApiVersions apiVersions,
                                                           final Metrics metrics,
                                                           final Sensor throttleTimeSensor,
                                                           final ClientTelemetrySender clientTelemetrySender,
                                                           final BackgroundEventHandler backgroundEventHandler) {
        return new CachedSupplier<>() {
            @Override
            protected NetworkClientDelegate create() {
                KafkaClient client = ClientUtils.createNetworkClient(config,
                        metrics,
                        CONSUMER_METRIC_GROUP_PREFIX,
                        logContext,
                        apiVersions,
                        time,
                        CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION,
                        metadata,
                        throttleTimeSensor,
                        clientTelemetrySender);
                return new NetworkClientDelegate(time, config, logContext, client, metadata, backgroundEventHandler);
            }
        };
    }
}
