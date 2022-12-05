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

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

/**
 * A wrapper around the {@link org.apache.kafka.clients.NetworkClient} to handle poll and send operations.
 */
public class NetworkClientDelegate implements AutoCloseable {
    private final KafkaClient client;
    private final Time time;
    private final Logger log;
    private final int requestTimeoutMs;
    private boolean wakeup = false;
    private final Queue<UnsentRequest> unsentRequests;
    private final Set<Node> activeNodes;

    public NetworkClientDelegate(
            final Time time,
            final ConsumerConfig config,
            final LogContext logContext,
            final KafkaClient client) {
        this.time = time;
        this.client = client;
        this.log = logContext.logger(getClass());
        this.unsentRequests = new ArrayDeque<>();
        this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.activeNodes = new HashSet<>();
    }

    public List<ClientResponse> poll(Timer timer, boolean disableWakeup) {
        final long currentTimeMs = time.milliseconds();
        // 1. Try to send request in the unsentRequests queue. It is either caused by timeout or network error (node
        // not available)
        // 2. poll for the results if there's any.
        // 3. Check connection status for each node, disconnect ones that are not reachable.
        client.wakeup();
        trySend(currentTimeMs);
        List<ClientResponse> res = this.client.poll(timer.timeoutMs(), currentTimeMs);
        checkDisconnects();
        maybeTriggerWakeup(disableWakeup);
        return res;
    }

    /**
     * Walk through the unsentRequests queue and try to perform a client.send() for each unsent request. If the
     * request doesn't have an assigned node, we will find the leastLoadedOne.
     * Here we also register all the nodes to the {@code activeNodes} set, which will then be used to check the
     * connection.
     */
    void trySend(final long currentTimeMs) {
        Queue<UnsentRequest> unsentAndUnreadyRequests = new LinkedList<>();
        while (!unsentRequests.isEmpty()) {
            UnsentRequest unsent = unsentRequests.poll();
            unsent.timer.update(currentTimeMs);
            if (unsent.timer.isExpired()) {
                unsent.callback.ifPresent(c -> c.onFailure(new TimeoutException(
                        "Failed to send request after " + unsent.timer.timeoutMs() + " " + "ms.")));
                continue;
            }

            if (!doSend(unsent, currentTimeMs, unsentAndUnreadyRequests)) {
                log.debug("No broker available to send the request: {}", unsent);
                unsent.callback.ifPresent(v -> v.onFailure(Errors.NETWORK_EXCEPTION.exception(
                        "No node available in the kafka cluster to send the request")));
            }
        }

        if (!unsentAndUnreadyRequests.isEmpty()) {
            // Handle the unready requests in the next event loop
            unsentRequests.addAll(unsentAndUnreadyRequests);
        }
    }

    // Visible for testing
    boolean doSend(final UnsentRequest r,
                   final long currentTimeMs,
                   final Queue<UnsentRequest> unsentAndUnreadyRequests) {
        Node node = r.node.orElse(client.leastLoadedNode(currentTimeMs));
        if (node == null || nodeUnavailable(node)) {
            return false;
        }
        ClientRequest request = makeClientRequest(r, node);
        if (client.isReady(node, currentTimeMs)) {
            activeNodes.add(node);
            client.send(request, currentTimeMs);
        } else {
            // enqueue the request again if the node isn't ready yet. The request will be handled in the next iteration
            // of the event loop
            log.debug("Node is not ready, handle the request in the next event loop: node={}, request={}", node, r);
            unsentAndUnreadyRequests.add(r);
        }
        return true;
    }

    private void checkDisconnects() {
        // Check the connection status by all the nodes that are active. Disconnect the disconnected node if it is
        // unable to be connected.
        Iterator<Node> iter = activeNodes.iterator();
        while (iter.hasNext()) {
            Node node = iter.next();
            iter.remove();
            if (client.connectionFailed(node)) {
                client.disconnect(node.idString());
            }
        }
    }

    private ClientRequest makeClientRequest(UnsentRequest unsent, Node node) {
        return client.newClientRequest(
                node.idString(),
                unsent.abstractBuilder,
                time.milliseconds(),
                true,
                (int) unsent.timer.remainingMs(),
                unsent.callback.orElse(new DefaultRequestFutureCompletionHandler()));
    }

    public void maybeTriggerWakeup(boolean disableWakeup) {
        if (disableWakeup) return;
        if (this.wakeup) {
            this.wakeup = false;
            throw new WakeupException();
        }
    }

    public void wakeup() {
        this.wakeup = true;
        this.client.wakeup();
    }

    public Node leastLoadedNode() {
        return this.client.leastLoadedNode(time.milliseconds());
    }

    public void add(UnsentRequest r) {
        if (r.timer == null)
            r.timer = time.timer(requestTimeoutMs);
        unsentRequests.add(r);
    }

    public void ready(Node node) {
        client.ready(node, time.milliseconds());
    }

    /**
     * Check if the code is disconnected and unavailable for immediate reconnection (i.e. if it is in
     * reconnect backoff window following the disconnect).
     */
    public boolean nodeUnavailable(Node node) {
        return client.connectionFailed(node) && client.connectionDelay(node, time.milliseconds()) > 0;
    }

    public void close() throws IOException {
        this.client.close();
    }

    public void addAll(List<UnsentRequest> unsentRequests) {
        unsentRequests.forEach(this::add);
    }

    public static class PollResult {
        public final long timeMsTillNextPoll;
        public final List<UnsentRequest> unsentRequests;

        public PollResult(final long timeMsTillNextPoll, final List<UnsentRequest> unsentRequests) {
            this.timeMsTillNextPoll = timeMsTillNextPoll;
            this.unsentRequests = Collections.unmodifiableList(unsentRequests);
        }
    }

    public static class UnsentRequest {
        private final Optional<AbstractRequestFutureCompletionHandler> callback;
        private final AbstractRequest.Builder abstractBuilder;
        private Optional<Node> node; // empty if random node can be choosen
        private Timer timer;

        public UnsentRequest(final AbstractRequest.Builder abstractBuilder,
                             final AbstractRequestFutureCompletionHandler callback,
                             final Timer timer) {
            this(abstractBuilder, callback, timer, null);
        }

        public UnsentRequest(final AbstractRequest.Builder abstractBuilder,
                             final AbstractRequestFutureCompletionHandler callback,
                             final Timer timer,
                             final Node node) {
            Objects.requireNonNull(abstractBuilder);
            this.abstractBuilder = abstractBuilder;
            this.node = Optional.ofNullable(node);
            this.callback = Optional.ofNullable(callback);
            this.timer = timer;
        }

        public static UnsentRequest makeUnsentRequest(
                final Timer timeoutTimer,
                final AbstractRequest.Builder<?> requestBuilder,
                final AbstractRequestFutureCompletionHandler callback) {
            return new UnsentRequest(
                    requestBuilder,
                    callback,
                    timeoutTimer);
        }

        @Override
        public String toString() {
            return abstractBuilder.toString();
        }
    }

    public static class DefaultRequestFutureCompletionHandler extends AbstractRequestFutureCompletionHandler {
        @Override
        public void handleResponse(ClientResponse r, Throwable t) {}
    }

    public abstract static class AbstractRequestFutureCompletionHandler implements RequestCompletionHandler {
        private final RequestFuture<ClientResponse> future;

        AbstractRequestFutureCompletionHandler() {
            this.future = new RequestFuture<>();
        }

        abstract public void handleResponse(ClientResponse r, Throwable t);

        public void onFailure(RuntimeException e) {
            future.raise(e);
            handleResponse(null, e);
        }

        @Override
        public void onComplete(ClientResponse response) {
            // TODO: pendingCompletion in the orignal implementation: why did we batch it?
            fireCompletion(response);
            handleResponse(response, null);
        }

        private void fireCompletion(ClientResponse response) {
            if (response.authenticationException() != null) {
                future.raise(response.authenticationException());
            } else if (response.wasDisconnected()) {
                future.raise(DisconnectException.INSTANCE);
            } else if (response.versionMismatch() != null) {
                future.raise(response.versionMismatch());
            } else {
                future.complete(response);
            }
        }
    }

}
