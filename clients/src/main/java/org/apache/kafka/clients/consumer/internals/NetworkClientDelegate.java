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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;

/**
 * A wrapper around the {@link org.apache.kafka.clients.NetworkClient} to handle poll and send operations.
 */
public class NetworkClientDelegate implements AutoCloseable {
    private final KafkaClient client;
    private final Time time;
    private final Logger log;
    private boolean wakeup = false;
    private final Queue<UnsentRequest> unsentRequests;

    public NetworkClientDelegate(
            final Time time,
            final LogContext logContext,
            final KafkaClient client) {
        this.time = time;
        this.client = client;
        this.log = logContext.logger(getClass());
        this.unsentRequests = new ArrayDeque<>();
    }

    public List<ClientResponse> poll(Timer timer, boolean disableWakeup) {
        if (!disableWakeup) {
            // trigger wakeups after checking for disconnects so that the callbacks will be ready
            // to be fired on the next call to poll()
            maybeTriggerWakeup();
        }

        trySend();
        return this.client.poll(timer.timeoutMs(), time.milliseconds());
    }

    private void trySend() {
        while (unsentRequests.size() > 0) {
            UnsentRequest unsent = unsentRequests.poll();
            if (unsent.timer.isExpired()) {
                // TODO: expired request should be marked
                unsent.callback.ifPresent(c -> c.onFailure(new TimeoutException(
                        "Failed to send request after " + unsent.timer.timeoutMs() + " " + "ms.")));
                continue;
            }

            doSend(unsent);
        }
    }

    static boolean isReady(KafkaClient client, Node node, long currentTime) {
        client.poll(0, currentTime);
        return client.isReady(node, currentTime);
    }

    public void doSend(UnsentRequest r) {
        long now = time.milliseconds();
        Node node = r.node.orElse(client.leastLoadedNode(now));
        ClientRequest request = makeClientRequest(r, node);
        // TODO: Sounds like we need to check disconnections for each node and complete the request with
        //  authentication error
        if (isReady(client, node, now)) {
            client.send(request, now);
        }
    }

    private ClientRequest makeClientRequest(UnsentRequest unsent, Node node) {
        return client.newClientRequest(
                node.idString(),
                unsent.abstractBuilder,
                time.milliseconds(),
                true,
                (int) unsent.timer.remainingMs(),
                unsent.callback.orElse(new RequestFutureCompletionHandlerBase()));
    }

    public List<ClientResponse> poll() {
        return this.poll(time.timer(0), false);
    }

    public void maybeTriggerWakeup() {
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

    public void tryDisconnect(Optional<Node> coordinator) {
        coordinator.ifPresent(node -> client.disconnect(node.idString()));
    }

    public void close() throws IOException {
        this.client.close();
    }

    public static class UnsentRequest {
        private final Optional<RequestFutureCompletionHandlerBase> callback;
        private final AbstractRequest.Builder abstractBuilder;
        private final Optional<Node> node; // empty if random node can be choosen
        private final Timer timer;

        public UnsentRequest(final Timer timer,
                             final AbstractRequest.Builder abstractBuilder,
                             final RequestFutureCompletionHandlerBase callback) {
            this(timer, abstractBuilder, callback, null);
        }

        public UnsentRequest(final Timer timer,
                             final AbstractRequest.Builder abstractBuilder,
                             final RequestFutureCompletionHandlerBase callback,
                             final Node node) {
            Objects.requireNonNull(abstractBuilder);
            this.abstractBuilder = abstractBuilder;
            this.node = Optional.ofNullable(node);
            this.callback = Optional.ofNullable(callback);
            this.timer = timer;
        }
    }

    public static class RequestFutureCompletionHandlerBase implements RequestCompletionHandler {
        private final RequestFuture<ClientResponse> future;
        private ClientResponse response;
        private RuntimeException e;

        RequestFutureCompletionHandlerBase() {
            this.future = new RequestFuture<>();
        }

        public void fireCompletion() {
            if (e != null) {
                future.raise(e);
            } else if (response.authenticationException() != null) {
                future.raise(response.authenticationException());
            } else if (response.wasDisconnected()) {
                //log.debug("Cancelled request with header {} due to node {} being disconnected", response
                // .requestHeader(), response.destination());
                future.raise(DisconnectException.INSTANCE);
            } else if (response.versionMismatch() != null) {
                future.raise(response.versionMismatch());
            } else {
                future.complete(response);
            }
        }

        public void onFailure(RuntimeException e) {
            this.e = e;
            fireCompletion();
            handleResponse(response, e);
        }

        public void handleResponse(ClientResponse r, Throwable t) {}

        @Override
        public void onComplete(ClientResponse response) {
            this.response = response;
            // TODO: pendingCompletion in the orignal implementation: why did we batch it?
            fireCompletion();
            handleResponse(response, null);
        }
    }

}
