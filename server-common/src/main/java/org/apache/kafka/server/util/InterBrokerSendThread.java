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
package org.apache.kafka.server.util;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.internals.FatalExitError;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

/**
 * An inter-broker send thread that utilizes a non-blocking network client.
 */
public abstract class InterBrokerSendThread extends ShutdownableThread {

    protected volatile KafkaClient networkClient;

    private final int requestTimeoutMs;
    private final Time time;
    private final UnsentRequests unsentRequests;

    public InterBrokerSendThread(
        String name,
        KafkaClient networkClient,
        int requestTimeoutMs,
        Time time
    ) {
        this(name, networkClient, requestTimeoutMs, time, true);
    }

    public InterBrokerSendThread(
        String name,
        KafkaClient networkClient,
        int requestTimeoutMs,
        Time time,
        boolean isInterruptible
    ) {
        super(name, isInterruptible);
        this.networkClient = networkClient;
        this.requestTimeoutMs = requestTimeoutMs;
        this.time = time;
        this.unsentRequests = new UnsentRequests();
    }

    public abstract Collection<RequestAndCompletionHandler> generateRequests();

    public boolean hasUnsentRequests() {
        return unsentRequests.iterator().hasNext();
    }

    @Override
    public void shutdown() throws InterruptedException {
        initiateShutdown();
        networkClient.initiateClose();
        awaitShutdown();
        Utils.closeQuietly(networkClient, "InterBrokerSendThread network client");
    }

    private void drainGeneratedRequests() {
        generateRequests().forEach(request ->
            unsentRequests.put(
                request.destination,
                networkClient.newClientRequest(
                    request.destination.idString(),
                    request.request,
                    request.creationTimeMs,
                    true,
                    requestTimeoutMs,
                    request.handler
                )
            )
        );
    }

    protected void pollOnce(long maxTimeoutMs) {
        try {
            drainGeneratedRequests();
            long now = time.milliseconds();
            final long timeout = sendRequests(now, maxTimeoutMs);
            networkClient.poll(timeout, now);
            now = time.milliseconds();
            checkDisconnects(now);
            failExpiredRequests(now);
            unsentRequests.clean();
        } catch (FatalExitError fee) {
            throw fee;
        } catch (Throwable t) {
            if (t instanceof DisconnectException && !networkClient.active()) {
                // DisconnectException is expected when NetworkClient#initiateClose is called
                return;
            }
            if (t instanceof InterruptedException && !isRunning()) {
                // InterruptedException is expected when shutting down. Throw the error to ShutdownableThread to handle
                throw t;
            }
            log.error("unhandled exception caught in InterBrokerSendThread", t);
            // rethrow any unhandled exceptions as FatalExitError so the JVM will be terminated
            // as we will be in an unknown state with potentially some requests dropped and not
            // being able to make progress. Known and expected Errors should have been appropriately
            // dealt with already.
            throw new FatalExitError();
        }
    }

    @Override
    public void doWork() {
        pollOnce(Long.MAX_VALUE);
    }

    private long sendRequests(long now, long maxTimeoutMs) {
        long pollTimeout = maxTimeoutMs;
        for (Node node : unsentRequests.nodes()) {
            final Iterator<ClientRequest> requestIterator = unsentRequests.requestIterator(node);
            while (requestIterator.hasNext()) {
                final ClientRequest request = requestIterator.next();
                if (networkClient.ready(node, now)) {
                    networkClient.send(request, now);
                    requestIterator.remove();
                } else {
                    pollTimeout = Math.min(pollTimeout, networkClient.connectionDelay(node, now));
                }
            }
        }
        return pollTimeout;
    }

    private void checkDisconnects(long now) {
        // any disconnects affecting requests that have already been transmitted will be handled
        // by NetworkClient, so we just need to check whether connections for any of the unsent
        // requests have been disconnected; if they have, then we complete the corresponding future
        // and set the disconnect flag in the ClientResponse
        final Iterator<Map.Entry<Node, ArrayDeque<ClientRequest>>> iterator = unsentRequests.iterator();
        while (iterator.hasNext()) {
            final Map.Entry<Node, ArrayDeque<ClientRequest>> entry = iterator.next();
            final Node node = entry.getKey();
            final ArrayDeque<ClientRequest> requests = entry.getValue();
            if (!requests.isEmpty() && networkClient.connectionFailed(node)) {
                iterator.remove();
                for (ClientRequest request : requests) {
                    final AuthenticationException authenticationException = networkClient.authenticationException(node);
                    if (authenticationException != null) {
                        log.error("Failed to send the following request due to authentication error: {}", request);
                    }
                    completeWithDisconnect(request, now, authenticationException);
                }
            }
        }
    }

    private void failExpiredRequests(long now) {
        // clear all expired unsent requests
        final Collection<ClientRequest> timedOutRequests = unsentRequests.removeAllTimedOut(now);
        for (ClientRequest request : timedOutRequests) {
            log.debug("Failed to send the following request after {} ms: {}", request.requestTimeoutMs(), request);
            completeWithDisconnect(request, now, null);
        }
    }

    private static void completeWithDisconnect(
        ClientRequest request,
        long now,
        AuthenticationException authenticationException
    ) {
        final RequestCompletionHandler handler = request.callback();
        handler.onComplete(
            new ClientResponse(
                request.makeHeader(request.requestBuilder().latestAllowedVersion()),
                handler,
                request.destination(),
                now /* createdTimeMs */,
                now /* receivedTimeMs */,
                true /* disconnected */,
                null /* versionMismatch */,
                authenticationException,
                null
            )
        );
    }

    public void wakeup() {
        networkClient.wakeup();
    }

    private static final class UnsentRequests {

        private final Map<Node, ArrayDeque<ClientRequest>> unsent = new HashMap<>();

        void put(Node node, ClientRequest request) {
            ArrayDeque<ClientRequest> requests = unsent.computeIfAbsent(node, n -> new ArrayDeque<>());
            requests.add(request);
        }

        Collection<ClientRequest> removeAllTimedOut(long now) {
            final List<ClientRequest> expiredRequests = new ArrayList<>();
            for (ArrayDeque<ClientRequest> requests : unsent.values()) {
                final Iterator<ClientRequest> requestIterator = requests.iterator();
                boolean foundExpiredRequest = false;
                while (requestIterator.hasNext() && !foundExpiredRequest) {
                    final ClientRequest request = requestIterator.next();
                    final long elapsedMs = Math.max(0, now - request.createdTimeMs());
                    if (elapsedMs > request.requestTimeoutMs()) {
                        expiredRequests.add(request);
                        requestIterator.remove();
                        foundExpiredRequest = true;
                    }
                }
            }
            return expiredRequests;
        }

        void clean() {
            unsent.values().removeIf(ArrayDeque::isEmpty);
        }

        Iterator<Entry<Node, ArrayDeque<ClientRequest>>> iterator() {
            return unsent.entrySet().iterator();
        }

        Iterator<ClientRequest> requestIterator(Node node) {
            ArrayDeque<ClientRequest> requests = unsent.get(node);
            return (requests == null) ? Collections.emptyIterator() : requests.iterator();
        }

        Set<Node> nodes() {
            return unsent.keySet();
        }
    }
}
