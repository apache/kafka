/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Higher level consumer access to the network layer with basic support for futures and
 * task scheduling. NOT thread-safe!
 */
public class ConsumerNetworkClient implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(ConsumerNetworkClient.class);

    private final KafkaClient client;
    private final AtomicBoolean wakeup = new AtomicBoolean(false);
    private final DelayedTaskQueue delayedTasks = new DelayedTaskQueue();
    private final Map<Node, List<ClientRequest>> unsent = new HashMap<>();
    private final Metadata metadata;
    private final Time time;
    private final long retryBackoffMs;
    private final long unsentExpiryMs;
    // wakeup enabled flag need to be volatile since it is allowed to be accessed concurrently
    volatile private boolean wakeupsEnabled = true;

    public ConsumerNetworkClient(KafkaClient client,
                                 Metadata metadata,
                                 Time time,
                                 long retryBackoffMs,
                                 long requestTimeoutMs) {
        this.client = client;
        this.metadata = metadata;
        this.time = time;
        this.retryBackoffMs = retryBackoffMs;
        this.unsentExpiryMs = requestTimeoutMs;
    }

    /**
     * Schedule a new task to be executed at the given time. This is "best-effort" scheduling and
     * should only be used for coarse synchronization.
     * @param task The task to be scheduled
     * @param at The time it should run
     */
    public void schedule(DelayedTask task, long at) {
        delayedTasks.add(task, at);
    }

    /**
     * Unschedule a task. This will remove all instances of the task from the task queue.
     * This is a no-op if the task is not scheduled.
     * @param task The task to be unscheduled.
     */
    public void unschedule(DelayedTask task) {
        delayedTasks.remove(task);
    }

    /**
     * Send a new request. Note that the request is not actually transmitted on the
     * network until one of the {@link #poll(long)} variants is invoked. At this
     * point the request will either be transmitted successfully or will fail.
     * Use the returned future to obtain the result of the send. Note that there is no
     * need to check for disconnects explicitly on the {@link ClientResponse} object;
     * instead, the future will be failed with a {@link DisconnectException}.
     * @param node The destination of the request
     * @param api The Kafka API call
     * @param request The request payload
     * @return A future which indicates the result of the send.
     */
    public RequestFuture<ClientResponse> send(Node node,
                                              ApiKeys api,
                                              AbstractRequest request) {
        long now = time.milliseconds();
        RequestFutureCompletionHandler future = new RequestFutureCompletionHandler();
        RequestHeader header = client.nextRequestHeader(api);
        RequestSend send = new RequestSend(node.idString(), header, request.toStruct());
        put(node, new ClientRequest(now, true, send, future));
        return future;
    }

    private void put(Node node, ClientRequest request) {
        List<ClientRequest> nodeUnsent = unsent.get(node);
        if (nodeUnsent == null) {
            nodeUnsent = new ArrayList<>();
            unsent.put(node, nodeUnsent);
        }
        nodeUnsent.add(request);
    }

    public Node leastLoadedNode() {
        return client.leastLoadedNode(time.milliseconds());
    }

    /**
     * Block until the metadata has been refreshed.
     */
    public void awaitMetadataUpdate() {
        int version = this.metadata.requestUpdate();
        do {
            poll(Long.MAX_VALUE);
        } while (this.metadata.version() == version);
    }

    /**
     * Ensure our metadata is fresh (if an update is expected, this will block
     * until it has completed).
     */
    public void ensureFreshMetadata() {
        if (this.metadata.updateRequested() || this.metadata.timeToNextUpdate(time.milliseconds()) == 0)
            awaitMetadataUpdate();
    }

    /**
     * Wakeup an active poll. This will cause the polling thread to throw an exception either
     * on the current poll if one is active, or the next poll.
     */
    public void wakeup() {
        this.wakeup.set(true);
        this.client.wakeup();
    }

    /**
     * Block indefinitely until the given request future has finished.
     * @param future The request future to await.
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     */
    public void poll(RequestFuture<?> future) {
        while (!future.isDone())
            poll(Long.MAX_VALUE);
    }

    /**
     * Block until the provided request future request has finished or the timeout has expired.
     * @param future The request future to wait for
     * @param timeout The maximum duration (in ms) to wait for the request
     * @return true if the future is done, false otherwise
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     */
    public boolean poll(RequestFuture<?> future, long timeout) {
        long begin = time.milliseconds();
        long remaining = timeout;
        long now = begin;
        do {
            poll(remaining, now, true);
            now = time.milliseconds();
            long elapsed = now - begin;
            remaining = timeout - elapsed;
        } while (!future.isDone() && remaining > 0);
        return future.isDone();
    }

    /**
     * Poll for any network IO. All send requests will either be transmitted on the network
     * or failed when this call completes.
     * @param timeout The maximum time to wait for an IO event.
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     */
    public void poll(long timeout) {
        poll(timeout, time.milliseconds(), true);
    }

    /**
     * Poll for network IO and return immediately. This will not trigger wakeups,
     * nor will it execute any delayed tasks.
     * @param executeDelayedTasks Whether to allow delayed task execution (true allows)
     */
    public void quickPoll(boolean executeDelayedTasks) {
        disableWakeups();
        poll(0, time.milliseconds(), executeDelayedTasks);
        enableWakeups();
    }

    private void poll(long timeout, long now, boolean executeDelayedTasks) {
        // send all the requests we can send now
        trySend(now);

        // ensure we don't poll any longer than the deadline for
        // the next scheduled task
        timeout = Math.min(timeout, delayedTasks.nextTimeout(now));
        clientPoll(timeout, now);
        now = time.milliseconds();

        // handle any disconnects by failing the active requests. note that disconnects must
        // be checked immediately following poll since any subsequent call to client.ready()
        // will reset the disconnect status
        checkDisconnects(now);

        // execute scheduled tasks
        if (executeDelayedTasks)
            delayedTasks.poll(now);

        // try again to send requests since buffer space may have been
        // cleared or a connect finished in the poll
        trySend(now);

        // fail requests that couldn't be sent if they have expired
        failExpiredRequests(now);
    }

    /**
     * Block until all pending requests from the given node have finished.
     * @param node The node to await requests from
     */
    public void awaitPendingRequests(Node node) {
        while (pendingRequestCount(node) > 0)
            poll(retryBackoffMs);
    }

    /**
     * Get the count of pending requests to the given node. This includes both request that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * @param node The node in question
     * @return The number of pending requests
     */
    public int pendingRequestCount(Node node) {
        List<ClientRequest> pending = unsent.get(node);
        int unsentCount = pending == null ? 0 : pending.size();
        return unsentCount + client.inFlightRequestCount(node.idString());
    }

    /**
     * Get the total count of pending requests from all nodes. This includes both requests that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * @return The total count of pending requests
     */
    public int pendingRequestCount() {
        int total = 0;
        for (List<ClientRequest> requests: unsent.values())
            total += requests.size();
        return total + client.inFlightRequestCount();
    }

    private void checkDisconnects(long now) {
        // any disconnects affecting requests that have already been transmitted will be handled
        // by NetworkClient, so we just need to check whether connections for any of the unsent
        // requests have been disconnected; if they have, then we complete the corresponding future
        // and set the disconnect flag in the ClientResponse
        Iterator<Map.Entry<Node, List<ClientRequest>>> iterator = unsent.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Node, List<ClientRequest>> requestEntry = iterator.next();
            Node node = requestEntry.getKey();
            if (client.connectionFailed(node)) {
                // Remove entry before invoking request callback to avoid callbacks handling
                // coordinator failures traversing the unsent list again.
                iterator.remove();
                for (ClientRequest request : requestEntry.getValue()) {
                    RequestFutureCompletionHandler handler =
                            (RequestFutureCompletionHandler) request.callback();
                    handler.onComplete(new ClientResponse(request, now, true, null));
                }
            }
        }
    }

    private void failExpiredRequests(long now) {
        // clear all expired unsent requests and fail their corresponding futures
        Iterator<Map.Entry<Node, List<ClientRequest>>> iterator = unsent.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Node, List<ClientRequest>> requestEntry = iterator.next();
            Iterator<ClientRequest> requestIterator = requestEntry.getValue().iterator();
            while (requestIterator.hasNext()) {
                ClientRequest request = requestIterator.next();
                if (request.createdTimeMs() < now - unsentExpiryMs) {
                    RequestFutureCompletionHandler handler =
                            (RequestFutureCompletionHandler) request.callback();
                    handler.raise(new TimeoutException("Failed to send request after " + unsentExpiryMs + " ms."));
                    requestIterator.remove();
                } else
                    break;
            }
            if (requestEntry.getValue().isEmpty())
                iterator.remove();
        }
    }

    protected void failUnsentRequests(Node node, RuntimeException e) {
        // clear unsent requests to node and fail their corresponding futures
        List<ClientRequest> unsentRequests = unsent.remove(node);
        if (unsentRequests != null) {
            for (ClientRequest request : unsentRequests) {
                RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) request.callback();
                handler.raise(e);
            }
        }
    }

    private boolean trySend(long now) {
        // send any requests that can be sent now
        boolean requestsSent = false;
        for (Map.Entry<Node, List<ClientRequest>> requestEntry: unsent.entrySet()) {
            Node node = requestEntry.getKey();
            Iterator<ClientRequest> iterator = requestEntry.getValue().iterator();
            while (iterator.hasNext()) {
                ClientRequest request = iterator.next();
                if (client.ready(node, now)) {
                    client.send(request, now);
                    iterator.remove();
                    requestsSent = true;
                }
            }
        }
        return requestsSent;
    }

    private void clientPoll(long timeout, long now) {
        client.poll(timeout, now);
        if (wakeupsEnabled && wakeup.get()) {
            wakeup.set(false);
            throw new WakeupException();
        }
    }

    public void disableWakeups() {
        this.wakeupsEnabled = false;
    }

    public void enableWakeups() {
        this.wakeupsEnabled = true;

        // re-wakeup the client if the flag was set since previous wake-up call
        // could be cleared by poll(0) while wakeups were disabled
        if (wakeup.get())
            this.client.wakeup();
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    /**
     * Find whether a previous connection has failed. Note that the failure state will persist until either
     * {@link #tryConnect(Node)} or {@link #send(Node, ApiKeys, AbstractRequest)} has been called.
     * @param node Node to connect to if possible
     */
    public boolean connectionFailed(Node node) {
        return client.connectionFailed(node);
    }

    /**
     * Initiate a connection if currently possible. This is only really useful for resetting the failed
     * status of a socket. If there is an actual request to send, then {@link #send(Node, ApiKeys, AbstractRequest)}
     * should be used.
     * @param node The node to connect to
     */
    public void tryConnect(Node node) {
        client.ready(node, time.milliseconds());
    }

    public static class RequestFutureCompletionHandler
            extends RequestFuture<ClientResponse>
            implements RequestCompletionHandler {

        @Override
        public void onComplete(ClientResponse response) {
            if (response.wasDisconnected()) {
                ClientRequest request = response.request();
                RequestSend send = request.request();
                ApiKeys api = ApiKeys.forId(send.header().apiKey());
                int correlation = send.header().correlationId();
                log.debug("Cancelled {} request {} with correlation id {} due to node {} being disconnected",
                        api, request, correlation, send.destination());
                raise(DisconnectException.INSTANCE);
            } else {
                complete(response);
            }
        }
    }
}
