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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.errors.InterruptException;

/**
 * Higher level consumer access to the network layer with basic support for request futures. This class
 * is thread-safe, but provides no synchronization for response callbacks. This guarantees that no locks
 * are held when they are invoked.
 */
public class ConsumerNetworkClient implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(ConsumerNetworkClient.class);
    private static final long MAX_POLL_TIMEOUT_MS = 5000L;

    // the mutable state of this class is protected by the object's monitor (excluding the wakeup
    // flag and the request completion queue below).
    private final KafkaClient client;
    private final Map<Node, List<ClientRequest>> unsent = new HashMap<>();
    private final Metadata metadata;
    private final Time time;
    private final long retryBackoffMs;
    private final long unsentExpiryMs;
    private int wakeupDisabledCount = 0;

    // when requests complete, they are transferred to this queue prior to invocation. The purpose
    // is to avoid invoking them while holding this object's monitor which can open the door for deadlocks.
    private final ConcurrentLinkedQueue<RequestFutureCompletionHandler> pendingCompletion = new ConcurrentLinkedQueue<>();

    // this flag allows the client to be safely woken up without waiting on the lock above. It is
    // atomic to avoid the need to acquire the lock above in order to enable it concurrently.
    private final AtomicBoolean wakeup = new AtomicBoolean(false);

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
     * Send a new request. Note that the request is not actually transmitted on the
     * network until one of the {@link #poll(long)} variants is invoked. At this
     * point the request will either be transmitted successfully or will fail.
     * Use the returned future to obtain the result of the send. Note that there is no
     * need to check for disconnects explicitly on the {@link ClientResponse} object;
     * instead, the future will be failed with a {@link DisconnectException}.
     *
     * @param node The destination of the request
     * @param requestBuilder A builder for the request payload
     * @return A future which indicates the result of the send.
     */
    public RequestFuture<ClientResponse> send(Node node, AbstractRequest.Builder<?> requestBuilder) {
        long now = time.milliseconds();
        RequestFutureCompletionHandler completionHandler = new RequestFutureCompletionHandler();
        ClientRequest clientRequest = client.newClientRequest(node.idString(), requestBuilder, now, true,
                completionHandler);
        put(node, clientRequest);

        // wakeup the client in case it is blocking in poll so that we can send the queued request
        client.wakeup();
        return completionHandler.future;
    }

    private void put(Node node, ClientRequest request) {
        synchronized (this) {
            List<ClientRequest> nodeUnsent = unsent.get(node);
            if (nodeUnsent == null) {
                nodeUnsent = new ArrayList<>();
                unsent.put(node, nodeUnsent);
            }
            nodeUnsent.add(request);
        }
    }

    public Node leastLoadedNode() {
        synchronized (this) {
            return client.leastLoadedNode(time.milliseconds());
        }
    }

    /**
     * Block until the metadata has been refreshed.
     */
    public void awaitMetadataUpdate() {
        awaitMetadataUpdate(Long.MAX_VALUE);
    }

    /**
     * Block waiting on the metadata refresh with a timeout.
     *
     * @return true if update succeeded, false otherwise.
     */
    public boolean awaitMetadataUpdate(long timeout) {
        long startMs = time.milliseconds();
        int version = this.metadata.requestUpdate();
        do {
            poll(timeout);
        } while (this.metadata.version() == version && time.milliseconds() - startMs < timeout);
        return this.metadata.version() > version;
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
        // wakeup should be safe without holding the client lock since it simply delegates to
        // Selector's wakeup, which is threadsafe
        log.trace("Received user wakeup");
        this.wakeup.set(true);
        this.client.wakeup();
    }

    /**
     * Block indefinitely until the given request future has finished.
     * @param future The request future to await.
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public void poll(RequestFuture<?> future) {
        while (!future.isDone())
            poll(MAX_POLL_TIMEOUT_MS, time.milliseconds(), future);
    }

    /**
     * Block until the provided request future request has finished or the timeout has expired.
     * @param future The request future to wait for
     * @param timeout The maximum duration (in ms) to wait for the request
     * @return true if the future is done, false otherwise
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public boolean poll(RequestFuture<?> future, long timeout) {
        long begin = time.milliseconds();
        long remaining = timeout;
        long now = begin;
        do {
            poll(remaining, now, future);
            now = time.milliseconds();
            long elapsed = now - begin;
            remaining = timeout - elapsed;
        } while (!future.isDone() && remaining > 0);
        return future.isDone();
    }

    /**
     * Poll for any network IO.
     * @param timeout The maximum time to wait for an IO event.
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public void poll(long timeout) {
        poll(timeout, time.milliseconds(), null);
    }

    /**
     * Poll for any network IO.
     * @param timeout timeout in milliseconds
     * @param now current time in milliseconds
     */
    public void poll(long timeout, long now, PollCondition pollCondition) {
        // there may be handlers which need to be invoked if we woke up the previous call to poll
        firePendingCompletedRequests();

        synchronized (this) {
            // send all the requests we can send now
            trySend(now);

            // check whether the poll is still needed by the caller. Note that if the expected completion
            // condition becomes satisfied after the call to shouldBlock() (because of a fired completion
            // handler), the client will be woken up.
            if (pollCondition == null || pollCondition.shouldBlock()) {
                // if there are no requests in flight, do not block longer than the retry backoff
                if (client.inFlightRequestCount() == 0)
                    timeout = Math.min(timeout, retryBackoffMs);
                client.poll(Math.min(MAX_POLL_TIMEOUT_MS, timeout), now);
                now = time.milliseconds();
            } else {
                client.poll(0, now);
            }

            // handle any disconnects by failing the active requests. note that disconnects must
            // be checked immediately following poll since any subsequent call to client.ready()
            // will reset the disconnect status
            checkDisconnects(now);

            // trigger wakeups after checking for disconnects so that the callbacks will be ready
            // to be fired on the next call to poll()
            maybeTriggerWakeup();
            
            // throw InterruptException if this thread is interrupted
            maybeThrowInterruptException();

            // try again to send requests since buffer space may have been
            // cleared or a connect finished in the poll
            trySend(now);

            // fail requests that couldn't be sent if they have expired
            failExpiredRequests(now);
        }

        // called without the lock to avoid deadlock potential if handlers need to acquire locks
        firePendingCompletedRequests();
    }

    /**
     * Poll for network IO and return immediately. This will not trigger wakeups,
     * nor will it execute any delayed tasks.
     */
    public void pollNoWakeup() {
        disableWakeups();
        try {
            poll(0, time.milliseconds(), null);
        } finally {
            enableWakeups();
        }
    }

    /**
     * Block until all pending requests from the given node have finished.
     * @param node The node to await requests from
     * @param timeoutMs The maximum time in milliseconds to block
     * @return true If all requests finished, false if the timeout expired first
     */
    public boolean awaitPendingRequests(Node node, long timeoutMs) {
        long startMs = time.milliseconds();
        long remainingMs = timeoutMs;

        while (pendingRequestCount(node) > 0 && remainingMs > 0) {
            poll(remainingMs);
            remainingMs = timeoutMs - (time.milliseconds() - startMs);
        }

        return pendingRequestCount(node) == 0;
    }

    /**
     * Get the count of pending requests to the given node. This includes both request that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * @param node The node in question
     * @return The number of pending requests
     */
    public int pendingRequestCount(Node node) {
        synchronized (this) {
            List<ClientRequest> pending = unsent.get(node);
            int unsentCount = pending == null ? 0 : pending.size();
            return unsentCount + client.inFlightRequestCount(node.idString());
        }
    }

    /**
     * Get the total count of pending requests from all nodes. This includes both requests that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * @return The total count of pending requests
     */
    public int pendingRequestCount() {
        synchronized (this) {
            int total = 0;
            for (List<ClientRequest> requests: unsent.values())
                total += requests.size();
            return total + client.inFlightRequestCount();
        }
    }

    private void firePendingCompletedRequests() {
        boolean completedRequestsFired = false;
        for (;;) {
            RequestFutureCompletionHandler completionHandler = pendingCompletion.poll();
            if (completionHandler == null)
                break;

            completionHandler.fireCompletion();
            completedRequestsFired = true;
        }

        // wakeup the client in case it is blocking in poll for this future's completion
        if (completedRequestsFired)
            client.wakeup();
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
                    RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) request.callback();
                    handler.onComplete(new ClientResponse(request.makeHeader(), request.callback(), request.destination(),
                            request.createdTimeMs(), now, true, null, null));
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
                    RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) request.callback();
                    handler.onFailure(new TimeoutException("Failed to send request after " + unsentExpiryMs + " ms."));
                    requestIterator.remove();
                } else
                    break;
            }
            if (requestEntry.getValue().isEmpty())
                iterator.remove();
        }
    }

    public void failUnsentRequests(Node node, RuntimeException e) {
        // clear unsent requests to node and fail their corresponding futures
        synchronized (this) {
            List<ClientRequest> unsentRequests = unsent.remove(node);
            if (unsentRequests != null) {
                for (ClientRequest unsentRequest : unsentRequests) {
                    RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) unsentRequest.callback();
                    handler.onFailure(e);
                }
            }
        }

        // called without the lock to avoid deadlock potential
        firePendingCompletedRequests();
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

    private void maybeTriggerWakeup() {
        if (wakeupDisabledCount == 0 && wakeup.get()) {
            log.trace("Raising wakeup exception in response to user wakeup");
            wakeup.set(false);
            throw new WakeupException();
        }
    }
    
    private void maybeThrowInterruptException() {
        if (Thread.interrupted()) {
            throw new InterruptException(new InterruptedException());
        }
    }

    public void disableWakeups() {
        synchronized (this) {
            wakeupDisabledCount++;
        }
    }

    public void enableWakeups() {
        synchronized (this) {
            if (wakeupDisabledCount <= 0)
                throw new IllegalStateException("Cannot enable wakeups since they were never disabled");

            wakeupDisabledCount--;

            // re-wakeup the client if the flag was set since previous wake-up call
            // could be cleared by poll(0) while wakeups were disabled
            if (wakeupDisabledCount == 0 && wakeup.get())
                this.client.wakeup();
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            client.close();
        }
    }

    /**
     * Find whether a previous connection has failed. Note that the failure state will persist until either
     * {@link #tryConnect(Node)} or {@link #send(Node, AbstractRequest.Builder)} has been called.
     * @param node Node to connect to if possible
     */
    public boolean connectionFailed(Node node) {
        synchronized (this) {
            return client.connectionFailed(node);
        }
    }

    /**
     * Initiate a connection if currently possible. This is only really useful for resetting the failed
     * status of a socket. If there is an actual request to send, then {@link #send(Node, AbstractRequest.Builder)}
     * should be used.
     * @param node The node to connect to
     */
    public void tryConnect(Node node) {
        synchronized (this) {
            client.ready(node, time.milliseconds());
        }
    }

    public class RequestFutureCompletionHandler implements RequestCompletionHandler {
        private final RequestFuture<ClientResponse> future;
        private ClientResponse response;
        private RuntimeException e;

        public RequestFutureCompletionHandler() {
            this.future = new RequestFuture<>();
        }

        public void fireCompletion() {
            if (e != null) {
                future.raise(e);
            } else if (response.wasDisconnected()) {
                RequestHeader requestHeader = response.requestHeader();
                ApiKeys api = ApiKeys.forId(requestHeader.apiKey());
                int correlation = requestHeader.correlationId();
                log.debug("Cancelled {} request {} with correlation id {} due to node {} being disconnected",
                        api, requestHeader, correlation, response.destination());
                future.raise(DisconnectException.INSTANCE);
            } else if (response.versionMismatch() != null) {
                future.raise(response.versionMismatch());
            } else {
                future.complete(response);
            }
        }

        public void onFailure(RuntimeException e) {
            this.e = e;
            pendingCompletion.add(this);
        }

        @Override
        public void onComplete(ClientResponse response) {
            this.response = response;
            pendingCompletion.add(this);
        }
    }

    /**
     * When invoking poll from a multi-threaded environment, it is possible that the condition that
     * the caller is awaiting has already been satisfied prior to the invocation of poll. We therefore
     * introduce this interface to push the condition checking as close as possible to the invocation
     * of poll. In particular, the check will be done while holding the lock used to protect concurrent
     * access to {@link org.apache.kafka.clients.NetworkClient}, which means implementations must be
     * very careful about locking order if the callback must acquire additional locks.
     */
    public interface PollCondition {
        /**
         * Return whether the caller is still awaiting an IO event.
         * @return true if so, false otherwise.
         */
        boolean shouldBlock();
    }

}
