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
import org.apache.kafka.clients.consumer.ConsumerWakeupException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.utils.Time;

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
 *
 * TODO: The current implementation is simplistic in that it provides a facility for queueing requests
 * prior to delivery, but it makes no effort to retry requests which cannot be sent at the time
 * {@link #poll(long)} is called. This makes the behavior of the queue predictable and easy to
 * understand, but there are opportunities to provide timeout or retry capabilities in the future.
 * How we do this may depend on KAFKA-2120, so for now, we retain the simplistic behavior.
 */
public class ConsumerNetworkClient implements Closeable {
    private final KafkaClient client;
    private final AtomicBoolean wakeup = new AtomicBoolean(false);
    private final DelayedTaskQueue delayedTasks = new DelayedTaskQueue();
    private final Map<Node, List<ClientRequest>> unsent = new HashMap<Node, List<ClientRequest>>();
    private final Metadata metadata;
    private final Time time;
    private final long retryBackoffMs;

    public ConsumerNetworkClient(KafkaClient client,
                                 Metadata metadata,
                                 Time time,
                                 long retryBackoffMs) {
        this.client = client;
        this.metadata = metadata;
        this.time = time;
        this.retryBackoffMs = retryBackoffMs;
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
     * Use the returned future to obtain the result of the send.
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
            nodeUnsent = new ArrayList<ClientRequest>();
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
        if (this.metadata.timeToNextUpdate(time.milliseconds()) == 0)
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
     * @throws ConsumerWakeupException if {@link #wakeup()} is called from another thread
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
     * @throws ConsumerWakeupException if {@link #wakeup()} is called from another thread
     */
    public boolean poll(RequestFuture<?> future, long timeout) {
        long now = time.milliseconds();
        long deadline = now + timeout;
        while (!future.isDone() && now < deadline) {
            poll(deadline - now, now);
            now = time.milliseconds();
        }
        return future.isDone();
    }

    /**
     * Poll for any network IO. All send requests will either be transmitted on the network
     * or failed when this call completes.
     * @param timeout The maximum time to wait for an IO event.
     * @throws ConsumerWakeupException if {@link #wakeup()} is called from another thread
     */
    public void poll(long timeout) {
        poll(timeout, time.milliseconds());
    }

    private void poll(long timeout, long now) {
        // send all the requests we can send now
        pollUnsentRequests(now);
        now = time.milliseconds();
        
        // ensure we don't poll any longer than the deadline for
        // the next scheduled task
        timeout = Math.min(timeout, delayedTasks.nextTimeout(now));
        clientPoll(timeout, now);

        // execute scheduled tasks
        now = time.milliseconds();
        delayedTasks.poll(now);

        // try again to send requests since buffer space may have been
        // cleared or a connect finished in the poll
        pollUnsentRequests(now);

        // fail all requests that couldn't be sent
        clearUnsentRequests();

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

    private void pollUnsentRequests(long now) {
        while (trySend(now)) {
            clientPoll(0, now);
            now = time.milliseconds();
        }
    }

    private void clearUnsentRequests() {
        // clear all unsent requests and fail their corresponding futures
        for (Map.Entry<Node, List<ClientRequest>> requestEntry: unsent.entrySet()) {
            Iterator<ClientRequest> iterator = requestEntry.getValue().iterator();
            while (iterator.hasNext()) {
                ClientRequest request = iterator.next();
                RequestFutureCompletionHandler handler =
                        (RequestFutureCompletionHandler) request.callback();
                handler.raise(SendFailedException.INSTANCE);
                iterator.remove();
            }
        }
        unsent.clear();
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
                } else if (client.connectionFailed(node)) {
                    RequestFutureCompletionHandler handler =
                            (RequestFutureCompletionHandler) request.callback();
                    handler.onComplete(new ClientResponse(request, now, true, null));
                    iterator.remove();
                }
            }
        }
        return requestsSent;
    }

    private void clientPoll(long timeout, long now) {
        client.poll(timeout, now);
        if (wakeup.get()) {
            clearUnsentRequests();
            wakeup.set(false);
            throw new ConsumerWakeupException();
        }
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    public static class RequestFutureCompletionHandler
            extends RequestFuture<ClientResponse>
            implements RequestCompletionHandler {

        @Override
        public void onComplete(ClientResponse response) {
            complete(response);
        }
    }
}
