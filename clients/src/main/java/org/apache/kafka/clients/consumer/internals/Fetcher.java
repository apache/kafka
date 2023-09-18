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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * This class manages the fetching process with the brokers.
 * <p>
 * Thread-safety:
 * Requests and responses of Fetcher may be processed by different threads since heartbeat
 * thread may process responses. Other operations are single-threaded and invoked only from
 * the thread polling the consumer.
 * <ul>
 *     <li>If a response handler accesses any shared state of the Fetcher (e.g. FetchSessionHandler),
 *     all access to that state must be synchronized on the Fetcher instance.</li>
 *     <li>If a response handler accesses any shared state of the coordinator (e.g. SubscriptionState),
 *     it is assumed that all access to that state is synchronized on the coordinator instance by
 *     the caller.</li>
 *     <li>At most one request is pending for each node at any time. Nodes with pending requests are
 *     tracked and updated after processing the response. This ensures that any state (e.g. epoch)
 *     updated while processing responses on one thread are visible while creating the subsequent request
 *     on a different thread.</li>
 * </ul>
 */
public class Fetcher<K, V> extends AbstractFetch<K, V> {

    private final Logger log;
    private final ConsumerNetworkClient client;
    private final FetchCollector<K, V> fetchCollector;

    public Fetcher(LogContext logContext,
                   ConsumerNetworkClient client,
                   ConsumerMetadata metadata,
                   SubscriptionState subscriptions,
                   FetchConfig<K, V> fetchConfig,
                   FetchMetricsManager metricsManager,
                   Time time) {
        super(logContext, metadata, subscriptions, fetchConfig, metricsManager, time);
        this.log = logContext.logger(Fetcher.class);
        this.client = client;
        this.fetchCollector = new FetchCollector<>(logContext,
                metadata,
                subscriptions,
                fetchConfig,
                metricsManager,
                time);
    }

    @Override
    protected boolean isUnavailable(Node node) {
        return client.isUnavailable(node);
    }

    @Override
    protected void maybeThrowAuthFailure(Node node) {
        client.maybeThrowAuthFailure(node);
    }

    public void clearBufferedDataForUnassignedPartitions(Collection<TopicPartition> assignedPartitions) {
        fetchBuffer.retainAll(new HashSet<>(assignedPartitions));
    }

    /**
     * Set-up a fetch request for any node that we have assigned partitions for which doesn't already have
     * an in-flight fetch or pending fetch data.
     * @return number of fetches sent
     */
    public synchronized int sendFetches() {
        Map<Node, FetchSessionHandler.FetchRequestData> fetchRequestMap = prepareFetchRequests();


        for (Map.Entry<Node, FetchSessionHandler.FetchRequestData> entry : fetchRequestMap.entrySet()) {
            final Node fetchTarget = entry.getKey();
            final FetchSessionHandler.FetchRequestData data = entry.getValue();
            final FetchRequest.Builder request = createFetchRequest(fetchTarget, data);
            RequestFutureListener<ClientResponse> listener = new RequestFutureListener<ClientResponse>() {
                @Override
                public void onSuccess(ClientResponse resp) {
                    synchronized (Fetcher.this) {
                        handleFetchResponse(fetchTarget, data, resp);
                    }
                }

                @Override
                public void onFailure(RuntimeException e) {
                    synchronized (Fetcher.this) {
                        handleFetchResponse(fetchTarget, e);
                    }
                }
            };

            final RequestFuture<ClientResponse> future = client.send(fetchTarget, request);
            future.addListener(listener);
        }

        return fetchRequestMap.size();
    }

    protected void maybeCloseFetchSessions(final Timer timer) {
        final List<RequestFuture<ClientResponse>> requestFutures = new ArrayList<>();
        Map<Node, FetchSessionHandler.FetchRequestData> fetchRequestMap = prepareCloseFetchSessionRequests();

        for (Map.Entry<Node, FetchSessionHandler.FetchRequestData> entry : fetchRequestMap.entrySet()) {
            final Node fetchTarget = entry.getKey();
            final FetchSessionHandler.FetchRequestData data = entry.getValue();
            final FetchRequest.Builder request = createFetchRequest(fetchTarget, data);
            final RequestFuture<ClientResponse> responseFuture = client.send(fetchTarget, request);

            responseFuture.addListener(new RequestFutureListener<ClientResponse>() {
                @Override
                public void onSuccess(ClientResponse value) {
                    handleCloseFetchSessionResponse(fetchTarget, data);
                }

                @Override
                public void onFailure(RuntimeException e) {
                    handleCloseFetchSessionResponse(fetchTarget, data, e);
                }
            });

            requestFutures.add(responseFuture);
        }

        // Poll to ensure that request has been written to the socket. Wait until either the timer has expired or until
        // all requests have received a response.
        while (timer.notExpired() && !requestFutures.stream().allMatch(RequestFuture::isDone)) {
            client.poll(timer, null, true);
        }

        if (!requestFutures.stream().allMatch(RequestFuture::isDone)) {
            // we ran out of time before completing all futures. It is ok since we don't want to block the shutdown
            // here.
            log.debug("All requests couldn't be sent in the specific timeout period {}ms. " +
                    "This may result in unnecessary fetch sessions at the broker. Consider increasing the timeout passed for " +
                    "KafkaConsumer.close(Duration timeout)", timer.timeoutMs());
        }
    }

    public Fetch<K, V> collectFetch() {
        return fetchCollector.collectFetch(fetchBuffer);
    }

    /**
     * This method is called by {@link #close(Timer)} which is guarded by the {@link IdempotentCloser}) such as to only
     * be executed once the first time that any of the {@link #close()} methods are called. Subclasses can override
     * this method without the need for extra synchronization at the instance level.
     *
     * @param timer Timer to enforce time limit
     */
    // Visible for testing
    protected void closeInternal(Timer timer) {
        // we do not need to re-enable wake-ups since we are closing already
        client.disableWakeups();
        maybeCloseFetchSessions(timer);
        super.closeInternal(timer);
    }
}