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

import java.util.ArrayDeque;
import java.util.Queue;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.requests.FetchRequest;

import java.util.Map;
import org.apache.kafka.common.requests.FetchResponse;
import org.slf4j.Logger;

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
 *     <li>Responses that collate partial responses from multiple brokers (e.g. to list offsets) are
 *     synchronized on the response future.</li>
 *     <li>At most one request is pending for each node at any time. Nodes with pending requests are
 *     tracked and updated after processing the response. This ensures that any state (e.g. epoch)
 *     updated while processing responses on one thread are visible while creating the subsequent request
 *     on a different thread.</li>
 * </ul>
 */
public class Fetcher<K, V> extends AbstractFetcher<K, V> {

    private final Logger log;
    private final ConsumerNetworkClient client;

    public Fetcher(FetchContext<K, V> fetchContext,
                   ApiVersions apiVersions,
                   ConsumerNetworkClient client) {
        super(fetchContext, apiVersions);
        this.log = fetchContext.logContext.logger(Fetcher.class);
        this.client = client;
    }

    @Override
    protected boolean isUnavailable(final Node node) {
        return client.isUnavailable(node);
    }

    @Override
    protected void maybeThrowAuthFailure(final Node node) {
        client.maybeThrowAuthFailure(node);
    }

    /**
     * Return whether we have any completed fetches pending return to the user. This method is thread-safe. Has
     * visibility for testing.
     * @return true if there are completed fetches, false otherwise
     */
    protected boolean hasCompletedFetches() {
        return !completedFetches.isEmpty();
    }

    /**
     * Return whether we have any completed fetches that are fetchable. This method is thread-safe.
     * @return true if there are completed fetches that can be returned, false otherwise
     */
    public boolean hasAvailableFetches(SubscriptionState subscriptions) {
        return completedFetches.stream().anyMatch(fetch -> subscriptions.isFetchable(fetch.partition()));
    }

    /**
     * Set-up a fetch request for any node that we have assigned partitions for which doesn't already have
     * an in-flight fetch or pending fetch data.
     * @return number of fetches sent
     */
    public synchronized int sendFetches(Metadata metadata, SubscriptionState subscriptions) {
        Map<Node, FetchSessionHandler.FetchRequestData> fetchRequestMap = prepareFetchRequests(metadata, subscriptions);

        for (Map.Entry<Node, FetchSessionHandler.FetchRequestData> entry : fetchRequestMap.entrySet()) {
            final Node fetchTarget = entry.getKey();
            final FetchSessionHandler.FetchRequestData data = entry.getValue();
            final FetchRequest.Builder request = createFetchRequest(data);

            if (log.isDebugEnabled())
                log.debug("Sending {} {} to broker {}", fetchContext.isolationLevel, data, fetchTarget);

            RequestFuture<ClientResponse> future = client.send(fetchTarget, request);
            // We add the node to the set of nodes with pending fetch requests before adding the
            // listener because the future may have been fulfilled on another thread (e.g. during a
            // disconnection being handled by the heartbeat thread) which will mean the listener
            // will be invoked synchronously.
            this.nodesWithPendingFetchRequests.add(fetchTarget.id());
            future.addListener(new RequestFutureListener<ClientResponse>() {
                @Override
                public void onSuccess(final ClientResponse resp) {
                    synchronized (Fetcher.this) {
                        try {
                            handleSuccess(metadata, fetchTarget, resp, data);
                        } finally {
                            nodesWithPendingFetchRequests.remove(fetchTarget.id());
                        }
                    }
                }

                @Override
                public void onFailure(final RuntimeException e) {
                    synchronized (Fetcher.this) {
                        try {
                            handleError(subscriptions, fetchTarget, e);
                        } finally {
                            nodesWithPendingFetchRequests.remove(fetchTarget.id());
                        }
                    }
                }
            });
        }
        return fetchRequestMap.size();
    }

    /**
     * Return the fetched records, empty the record buffer and update the consumed position.
     * <p/>
     * NOTE: returning an {@link Fetch#isEmpty empty} fetch guarantees the consumed position is not updated.
     *
     * @return A {@link Fetch} for the requested partitions
     * @throws OffsetOutOfRangeException If there is OffsetOutOfRange error in fetchResponse and
     *         the defaultResetPolicy is NONE
     * @throws TopicAuthorizationException If there is TopicAuthorization error in fetchResponse.
     */
    public Fetch<K, V> collectFetch(Metadata metadata, SubscriptionState subscriptions) {
        Fetch<K, V> fetch = Fetch.empty();
        Queue<CompletedFetch<K, V>> pausedCompletedFetches = new ArrayDeque<>();
        int recordsRemaining = fetchContext.maxPollRecords;

        try {
            while (recordsRemaining > 0) {
                if (nextInLineFetch == null || nextInLineFetch.isConsumed()) {
                    CompletedFetch<K, V> records = completedFetches.peek();
                    if (records == null) break;

                    if (!records.isInitialized()) {
                        try {
                            nextInLineFetch = initializeCompletedFetch(metadata, subscriptions, records);
                        } catch (Exception e) {
                            // Remove a completedFetch upon a parse with exception if (1) it contains no records, and
                            // (2) there are no fetched records with actual content preceding this exception.
                            // The first condition ensures that the completedFetches is not stuck with the same completedFetch
                            // in cases such as the TopicAuthorizationException, and the second condition ensures that no
                            // potential data loss due to an exception in a following record.
                            FetchResponseData.PartitionData partition = records.partitionData();
                            if (fetch.isEmpty() && FetchResponse.recordsOrFail(partition).sizeInBytes() == 0) {
                                completedFetches.poll();
                            }
                            throw e;
                        }
                    } else {
                        nextInLineFetch = records;
                    }
                    completedFetches.poll();
                } else if (subscriptions.isPaused(nextInLineFetch.partition())) {
                    // when the partition is paused we add the records back to the completedFetches queue instead of draining
                    // them so that they can be returned on a subsequent poll if the partition is resumed at that time
                    log.debug("Skipping fetching records for assigned partition {} because it is paused", nextInLineFetch.partition());
                    pausedCompletedFetches.add(nextInLineFetch);
                    nextInLineFetch = null;
                } else {
                    Fetch<K, V> nextFetch = fetchRecords(subscriptions, nextInLineFetch, recordsRemaining);
                    recordsRemaining -= nextFetch.numRecords();
                    fetch.add(nextFetch);
                }
            }
        } catch (KafkaException e) {
            if (fetch.isEmpty())
                throw e;
        } finally {
            // add any polled completed fetches for paused partitions back to the completed fetches queue to be
            // re-evaluated in the next poll
            completedFetches.addAll(pausedCompletedFetches);
        }

        return fetch;
    }

}
