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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.FetchSessionHandler.FetchRequestData;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;

public class FetchRequestManager<K, V> extends AbstractFetcher<K, V> implements RequestManager {

    private final ErrorEventHandler nonRetriableErrorHandler;

    private final RequestState requestState;

    FetchRequestManager(final FetchContext<K, V> fetchContext,
                        final ApiVersions apiVersions,
                        final ErrorEventHandler nonRetriableErrorHandler,
                        final RequestState requestState) {
        super(fetchContext, apiVersions);
        this.nonRetriableErrorHandler = nonRetriableErrorHandler;
        this.requestState = requestState;
    }

    @Override
    protected boolean isUnavailable(final Node node) {
        return false;
    }

    @Override
    protected void maybeThrowAuthFailure(final Node node) {
        // Do nothing...
    }

    @Override
    public PollResult poll(final Metadata metadata,
                           final SubscriptionState subscriptions,
                           final long currentTimeMs) {
        if (!requestState.canSendRequest(currentTimeMs))
            return new NetworkClientDelegate.PollResult(requestState.remainingBackoffMs(currentTimeMs));

        List<UnsentRequest> requests = new ArrayList<>();
        requestState.onSendAttempt(currentTimeMs);

        Map<Node, FetchRequestData> fetchRequestMap = prepareFetchRequests(metadata, subscriptions);

        for (Map.Entry<Node, FetchSessionHandler.FetchRequestData> entry : fetchRequestMap.entrySet()) {
            final Node fetchTarget = entry.getKey();
            final FetchSessionHandler.FetchRequestData data = entry.getValue();
            final FetchRequest.Builder request = createFetchRequest(data);
            nodesWithPendingFetchRequests.add(fetchTarget.id());

            UnsentRequest unsentRequest = new NetworkClientDelegate.UnsentRequest(request, Optional.of(fetchTarget));
            unsentRequest.future().whenComplete((clientResponse, t) -> {
                try {
                    if (t != null) {
                        requestState.onFailedAttempt(fetchContext.time.milliseconds());
                        if (t instanceof RetriableException) {
                            log.debug("FetchResponse request failed due to retriable exception", t);
                            return;
                        }

                        handleError(subscriptions, fetchTarget, t);

                        log.warn("FetchResponse request failed due to fatal exception", t);
                        nonRetriableErrorHandler.handle(t);
                    } else {
                        requestState.onSuccessfulAttempt(fetchContext.time.milliseconds());
                        handleSuccess(metadata, fetchTarget, clientResponse, data);
                        requestState.reset();
                    }
                } finally {
                    nodesWithPendingFetchRequests.remove(fetchTarget.id());
                }
            });

            requests.add(unsentRequest);
        }

        return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, requests);
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
    public Optional<Fetch<K, V>> fetch(Metadata metadata, SubscriptionState subscriptions) {
        Optional<Fetch<K, V>> fetch = Optional.empty();
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
                            if (!fetch.isPresent() && FetchResponse.recordsOrFail(partition).sizeInBytes() == 0) {
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

                    if (fetch.isPresent())
                        fetch.get().add(nextFetch);
                    else
                        fetch = Optional.of(nextFetch);
                }
            }
        } catch (KafkaException e) {
            if (!fetch.isPresent())
                throw e;
        } finally {
            // add any polled completed fetches for paused partitions back to the completed fetches queue to be
            // re-evaluated in the next poll
            completedFetches.addAll(pausedCompletedFetches);
        }

        return fetch;
    }

}
