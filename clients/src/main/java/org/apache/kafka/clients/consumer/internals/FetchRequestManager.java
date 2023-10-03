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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorBackgroundEvent;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

/**
 * {@code FetchRequestManager} is responsible for generating {@link FetchRequest} that represent the
 * {@link SubscriptionState#fetchablePartitions(Predicate)} based on the user's topic subscription/partition
 * assignment.
 */
public class FetchRequestManager extends AbstractFetch implements RequestManager {

    private final Logger log;
    private final BackgroundEventHandler backgroundEventHandler;
    private final NetworkClientDelegate networkClientDelegate;
    private final List<CompletableFuture<Queue<CompletedFetch>>> futures;

    FetchRequestManager(final LogContext logContext,
                        final Time time,
                        final BackgroundEventHandler backgroundEventHandler,
                        final ConsumerMetadata metadata,
                        final SubscriptionState subscriptions,
                        final FetchConfig fetchConfig,
                        final FetchMetricsManager metricsManager,
                        final NetworkClientDelegate networkClientDelegate) {
        super(logContext, metadata, subscriptions, fetchConfig, metricsManager, time);
        this.log = logContext.logger(FetchRequestManager.class);
        this.backgroundEventHandler = backgroundEventHandler;
        this.networkClientDelegate = networkClientDelegate;
        this.futures = new ArrayList<>();
    }

    @Override
    protected boolean isUnavailable(Node node) {
        return networkClientDelegate.isUnavailable(node);
    }

    @Override
    protected void maybeThrowAuthFailure(Node node) {
        networkClientDelegate.maybeThrowAuthFailure(node);
    }

    /**
     * Adds a new {@link Future future} to the list of futures awaiting results. Per the comments on
     * {@link #forwardResults()}, there is no guarantee that this particular future will be provided with
     * a non-empty result, but it is guaranteed to be completed with a result, assuming that it does not time out.
     *
     * @param future Future that will be {@link CompletableFuture#complete(Object) completed} if not timed out
     */
    public void requestFetch(CompletableFuture<Queue<CompletedFetch>> future) {
        futures.add(future);
    }

    /**
     * @see RequestManager#poll(long)
     */
    @Override
    public PollResult poll(long currentTimeMs) {
        List<UnsentRequest> requests = prepareFetchRequests().entrySet().stream().map(entry -> {
            final Node fetchTarget = entry.getKey();
            final FetchSessionHandler.FetchRequestData data = entry.getValue();
            final FetchRequest.Builder request = createFetchRequest(fetchTarget, data);
            final BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, t) -> {
                if (t != null) {
                    handleFetchResponse(fetchTarget, t);
                    log.debug("Attempt to fetch data from node {} failed due to fatal exception", fetchTarget, t);
                    backgroundEventHandler.add(new ErrorBackgroundEvent(t));
                } else {
                    handleFetchResponse(fetchTarget, data, clientResponse);
                    forwardResults();
                }
            };

            return new UnsentRequest(request, fetchTarget, responseHandler);
        }).collect(Collectors.toList());

        return new PollResult(requests);
    }

    /**
     * @see RequestManager#pollOnClose()
     */
    @Override
    public PollResult pollOnClose() {
        List<UnsentRequest> requests = prepareCloseFetchSessionRequests().entrySet().stream().map(entry -> {
            final Node fetchTarget = entry.getKey();
            final FetchSessionHandler.FetchRequestData data = entry.getValue();
            final FetchRequest.Builder request = createFetchRequest(fetchTarget, data);
            final BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, t) -> {
                if (t != null) {
                    handleCloseFetchSessionResponse(fetchTarget, data, t);
                    log.warn("Attempt to close fetch session on node {} failed due to fatal exception", fetchTarget, t);
                    backgroundEventHandler.add(new ErrorBackgroundEvent(t));
                } else {
                    handleCloseFetchSessionResponse(fetchTarget, data);
                }
            };

            return new UnsentRequest(request, fetchTarget, responseHandler);
        }).collect(Collectors.toList());

        return new PollResult(requests);
    }

    /**
     * Drains any of the {@link CompletedFetch completed fetch} objects from the
     * {@link FetchBuffer internal fetch buffer} and provides the results to awaiting {@link Future futures}
     * from the application thread.
     *
     * <p/>
     *
     * For each future in {@link #futures} that isn't completed at the time that this method is invoked, we will
     * drain the available, buffered data from the underlying fetch buffer and provide it to the future. As the
     * {@link NetworkClient} loops through the completed I/O, each response handler is invoked. Those response handlers
     * will then re-populate the internal fetch buffer before calling this method.
     *
     * <p/>
     *
     * It may be that some of the futures will be completed with an <em>empty</em> queue if the futures
     * ahead of it read all the results. Empty results should be accounted for in the code.
     */
    private void forwardResults() {
        for (CompletableFuture<Queue<CompletedFetch>> f : futures) {
            if (f.isDone())
                continue;

            Queue<CompletedFetch> q = drain();
            f.complete(q);
        }

        // Clear the list of futures here as they have fulfilled their purpose.
        futures.clear();
    }

    /**
     * Drains any of the {@link CompletedFetch} objects from the internal buffer to the returned {@link Queue}.
     *
     * <p/>
     *
     * This is used by the {@link org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor} to
     * pull off any fetch results that are stored in the background thread to provide them to the application thread.
     *
     * @return {@link Queue} containing zero or more {@link CompletedFetch}
     */
    private Queue<CompletedFetch> drain() {
        Queue<CompletedFetch> q = new LinkedList<>();
        CompletedFetch completedFetch = fetchBuffer.poll();

        while (completedFetch != null) {
            q.add(completedFetch);
            completedFetch = fetchBuffer.poll();
        }

        return q;
    }
}
