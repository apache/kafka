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
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.FetchSessionHandler.FetchRequestData;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.ErrorBackgroundEvent;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

public class FetchRequestManager<K, V> extends AbstractFetch<K, V> implements RequestManager {

    private final Logger log;
    private final Time time;
    private final RequestState requestState;
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;

    private boolean isClosed;

    FetchRequestManager(final LogContext logContext,
                        final Time time,
                        final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                        final ConsumerMetadata metadata,
                        final SubscriptionState subscriptions,
                        final FetchConfig<K, V> fetchConfig,
                        final FetchMetricsManager metricsManager,
                        final NodeStatusDeterminator nodeStatusDeterminator,
                        final long retryBackoffMs) {
        super(logContext,
                time,
                metadata,
                subscriptions,
                fetchConfig,
                metricsManager,
                nodeStatusDeterminator);
        this.log = logContext.logger(FetchRequestManager.class);
        this.time = time;
        this.backgroundEventQueue = backgroundEventQueue;
        this.requestState = new RequestState(retryBackoffMs);
    }

    @Override
    public PollResult poll(long currentTimeMs) {
        log.debug("Calling poll with currentTimeMs {}", currentTimeMs);

        if (!requestState.canSendRequest(currentTimeMs))
            return new PollResult(requestState.remainingBackoffMs(currentTimeMs));

        requestState.onSendAttempt(currentTimeMs);
        List<UnsentRequest> requests;

        if (isClosed)
            requests = pollCloseFetchSessionRequests();
        else
            requests = pollFetchRequests();

        return new PollResult(Long.MAX_VALUE, requests);
    }

    private List<UnsentRequest> pollFetchRequests() {
        List<UnsentRequest> requests = new ArrayList<>();
        Map<Node, FetchRequestData> fetchRequestMap = prepareFetchRequests();

        for (Map.Entry<Node, FetchSessionHandler.FetchRequestData> entry : fetchRequestMap.entrySet()) {
            final Node fetchTarget = entry.getKey();
            final FetchSessionHandler.FetchRequestData data = entry.getValue();
            final FetchRequest.Builder request = createFetchRequest(fetchTarget, data);

            UnsentRequest unsentRequest = new UnsentRequest(request, Optional.of(fetchTarget));
            unsentRequest.future().whenComplete((clientResponse, t) -> {
                if (t != null) {
                    requestState.onFailedAttempt(time.milliseconds());
                    if (t instanceof RetriableException) {
                        log.debug("FetchResponse request failed due to retriable exception", t);
                        return;
                    }

                    handleFetchResponse(fetchTarget, t);

                    log.warn("FetchResponse request failed due to fatal exception", t);
                    backgroundEventQueue.add(new ErrorBackgroundEvent(t));
                } else {
                    requestState.onSuccessfulAttempt(time.milliseconds());
                    handleFetchResponse(fetchTarget, data, clientResponse);
                    requestState.reset();
                }
            });

            requests.add(unsentRequest);
        }

        return requests;
    }

    private List<UnsentRequest> pollCloseFetchSessionRequests() {
        List<UnsentRequest> requests = new ArrayList<>();

        Map<Node, FetchSessionHandler.FetchRequestData> fetchRequestMap = prepareCloseFetchSessionRequests();

        for (Map.Entry<Node, FetchSessionHandler.FetchRequestData> entry : fetchRequestMap.entrySet()) {
            final Node fetchTarget = entry.getKey();
            final FetchSessionHandler.FetchRequestData data = entry.getValue();
            final FetchRequest.Builder request = createFetchRequest(fetchTarget, data);

            UnsentRequest unsentRequest = new UnsentRequest(request, Optional.of(fetchTarget));
            unsentRequest.future().whenComplete((clientResponse, t) -> {
                if (t != null) {
                    requestState.onFailedAttempt(time.milliseconds());
                    if (t instanceof RetriableException) {
                        log.debug("FetchResponse request failed due to retriable exception", t);
                        return;
                    }

                    handleCloseFetchSessionResponse(fetchTarget, data, t);

                    log.warn("FetchResponse request failed due to fatal exception", t);
                    backgroundEventQueue.add(new ErrorBackgroundEvent(t));
                } else {
                    requestState.onSuccessfulAttempt(time.milliseconds());
                    handleCloseFetchSessionResponse(fetchTarget, data);
                    requestState.reset();
                }
            });

            requests.add(unsentRequest);
        }

        return requests;
    }

    public Queue<CompletedFetch<K, V>> drain() {
        Queue<CompletedFetch<K, V>> q = new LinkedList<>();
        CompletedFetch<K, V> completedFetch = fetchBuffer.poll();

        while (completedFetch != null) {
            q.add(completedFetch);
            completedFetch = fetchBuffer.poll();
        }

        return q;
    }

    @Override
    public void close() {
        this.isClosed = true;
    }
}
