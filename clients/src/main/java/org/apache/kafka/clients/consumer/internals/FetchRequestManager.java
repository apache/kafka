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

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

/**
 * {@code FetchRequestManager} is responsible for generating {@link FetchRequest} that represent the
 * {@link SubscriptionState#fetchablePartitions(Predicate)} based on the user's topic subscription/partition
 * assignment.
 *
 * @param <K> Record key type
 * @param <V> Record value type
 */
public class FetchRequestManager<K, V> extends AbstractFetch<K, V> implements RequestManager {

    private final Logger log;
    private final ErrorEventHandler errorEventHandler;

    FetchRequestManager(final LogContext logContext,
                        final Time time,
                        final ErrorEventHandler errorEventHandler,
                        final ConsumerMetadata metadata,
                        final SubscriptionState subscriptions,
                        final FetchConfig<K, V> fetchConfig,
                        final FetchMetricsManager metricsManager,
                        final NodeStatusDetector nodeStatusDetector) {
        super(logContext, nodeStatusDetector, metadata, subscriptions, fetchConfig, metricsManager, time);
        this.log = logContext.logger(FetchRequestManager.class);
        this.errorEventHandler = errorEventHandler;
    }

    @Override
    public PollResult poll(long currentTimeMs) {
        List<UnsentRequest> requests;

        if (!idempotentCloser.isClosed()) {
            // If the fetcher is open (i.e. not closed), we will issue the normal fetch requests
            requests = prepareFetchRequests().entrySet().stream().map(entry -> {
                final Node fetchTarget = entry.getKey();
                final FetchSessionHandler.FetchRequestData data = entry.getValue();
                final FetchRequest.Builder request = createFetchRequest(fetchTarget, data);
                final BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, t) -> {
                    if (t != null) {
                        handleFetchResponse(fetchTarget, t);
                        log.warn("Attempt to fetch data from node {} failed due to fatal exception", fetchTarget, t);
                        errorEventHandler.handle(t);
                    } else {
                        handleFetchResponse(fetchTarget, data, clientResponse);
                    }
                };

                return new UnsentRequest(request, fetchTarget, responseHandler);
            }).collect(Collectors.toList());
        } else {
            requests = prepareCloseFetchSessionRequests().entrySet().stream().map(entry -> {
                final Node fetchTarget = entry.getKey();
                final FetchSessionHandler.FetchRequestData data = entry.getValue();
                final FetchRequest.Builder request = createFetchRequest(fetchTarget, data);
                final BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, t) -> {
                    if (t != null) {
                        handleCloseFetchSessionResponse(fetchTarget, data, t);
                        log.warn("Attempt to close fetch session on node {} failed due to fatal exception", fetchTarget, t);
                        errorEventHandler.handle(t);
                    } else {
                        handleCloseFetchSessionResponse(fetchTarget, data);
                    }
                };

                return new UnsentRequest(request, fetchTarget, responseHandler);
            }).collect(Collectors.toList());
        }

        return new PollResult(Long.MAX_VALUE, requests);
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
    public Queue<CompletedFetch> drain() {
        Queue<CompletedFetch> q = new LinkedList<>();
        CompletedFetch completedFetch = fetchBuffer.poll();

        while (completedFetch != null) {
            q.add(completedFetch);
            completedFetch = fetchBuffer.poll();
        }

        return q;
    }

    @Override
    protected void closeInternal(Timer timer) {
        // We can't clear out the session handlers just yet as we need them for the next poll to send the
        // 'close fetch session' requests.
        Utils.closeQuietly(decompressionBufferSupplier, "decompressionBufferSupplier");
    }
}
