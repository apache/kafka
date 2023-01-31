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
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollContext;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.slf4j.Logger;

public class FetchRequestManager<K, V> extends AbstractFetcher<K, V> implements RequestManager {

    private final Logger log;
    private final ErrorEventHandler nonRetriableErrorHandler;
    private final RequestState requestState;

    FetchRequestManager(final FetchContext<K, V> fetchContext,
                        final ApiVersions apiVersions,
                        final ErrorEventHandler nonRetriableErrorHandler,
                        final RequestState requestState) {
        super(fetchContext, apiVersions);
        this.log = fetchContext.logContext.logger(FetchRequestManager.class);
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
    public PollResult poll(final PollContext pollContext) {
        log.debug("Calling poll with pollContext {}", pollContext);

        if (!requestState.canSendRequest(pollContext.currentTimeMs))
            return new PollResult(requestState.remainingBackoffMs(pollContext.currentTimeMs));

        List<UnsentRequest> requests = new ArrayList<>();
        requestState.onSendAttempt(pollContext.currentTimeMs);

        Map<Node, FetchRequestData> fetchRequestMap = prepareFetchRequests(pollContext.metadata, pollContext.subscriptions);

        for (Map.Entry<Node, FetchSessionHandler.FetchRequestData> entry : fetchRequestMap.entrySet()) {
            final Node fetchTarget = entry.getKey();
            final FetchSessionHandler.FetchRequestData data = entry.getValue();
            final FetchRequest.Builder request = createFetchRequest(data);
            nodesWithPendingFetchRequests.add(fetchTarget.id());

            UnsentRequest unsentRequest = new UnsentRequest(request, Optional.of(fetchTarget));
            unsentRequest.future().whenComplete((clientResponse, t) -> {
                try {
                    if (t != null) {
                        requestState.onFailedAttempt(fetchContext.time.milliseconds());
                        if (t instanceof RetriableException) {
                            log.debug("FetchResponse request failed due to retriable exception", t);
                            return;
                        }

                        handleError(pollContext.subscriptions, fetchTarget, t);

                        log.warn("FetchResponse request failed due to fatal exception", t);
                        nonRetriableErrorHandler.handle(t);
                    } else {
                        requestState.onSuccessfulAttempt(fetchContext.time.milliseconds());
                        handleSuccess(pollContext.metadata, fetchTarget, clientResponse, data);
                        requestState.reset();
                    }
                } finally {
                    nodesWithPendingFetchRequests.remove(fetchTarget.id());
                }
            });

            requests.add(unsentRequest);
        }

        return new PollResult(Long.MAX_VALUE, requests);
    }
}
