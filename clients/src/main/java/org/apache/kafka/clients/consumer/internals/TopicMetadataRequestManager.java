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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult.EMPTY;

/**
 * <p>
 * Manages the state of topic metadata requests. This manager returns a
 * {@link NetworkClientDelegate.PollResult} when a request is ready to
 * be sent. Specifically, this manager handles the following user API calls:
 * </p>
 * <ul>
 * <li>listTopics</li>
 * <li>partitionsFor</li>
 * </ul>
 * <p>
 * The manager checks the state of the {@link TopicMetadataRequestState} before sending a new one to
 * prevent sending it without backing off from previous attempts.
 * Once a request is completed successfully or times out, its corresponding entry is removed.
 * </p>
 */

public class TopicMetadataRequestManager implements RequestManager {
    private final Time time;
    private final boolean allowAutoTopicCreation;
    private final List<TopicMetadataRequestState> inflightRequests;
    private final int requestTimeoutMs;
    private final long retryBackoffMs;
    private final long retryBackoffMaxMs;
    private final Logger log;
    private final LogContext logContext;

    public TopicMetadataRequestManager(final LogContext context, final Time time, final ConsumerConfig config) {
        logContext = context;
        log = logContext.logger(getClass());
        this.time = time;
        inflightRequests = new LinkedList<>();
        requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
        allowAutoTopicCreation = config.getBoolean(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG);
    }

    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        List<NetworkClientDelegate.UnsentRequest> requests = inflightRequests.stream()
            .map(req -> req.send(currentTimeMs))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

        return requests.isEmpty() ? EMPTY : new NetworkClientDelegate.PollResult(0, requests);
    }

    /**
     * Return the future of the metadata request.
     *
     * @return the future of the metadata request.
     */
    public CompletableFuture<Map<String, List<PartitionInfo>>> requestAllTopicsMetadata(final Timer timer) {
        TopicMetadataRequestState newRequest = new TopicMetadataRequestState(
                logContext,
                timer,
                retryBackoffMs,
                retryBackoffMaxMs);
        inflightRequests.add(newRequest);
        return newRequest.future;
    }

    /**
     * Return the future of the metadata request.
     *
     * @param topic to be requested.
     * @return the future of the metadata request.
     */
    public CompletableFuture<Map<String, List<PartitionInfo>>> requestTopicMetadata(final String topic, final Timer timer) {
        TopicMetadataRequestState newRequest = new TopicMetadataRequestState(
                logContext,
                topic,
                timer,
                retryBackoffMs,
                retryBackoffMaxMs);
        inflightRequests.add(newRequest);
        return newRequest.future;
    }

    // Visible for testing
    List<TopicMetadataRequestState> inflightRequests() {
        return inflightRequests;
    }

    class TopicMetadataRequestState extends TimedRequestState {
        private final String topic;
        private final boolean allTopics;
        CompletableFuture<Map<String, List<PartitionInfo>>> future;

        public TopicMetadataRequestState(final LogContext logContext,
                                         final Timer timer,
                                         final long retryBackoffMs,
                                         final long retryBackoffMaxMs) {
            super(logContext, TopicMetadataRequestState.class.getSimpleName(), retryBackoffMs,
                    retryBackoffMaxMs, timer);
            future = new CompletableFuture<>();
            this.topic = null;
            this.allTopics = true;
        }

        public TopicMetadataRequestState(final LogContext logContext,
                                         final String topic,
                                         final Timer timer,
                                         final long retryBackoffMs,
                                         final long retryBackoffMaxMs) {
            super(logContext, TopicMetadataRequestState.class.getSimpleName(), retryBackoffMs,
                retryBackoffMaxMs, timer);
            future = new CompletableFuture<>();
            this.topic = topic;
            this.allTopics = false;
        }

        /**
         * prepare the metadata request and return an
         * {@link org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest} if needed.
         */
        private Optional<NetworkClientDelegate.UnsentRequest> send(final long currentTimeMs) {
            if (!canSendRequest(currentTimeMs)) {
                return Optional.empty();
            }
            onSendAttempt(currentTimeMs);

            final MetadataRequest.Builder request = allTopics
                ? MetadataRequest.Builder.allTopics()
                : new MetadataRequest.Builder(Collections.singletonList(topic), allowAutoTopicCreation);

            return Optional.of(createUnsentRequest(request));
        }

        private NetworkClientDelegate.UnsentRequest createUnsentRequest(
                final MetadataRequest.Builder request) {
            Timer t = time.timer(requestTimeoutMs);
            NetworkClientDelegate.UnsentRequest unsent = new NetworkClientDelegate.UnsentRequest(
                request,
                Optional.empty(),
                t
            );

            return unsent.whenComplete((response, exception) -> {
                if (response == null) {
                    handleError(exception, unsent.handler().completionTimeMs());
                } else {
                    handleResponse(response);
                }
            });
        }

        private void handleError(final Throwable exception,
                                 final long completionTimeMs) {
            if (exception instanceof RetriableException) {
                if (isExpired()) {
                    completeFutureAndRemoveRequest(
                        new TimeoutException("Timeout expired while fetching topic metadata"));
                } else {
                    onFailedAttempt(completionTimeMs);
                }
            } else {
                completeFutureAndRemoveRequest(exception);
            }
        }

        private void handleResponse(final ClientResponse response) {
            long responseTimeMs = response.receivedTimeMs();
            try {
                Map<String, List<PartitionInfo>> res = handleTopicMetadataResponse((MetadataResponse) response.responseBody());
                future.complete(res);
                inflightRequests.remove(this);
            } catch (RetriableException e) {
                if (isExpired()) {
                    completeFutureAndRemoveRequest(
                        new TimeoutException("Timeout expired while fetching topic metadata"));
                } else {
                    onFailedAttempt(responseTimeMs);
                }
            } catch (Exception t) {
                completeFutureAndRemoveRequest(t);
            }
        }

        private void completeFutureAndRemoveRequest(final Throwable throwable) {
            future.completeExceptionally(throwable);
            inflightRequests.remove(this);
        }

        private Map<String, List<PartitionInfo>> handleTopicMetadataResponse(final MetadataResponse response) {
            Cluster cluster = response.buildCluster();

            final Set<String> unauthorizedTopics = cluster.unauthorizedTopics();
            if (!unauthorizedTopics.isEmpty())
                throw new TopicAuthorizationException(unauthorizedTopics);

            Map<String, Errors> errors = response.errors();
            if (!errors.isEmpty()) {
                // if there were errors, we need to check whether they were fatal or whether
                // we should just retry

                log.debug("Topic metadata fetch included errors: {}", errors);

                for (Map.Entry<String, Errors> errorEntry : errors.entrySet()) {
                    String topic = errorEntry.getKey();
                    Errors error = errorEntry.getValue();

                    if (error == Errors.INVALID_TOPIC_EXCEPTION)
                        throw new InvalidTopicException("Topic '" + topic + "' is invalid");
                    else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION)
                        // if a requested topic is unknown, we just continue and let it be absent
                        // in the returned map
                        continue;
                    else if (error.exception() instanceof RetriableException)
                        throw error.exception();
                    else
                        throw new KafkaException("Unexpected error fetching metadata for topic " + topic,
                            error.exception());
                }
            }

            HashMap<String, List<PartitionInfo>> topicsPartitionInfos = new HashMap<>();
            for (String topic : cluster.topics())
                topicsPartitionInfos.put(topic, cluster.partitionsForTopic(topic));
            return topicsPartitionInfos;
        }

        public String topic() {
            return topic;
        }

        @Override
        protected String toStringBase() {
            return super.toStringBase() +
                    ", topic=" + Objects.toString(topic, "<all>") +
                    ", allTopics=" + allTopics +
                    ", future=" + future;
        }
    }
}
