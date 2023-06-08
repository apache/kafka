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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * <p>
 * Manages the state of the topic metadata requests.  The manager returns the
 * {@link org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult} when the request is ready to
 * be sent. This manager specifically handles the following user API calls:
 * <ul>
 * <li>listOffsets</li>
 * <li>partitionsFor</li>
 * </ul>
 * </p>
 * <p>
 *     The manager checks the state of the {@link CompletableTopicMetadataRequest} before sending a new one, to
 *     prevent sending without backing off from the previous attempts.
 *     It also checks the state of the inflight requests to prevent overwhelming the broker with duplicated requests.
 *     The {@code inflightRequests} is memoized by the topic name.  If all topics are requested, then we use {@code
 *     null} as the key. Once the request is completed successfully, the entry is removed.
 * </p>
 */
public class TopicMetadataRequestManager implements RequestManager {
    private final boolean allowAutoTopicCreation;
    private final Map<String, CompletableTopicMetadataRequest> inflightRequests;
    private final long retryBackoffMs;
    private final Logger log;
    private final LogContext logContext;

    public TopicMetadataRequestManager(final LogContext logContext, final ConsumerConfig config) {
        this.logContext = logContext;
        this.log = logContext.logger(this.getClass());
        this.inflightRequests = new HashMap<>();
        this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        this.allowAutoTopicCreation = config.getBoolean(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG);
    }

    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        List<NetworkClientDelegate.UnsentRequest> requests = inflightRequests.values().stream()
            .map(req -> req.send(currentTimeMs))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
        return requests.isEmpty() ?
            new NetworkClientDelegate.PollResult(Long.MAX_VALUE, new ArrayList<>()) :
            new NetworkClientDelegate.PollResult(0, Collections.unmodifiableList(requests));
    }

    /**
     * return the future of the metadata request. If the same topic was requested but incompleted, return the same
     * future.
     *
     * @param topic to be requested. If empty, return the metadata for all topics.
     * @return the future of the metadata request.
     */
    public CompletableFuture<Map<String, List<PartitionInfo>>> requestTopicMetadata(final Optional<String> topic) {
        String topicName = topic.orElse(null);
        if (inflightRequests.containsKey(topicName)) {
            return inflightRequests.get(topicName).future();
        }

        CompletableTopicMetadataRequest newRequest = new CompletableTopicMetadataRequest(
            logContext,
            topic,
            retryBackoffMs);
        inflightRequests.put(topicName, newRequest);
        return newRequest.future();
    }

    // Visible for testing
    List<CompletableTopicMetadataRequest> inflightRequests() {
        return new ArrayList<>(inflightRequests.values());
    }

    class CompletableTopicMetadataRequest extends CompletableRequest<Map<String, List<PartitionInfo>>> {
        private final Optional<String> topic;
        private final RequestState state;

        public CompletableTopicMetadataRequest(final LogContext logContext,
                                               final Optional<String> topic,
                                               final long retryBackoffMs) {
            this.topic = topic;
            this.state = new RequestState(logContext, retryBackoffMs);
        }

        /**
         * prepare the metadata request and return an
         * {@link org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest} if needed.
         */
        private Optional<NetworkClientDelegate.UnsentRequest> send(final long currentTimeMs) {
            if (!this.state.canSendRequest(currentTimeMs)) {
                return Optional.empty();
            }
            this.state.onSendAttempt(currentTimeMs);

            final MetadataRequest.Builder request;
            if (topic.isPresent()) {
                request = new MetadataRequest.Builder(Collections.singletonList(topic.get()), allowAutoTopicCreation);
            } else {
                request = MetadataRequest.Builder.allTopics();
            }

            final NetworkClientDelegate.UnsentRequest unsent = new NetworkClientDelegate.UnsentRequest(
                request,
                Optional.empty(),
                (response, exception) -> {
                    if (exception != null) {
                        this.future().completeExceptionally(new KafkaException(exception));
                        inflightRequests.remove(topic.orElse(null));
                        return;
                    }

                    try {
                        Map<String, List<PartitionInfo>> res = handleTopicMetadataResponse((MetadataResponse) response.responseBody());
                        future().complete(res);
                        inflightRequests.remove(topic.orElse(null));
                    } catch (RetriableException e) {
                        this.state.onFailedAttempt(currentTimeMs);
                    } catch (Exception t) {
                        this.future().completeExceptionally(t);
                        inflightRequests.remove(topic.orElse(null));
                    }
                });
            return Optional.of(unsent);
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
                    else if (error.exception() instanceof RetriableException) {
                        throw error.exception();
                    } else
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
            return topic.orElse(null);
        }
    }
}
