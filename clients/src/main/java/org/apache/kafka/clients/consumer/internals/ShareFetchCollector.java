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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.Collections;

import static org.apache.kafka.clients.consumer.internals.FetchUtils.requestMetadataUpdate;

/**
 * {@code ShareFetchCollector} operates at the {@link RecordBatch} level, as that is what is stored in the
 * {@link ShareFetchBuffer}. Each {@link org.apache.kafka.common.record.internal.Record} in the {@link RecordBatch} is converted
 * to a {@link ConsumerRecord} and added to the returned {@link Fetch}.
 *
 * @param <K> Record key type
 * @param <V> Record value type
 */
public class ShareFetchCollector<K, V> {

    private final Logger log;
    private final ShareConsumerMetadata metadata;
    private final SubscriptionState subscriptions;
    private final ShareFetchConfig shareFetchConfig;
    private final Deserializers<K, V> deserializers;

    public ShareFetchCollector(final LogContext logContext,
                               final ShareConsumerMetadata metadata,
                               final SubscriptionState subscriptions,
                               final ShareFetchConfig shareFetchConfig,
                               final Deserializers<K, V> deserializers) {
        this.log = logContext.logger(ShareFetchCollector.class);
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.shareFetchConfig = shareFetchConfig;
        this.deserializers = deserializers;
    }

    /**
     * Return the fetched {@link ConsumerRecord records}.
     *
     * @param fetchBuffer {@link ShareFetchBuffer} from which to retrieve the {@link ConsumerRecord records}
     *
     * @return A {@link ShareFetch} for the requested partitions
     * @throws TopicAuthorizationException If there is TopicAuthorization error in fetchResponse.
     */
    public ShareFetch<K, V> collect(final ShareFetchBuffer fetchBuffer) {
        ShareFetch<K, V> fetch = ShareFetch.empty();
        int recordsRemaining = shareFetchConfig.maxPollRecords;

        try {
            while (recordsRemaining > 0) {
                final ShareCompletedFetch nextInLineFetch = fetchBuffer.nextInLineFetch();

                if (nextInLineFetch == null || nextInLineFetch.isConsumed()) {
                    final ShareCompletedFetch completedFetch = fetchBuffer.peek();

                    if (completedFetch == null) {
                        break;
                    }

                    if (!completedFetch.isInitialized()) {
                        try {
                            fetchBuffer.setNextInLineFetch(initialize(completedFetch));
                        } catch (Exception e) {
                            if (fetch.isEmpty()) {
                                fetchBuffer.poll();
                            }
                            throw e;
                        }
                    } else {
                        fetchBuffer.setNextInLineFetch(completedFetch);
                    }

                    fetchBuffer.poll();
                } else {
                    final TopicIdPartition tp = nextInLineFetch.partition;

                    ShareInFlightBatch<K, V> batch = nextInLineFetch.fetchRecords(
                            deserializers,
                            recordsRemaining,
                            shareFetchConfig.checkCrcs);

                    if (batch.isEmpty()) {
                        nextInLineFetch.drain();
                    }

                    recordsRemaining -= batch.numRecords();
                    fetch.add(tp, batch);

                    if (batch.getException() != null) {
                        throw new ShareFetchException(fetch, batch.getException().cause());
                    } else if (batch.hasCachedException()) {
                        break;
                    }
                }
            }
        } catch (KafkaException e) {
            if (fetch.isEmpty()) {
                throw e;
            }
        }

        return fetch;
    }

    /**
     * Initialize a ShareCompletedFetch object.
     */
    protected ShareCompletedFetch initialize(final ShareCompletedFetch completedFetch) {
        final Errors error = Errors.forCode(completedFetch.partitionData.errorCode());

        if (error == Errors.NONE) {
            return handleInitializeSuccess(completedFetch);
        } else {
            handleInitializeErrors(completedFetch, error);
            return null;
        }
    }

    private ShareCompletedFetch handleInitializeSuccess(final ShareCompletedFetch completedFetch) {
        log.trace("Preparing to read {} bytes of data for partition {}",
                ShareFetchResponse.recordsSize(completedFetch.partitionData),
                completedFetch.partition.topicPartition());

        completedFetch.setInitialized();
        return completedFetch;
    }

    private void handleInitializeErrors(final ShareCompletedFetch completedFetch, final Errors error) {
        final TopicIdPartition tp = completedFetch.partition;

        if (error == Errors.NOT_LEADER_OR_FOLLOWER ||
                error == Errors.REPLICA_NOT_AVAILABLE ||
                error == Errors.KAFKA_STORAGE_ERROR ||
                error == Errors.FENCED_LEADER_EPOCH ||
                error == Errors.OFFSET_NOT_AVAILABLE) {
            log.debug("Error in fetch for partition {}: {}", tp, error.exceptionName());
            requestMetadataUpdate(metadata, subscriptions, tp.topicPartition());
        } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
            log.warn("Received unknown topic or partition error in fetch for partition {}.", tp);
            requestMetadataUpdate(metadata, subscriptions, tp.topicPartition());
        } else if (error == Errors.UNKNOWN_TOPIC_ID) {
            log.warn("Received unknown topic ID error in fetch for partition {}.", tp);
            requestMetadataUpdate(metadata, subscriptions, tp.topicPartition());
        } else if (error == Errors.INCONSISTENT_TOPIC_ID) {
            log.warn("Received inconsistent topic ID error in fetch for partition {}.", tp);
            requestMetadataUpdate(metadata, subscriptions, tp.topicPartition());
        } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
            // Log the actual partition and not just the topic to help with ACL propagation issues in large clusters
            log.warn("Not authorized to read from partition {}.", tp.topicPartition());
            throw new TopicAuthorizationException(Collections.singleton(tp.topic()));
        } else if (error == Errors.UNKNOWN_LEADER_EPOCH) {
            log.debug("Received unknown leader epoch error in fetch for partition {}.", tp);
        } else if (error == Errors.UNKNOWN_SERVER_ERROR) {
            log.warn("Unknown server error while fetching topic-partition {}.",
                    tp.topicPartition());
        } else if (error == Errors.CORRUPT_MESSAGE) {
            throw new KafkaException("Encountered corrupt message when fetching topic-partition "
                    + tp.topicPartition());
        } else {
            throw new IllegalStateException("Unexpected error code " + error.code()
                    + " while fetching from topic-partition " + tp.topicPartition());
        }
    }
}
