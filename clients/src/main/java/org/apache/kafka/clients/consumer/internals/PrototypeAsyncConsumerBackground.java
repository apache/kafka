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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

public class PrototypeAsyncConsumerBackground<K, V> {

    private final Logger log;

    private final PrototypeAsyncFetcher<K, V> fetcher;

    private final ConsumerCoordinator coordinator;

    private final SubscriptionState subscriptions;

    private final ConsumerNetworkClient client;

    private final ConsumerMetadata metadata;

    private final Time time;
    // to keep from repeatedly scanning subscriptions in poll(), cache the result during metadata updates
    private boolean cachedSubscriptionHasAllFetchPositions;


    private final long retryBackoffMs;

    private final IsolationLevel isolationLevel;

    @SuppressWarnings("unchecked")
    public PrototypeAsyncConsumerBackground(ConsumerConfig config,
                                            Deserializer<K> keyDeserializer,
                                            Deserializer<V> valueDeserializer,
                                            String clientId,
                                            LogContext logContext,
                                            ConsumerNetworkClient client,
                                            ConsumerMetadata metadata,
                                            SubscriptionState subscriptions,
                                            Metrics metrics,
                                            FetcherMetricsRegistry metricsRegistry,
                                            Time time,
                                            long retryBackoffMs,
                                            long requestTimeoutMs,
                                            IsolationLevel isolationLevel,
                                            ApiVersions apiVersions,
                                            ConsumerCoordinator coordinator) {
        this.log = logContext.logger(PrototypeAsyncConsumerBackground.class);

        if (keyDeserializer == null) {
            keyDeserializer = config.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
            keyDeserializer.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)), true);
        } else {
            config.ignore(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        }
        if (valueDeserializer == null) {
            valueDeserializer = config.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
            valueDeserializer.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)), false);
        } else {
            config.ignore(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        }

        this.fetcher = new PrototypeAsyncFetcher<>(
                logContext,
                client,
                config.getInt(ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
                config.getInt(ConsumerConfig.FETCH_MAX_BYTES_CONFIG),
                config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG),
                config.getInt(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG),
                config.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG),
                config.getBoolean(ConsumerConfig.CHECK_CRCS_CONFIG),
                config.getString(ConsumerConfig.CLIENT_RACK_CONFIG),
                keyDeserializer,
                valueDeserializer,
                metadata,
                subscriptions,
                metrics,
                metricsRegistry,
                time,
                retryBackoffMs,
                requestTimeoutMs,
                isolationLevel,
                apiVersions);
        this.subscriptions = subscriptions;
        this.time = time;
        this.client = client;
        this.coordinator = coordinator;
        this.metadata = metadata;
        this.retryBackoffMs = retryBackoffMs;
        this.isolationLevel = isolationLevel;
    }

    public ConsumerRecords<K, V> poll(final Timer timer, final boolean includeMetadataInTimeout) {
        if (this.subscriptions.hasNoSubscriptionOrUserAssignment()) {
            throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
        }

        do {
            client.maybeTriggerWakeup();

            if (includeMetadataInTimeout) {
                // try to update assignment metadata BUT do not need to block on the timer for join group
                updateAssignmentMetadataIfNeeded(timer, false);
            } else {
                while (!updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE), true)) {
                    log.warn("Still waiting for metadata");
                }
            }

            final Fetch<K, V> fetch = pollForFetches(timer, coordinator.timeToNextPoll(timer.currentTimeMs()));
            if (!fetch.isEmpty()) {
                // before returning the fetched records, we can send off the next round of fetches
                // and avoid block waiting for their responses to enable pipelining while the user
                // is handling the fetched records.
                //
                // NOTE: since the consumed position has already been updated, we must not allow
                // wakeups or any other errors to be triggered prior to returning the fetched records.
                if (fetcher.sendFetches() > 0 || client.hasPendingRequests()) {
                    client.transmitSends();
                }

                if (fetch.records().isEmpty()) {
                    log.trace("Returning empty records from `poll()` "
                            + "since the consumer's position has advanced for at least one topic partition");
                }

                return new ConsumerRecords<>(fetch.records());
            }
        } while (timer.notExpired());

        return ConsumerRecords.empty();
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    private Fetch<K, V> pollForFetches(Timer timer, Long coordinatorTimeToNextPool) {
        long pollTimeout = coordinatorTimeToNextPool == null ? timer.remainingMs() :
                Math.min(coordinatorTimeToNextPool, timer.remainingMs());

        // if data is available already, return it immediately
        final Fetch<K, V> fetch = fetcher.collectFetch();
        if (!fetch.isEmpty()) {
            return postPollForFetches(fetch);
        }

        // send any new fetches (won't resend pending fetches)
        fetcher.sendFetches();

        // We do not want to be stuck blocking in poll if we are missing some positions
        // since the offset lookup may be backing off after a failure

        // NOTE: the use of cachedSubscriptionHasAllFetchPositions means we MUST call
        // updateAssignmentMetadataIfNeeded before this method.
        if (!cachedSubscriptionHasAllFetchPositions && pollTimeout > retryBackoffMs) {
            pollTimeout = retryBackoffMs;
        }

        log.trace("Polling for fetches with timeout {}", pollTimeout);

        Timer pollTimer = time.timer(pollTimeout);
        client.poll(pollTimer, () -> {
            // since a fetch might be completed by the background thread, we need this poll condition
            // to ensure that we do not block unnecessarily in poll()
            return !fetcher.hasAvailableFetches();
        });
        timer.update(pollTimer.currentTimeMs());

        return postPollForFetches(fetcher.collectFetch());
    }

    private Fetch<K, V> postPollForFetches(final Fetch<K, V> fetch) {
        if (!fetch.isEmpty()) {
            // before returning the fetched records, we can send off the next round of fetches
            // and avoid block waiting for their responses to enable pipelining while the user
            // is handling the fetched records.
            //
            // NOTE: since the consumed position has already been updated, we must not allow
            // wakeups or any other errors to be triggered prior to returning the fetched records.
            if (fetcher.sendFetches() > 0 || client.hasPendingRequests()) {
                client.transmitSends();
            }

            if (fetch.records().isEmpty()) {
                log.trace("Returning empty records from `poll()` "
                        + "since the consumer's position has advanced for at least one topic partition");
            }
        }

        return fetch;
    }

    boolean updateAssignmentMetadataIfNeeded(final Timer timer, final boolean waitForJoinGroup) {
        if (coordinator != null && !coordinator.poll(timer, waitForJoinGroup)) {
            return false;
        }

        return updateFetchPositions(timer);
    }

    boolean updateAssignmentMetadataIfNeeded(final Timer timer) {
        return updateAssignmentMetadataIfNeeded(timer, true);
    }

    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        fetcher.clearBufferedDataForUnassignedTopics(topics);
        log.info("Subscribed to topic(s): {}", Utils.join(topics, ", "));
        if (this.subscriptions.subscribe(new HashSet<>(topics), listener))
            metadata.requestUpdateForNewTopics();
    }

    public void unsubscribe() {
        fetcher.clearBufferedDataForUnassignedPartitions(Collections.emptySet());
        if (this.coordinator != null) {
            this.coordinator.onLeavePrepare();
            this.coordinator.maybeLeaveGroup("the consumer unsubscribed from all topics");
        }
        this.subscriptions.unsubscribe();
        log.info("Unsubscribed all topics or patterns and assigned partitions");
    }
    public void assign(Collection<TopicPartition> partitions) {
        if (partitions == null) {
            throw new IllegalArgumentException("Topic partition collection to assign to cannot be null");
        } else if (partitions.isEmpty()) {
            this.unsubscribe();
        } else {
            for (TopicPartition tp : partitions) {
                String topic = (tp != null) ? tp.topic() : null;
                if (Utils.isBlank(topic))
                    throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
            }
            fetcher.clearBufferedDataForUnassignedPartitions(partitions);

            // make sure the offsets of topic partitions the consumer is unsubscribing from
            // are committed since there will be no following rebalance
            if (coordinator != null)
                this.coordinator.maybeAutoCommitOffsetsAsync(time.milliseconds());

            log.info("Assigned to partition(s): {}", Utils.join(partitions, ", "));
            if (this.subscriptions.assignFromUser(new HashSet<>(partitions)))
                metadata.requestUpdateForNewTopics();
        }
    }
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        Cluster cluster = this.metadata.fetch();
        List<PartitionInfo> parts = cluster.partitionsForTopic(topic);
        if (!parts.isEmpty())
            return parts;

        Timer timer = time.timer(timeout);
        Map<String, List<PartitionInfo>> topicMetadata = fetcher.getTopicMetadata(
                new MetadataRequest.Builder(Collections.singletonList(topic), metadata.allowAutoTopicCreation()), timer);
        return topicMetadata.getOrDefault(topic, Collections.emptyList());
    }

    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return fetcher.getAllTopicMetadata(time.timer(timeout));
    }
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
            // we explicitly exclude the earliest and latest offset here so the timestamp in the returned
            // OffsetAndTimestamp is always positive.
            if (entry.getValue() < 0)
                throw new IllegalArgumentException("The target time for partition " + entry.getKey() + " is " +
                        entry.getValue() + ". The target time cannot be negative.");
        }
        return fetcher.offsetsForTimes(timestampsToSearch, time.timer(timeout));
    }

    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return fetcher.beginningOffsets(partitions, time.timer(timeout));
    }

    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return fetcher.endOffsets(partitions, time.timer(timeout));
    }

    public OptionalLong currentLag(TopicPartition topicPartition) {
        final Long lag = subscriptions.partitionLag(topicPartition, isolationLevel);

        // if the log end offset is not known and hence cannot return lag and there is
        // no in-flight list offset requested yet,
        // issue a list offset request for that partition so that next time
        // we may get the answer; we do not need to wait for the return value
        // since we would not try to poll the network client synchronously
        if (lag == null) {
            if (subscriptions.partitionEndOffset(topicPartition, isolationLevel) == null &&
                    !subscriptions.partitionEndOffsetRequested(topicPartition)) {
                log.info("Requesting the log end offset for {} in order to compute lag", topicPartition);
                subscriptions.requestPartitionEndOffset(topicPartition);
                fetcher.endOffsets(Collections.singleton(topicPartition), time.timer(0L));
            }

            return OptionalLong.empty();
        }

        return OptionalLong.of(lag);
    }

    private boolean updateFetchPositions(final Timer timer) {
        // If any partitions have been truncated due to a leader change, we need to validate the offsets
        fetcher.validateOffsetsIfNeeded();

        cachedSubscriptionHasAllFetchPositions = subscriptions.hasAllFetchPositions();
        if (cachedSubscriptionHasAllFetchPositions) return true;

        // If there are any partitions which do not have a valid position and are not
        // awaiting reset, then we need to fetch committed offsets. We will only do a
        // coordinator lookup if there are partitions which have missing positions, so
        // a consumer with manually assigned partitions can avoid a coordinator dependence
        // by always ensuring that assigned partitions have an initial position.
        if (coordinator != null && !coordinator.refreshCommittedOffsetsIfNeeded(timer)) return false;

        // If there are partitions still needing a position and a reset policy is defined,
        // request reset using the default policy. If no reset strategy is defined and there
        // are partitions with a missing position, then we will raise an exception.
        subscriptions.resetInitializingPositions();

        // Finally send an asynchronous request to lookup and update the positions of any
        // partitions which are awaiting reset.
        fetcher.resetOffsetsIfNeeded();

        return true;
    }
}
