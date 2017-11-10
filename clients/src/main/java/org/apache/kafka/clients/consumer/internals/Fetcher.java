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
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.BufferSupplier;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.ExtendedDeserializer;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyList;
import static org.apache.kafka.common.serialization.ExtendedDeserializer.Wrapper.ensureExtended;

/**
 * This class manage the fetching process with the brokers.
 */
public class Fetcher<K, V> implements SubscriptionState.Listener, Closeable {
    private final Logger log;
    private final ConsumerNetworkClient client;
    private final Time time;
    private final int minBytes;
    private final int maxBytes;
    private final int maxWaitMs;
    private final int fetchSize;
    private final long retryBackoffMs;
    private final int maxPollRecords;
    private final boolean checkCrcs;
    private final Metadata metadata;
    private final FetchManagerMetrics sensors;
    private final SubscriptionState subscriptions;
    private final ConcurrentLinkedQueue<CompletedFetch> completedFetches;
    private final BufferSupplier decompressionBufferSupplier = BufferSupplier.create();

    private final ExtendedDeserializer<K> keyDeserializer;
    private final ExtendedDeserializer<V> valueDeserializer;
    private final IsolationLevel isolationLevel;

    private PartitionRecords nextInLineRecords = null;

    public Fetcher(LogContext logContext,
                   ConsumerNetworkClient client,
                   int minBytes,
                   int maxBytes,
                   int maxWaitMs,
                   int fetchSize,
                   int maxPollRecords,
                   boolean checkCrcs,
                   Deserializer<K> keyDeserializer,
                   Deserializer<V> valueDeserializer,
                   Metadata metadata,
                   SubscriptionState subscriptions,
                   Metrics metrics,
                   FetcherMetricsRegistry metricsRegistry,
                   Time time,
                   long retryBackoffMs,
                   IsolationLevel isolationLevel) {
        this.log = logContext.logger(Fetcher.class);
        this.time = time;
        this.client = client;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
        this.maxWaitMs = maxWaitMs;
        this.fetchSize = fetchSize;
        this.maxPollRecords = maxPollRecords;
        this.checkCrcs = checkCrcs;
        this.keyDeserializer = ensureExtended(keyDeserializer);
        this.valueDeserializer = ensureExtended(valueDeserializer);
        this.completedFetches = new ConcurrentLinkedQueue<>();
        this.sensors = new FetchManagerMetrics(metrics, metricsRegistry);
        this.retryBackoffMs = retryBackoffMs;
        this.isolationLevel = isolationLevel;

        subscriptions.addListener(this);
    }

    /**
     * Represents data about an offset returned by a broker.
     */
    private static class OffsetData {
        /**
         * The offset
         */
        final long offset;

        /**
         * The timestamp.
         *
         * Will be null if the broker does not support returning timestamps.
         */
        final Long timestamp;

        OffsetData(long offset, Long timestamp) {
            this.offset = offset;
            this.timestamp = timestamp;
        }
    }

    /**
     * Return whether we have any completed fetches pending return to the user. This method is thread-safe.
     * @return true if there are completed fetches, false otherwise
     */
    public boolean hasCompletedFetches() {
        return !completedFetches.isEmpty();
    }

    private boolean matchesRequestedPartitions(FetchRequest.Builder request, FetchResponse response) {
        Set<TopicPartition> requestedPartitions = request.fetchData().keySet();
        Set<TopicPartition> fetchedPartitions = response.responseData().keySet();
        return fetchedPartitions.equals(requestedPartitions);
    }

    /**
     * Set-up a fetch request for any node that we have assigned partitions for which doesn't already have
     * an in-flight fetch or pending fetch data.
     * @return number of fetches sent
     */
    public int sendFetches() {
        Map<Node, FetchRequest.Builder> fetchRequestMap = createFetchRequests();
        for (Map.Entry<Node, FetchRequest.Builder> fetchEntry : fetchRequestMap.entrySet()) {
            final FetchRequest.Builder request = fetchEntry.getValue();
            final Node fetchTarget = fetchEntry.getKey();

            log.debug("Sending {} fetch for partitions {} to broker {}", isolationLevel, request.fetchData().keySet(),
                    fetchTarget);
            client.send(fetchTarget, request)
                    .addListener(new RequestFutureListener<ClientResponse>() {
                        @Override
                        public void onSuccess(ClientResponse resp) {
                            FetchResponse response = (FetchResponse) resp.responseBody();
                            if (!matchesRequestedPartitions(request, response)) {
                                // obviously we expect the broker to always send us valid responses, so this check
                                // is mainly for test cases where mock fetch responses must be manually crafted.
                                log.warn("Ignoring fetch response containing partitions {} since it does not match " +
                                        "the requested partitions {}", response.responseData().keySet(),
                                        request.fetchData().keySet());
                                return;
                            }

                            Set<TopicPartition> partitions = new HashSet<>(response.responseData().keySet());
                            FetchResponseMetricAggregator metricAggregator = new FetchResponseMetricAggregator(sensors, partitions);

                            for (Map.Entry<TopicPartition, FetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                                TopicPartition partition = entry.getKey();
                                long fetchOffset = request.fetchData().get(partition).fetchOffset;
                                FetchResponse.PartitionData fetchData = entry.getValue();

                                log.debug("Fetch {} at offset {} for partition {} returned fetch data {}",
                                        isolationLevel, fetchOffset, partition, fetchData);
                                completedFetches.add(new CompletedFetch(partition, fetchOffset, fetchData, metricAggregator,
                                        resp.requestHeader().apiVersion()));
                            }

                            sensors.fetchLatency.record(resp.requestLatencyMs());
                        }

                        @Override
                        public void onFailure(RuntimeException e) {
                            log.debug("Fetch request {} to {} failed", request.fetchData(), fetchTarget, e);
                        }
                    });
        }
        return fetchRequestMap.size();
    }

    /**
     * Lookup and set offsets for any partitions which are awaiting an explicit reset.
     * @param partitions the partitions to reset
     */
    public void resetOffsetsIfNeeded(Set<TopicPartition> partitions) {
        final Set<TopicPartition> needsOffsetReset = new HashSet<>();
        for (TopicPartition tp : partitions) {
            if (subscriptions.isAssigned(tp) && subscriptions.isOffsetResetNeeded(tp))
                needsOffsetReset.add(tp);
        }
        if (!needsOffsetReset.isEmpty()) {
            resetOffsets(needsOffsetReset);
        }
    }

    /**
     * Update the fetch positions for the provided partitions.
     * @param partitions the partitions to update positions for
     * @throws NoOffsetForPartitionException If no offset is stored for a given partition and no reset policy is available
     */
    public void updateFetchPositions(Set<TopicPartition> partitions) {
        final Set<TopicPartition> needsOffsetReset = new HashSet<>();
        // reset the fetch position to the committed position
        for (TopicPartition tp : partitions) {
            if (!subscriptions.isAssigned(tp) || subscriptions.hasValidPosition(tp))
                continue;

            if (subscriptions.isOffsetResetNeeded(tp)) {
                needsOffsetReset.add(tp);
            } else if (subscriptions.committed(tp) == null) {
                // there's no committed position, so we need to reset with the default strategy
                subscriptions.needOffsetReset(tp);
                needsOffsetReset.add(tp);
            } else {
                long committed = subscriptions.committed(tp).offset();
                log.debug("Resetting offset for partition {} to the committed offset {}", tp, committed);
                subscriptions.seek(tp, committed);
            }
        }

        if (!needsOffsetReset.isEmpty()) {
            resetOffsets(needsOffsetReset);
        }
    }

    /**
     * Get topic metadata for all topics in the cluster
     * @param timeout time for which getting topic metadata is attempted
     * @return The map of topics with their partition information
     */
    public Map<String, List<PartitionInfo>> getAllTopicMetadata(long timeout) {
        return getTopicMetadata(MetadataRequest.Builder.allTopics(), timeout);
    }

    /**
     * Get metadata for all topics present in Kafka cluster
     *
     * @param request The MetadataRequest to send
     * @param timeout time for which getting topic metadata is attempted
     * @return The map of topics with their partition information
     */
    public Map<String, List<PartitionInfo>> getTopicMetadata(MetadataRequest.Builder request, long timeout) {
        // Save the round trip if no topics are requested.
        if (!request.isAllTopics() && request.topics().isEmpty())
            return Collections.emptyMap();

        long start = time.milliseconds();
        long remaining = timeout;

        do {
            RequestFuture<ClientResponse> future = sendMetadataRequest(request);
            client.poll(future, remaining);

            if (future.failed() && !future.isRetriable())
                throw future.exception();

            if (future.succeeded()) {
                MetadataResponse response = (MetadataResponse) future.value().responseBody();
                Cluster cluster = response.cluster();

                Set<String> unauthorizedTopics = cluster.unauthorizedTopics();
                if (!unauthorizedTopics.isEmpty())
                    throw new TopicAuthorizationException(unauthorizedTopics);

                boolean shouldRetry = false;
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
                            shouldRetry = true;
                        else
                            throw new KafkaException("Unexpected error fetching metadata for topic " + topic,
                                    error.exception());
                    }
                }

                if (!shouldRetry) {
                    HashMap<String, List<PartitionInfo>> topicsPartitionInfos = new HashMap<>();
                    for (String topic : cluster.topics())
                        topicsPartitionInfos.put(topic, cluster.availablePartitionsForTopic(topic));
                    return topicsPartitionInfos;
                }
            }

            long elapsed = time.milliseconds() - start;
            remaining = timeout - elapsed;

            if (remaining > 0) {
                long backoff = Math.min(remaining, retryBackoffMs);
                time.sleep(backoff);
                remaining -= backoff;
            }
        } while (remaining > 0);

        throw new TimeoutException("Timeout expired while fetching topic metadata");
    }

    /**
     * Send Metadata Request to least loaded node in Kafka cluster asynchronously
     * @return A future that indicates result of sent metadata request
     */
    private RequestFuture<ClientResponse> sendMetadataRequest(MetadataRequest.Builder request) {
        final Node node = client.leastLoadedNode();
        if (node == null)
            return RequestFuture.noBrokersAvailable();
        else
            return client.send(node, request);
    }

    private void offsetResetStrategyTimestamp(
            final TopicPartition partition,
            final Map<TopicPartition, Long> output,
            final Set<TopicPartition> partitionsWithNoOffsets) {
        OffsetResetStrategy strategy = subscriptions.resetStrategy(partition);
        if (strategy == OffsetResetStrategy.EARLIEST)
            output.put(partition, ListOffsetRequest.EARLIEST_TIMESTAMP);
        else if (strategy == OffsetResetStrategy.LATEST)
            output.put(partition, endTimestamp());
        else
            partitionsWithNoOffsets.add(partition);
    }

    /**
     * Reset offsets for the given partition using the offset reset strategy.
     *
     * @param partitions  The partitions that need offsets reset
     * @throws org.apache.kafka.clients.consumer.NoOffsetForPartitionException If no offset reset strategy is defined
     */
    private void resetOffsets(final Set<TopicPartition> partitions) {
        final Map<TopicPartition, Long> offsetResets = new HashMap<>();
        final Set<TopicPartition> partitionsWithNoOffsets = new HashSet<>();
        for (final TopicPartition partition : partitions) {
            offsetResetStrategyTimestamp(partition, offsetResets, partitionsWithNoOffsets);
        }
        final Map<TopicPartition, OffsetData> offsetsByTimes = retrieveOffsetsByTimes(offsetResets, Long.MAX_VALUE, false);
        for (final TopicPartition partition : partitions) {
            final OffsetData offsetData = offsetsByTimes.get(partition);
            if (offsetData == null) {
                partitionsWithNoOffsets.add(partition);
                continue;
            }
            // we might lose the assignment while fetching the offset, so check it is still active
            if (subscriptions.isAssigned(partition)) {
                log.debug("Resetting offset for partition {} to offset {}.", partition, offsetData.offset);
                this.subscriptions.seek(partition, offsetData.offset);
            }
        }
        if (!partitionsWithNoOffsets.isEmpty()) {
            throw new NoOffsetForPartitionException(partitionsWithNoOffsets);
        }
    }

    public Map<TopicPartition, OffsetAndTimestamp> getOffsetsByTimes(Map<TopicPartition, Long> timestampsToSearch,
                                                                     long timeout) {
        Map<TopicPartition, OffsetData> offsetData = retrieveOffsetsByTimes(timestampsToSearch, timeout, true);

        HashMap<TopicPartition, OffsetAndTimestamp> offsetsByTimes = new HashMap<>(timestampsToSearch.size());
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet())
            offsetsByTimes.put(entry.getKey(), null);

        for (Map.Entry<TopicPartition, OffsetData> entry : offsetData.entrySet()) {
            // 'entry.getValue().timestamp' will not be null since we are guaranteed
            // to work with a v1 (or later) ListOffset request
            offsetsByTimes.put(entry.getKey(), new OffsetAndTimestamp(entry.getValue().offset, entry.getValue().timestamp));
        }

        return offsetsByTimes;
    }

    private Map<TopicPartition, OffsetData> retrieveOffsetsByTimes(
            Map<TopicPartition, Long> timestampsToSearch, long timeout, boolean requireTimestamps) {
        if (timestampsToSearch.isEmpty())
            return Collections.emptyMap();

        long startMs = time.milliseconds();
        long remaining = timeout;
        do {
            RequestFuture<Map<TopicPartition, OffsetData>> future =
                    sendListOffsetRequests(requireTimestamps, timestampsToSearch);
            client.poll(future, remaining);

            if (!future.isDone())
                break;

            if (future.succeeded())
                return future.value();

            if (!future.isRetriable())
                throw future.exception();

            long elapsed = time.milliseconds() - startMs;
            remaining = timeout - elapsed;
            if (remaining <= 0)
                break;

            if (future.exception() instanceof InvalidMetadataException)
                client.awaitMetadataUpdate(remaining);
            else
                time.sleep(Math.min(remaining, retryBackoffMs));

            elapsed = time.milliseconds() - startMs;
            remaining = timeout - elapsed;
        } while (remaining > 0);
        throw new TimeoutException("Failed to get offsets by times in " + timeout + " ms");
    }

    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, long timeout) {
        return beginningOrEndOffset(partitions, ListOffsetRequest.EARLIEST_TIMESTAMP, timeout);
    }

    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, long timeout) {
        return beginningOrEndOffset(partitions, endTimestamp(), timeout);
    }

    private long endTimestamp() {
        return ListOffsetRequest.LATEST_TIMESTAMP;
    }

    private Map<TopicPartition, Long> beginningOrEndOffset(Collection<TopicPartition> partitions,
                                                           long timestamp,
                                                           long timeout) {
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        for (TopicPartition tp : partitions)
            timestampsToSearch.put(tp, timestamp);
        Map<TopicPartition, Long> result = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetData> entry :
                retrieveOffsetsByTimes(timestampsToSearch, timeout, false).entrySet()) {
            result.put(entry.getKey(), entry.getValue().offset);
        }
        return result;
    }

    /**
     * Return the fetched records, empty the record buffer and update the consumed position.
     *
     * NOTE: returning empty records guarantees the consumed position are NOT updated.
     *
     * @return The fetched records per partition
     * @throws OffsetOutOfRangeException If there is OffsetOutOfRange error in fetchResponse and
     *         the defaultResetPolicy is NONE
     */
    public Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchedRecords() {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> fetched = new HashMap<>();
        int recordsRemaining = maxPollRecords;

        try {
            while (recordsRemaining > 0) {
                if (nextInLineRecords == null || nextInLineRecords.isFetched) {
                    CompletedFetch completedFetch = completedFetches.peek();
                    if (completedFetch == null) break;

                    nextInLineRecords = parseCompletedFetch(completedFetch);
                    completedFetches.poll();
                } else {
                    List<ConsumerRecord<K, V>> records = fetchRecords(nextInLineRecords, recordsRemaining);
                    TopicPartition partition = nextInLineRecords.partition;
                    if (!records.isEmpty()) {
                        List<ConsumerRecord<K, V>> currentRecords = fetched.get(partition);
                        if (currentRecords == null) {
                            fetched.put(partition, records);
                        } else {
                            // this case shouldn't usually happen because we only send one fetch at a time per partition,
                            // but it might conceivably happen in some rare cases (such as partition leader changes).
                            // we have to copy to a new list because the old one may be immutable
                            List<ConsumerRecord<K, V>> newRecords = new ArrayList<>(records.size() + currentRecords.size());
                            newRecords.addAll(currentRecords);
                            newRecords.addAll(records);
                            fetched.put(partition, newRecords);
                        }
                        recordsRemaining -= records.size();
                    }
                }
            }
        } catch (KafkaException e) {
            if (fetched.isEmpty())
                throw e;
        }
        return fetched;
    }

    private List<ConsumerRecord<K, V>> fetchRecords(PartitionRecords partitionRecords, int maxRecords) {
        if (!subscriptions.isAssigned(partitionRecords.partition)) {
            // this can happen when a rebalance happened before fetched records are returned to the consumer's poll call
            log.debug("Not returning fetched records for partition {} since it is no longer assigned",
                    partitionRecords.partition);
        } else {
            // note that the consumed position should always be available as long as the partition is still assigned
            long position = subscriptions.position(partitionRecords.partition);
            if (!subscriptions.isFetchable(partitionRecords.partition)) {
                // this can happen when a partition is paused before fetched records are returned to the consumer's poll call
                log.debug("Not returning fetched records for assigned partition {} since it is no longer fetchable",
                        partitionRecords.partition);
            } else if (partitionRecords.nextFetchOffset == position) {
                List<ConsumerRecord<K, V>> partRecords = partitionRecords.fetchRecords(maxRecords);

                long nextOffset = partitionRecords.nextFetchOffset;
                log.trace("Returning fetched records at offset {} for assigned partition {} and update " +
                        "position to {}", position, partitionRecords.partition, nextOffset);
                subscriptions.position(partitionRecords.partition, nextOffset);

                Long partitionLag = subscriptions.partitionLag(partitionRecords.partition, isolationLevel);
                if (partitionLag != null)
                    this.sensors.recordPartitionLag(partitionRecords.partition, partitionLag);

                return partRecords;
            } else {
                // these records aren't next in line based on the last consumed position, ignore them
                // they must be from an obsolete request
                log.debug("Ignoring fetched records for {} at offset {} since the current position is {}",
                        partitionRecords.partition, partitionRecords.nextFetchOffset, position);
            }
        }

        partitionRecords.drain();
        return emptyList();
    }

    /**
     * Search the offsets by target times for the specified partitions.
     *
     * @param requireTimestamps true if we should fail with an UnsupportedVersionException if the broker does
     *                         not support fetching precise timestamps for offsets
     * @param timestampsToSearch the mapping between partitions and target time
     * @return A response which can be polled to obtain the corresponding timestamps and offsets.
     */
    private RequestFuture<Map<TopicPartition, OffsetData>> sendListOffsetRequests(
            final boolean requireTimestamps,
            final Map<TopicPartition, Long> timestampsToSearch) {
        // Group the partitions by node.
        final Map<Node, Map<TopicPartition, Long>> timestampsToSearchByNode = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry: timestampsToSearch.entrySet()) {
            TopicPartition tp  = entry.getKey();
            PartitionInfo info = metadata.fetch().partition(tp);
            if (info == null) {
                metadata.add(tp.topic());
                log.debug("Partition {} is unknown for fetching offset, wait for metadata refresh", tp);
                return RequestFuture.staleMetadata();
            } else if (info.leader() == null) {
                log.debug("Leader for partition {} unavailable for fetching offset, wait for metadata refresh", tp);
                return RequestFuture.leaderNotAvailable();
            } else {
                Node node = info.leader();
                Map<TopicPartition, Long> topicData = timestampsToSearchByNode.get(node);
                if (topicData == null) {
                    topicData = new HashMap<>();
                    timestampsToSearchByNode.put(node, topicData);
                }
                topicData.put(entry.getKey(), entry.getValue());
            }
        }

        final RequestFuture<Map<TopicPartition, OffsetData>> listOffsetRequestsFuture = new RequestFuture<>();
        final Map<TopicPartition, OffsetData> fetchedTimestampOffsets = new HashMap<>();
        final AtomicInteger remainingResponses = new AtomicInteger(timestampsToSearchByNode.size());
        for (Map.Entry<Node, Map<TopicPartition, Long>> entry : timestampsToSearchByNode.entrySet()) {
            sendListOffsetRequest(entry.getKey(), entry.getValue(), requireTimestamps)
                    .addListener(new RequestFutureListener<Map<TopicPartition, OffsetData>>() {
                        @Override
                        public void onSuccess(Map<TopicPartition, OffsetData> value) {
                            synchronized (listOffsetRequestsFuture) {
                                fetchedTimestampOffsets.putAll(value);
                                if (remainingResponses.decrementAndGet() == 0 && !listOffsetRequestsFuture.isDone())
                                    listOffsetRequestsFuture.complete(fetchedTimestampOffsets);
                            }
                        }

                        @Override
                        public void onFailure(RuntimeException e) {
                            synchronized (listOffsetRequestsFuture) {
                                // This may cause all the requests to be retried, but should be rare.
                                if (!listOffsetRequestsFuture.isDone())
                                    listOffsetRequestsFuture.raise(e);
                            }
                        }
                    });
        }
        return listOffsetRequestsFuture;
    }

    /**
     * Send the ListOffsetRequest to a specific broker for the partitions and target timestamps.
     *
     * @param node The node to send the ListOffsetRequest to.
     * @param timestampsToSearch The mapping from partitions to the target timestamps.
     * @param requireTimestamp  True if we require a timestamp in the response.
     * @return A response which can be polled to obtain the corresponding timestamps and offsets.
     */
    private RequestFuture<Map<TopicPartition, OffsetData>> sendListOffsetRequest(final Node node,
                                                                                 final Map<TopicPartition, Long> timestampsToSearch,
                                                                                 boolean requireTimestamp) {
        ListOffsetRequest.Builder builder = ListOffsetRequest.Builder
                .forConsumer(requireTimestamp, isolationLevel)
                .setTargetTimes(timestampsToSearch);

        log.trace("Sending ListOffsetRequest {} to broker {}", builder, node);
        return client.send(node, builder)
                .compose(new RequestFutureAdapter<ClientResponse, Map<TopicPartition, OffsetData>>() {
                    @Override
                    public void onSuccess(ClientResponse response, RequestFuture<Map<TopicPartition, OffsetData>> future) {
                        ListOffsetResponse lor = (ListOffsetResponse) response.responseBody();
                        log.trace("Received ListOffsetResponse {} from broker {}", lor, node);
                        handleListOffsetResponse(timestampsToSearch, lor, future);
                    }
                });
    }

    /**
     * Callback for the response of the list offset call above.
     * @param timestampsToSearch The mapping from partitions to target timestamps
     * @param listOffsetResponse The response from the server.
     * @param future The future to be completed when the response returns. Note that any partition-level errors will
     *               generally fail the entire future result. The one exception is UNSUPPORTED_FOR_MESSAGE_FORMAT,
     *               which indicates that the broker does not support the v1 message format. Partitions with this
     *               particular error are simply left out of the future map. Note that the corresponding timestamp
     *               value of each partition may be null only for v0. In v1 and later the ListOffset API would not
     *               return a null timestamp (-1 is returned instead when necessary).
     */
    @SuppressWarnings("deprecation")
    private void handleListOffsetResponse(Map<TopicPartition, Long> timestampsToSearch,
                                          ListOffsetResponse listOffsetResponse,
                                          RequestFuture<Map<TopicPartition, OffsetData>> future) {
        Map<TopicPartition, OffsetData> timestampOffsetMap = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            ListOffsetResponse.PartitionData partitionData = listOffsetResponse.responseData().get(topicPartition);
            Errors error = partitionData.error;
            if (error == Errors.NONE) {
                if (partitionData.offsets != null) {
                    // Handle v0 response
                    long offset;
                    if (partitionData.offsets.size() > 1) {
                        future.raise(new IllegalStateException("Unexpected partitionData response of length " +
                                partitionData.offsets.size()));
                        return;
                    } else if (partitionData.offsets.isEmpty()) {
                        offset = ListOffsetResponse.UNKNOWN_OFFSET;
                    } else {
                        offset = partitionData.offsets.get(0);
                    }
                    log.debug("Handling v0 ListOffsetResponse response for {}. Fetched offset {}",
                            topicPartition, offset);
                    if (offset != ListOffsetResponse.UNKNOWN_OFFSET) {
                        OffsetData offsetData = new OffsetData(offset, null);
                        timestampOffsetMap.put(topicPartition, offsetData);
                    }
                } else {
                    // Handle v1 and later response
                    log.debug("Handling ListOffsetResponse response for {}. Fetched offset {}, timestamp {}",
                            topicPartition, partitionData.offset, partitionData.timestamp);
                    if (partitionData.offset != ListOffsetResponse.UNKNOWN_OFFSET) {
                        OffsetData offsetData = new OffsetData(partitionData.offset, partitionData.timestamp);
                        timestampOffsetMap.put(topicPartition, offsetData);
                    }
                }
            } else if (error == Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT) {
                // The message format on the broker side is before 0.10.0, we simply put null in the response.
                log.debug("Cannot search by timestamp for partition {} because the message format version " +
                        "is before 0.10.0", topicPartition);
            } else if (error == Errors.NOT_LEADER_FOR_PARTITION) {
                log.debug("Attempt to fetch offsets for partition {} failed due to obsolete leadership information, retrying.",
                        topicPartition);
                future.raise(error);
                return;
            } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                log.warn("Received unknown topic or partition error in ListOffset request for partition {}. The topic/partition " +
                        "may not exist or the user may not have Describe access to it.", topicPartition);
                future.raise(error);
                return;
            } else {
                log.warn("Attempt to fetch offsets for partition {} failed due to: {}", topicPartition, error.message());
                future.raise(new StaleMetadataException());
                return;
            }
        }
        if (!future.isDone())
            future.complete(timestampOffsetMap);
    }

    private List<TopicPartition> fetchablePartitions() {
        Set<TopicPartition> exclude = new HashSet<>();
        List<TopicPartition> fetchable = subscriptions.fetchablePartitions();
        if (nextInLineRecords != null && !nextInLineRecords.isFetched) {
            exclude.add(nextInLineRecords.partition);
        }
        for (CompletedFetch completedFetch : completedFetches) {
            exclude.add(completedFetch.partition);
        }
        fetchable.removeAll(exclude);
        return fetchable;
    }

    /**
     * Create fetch requests for all nodes for which we have assigned partitions
     * that have no existing requests in flight.
     */
    private Map<Node, FetchRequest.Builder> createFetchRequests() {
        // create the fetch info
        Cluster cluster = metadata.fetch();
        Map<Node, LinkedHashMap<TopicPartition, FetchRequest.PartitionData>> fetchable = new LinkedHashMap<>();
        for (TopicPartition partition : fetchablePartitions()) {
            Node node = cluster.leaderFor(partition);
            if (node == null) {
                metadata.requestUpdate();
            } else if (!this.client.hasPendingRequests(node)) {
                // if there is a leader and no in-flight requests, issue a new fetch
                LinkedHashMap<TopicPartition, FetchRequest.PartitionData> fetch = fetchable.get(node);
                if (fetch == null) {
                    fetch = new LinkedHashMap<>();
                    fetchable.put(node, fetch);
                }

                long position = this.subscriptions.position(partition);
                fetch.put(partition, new FetchRequest.PartitionData(position, FetchRequest.INVALID_LOG_START_OFFSET,
                        this.fetchSize));
                log.debug("Added {} fetch request for partition {} at offset {} to node {}", isolationLevel,
                        partition, position, node);
            } else {
                log.trace("Skipping fetch for partition {} because there is an in-flight request to {}", partition, node);
            }
        }

        // create the fetches
        Map<Node, FetchRequest.Builder> requests = new HashMap<>();
        for (Map.Entry<Node, LinkedHashMap<TopicPartition, FetchRequest.PartitionData>> entry : fetchable.entrySet()) {
            Node node = entry.getKey();
            FetchRequest.Builder fetch = FetchRequest.Builder.forConsumer(this.maxWaitMs, this.minBytes,
                    entry.getValue(), isolationLevel)
                    .setMaxBytes(this.maxBytes);
            requests.put(node, fetch);
        }
        return requests;
    }

    /**
     * The callback for fetch completion
     */
    private PartitionRecords parseCompletedFetch(CompletedFetch completedFetch) {
        TopicPartition tp = completedFetch.partition;
        FetchResponse.PartitionData partition = completedFetch.partitionData;
        long fetchOffset = completedFetch.fetchedOffset;
        PartitionRecords partitionRecords = null;
        Errors error = partition.error;

        try {
            if (!subscriptions.isFetchable(tp)) {
                // this can happen when a rebalance happened or a partition consumption paused
                // while fetch is still in-flight
                log.debug("Ignoring fetched records for partition {} since it is no longer fetchable", tp);
            } else if (error == Errors.NONE) {
                // we are interested in this fetch only if the beginning offset matches the
                // current consumed position
                Long position = subscriptions.position(tp);
                if (position == null || position != fetchOffset) {
                    log.debug("Discarding stale fetch response for partition {} since its offset {} does not match " +
                            "the expected offset {}", tp, fetchOffset, position);
                    return null;
                }

                log.trace("Preparing to read {} bytes of data for partition {} with offset {}",
                        partition.records.sizeInBytes(), tp, position);
                Iterator<? extends RecordBatch> batches = partition.records.batches().iterator();
                partitionRecords = new PartitionRecords(tp, completedFetch, batches);

                if (!batches.hasNext() && partition.records.sizeInBytes() > 0) {
                    if (completedFetch.responseVersion < 3) {
                        // Implement the pre KIP-74 behavior of throwing a RecordTooLargeException.
                        Map<TopicPartition, Long> recordTooLargePartitions = Collections.singletonMap(tp, fetchOffset);
                        throw new RecordTooLargeException("There are some messages at [Partition=Offset]: " +
                                recordTooLargePartitions + " whose size is larger than the fetch size " + this.fetchSize +
                                " and hence cannot be returned. Please considering upgrading your broker to 0.10.1.0 or " +
                                "newer to avoid this issue. Alternately, increase the fetch size on the client (using " +
                                ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG + ")",
                                recordTooLargePartitions);
                    } else {
                        // This should not happen with brokers that support FetchRequest/Response V3 or higher (i.e. KIP-74)
                        throw new KafkaException("Failed to make progress reading messages at " + tp + "=" +
                            fetchOffset + ". Received a non-empty fetch response from the server, but no " +
                            "complete records were found.");
                    }
                }

                if (partition.highWatermark >= 0) {
                    log.trace("Updating high watermark for partition {} to {}", tp, partition.highWatermark);
                    subscriptions.updateHighWatermark(tp, partition.highWatermark);
                }

                if (partition.lastStableOffset >= 0) {
                    log.trace("Updating last stable offset for partition {} to {}", tp, partition.lastStableOffset);
                    subscriptions.updateLastStableOffset(tp, partition.lastStableOffset);
                }
            } else if (error == Errors.NOT_LEADER_FOR_PARTITION ||
                       error == Errors.KAFKA_STORAGE_ERROR) {
                log.debug("Error in fetch for partition {}: {}", tp, error.exceptionName());
                this.metadata.requestUpdate();
            } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                log.warn("Received unknown topic or partition error in fetch for partition {}. The topic/partition " +
                        "may not exist or the user may not have Describe access to it", tp);
                this.metadata.requestUpdate();
            } else if (error == Errors.OFFSET_OUT_OF_RANGE) {
                if (fetchOffset != subscriptions.position(tp)) {
                    log.debug("Discarding stale fetch response for partition {} since the fetched offset {}" +
                            "does not match the current offset {}", tp, fetchOffset, subscriptions.position(tp));
                } else if (subscriptions.hasDefaultOffsetResetPolicy()) {
                    log.info("Fetch offset {} is out of range for partition {}, resetting offset", fetchOffset, tp);
                    subscriptions.needOffsetReset(tp);
                } else {
                    throw new OffsetOutOfRangeException(Collections.singletonMap(tp, fetchOffset));
                }
            } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                log.warn("Not authorized to read from topic {}.", tp.topic());
                throw new TopicAuthorizationException(Collections.singleton(tp.topic()));
            } else if (error == Errors.UNKNOWN_SERVER_ERROR) {
                log.warn("Unknown error fetching data for topic-partition {}", tp);
            } else {
                throw new IllegalStateException("Unexpected error code " + error.code() + " while fetching data");
            }
        } finally {
            if (partitionRecords == null)
                completedFetch.metricAggregator.record(tp, 0, 0);

            if (error != Errors.NONE)
                // we move the partition to the end if there was an error. This way, it's more likely that partitions for
                // the same topic can remain together (allowing for more efficient serialization).
                subscriptions.movePartitionToEnd(tp);
        }

        return partitionRecords;
    }


    /**
     * Parse the record entry, deserializing the key / value fields if necessary
     */
    private ConsumerRecord<K, V> parseRecord(TopicPartition partition,
                                             RecordBatch batch,
                                             Record record) {
        try {
            long offset = record.offset();
            long timestamp = record.timestamp();
            TimestampType timestampType = batch.timestampType();
            Headers headers = new RecordHeaders(record.headers());
            ByteBuffer keyBytes = record.key();
            byte[] keyByteArray = keyBytes == null ? null : Utils.toArray(keyBytes);
            K key = keyBytes == null ? null : this.keyDeserializer.deserialize(partition.topic(), headers, keyByteArray);
            ByteBuffer valueBytes = record.value();
            byte[] valueByteArray = valueBytes == null ? null : Utils.toArray(valueBytes);
            V value = valueBytes == null ? null : this.valueDeserializer.deserialize(partition.topic(), headers, valueByteArray);
            return new ConsumerRecord<>(partition.topic(), partition.partition(), offset,
                                        timestamp, timestampType, record.checksumOrNull(),
                                        keyByteArray == null ? ConsumerRecord.NULL_SIZE : keyByteArray.length,
                                        valueByteArray == null ? ConsumerRecord.NULL_SIZE : valueByteArray.length,
                                        key, value, headers);
        } catch (RuntimeException e) {
            throw new SerializationException("Error deserializing key/value for partition " + partition +
                    " at offset " + record.offset() + ". If needed, please seek past the record to continue consumption.", e);
        }
    }

    @Override
    public void onAssignment(Set<TopicPartition> assignment) {
        sensors.updatePartitionLagSensors(assignment);
    }

    public static Sensor throttleTimeSensor(Metrics metrics, FetcherMetricsRegistry metricsRegistry) {
        Sensor fetchThrottleTimeSensor = metrics.sensor("fetch-throttle-time");
        fetchThrottleTimeSensor.add(metrics.metricInstance(metricsRegistry.fetchThrottleTimeAvg), new Avg());

        fetchThrottleTimeSensor.add(metrics.metricInstance(metricsRegistry.fetchThrottleTimeMax), new Max());

        return fetchThrottleTimeSensor;
    }

    private class PartitionRecords {
        private final TopicPartition partition;
        private final CompletedFetch completedFetch;
        private final Iterator<? extends RecordBatch> batches;
        private final Set<Long> abortedProducerIds;
        private final PriorityQueue<FetchResponse.AbortedTransaction> abortedTransactions;

        private int recordsRead;
        private int bytesRead;
        private RecordBatch currentBatch;
        private Record lastRecord;
        private CloseableIterator<Record> records;
        private long nextFetchOffset;
        private boolean isFetched = false;
        private Exception cachedRecordException = null;
        private boolean corruptLastRecord = false;

        private PartitionRecords(TopicPartition partition,
                                 CompletedFetch completedFetch,
                                 Iterator<? extends RecordBatch> batches) {
            this.partition = partition;
            this.completedFetch = completedFetch;
            this.batches = batches;
            this.nextFetchOffset = completedFetch.fetchedOffset;
            this.abortedProducerIds = new HashSet<>();
            this.abortedTransactions = abortedTransactions(completedFetch.partitionData);
        }

        private void drain() {
            if (!isFetched) {
                maybeCloseRecordStream();
                cachedRecordException = null;
                this.isFetched = true;
                this.completedFetch.metricAggregator.record(partition, bytesRead, recordsRead);

                // we move the partition to the end if we received some bytes. This way, it's more likely that partitions
                // for the same topic can remain together (allowing for more efficient serialization).
                if (bytesRead > 0)
                    subscriptions.movePartitionToEnd(partition);
            }
        }

        private void maybeEnsureValid(RecordBatch batch) {
            if (checkCrcs && currentBatch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                try {
                    batch.ensureValid();
                } catch (InvalidRecordException e) {
                    throw new KafkaException("Record batch for partition " + partition + " at offset " +
                            batch.baseOffset() + " is invalid, cause: " + e.getMessage());
                }
            }
        }

        private void maybeEnsureValid(Record record) {
            if (checkCrcs) {
                try {
                    record.ensureValid();
                } catch (InvalidRecordException e) {
                    throw new KafkaException("Record for partition " + partition + " at offset " + record.offset()
                            + " is invalid, cause: " + e.getMessage());
                }
            }
        }

        private void maybeCloseRecordStream() {
            if (records != null) {
                records.close();
                records = null;
            }
        }

        private Record nextFetchedRecord() {
            while (true) {
                if (records == null || !records.hasNext()) {
                    maybeCloseRecordStream();

                    if (!batches.hasNext()) {
                        // Message format v2 preserves the last offset in a batch even if the last record is removed
                        // through compaction. By using the next offset computed from the last offset in the batch,
                        // we ensure that the offset of the next fetch will point to the next batch, which avoids
                        // unnecessary re-fetching of the same batch (in the worst case, the consumer could get stuck
                        // fetching the same batch repeatedly).
                        if (currentBatch != null)
                            nextFetchOffset = currentBatch.nextOffset();
                        drain();
                        return null;
                    }

                    currentBatch = batches.next();
                    maybeEnsureValid(currentBatch);

                    if (isolationLevel == IsolationLevel.READ_COMMITTED && currentBatch.hasProducerId()) {
                        // remove from the aborted transaction queue all aborted transactions which have begun
                        // before the current batch's last offset and add the associated producerIds to the
                        // aborted producer set
                        consumeAbortedTransactionsUpTo(currentBatch.lastOffset());

                        long producerId = currentBatch.producerId();
                        if (containsAbortMarker(currentBatch)) {
                            abortedProducerIds.remove(producerId);
                        } else if (isBatchAborted(currentBatch)) {
                            log.debug("Skipping aborted record batch from partition {} with producerId {} and " +
                                          "offsets {} to {}",
                                      partition, producerId, currentBatch.baseOffset(), currentBatch.lastOffset());
                            nextFetchOffset = currentBatch.nextOffset();
                            continue;
                        }
                    }

                    records = currentBatch.streamingIterator(decompressionBufferSupplier);
                } else {
                    Record record = records.next();
                    // skip any records out of range
                    if (record.offset() >= nextFetchOffset) {
                        // we only do validation when the message should not be skipped.
                        maybeEnsureValid(record);

                        // control records are not returned to the user
                        if (!currentBatch.isControlBatch()) {
                            return record;
                        } else {
                            // Increment the next fetch offset when we skip a control batch.
                            nextFetchOffset = record.offset() + 1;
                        }
                    }
                }
            }
        }

        private List<ConsumerRecord<K, V>> fetchRecords(int maxRecords) {
            // Error when fetching the next record before deserialization.
            if (corruptLastRecord)
                throw new KafkaException("Received exception when fetching the next record from " + partition
                                             + ". If needed, please seek past the record to "
                                             + "continue consumption.", cachedRecordException);

            if (isFetched)
                return Collections.emptyList();

            List<ConsumerRecord<K, V>> records = new ArrayList<>();
            try {
                for (int i = 0; i < maxRecords; i++) {
                    // Only move to next record if there was no exception in the last fetch. Otherwise we should
                    // use the last record to do deserialization again.
                    if (cachedRecordException == null) {
                        corruptLastRecord = true;
                        lastRecord = nextFetchedRecord();
                        corruptLastRecord = false;
                    }
                    if (lastRecord == null)
                        break;
                    records.add(parseRecord(partition, currentBatch, lastRecord));
                    recordsRead++;
                    bytesRead += lastRecord.sizeInBytes();
                    nextFetchOffset = lastRecord.offset() + 1;
                    // In some cases, the deserialization may have thrown an exception and the retry may succeed,
                    // we allow user to move forward in this case.
                    cachedRecordException = null;
                }
            } catch (SerializationException se) {
                cachedRecordException = se;
                if (records.isEmpty())
                    throw se;
            } catch (KafkaException e) {
                cachedRecordException = e;
                if (records.isEmpty())
                    throw new KafkaException("Received exception when fetching the next record from " + partition
                                                 + ". If needed, please seek past the record to "
                                                 + "continue consumption.", e);
            }
            return records;
        }

        private void consumeAbortedTransactionsUpTo(long offset) {
            if (abortedTransactions == null)
                return;

            while (!abortedTransactions.isEmpty() && abortedTransactions.peek().firstOffset <= offset) {
                FetchResponse.AbortedTransaction abortedTransaction = abortedTransactions.poll();
                abortedProducerIds.add(abortedTransaction.producerId);
            }
        }

        private boolean isBatchAborted(RecordBatch batch) {
            return batch.isTransactional() && abortedProducerIds.contains(batch.producerId());
        }

        private PriorityQueue<FetchResponse.AbortedTransaction> abortedTransactions(FetchResponse.PartitionData partition) {
            if (partition.abortedTransactions == null || partition.abortedTransactions.isEmpty())
                return null;

            PriorityQueue<FetchResponse.AbortedTransaction> abortedTransactions = new PriorityQueue<>(
                    partition.abortedTransactions.size(),
                    new Comparator<FetchResponse.AbortedTransaction>() {
                        @Override
                        public int compare(FetchResponse.AbortedTransaction o1, FetchResponse.AbortedTransaction o2) {
                            return Long.compare(o1.firstOffset, o2.firstOffset);
                        }
                    }
            );
            abortedTransactions.addAll(partition.abortedTransactions);
            return abortedTransactions;
        }

        private boolean containsAbortMarker(RecordBatch batch) {
            if (!batch.isControlBatch())
                return false;

            Iterator<Record> batchIterator = batch.iterator();
            if (!batchIterator.hasNext())
                throw new InvalidRecordException("Invalid batch for partition " + partition + " at offset " +
                        batch.baseOffset() + " with control sequence set, but no records");

            Record firstRecord = batchIterator.next();
            return ControlRecordType.ABORT == ControlRecordType.parse(firstRecord.key());
        }
    }

    private static class CompletedFetch {
        private final TopicPartition partition;
        private final long fetchedOffset;
        private final FetchResponse.PartitionData partitionData;
        private final FetchResponseMetricAggregator metricAggregator;
        private final short responseVersion;

        private CompletedFetch(TopicPartition partition,
                               long fetchedOffset,
                               FetchResponse.PartitionData partitionData,
                               FetchResponseMetricAggregator metricAggregator,
                               short responseVersion) {
            this.partition = partition;
            this.fetchedOffset = fetchedOffset;
            this.partitionData = partitionData;
            this.metricAggregator = metricAggregator;
            this.responseVersion = responseVersion;
        }
    }

    /**
     * Since we parse the message data for each partition from each fetch response lazily, fetch-level
     * metrics need to be aggregated as the messages from each partition are parsed. This class is used
     * to facilitate this incremental aggregation.
     */
    private static class FetchResponseMetricAggregator {
        private final FetchManagerMetrics sensors;
        private final Set<TopicPartition> unrecordedPartitions;

        private final FetchMetrics fetchMetrics = new FetchMetrics();
        private final Map<String, FetchMetrics> topicFetchMetrics = new HashMap<>();

        private FetchResponseMetricAggregator(FetchManagerMetrics sensors,
                                              Set<TopicPartition> partitions) {
            this.sensors = sensors;
            this.unrecordedPartitions = partitions;
        }

        /**
         * After each partition is parsed, we update the current metric totals with the total bytes
         * and number of records parsed. After all partitions have reported, we write the metric.
         */
        public void record(TopicPartition partition, int bytes, int records) {
            this.unrecordedPartitions.remove(partition);
            this.fetchMetrics.increment(bytes, records);

            // collect and aggregate per-topic metrics
            String topic = partition.topic();
            FetchMetrics topicFetchMetric = this.topicFetchMetrics.get(topic);
            if (topicFetchMetric == null) {
                topicFetchMetric = new FetchMetrics();
                this.topicFetchMetrics.put(topic, topicFetchMetric);
            }
            topicFetchMetric.increment(bytes, records);

            if (this.unrecordedPartitions.isEmpty()) {
                // once all expected partitions from the fetch have reported in, record the metrics
                this.sensors.bytesFetched.record(topicFetchMetric.fetchBytes);
                this.sensors.recordsFetched.record(topicFetchMetric.fetchRecords);

                // also record per-topic metrics
                for (Map.Entry<String, FetchMetrics> entry: this.topicFetchMetrics.entrySet()) {
                    FetchMetrics metric = entry.getValue();
                    this.sensors.recordTopicFetchMetrics(entry.getKey(), metric.fetchBytes, metric.fetchRecords);
                }
            }
        }

        private static class FetchMetrics {
            private int fetchBytes;
            private int fetchRecords;

            protected void increment(int bytes, int records) {
                this.fetchBytes += bytes;
                this.fetchRecords += records;
            }
        }
    }

    private static class FetchManagerMetrics {
        private final Metrics metrics;
        private FetcherMetricsRegistry metricsRegistry;
        private final Sensor bytesFetched;
        private final Sensor recordsFetched;
        private final Sensor fetchLatency;
        private final Sensor recordsFetchLag;

        private Set<TopicPartition> assignedPartitions;

        private FetchManagerMetrics(Metrics metrics, FetcherMetricsRegistry metricsRegistry) {
            this.metrics = metrics;
            this.metricsRegistry = metricsRegistry;

            this.bytesFetched = metrics.sensor("bytes-fetched");
            this.bytesFetched.add(metrics.metricInstance(metricsRegistry.fetchSizeAvg), new Avg());
            this.bytesFetched.add(metrics.metricInstance(metricsRegistry.fetchSizeMax), new Max());
            this.bytesFetched.add(new Meter(metrics.metricInstance(metricsRegistry.bytesConsumedRate),
                    metrics.metricInstance(metricsRegistry.bytesConsumedTotal)));

            this.recordsFetched = metrics.sensor("records-fetched");
            this.recordsFetched.add(metrics.metricInstance(metricsRegistry.recordsPerRequestAvg), new Avg());
            this.recordsFetched.add(new Meter(metrics.metricInstance(metricsRegistry.recordsConsumedRate),
                    metrics.metricInstance(metricsRegistry.recordsConsumedTotal)));

            this.fetchLatency = metrics.sensor("fetch-latency");
            this.fetchLatency.add(metrics.metricInstance(metricsRegistry.fetchLatencyAvg), new Avg());
            this.fetchLatency.add(metrics.metricInstance(metricsRegistry.fetchLatencyMax), new Max());
            this.fetchLatency.add(new Meter(new Count(), metrics.metricInstance(metricsRegistry.fetchRequestRate),
                    metrics.metricInstance(metricsRegistry.fetchRequestTotal)));

            this.recordsFetchLag = metrics.sensor("records-lag");
            this.recordsFetchLag.add(metrics.metricInstance(metricsRegistry.recordsLagMax), new Max());
        }

        private void recordTopicFetchMetrics(String topic, int bytes, int records) {
            // record bytes fetched
            String name = "topic." + topic + ".bytes-fetched";
            Sensor bytesFetched = this.metrics.getSensor(name);
            if (bytesFetched == null) {
                Map<String, String> metricTags = Collections.singletonMap("topic", topic.replace('.', '_'));

                bytesFetched = this.metrics.sensor(name);
                bytesFetched.add(this.metrics.metricInstance(metricsRegistry.topicFetchSizeAvg,
                        metricTags), new Avg());
                bytesFetched.add(this.metrics.metricInstance(metricsRegistry.topicFetchSizeMax,
                        metricTags), new Max());
                bytesFetched.add(new Meter(this.metrics.metricInstance(metricsRegistry.topicBytesConsumedRate, metricTags),
                        this.metrics.metricInstance(metricsRegistry.topicBytesConsumedTotal, metricTags)));
            }
            bytesFetched.record(bytes);

            // record records fetched
            name = "topic." + topic + ".records-fetched";
            Sensor recordsFetched = this.metrics.getSensor(name);
            if (recordsFetched == null) {
                Map<String, String> metricTags = new HashMap<>(1);
                metricTags.put("topic", topic.replace('.', '_'));

                recordsFetched = this.metrics.sensor(name);
                recordsFetched.add(this.metrics.metricInstance(metricsRegistry.topicRecordsPerRequestAvg,
                        metricTags), new Avg());
                recordsFetched.add(new Meter(this.metrics.metricInstance(metricsRegistry.topicRecordsConsumedRate, metricTags),
                        this.metrics.metricInstance(metricsRegistry.topicRecordsConsumedTotal, metricTags)));
            }
            recordsFetched.record(records);
        }

        private void updatePartitionLagSensors(Set<TopicPartition> assignedPartitions) {
            if (this.assignedPartitions != null) {
                for (TopicPartition tp : this.assignedPartitions) {
                    if (!assignedPartitions.contains(tp))
                        metrics.removeSensor(partitionLagMetricName(tp));
                }
            }
            this.assignedPartitions = assignedPartitions;
        }

        private void recordPartitionLag(TopicPartition tp, long lag) {
            this.recordsFetchLag.record(lag);

            String name = partitionLagMetricName(tp);
            Sensor recordsLag = this.metrics.getSensor(name);
            if (recordsLag == null) {
                recordsLag = this.metrics.sensor(name);
                recordsLag.add(this.metrics.metricName(name,
                        metricsRegistry.partitionRecordsLag.group(),
                        metricsRegistry.partitionRecordsLag.description()), new Value());
                recordsLag.add(this.metrics.metricName(name + "-max",
                        metricsRegistry.partitionRecordsLagMax.group(),
                        metricsRegistry.partitionRecordsLagMax.description()), new Max());
                recordsLag.add(this.metrics.metricName(name + "-avg",
                        metricsRegistry.partitionRecordsLagAvg.group(),
                        metricsRegistry.partitionRecordsLagAvg.description()), new Avg());
            }
            recordsLag.record(lag);
        }

        private static String partitionLagMetricName(TopicPartition tp) {
            return tp + ".records-lag";
        }
    }

    @Override
    public void close() {
        if (nextInLineRecords != null)
            nextInLineRecords.drain();
        decompressionBufferSupplier.close();
    }

}
