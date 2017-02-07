/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.record.LogEntry;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class manage the fetching process with the brokers.
 */
public class Fetcher<K, V> implements SubscriptionState.Listener {

    private static final Logger log = LoggerFactory.getLogger(Fetcher.class);

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
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    private PartitionRecords<K, V> nextInLineRecords = null;

    public Fetcher(ConsumerNetworkClient client,
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
                   String metricGrpPrefix,
                   Time time,
                   long retryBackoffMs) {
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
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.completedFetches = new ConcurrentLinkedQueue<>();
        this.sensors = new FetchManagerMetrics(metrics, metricGrpPrefix);
        this.retryBackoffMs = retryBackoffMs;

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

            log.debug("Sending fetch for partitions {} to broker {}", request.fetchData().keySet(), fetchTarget);
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
                                long fetchOffset = request.fetchData().get(partition).offset;
                                FetchResponse.PartitionData fetchData = entry.getValue();
                                completedFetches.add(new CompletedFetch(partition, fetchOffset, fetchData, metricAggregator,
                                        request.version()));
                            }

                            sensors.fetchLatency.record(resp.requestLatencyMs());
                            sensors.fetchThrottleTimeSensor.record(response.getThrottleTime());
                        }

                        @Override
                        public void onFailure(RuntimeException e) {
                            log.debug("Fetch request to {} for partitions {} failed", fetchTarget, request.fetchData().keySet(), e);
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
        for (TopicPartition tp : partitions) {
            // TODO: If there are several offsets to reset, we could submit offset requests in parallel
            if (subscriptions.isAssigned(tp) && subscriptions.isOffsetResetNeeded(tp))
                resetOffset(tp);
        }
    }

    /**
     * Update the fetch positions for the provided partitions.
     * @param partitions the partitions to update positions for
     * @throws NoOffsetForPartitionException If no offset is stored for a given partition and no reset policy is available
     */
    public void updateFetchPositions(Set<TopicPartition> partitions) {
        // reset the fetch position to the committed position
        for (TopicPartition tp : partitions) {
            if (!subscriptions.isAssigned(tp) || subscriptions.hasValidPosition(tp))
                continue;

            if (subscriptions.isOffsetResetNeeded(tp)) {
                resetOffset(tp);
            } else if (subscriptions.committed(tp) == null) {
                // there's no committed position, so we need to reset with the default strategy
                subscriptions.needOffsetReset(tp);
                resetOffset(tp);
            } else {
                long committed = subscriptions.committed(tp).offset();
                log.debug("Resetting offset for partition {} to the committed offset {}", tp, committed);
                subscriptions.seek(tp, committed);
            }
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

    /**
     * Reset offsets for the given partition using the offset reset strategy.
     *
     * @param partition The given partition that needs reset offset
     * @throws org.apache.kafka.clients.consumer.NoOffsetForPartitionException If no offset reset strategy is defined
     */
    private void resetOffset(TopicPartition partition) {
        OffsetResetStrategy strategy = subscriptions.resetStrategy(partition);
        log.debug("Resetting offset for partition {} to {} offset.", partition, strategy.name().toLowerCase(Locale.ROOT));
        final long timestamp;
        if (strategy == OffsetResetStrategy.EARLIEST)
            timestamp = ListOffsetRequest.EARLIEST_TIMESTAMP;
        else if (strategy == OffsetResetStrategy.LATEST)
            timestamp = ListOffsetRequest.LATEST_TIMESTAMP;
        else
            throw new NoOffsetForPartitionException(partition);
        Map<TopicPartition, OffsetData> offsetsByTimes = retrieveOffsetsByTimes(
                Collections.singletonMap(partition, timestamp), Long.MAX_VALUE, false);
        OffsetData offsetData = offsetsByTimes.get(partition);
        if (offsetData == null)
            throw new NoOffsetForPartitionException(partition);
        long offset = offsetData.offset;
        // we might lose the assignment while fetching the offset, so check it is still active
        if (subscriptions.isAssigned(partition))
            this.subscriptions.seek(partition, offset);
    }

    public Map<TopicPartition, OffsetAndTimestamp> getOffsetsByTimes(Map<TopicPartition, Long> timestampsToSearch,
                                                                     long timeout) {
        Map<TopicPartition, OffsetData> offsetData = retrieveOffsetsByTimes(timestampsToSearch, timeout, true);
        HashMap<TopicPartition, OffsetAndTimestamp> offsetsByTimes = new HashMap<>(offsetData.size());
        for (Map.Entry<TopicPartition, OffsetData> entry : offsetData.entrySet()) {
            OffsetData data = entry.getValue();
            if (data == null)
                offsetsByTimes.put(entry.getKey(), null);
            else
                offsetsByTimes.put(entry.getKey(), new OffsetAndTimestamp(data.offset, data.timestamp));
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
        return beginningOrEndOffset(partitions, ListOffsetRequest.LATEST_TIMESTAMP, timeout);
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
        Map<TopicPartition, List<ConsumerRecord<K, V>>> drained = new HashMap<>();
        int recordsRemaining = maxPollRecords;

        while (recordsRemaining > 0) {
            if (nextInLineRecords == null || nextInLineRecords.isDrained()) {
                CompletedFetch completedFetch = completedFetches.poll();
                if (completedFetch == null)
                    break;

                nextInLineRecords = parseCompletedFetch(completedFetch);
            } else {
                TopicPartition partition = nextInLineRecords.partition;
                List<ConsumerRecord<K, V>> records = drainRecords(nextInLineRecords, recordsRemaining);
                if (!records.isEmpty()) {
                    List<ConsumerRecord<K, V>> currentRecords = drained.get(partition);
                    if (currentRecords == null) {
                        drained.put(partition, records);
                    } else {
                        // this case shouldn't usually happen because we only send one fetch at a time per partition,
                        // but it might conceivably happen in some rare cases (such as partition leader changes).
                        // we have to copy to a new list because the old one may be immutable
                        List<ConsumerRecord<K, V>> newRecords = new ArrayList<>(records.size() + currentRecords.size());
                        newRecords.addAll(currentRecords);
                        newRecords.addAll(records);
                        drained.put(partition, newRecords);
                    }
                    recordsRemaining -= records.size();
                }
            }
        }

        return drained;
    }

    private List<ConsumerRecord<K, V>> drainRecords(PartitionRecords<K, V> partitionRecords, int maxRecords) {
        if (!subscriptions.isAssigned(partitionRecords.partition)) {
            // this can happen when a rebalance happened before fetched records are returned to the consumer's poll call
            log.debug("Not returning fetched records for partition {} since it is no longer assigned", partitionRecords.partition);
        } else {
            // note that the consumed position should always be available as long as the partition is still assigned
            long position = subscriptions.position(partitionRecords.partition);
            if (!subscriptions.isFetchable(partitionRecords.partition)) {
                // this can happen when a partition is paused before fetched records are returned to the consumer's poll call
                log.debug("Not returning fetched records for assigned partition {} since it is no longer fetchable", partitionRecords.partition);
            } else if (partitionRecords.fetchOffset == position) {
                List<ConsumerRecord<K, V>> partRecords = partitionRecords.drainRecords(maxRecords);
                if (!partRecords.isEmpty()) {
                    long nextOffset = partRecords.get(partRecords.size() - 1).offset() + 1;
                    log.trace("Returning fetched records at offset {} for assigned partition {} and update " +
                            "position to {}", position, partitionRecords.partition, nextOffset);

                    subscriptions.position(partitionRecords.partition, nextOffset);
                }

                Long partitionLag = subscriptions.partitionLag(partitionRecords.partition);
                if (partitionLag != null)
                    this.sensors.recordPartitionLag(partitionRecords.partition, partitionLag);

                return partRecords;
            } else {
                // these records aren't next in line based on the last consumed position, ignore them
                // they must be from an obsolete request
                log.debug("Ignoring fetched records for {} at offset {} since the current position is {}",
                        partitionRecords.partition, partitionRecords.fetchOffset, position);
            }
        }

        partitionRecords.drain();
        return Collections.emptyList();
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
        ListOffsetRequest.Builder builder = new ListOffsetRequest.Builder().setTargetTimes(timestampsToSearch);

        // If we need a timestamp in the response, the minimum RPC version we can send is v1.
        // Otherwise, v0 is OK.
        builder.setMinVersion(requireTimestamp ? (short) 1 : (short) 0);

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
     * @param future The future to be completed by the response.
     */
    @SuppressWarnings("deprecation")
    private void handleListOffsetResponse(Map<TopicPartition, Long> timestampsToSearch,
                                          ListOffsetResponse listOffsetResponse,
                                          RequestFuture<Map<TopicPartition, OffsetData>> future) {
        Map<TopicPartition, OffsetData> timestampOffsetMap = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            ListOffsetResponse.PartitionData partitionData = listOffsetResponse.responseData().get(topicPartition);
            Errors error = Errors.forCode(partitionData.errorCode);
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
                timestampOffsetMap.put(topicPartition, null);
            } else if (error == Errors.NOT_LEADER_FOR_PARTITION) {
                log.debug("Attempt to fetch offsets for partition {} failed due to obsolete leadership information, retrying.",
                        topicPartition);
                future.raise(error);
            } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                log.warn("Received unknown topic or partition error in ListOffset request for partition {}. The topic/partition " +
                        "may not exist or the user may not have Describe access to it", topicPartition);
                future.raise(error);
            } else {
                log.warn("Attempt to fetch offsets for partition {} failed due to: {}", topicPartition, error.message());
                future.raise(new StaleMetadataException());
            }
        }
        if (!future.isDone())
            future.complete(timestampOffsetMap);
    }

    private List<TopicPartition> fetchablePartitions() {
        Set<TopicPartition> exclude = new HashSet<>();
        List<TopicPartition> fetchable = subscriptions.fetchablePartitions();
        if (nextInLineRecords != null && !nextInLineRecords.isDrained()) {
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
            } else if (this.client.pendingRequestCount(node) == 0) {
                // if there is a leader and no in-flight requests, issue a new fetch
                LinkedHashMap<TopicPartition, FetchRequest.PartitionData> fetch = fetchable.get(node);
                if (fetch == null) {
                    fetch = new LinkedHashMap<>();
                    fetchable.put(node, fetch);
                }

                long position = this.subscriptions.position(partition);
                fetch.put(partition, new FetchRequest.PartitionData(position, this.fetchSize));
                log.trace("Added fetch request for partition {} at offset {} to node {}", partition, position, node);
            } else {
                log.trace("Skipping fetch for partition {} because there is an in-flight request to {}", partition, node);
            }
        }

        // create the fetches
        Map<Node, FetchRequest.Builder> requests = new HashMap<>();
        for (Map.Entry<Node, LinkedHashMap<TopicPartition, FetchRequest.PartitionData>> entry : fetchable.entrySet()) {
            Node node = entry.getKey();
            FetchRequest.Builder fetch = new FetchRequest.Builder(this.maxWaitMs, this.minBytes, entry.getValue()).
                    setMaxBytes(this.maxBytes);
            requests.put(node, fetch);
        }
        return requests;
    }

    /**
     * The callback for fetch completion
     */
    private PartitionRecords<K, V> parseCompletedFetch(CompletedFetch completedFetch) {
        TopicPartition tp = completedFetch.partition;
        FetchResponse.PartitionData partition = completedFetch.partitionData;
        long fetchOffset = completedFetch.fetchedOffset;
        int bytes = 0;
        int recordsCount = 0;
        PartitionRecords<K, V> parsedRecords = null;
        Errors error = Errors.forCode(partition.errorCode);

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

                List<ConsumerRecord<K, V>> parsed = new ArrayList<>();
                boolean skippedRecords = false;
                for (LogEntry logEntry : partition.records.deepEntries()) {
                    // Skip the messages earlier than current position.
                    if (logEntry.offset() >= position) {
                        parsed.add(parseRecord(tp, logEntry));
                        bytes += logEntry.sizeInBytes();
                    } else
                        skippedRecords = true;
                }

                recordsCount = parsed.size();

                log.trace("Adding fetched record for partition {} with offset {} to buffered record list", tp, position);
                parsedRecords = new PartitionRecords<>(fetchOffset, tp, parsed);

                if (parsed.isEmpty() && !skippedRecords && (partition.records.sizeInBytes() > 0)) {
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
                    log.trace("Received {} records in fetch response for partition {} with offset {}", parsed.size(), tp, position);
                    subscriptions.updateHighWatermark(tp, partition.highWatermark);
                }
            } else if (error == Errors.NOT_LEADER_FOR_PARTITION) {
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
            } else if (error == Errors.UNKNOWN) {
                log.warn("Unknown error fetching data for topic-partition {}", tp);
            } else {
                throw new IllegalStateException("Unexpected error code " + error.code() + " while fetching data");
            }
        } finally {
            completedFetch.metricAggregator.record(tp, bytes, recordsCount);
        }

        // we move the partition to the end if we received some bytes or if there was an error. This way, it's more
        // likely that partitions for the same topic can remain together (allowing for more efficient serialization).
        if (bytes > 0 || error != Errors.NONE)
            subscriptions.movePartitionToEnd(tp);

        return parsedRecords;
    }

    /**
     * Parse the record entry, deserializing the key / value fields if necessary
     */
    private ConsumerRecord<K, V> parseRecord(TopicPartition partition, LogEntry logEntry) {
        Record record = logEntry.record();

        if (this.checkCrcs) {
            try {
                record.ensureValid();
            } catch (InvalidRecordException e) {
                throw new KafkaException("Record for partition " + partition + " at offset " + logEntry.offset()
                        + " is invalid, cause: " + e.getMessage());
            }
        }

        try {
            long offset = logEntry.offset();
            long timestamp = record.timestamp();
            TimestampType timestampType = record.timestampType();
            ByteBuffer keyBytes = record.key();
            byte[] keyByteArray = keyBytes == null ? null : Utils.toArray(keyBytes);
            K key = keyBytes == null ? null : this.keyDeserializer.deserialize(partition.topic(), keyByteArray);
            ByteBuffer valueBytes = record.value();
            byte[] valueByteArray = valueBytes == null ? null : Utils.toArray(valueBytes);
            V value = valueBytes == null ? null : this.valueDeserializer.deserialize(partition.topic(), valueByteArray);

            return new ConsumerRecord<>(partition.topic(), partition.partition(), offset,
                                        timestamp, timestampType, record.checksum(),
                                        keyByteArray == null ? ConsumerRecord.NULL_SIZE : keyByteArray.length,
                                        valueByteArray == null ? ConsumerRecord.NULL_SIZE : valueByteArray.length,
                                        key, value);
        } catch (RuntimeException e) {
            throw new SerializationException("Error deserializing key/value for partition " + partition +
                    " at offset " + logEntry.offset(), e);
        }
    }

    @Override
    public void onAssignment(Set<TopicPartition> assignment) {
        sensors.updatePartitionLagSensors(assignment);
    }

    private static class PartitionRecords<K, V> {
        private long fetchOffset;
        private TopicPartition partition;
        private List<ConsumerRecord<K, V>> records;
        private int position = 0;

        private PartitionRecords(long fetchOffset, TopicPartition partition, List<ConsumerRecord<K, V>> records) {
            this.fetchOffset = fetchOffset;
            this.partition = partition;
            this.records = records;
        }

        private boolean isDrained() {
            return records == null;
        }

        private void drain() {
            this.records = null;
        }

        private List<ConsumerRecord<K, V>> drainRecords(int n) {
            if (isDrained() || position >= records.size()) {
                drain();
                return Collections.emptyList();
            }

            // using a sublist avoids a potentially expensive list copy (depending on the size of the records
            // and the maximum we can return from poll). The cost is that we cannot mutate the returned sublist.
            int limit = Math.min(records.size(), position + n);
            List<ConsumerRecord<K, V>> res = Collections.unmodifiableList(records.subList(position, limit));

            position = limit;
            if (position < records.size())
                fetchOffset = records.get(position).offset();

            return res;
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
        private final String metricGrpName;
        private final Sensor bytesFetched;
        private final Sensor recordsFetched;
        private final Sensor fetchLatency;
        private final Sensor recordsFetchLag;
        private final Sensor fetchThrottleTimeSensor;

        private Set<TopicPartition> assignedPartitions;

        private FetchManagerMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metrics = metrics;
            this.metricGrpName = metricGrpPrefix + "-fetch-manager-metrics";

            this.bytesFetched = metrics.sensor("bytes-fetched");
            this.bytesFetched.add(metrics.metricName("fetch-size-avg",
                this.metricGrpName,
                "The average number of bytes fetched per request"), new Avg());
            this.bytesFetched.add(metrics.metricName("fetch-size-max",
                this.metricGrpName,
                "The maximum number of bytes fetched per request"), new Max());
            this.bytesFetched.add(metrics.metricName("bytes-consumed-rate",
                this.metricGrpName,
                "The average number of bytes consumed per second"), new Rate());

            this.recordsFetched = metrics.sensor("records-fetched");
            this.recordsFetched.add(metrics.metricName("records-per-request-avg",
                this.metricGrpName,
                "The average number of records in each request"), new Avg());
            this.recordsFetched.add(metrics.metricName("records-consumed-rate",
                this.metricGrpName,
                "The average number of records consumed per second"), new Rate());

            this.fetchLatency = metrics.sensor("fetch-latency");
            this.fetchLatency.add(metrics.metricName("fetch-latency-avg",
                this.metricGrpName,
                "The average time taken for a fetch request."), new Avg());
            this.fetchLatency.add(metrics.metricName("fetch-latency-max",
                this.metricGrpName,
                "The max time taken for any fetch request."), new Max());
            this.fetchLatency.add(metrics.metricName("fetch-rate",
                this.metricGrpName,
                "The number of fetch requests per second."), new Rate(new Count()));

            this.recordsFetchLag = metrics.sensor("records-lag");
            this.recordsFetchLag.add(metrics.metricName("records-lag-max",
                this.metricGrpName,
                "The maximum lag in terms of number of records for any partition in this window"), new Max());

            this.fetchThrottleTimeSensor = metrics.sensor("fetch-throttle-time");
            this.fetchThrottleTimeSensor.add(metrics.metricName("fetch-throttle-time-avg",
                                                         this.metricGrpName,
                                                         "The average throttle time in ms"), new Avg());

            this.fetchThrottleTimeSensor.add(metrics.metricName("fetch-throttle-time-max",
                                                         this.metricGrpName,
                                                         "The maximum throttle time in ms"), new Max());
        }

        private void recordTopicFetchMetrics(String topic, int bytes, int records) {
            // record bytes fetched
            String name = "topic." + topic + ".bytes-fetched";
            Sensor bytesFetched = this.metrics.getSensor(name);
            if (bytesFetched == null) {
                Map<String, String> metricTags = Collections.singletonMap("topic", topic.replace('.', '_'));

                bytesFetched = this.metrics.sensor(name);
                bytesFetched.add(this.metrics.metricName("fetch-size-avg",
                        this.metricGrpName,
                        "The average number of bytes fetched per request for topic " + topic,
                        metricTags), new Avg());
                bytesFetched.add(this.metrics.metricName("fetch-size-max",
                        this.metricGrpName,
                        "The maximum number of bytes fetched per request for topic " + topic,
                        metricTags), new Max());
                bytesFetched.add(this.metrics.metricName("bytes-consumed-rate",
                        this.metricGrpName,
                        "The average number of bytes consumed per second for topic " + topic,
                        metricTags), new Rate());
            }
            bytesFetched.record(bytes);

            // record records fetched
            name = "topic." + topic + ".records-fetched";
            Sensor recordsFetched = this.metrics.getSensor(name);
            if (recordsFetched == null) {
                Map<String, String> metricTags = new HashMap<>(1);
                metricTags.put("topic", topic.replace('.', '_'));

                recordsFetched = this.metrics.sensor(name);
                recordsFetched.add(this.metrics.metricName("records-per-request-avg",
                        this.metricGrpName,
                        "The average number of records in each request for topic " + topic,
                        metricTags), new Avg());
                recordsFetched.add(this.metrics.metricName("records-consumed-rate",
                        this.metricGrpName,
                        "The average number of records consumed per second for topic " + topic,
                        metricTags), new Rate());
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
                recordsLag.add(this.metrics.metricName(name, this.metricGrpName, "The latest lag of the partition"),
                               new Value());
                recordsLag.add(this.metrics.metricName(name + "-max",
                        this.metricGrpName,
                        "The max lag of the partition"), new Max());
                recordsLag.add(this.metrics.metricName(name + "-avg",
                        this.metricGrpName,
                        "The average lag of the partition"), new Avg());
            }
            recordsLag.record(lag);
        }

        private static String partitionLagMetricName(TopicPartition tp) {
            return tp + ".records-lag";
        }
    }

}
