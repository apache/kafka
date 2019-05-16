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
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MetadataCache;
import org.apache.kafka.clients.StaleMetadataException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.LogTruncationException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
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
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.BufferSupplier;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

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
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

/**
 * This class manages the fetching process with the brokers.
 * <p>
 * Thread-safety:
 * Requests and responses of Fetcher may be processed by different threads since heartbeat
 * thread may process responses. Other operations are single-threaded and invoked only from
 * the thread polling the consumer.
 * <ul>
 *     <li>If a response handler accesses any shared state of the Fetcher (e.g. FetchSessionHandler),
 *     all access to that state must be synchronized on the Fetcher instance.</li>
 *     <li>If a response handler accesses any shared state of the coordinator (e.g. SubscriptionState),
 *     it is assumed that all access to that state is synchronized on the coordinator instance by
 *     the caller.</li>
 *     <li>Responses that collate partial responses from multiple brokers (e.g. to list offsets) are
 *     synchronized on the response future.</li>
 * </ul>
 */
public class Fetcher<K, V> implements Closeable {
    private final Logger log;
    private final LogContext logContext;
    private final ConsumerNetworkClient client;
    private final Time time;
    private final int minBytes;
    private final int maxBytes;
    private final int maxWaitMs;
    private final int fetchSize;
    private final long retryBackoffMs;
    private final long requestTimeoutMs;
    private final int maxPollRecords;
    private final boolean checkCrcs;
    private final ConsumerMetadata metadata;
    private final FetchManagerMetrics sensors;
    private final SubscriptionState subscriptions;
    private final ConcurrentLinkedQueue<CompletedFetch> completedFetches;
    private final BufferSupplier decompressionBufferSupplier = BufferSupplier.create();
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final IsolationLevel isolationLevel;
    private final Map<Integer, FetchSessionHandler> sessionHandlers;
    private final AtomicReference<RuntimeException> cachedListOffsetsException = new AtomicReference<>();
    private final AtomicReference<RuntimeException> cachedOffsetForLeaderException = new AtomicReference<>();
    private final OffsetsForLeaderEpochClient offsetsForLeaderEpochClient;
    private final ListOffsetsClient listOffsetsClient;

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
                   ConsumerMetadata metadata,
                   SubscriptionState subscriptions,
                   Metrics metrics,
                   FetcherMetricsRegistry metricsRegistry,
                   Time time,
                   long retryBackoffMs,
                   long requestTimeoutMs,
                   IsolationLevel isolationLevel,
                   ListOffsetsClient listOffsetsClient) {
        this.log = logContext.logger(Fetcher.class);
        this.logContext = logContext;
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
        this.sensors = new FetchManagerMetrics(metrics, metricsRegistry);
        this.retryBackoffMs = retryBackoffMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.isolationLevel = isolationLevel;
        this.sessionHandlers = new HashMap<>();
        this.offsetsForLeaderEpochClient = new OffsetsForLeaderEpochClient(client, logContext);
        this.listOffsetsClient = listOffsetsClient;
    }

    /**
     * Represents data about an offset returned by a broker.
     */
    private static class ListOffsetData {
        final long offset;
        final Long timestamp; //  null if the broker does not support returning timestamps
        final Optional<Integer> leaderEpoch; // empty if the leader epoch is not known

        ListOffsetData(long offset, Long timestamp, Optional<Integer> leaderEpoch) {
            this.offset = offset;
            this.timestamp = timestamp;
            this.leaderEpoch = leaderEpoch;
        }
    }

    /**
     * Return whether we have any completed fetches pending return to the user. This method is thread-safe.
     * @return true if there are completed fetches, false otherwise
     */
    public boolean hasCompletedFetches() {
        return !completedFetches.isEmpty();
    }

    /**
     * Set-up a fetch request for any node that we have assigned partitions for which doesn't already have
     * an in-flight fetch or pending fetch data.
     * @return number of fetches sent
     */
    public synchronized int sendFetches() {
        // Update metrics in case there was an assignment change
        sensors.maybeUpdateAssignment(subscriptions);

        Map<Node, FetchSessionHandler.FetchRequestData> fetchRequestMap = prepareFetchRequests();
        for (Map.Entry<Node, FetchSessionHandler.FetchRequestData> entry : fetchRequestMap.entrySet()) {
            final Node fetchTarget = entry.getKey();
            final FetchSessionHandler.FetchRequestData data = entry.getValue();
            final FetchRequest.Builder request = FetchRequest.Builder
                    .forConsumer(this.maxWaitMs, this.minBytes, data.toSend())
                    .isolationLevel(isolationLevel)
                    .setMaxBytes(this.maxBytes)
                    .metadata(data.metadata())
                    .toForget(data.toForget());
            if (log.isDebugEnabled()) {
                log.debug("Sending {} {} to broker {}", isolationLevel, data.toString(), fetchTarget);
            }
            client.send(fetchTarget, request)
                    .addListener(new RequestFutureListener<ClientResponse>() {
                        @Override
                        public void onSuccess(ClientResponse resp) {
                            synchronized (Fetcher.this) {
                                @SuppressWarnings("unchecked")
                                FetchResponse<Records> response = (FetchResponse<Records>) resp.responseBody();
                                FetchSessionHandler handler = sessionHandler(fetchTarget.id());
                                if (handler == null) {
                                    log.error("Unable to find FetchSessionHandler for node {}. Ignoring fetch response.",
                                            fetchTarget.id());
                                    return;
                                }
                                if (!handler.handleResponse(response)) {
                                    return;
                                }

                                Set<TopicPartition> partitions = new HashSet<>(response.responseData().keySet());
                                FetchResponseMetricAggregator metricAggregator = new FetchResponseMetricAggregator(sensors, partitions);

                                for (Map.Entry<TopicPartition, FetchResponse.PartitionData<Records>> entry : response.responseData().entrySet()) {
                                    TopicPartition partition = entry.getKey();
                                    FetchRequest.PartitionData requestData = data.sessionPartitions().get(partition);
                                    if (requestData == null) {
                                        String message;
                                        if (data.metadata().isFull()) {
                                            message = MessageFormatter.arrayFormat(
                                                    "Response for missing full request partition: partition={}; metadata={}",
                                                    new Object[]{partition, data.metadata()}).getMessage();
                                        } else {
                                            message = MessageFormatter.arrayFormat(
                                                    "Response for missing session request partition: partition={}; metadata={}; toSend={}; toForget={}",
                                                    new Object[]{partition, data.metadata(), data.toSend(), data.toForget()}).getMessage();
                                        }

                                        // Received fetch response for missing session partition
                                        throw new IllegalStateException(message);
                                    } else {
                                        long fetchOffset = requestData.fetchOffset;
                                        FetchResponse.PartitionData<Records> fetchData = entry.getValue();

                                        log.debug("Fetch {} at offset {} for partition {} returned fetch data {}",
                                                isolationLevel, fetchOffset, partition, fetchData);
                                        completedFetches.add(new CompletedFetch(partition, fetchOffset, fetchData, metricAggregator,
                                                    resp.requestHeader().apiVersion()));
                                    }
                                }

                                sensors.fetchLatency.record(resp.requestLatencyMs());
                            }
                        }

                        @Override
                        public void onFailure(RuntimeException e) {
                            synchronized (Fetcher.this) {
                                FetchSessionHandler handler = sessionHandler(fetchTarget.id());
                                if (handler != null) {
                                    handler.handleError(e);
                                }
                            }
                        }
                    });
        }
        return fetchRequestMap.size();
    }

    /**
     * Get topic metadata for all topics in the cluster
     * @param timer Timer bounding how long this method can block
     * @return The map of topics with their partition information
     */
    public Map<String, List<PartitionInfo>> getAllTopicMetadata(Timer timer) {
        return getTopicMetadata(MetadataRequest.Builder.allTopics(), timer);
    }

    /**
     * Get metadata for all topics present in Kafka cluster
     *
     * @param request The MetadataRequest to send
     * @param timer Timer bounding how long this method can block
     * @return The map of topics with their partition information
     */
    public Map<String, List<PartitionInfo>> getTopicMetadata(MetadataRequest.Builder request, Timer timer) {
        // Save the round trip if no topics are requested.
        if (!request.isAllTopics() && request.emptyTopicList())
            return Collections.emptyMap();

        do {
            RequestFuture<ClientResponse> future = sendMetadataRequest(request);
            client.poll(future, timer);

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
                        topicsPartitionInfos.put(topic, cluster.partitionsForTopic(topic));
                    return topicsPartitionInfos;
                }
            }

            timer.sleep(retryBackoffMs);
        } while (timer.notExpired());

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

    private Long offsetResetStrategyTimestamp(final TopicPartition partition) {
        OffsetResetStrategy strategy = subscriptions.resetStrategy(partition);
        if (strategy == OffsetResetStrategy.EARLIEST)
            return ListOffsetRequest.EARLIEST_TIMESTAMP;
        else if (strategy == OffsetResetStrategy.LATEST)
            return ListOffsetRequest.LATEST_TIMESTAMP;
        else
            return null;
    }

    /**
     * Reset offsets for all assigned partitions that require it.
     *
     * @throws org.apache.kafka.clients.consumer.NoOffsetForPartitionException If no offset reset strategy is defined
     *   and one or more partitions aren't awaiting a seekToBeginning() or seekToEnd().
     */
    public void resetOffsetsIfNeeded() {
        // Raise exception from previous offset fetch if there is one
        RuntimeException exception = cachedListOffsetsException.getAndSet(null);
        if (exception != null)
            throw exception;

        Set<TopicPartition> partitions = subscriptions.partitionsNeedingReset(time.milliseconds());
        if (partitions.isEmpty())
            return;

        final Map<TopicPartition, Long> offsetResetTimestamps = new HashMap<>();
        for (final TopicPartition partition : partitions) {
            Long timestamp = offsetResetStrategyTimestamp(partition);
            if (timestamp != null)
                offsetResetTimestamps.put(partition, timestamp);
        }

        resetOffsetsAsync(offsetResetTimestamps);
    }

    /**
     *  Validate offsets for all assigned partitions for which a leader change has been detected.
     */
    public void validateOffsetsIfNeeded() {
        RuntimeException exception = cachedOffsetForLeaderException.getAndSet(null);
        if (exception != null)
            throw exception;

        // Validate each partition against the current leader and epoch
        subscriptions.assignedPartitions().forEach(topicPartition -> {
            ConsumerMetadata.LeaderAndEpoch leaderAndEpoch = metadata.leaderAndEpoch(topicPartition);
            subscriptions.maybeValidatePosition(topicPartition, leaderAndEpoch);
        });

        // Collect positions needing validation, with backoff
        Map<TopicPartition, SubscriptionState.FetchPosition> partitionsToValidate = subscriptions
                .partitionsNeedingValidation(time.milliseconds())
                .stream()
                .collect(Collectors.toMap(Function.identity(), subscriptions::position));

        validateOffsetsAsync(partitionsToValidate);
    }

    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch,
                                                                   Timer timer) {
        metadata.addTransientTopics(topicsForPartitions(timestampsToSearch.keySet()));

        try {
            Map<TopicPartition, ListOffsetsClient.ListOffsetData> fetchedOffsets = fetchOffsetsByTimes(timestampsToSearch,
                    timer, true).fetchedOffsets;

            HashMap<TopicPartition, OffsetAndTimestamp> offsetsByTimes = new HashMap<>(timestampsToSearch.size());
            for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet())
                offsetsByTimes.put(entry.getKey(), null);

            for (Map.Entry<TopicPartition, ListOffsetsClient.ListOffsetData> entry : fetchedOffsets.entrySet()) {
                // 'entry.getValue().timestamp' will not be null since we are guaranteed
                // to work with a v1 (or later) ListOffset request
                ListOffsetsClient.ListOffsetData offsetData = entry.getValue();
                offsetsByTimes.put(entry.getKey(), new OffsetAndTimestamp(offsetData.offset, offsetData.timestamp,
                        offsetData.leaderEpoch));
            }

            return offsetsByTimes;
        } finally {
            metadata.clearTransientTopics();
        }
    }

    private ListOffsetsClient.ResultData fetchOffsetsByTimes(Map<TopicPartition, Long> timestampsToSearch,
                                                             Timer timer,
                                                             boolean requireTimestamps) {
        ListOffsetsClient.ResultData result = new ListOffsetsClient.ResultData();
        if (timestampsToSearch.isEmpty())
            return result;

        Map<TopicPartition, Long> remainingToSearch = new HashMap<>(timestampsToSearch);
        do {
            RequestFuture<ListOffsetsClient.ResultData> future = sendListOffsetsRequests(remainingToSearch, requireTimestamps);
            client.poll(future, timer);

            if (!future.isDone())
                break;

            if (future.succeeded()) {
                ListOffsetsClient.ResultData value = future.value();
                result.fetchedOffsets.putAll(value.fetchedOffsets);
                if (value.partitionsToRetry.isEmpty())
                    return result;

                remainingToSearch.keySet().retainAll(value.partitionsToRetry);
            } else if (!future.isRetriable()) {
                throw future.exception();
            }

            if (metadata.updateRequested())
                client.awaitMetadataUpdate(timer);
            else
                timer.sleep(retryBackoffMs);
        } while (timer.notExpired());

        throw new TimeoutException("Failed to get offsets by times in " + timer.elapsedMs() + "ms");
    }

    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Timer timer) {
        return beginningOrEndOffset(partitions, ListOffsetRequest.EARLIEST_TIMESTAMP, timer);
    }

    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Timer timer) {
        return beginningOrEndOffset(partitions, ListOffsetRequest.LATEST_TIMESTAMP, timer);
    }

    private Map<TopicPartition, Long> beginningOrEndOffset(Collection<TopicPartition> partitions,
                                                           long timestamp,
                                                           Timer timer) {
        metadata.addTransientTopics(topicsForPartitions(partitions));
        try {
            Map<TopicPartition, Long> timestampsToSearch = partitions.stream()
                    .collect(Collectors.toMap(Function.identity(), tp -> timestamp));

            ListOffsetsClient.ResultData result = fetchOffsetsByTimes(timestampsToSearch, timer, false);

            return result.fetchedOffsets.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offset));
        } finally {
            metadata.clearTransientTopics();
        }
    }

    /**
     * Return the fetched records, empty the record buffer and update the consumed position.
     *
     * NOTE: returning empty records guarantees the consumed position are NOT updated.
     *
     * @return The fetched records per partition
     * @throws OffsetOutOfRangeException If there is OffsetOutOfRange error in fetchResponse and
     *         the defaultResetPolicy is NONE
     * @throws TopicAuthorizationException If there is TopicAuthorization error in fetchResponse.
     */
    public Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchedRecords() {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> fetched = new HashMap<>();
        int recordsRemaining = maxPollRecords;

        try {
            while (recordsRemaining > 0) {
                if (nextInLineRecords == null || nextInLineRecords.isFetched) {
                    CompletedFetch completedFetch = completedFetches.peek();
                    if (completedFetch == null) break;

                    try {
                        nextInLineRecords = parseCompletedFetch(completedFetch);
                    } catch (Exception e) {
                        // Remove a completedFetch upon a parse with exception if (1) it contains no records, and
                        // (2) there are no fetched records with actual content preceding this exception.
                        // The first condition ensures that the completedFetches is not stuck with the same completedFetch
                        // in cases such as the TopicAuthorizationException, and the second condition ensures that no
                        // potential data loss due to an exception in a following record.
                        FetchResponse.PartitionData partition = completedFetch.partitionData;
                        if (fetched.isEmpty() && (partition.records == null || partition.records.sizeInBytes() == 0)) {
                            completedFetches.poll();
                        }
                        throw e;
                    }
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
        } else if (!subscriptions.isFetchable(partitionRecords.partition)) {
            // this can happen when a partition is paused before fetched records are returned to the consumer's
            // poll call or if the offset is being reset
            log.debug("Not returning fetched records for assigned partition {} since it is no longer fetchable",
                    partitionRecords.partition);
        } else {
            SubscriptionState.FetchPosition position = subscriptions.position(partitionRecords.partition);
            if (partitionRecords.nextFetchOffset == position.offset) {
                List<ConsumerRecord<K, V>> partRecords = partitionRecords.fetchRecords(maxRecords);

                if (partitionRecords.nextFetchOffset > position.offset) {
                    SubscriptionState.FetchPosition nextPosition = new SubscriptionState.FetchPosition(
                            partitionRecords.nextFetchOffset,
                            partitionRecords.lastEpoch,
                            position.currentLeader);
                    log.trace("Returning fetched records at offset {} for assigned partition {} and update " +
                            "position to {}", position, partitionRecords.partition, nextPosition);
                    subscriptions.position(partitionRecords.partition, nextPosition);
                }

                Long partitionLag = subscriptions.partitionLag(partitionRecords.partition, isolationLevel);
                if (partitionLag != null)
                    this.sensors.recordPartitionLag(partitionRecords.partition, partitionLag);

                Long lead = subscriptions.partitionLead(partitionRecords.partition);
                if (lead != null) {
                    this.sensors.recordPartitionLead(partitionRecords.partition, lead);
                }

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

    private void resetOffsetIfNeeded(TopicPartition partition, Long requestedResetTimestamp, ListOffsetsClient.ListOffsetData offsetData) {
        // we might lose the assignment while fetching the offset, or the user might seek to a different offset,
        // so verify it is still assigned and still in need of the requested reset
        if (!subscriptions.isAssigned(partition)) {
            log.debug("Skipping reset of partition {} since it is no longer assigned", partition);
        } else if (!subscriptions.isOffsetResetNeeded(partition)) {
            log.debug("Skipping reset of partition {} since reset is no longer needed", partition);
        } else if (!requestedResetTimestamp.equals(offsetResetStrategyTimestamp(partition))) {
            log.debug("Skipping reset of partition {} since an alternative reset has been requested", partition);
        } else {
            SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(
                    offsetData.offset, offsetData.leaderEpoch, metadata.leaderAndEpoch(partition));
            log.info("Resetting offset for partition {} to offset {}.", partition, position);
            offsetData.leaderEpoch.ifPresent(epoch -> metadata.updateLastSeenEpochIfNewer(partition, epoch));
            subscriptions.seek(partition, position);
        }
    }

    private void resetOffsetsAsync(Map<TopicPartition, Long> partitionResetTimestamps) {
        Map<Node, Map<TopicPartition, ListOffsetRequest.PartitionData>> timestampsToSearchByNode =
                groupListOffsetRequests(partitionResetTimestamps, new HashSet<>());
        for (Map.Entry<Node, Map<TopicPartition, ListOffsetRequest.PartitionData>> entry : timestampsToSearchByNode.entrySet()) {
            Node node = entry.getKey();
            final Map<TopicPartition, ListOffsetRequest.PartitionData> resetTimestamps = entry.getValue();
            subscriptions.setNextAllowedRetry(resetTimestamps.keySet(), time.milliseconds() + requestTimeoutMs);

            RequestFuture<ListOffsetsClient.ResultData> future = listOffsetsClient.sendAsyncRequest(node,
                    new ListOffsetsClient.RequestData(resetTimestamps, false, isolationLevel));
            future.addListener(new RequestFutureListener<ListOffsetsClient.ResultData>() {
                @Override
                public void onSuccess(ListOffsetsClient.ResultData result) {
                    if (!result.partitionsToRetry.isEmpty()) {
                        subscriptions.requestFailed(result.partitionsToRetry, time.milliseconds() + retryBackoffMs);
                        metadata.requestUpdate();
                    }

                    for (Map.Entry<TopicPartition, ListOffsetsClient.ListOffsetData> fetchedOffset : result.fetchedOffsets.entrySet()) {
                        TopicPartition partition = fetchedOffset.getKey();
                        ListOffsetsClient.ListOffsetData offsetData = fetchedOffset.getValue();
                        ListOffsetRequest.PartitionData requestedReset = resetTimestamps.get(partition);
                        resetOffsetIfNeeded(partition, requestedReset.timestamp, offsetData);
                    }
                }

                @Override
                public void onFailure(RuntimeException e) {
                    subscriptions.requestFailed(resetTimestamps.keySet(), time.milliseconds() + retryBackoffMs);
                    metadata.requestUpdate();

                    if (!(e instanceof RetriableException) && !cachedListOffsetsException.compareAndSet(null, e))
                        log.error("Discarding error in ListOffsetResponse because another error is pending", e);
                }
            });
        }
    }

    /**
     * For each partition which needs validation, make an asynchronous request to get the end-offsets for the partition
     * with the epoch less than or equal to the epoch the partition last saw.
     *
     * Requests are grouped by Node for efficiency.
     */
    private void validateOffsetsAsync(Map<TopicPartition, SubscriptionState.FetchPosition> partitionsToValidate) {
        final Map<Node, Map<TopicPartition, SubscriptionState.FetchPosition>> regrouped =
                regroupPartitionMapByNode(partitionsToValidate, (tp, fetchPosition) -> fetchPosition.currentLeader.leader);

        regrouped.forEach((node, dataMap) -> {
            if (node.isEmpty()) {
                metadata.requestUpdate();
                return;
            }

            subscriptions.setNextAllowedRetry(dataMap.keySet(), time.milliseconds() + requestTimeoutMs);

            final Map<TopicPartition, Metadata.LeaderAndEpoch> cachedLeaderAndEpochs = partitionsToValidate.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().currentLeader));

            RequestFuture<OffsetsForLeaderEpochClient.OffsetForEpochResult> future = offsetsForLeaderEpochClient.sendAsyncRequest(node, partitionsToValidate);
            future.addListener(new RequestFutureListener<OffsetsForLeaderEpochClient.OffsetForEpochResult>() {
                @Override
                public void onSuccess(OffsetsForLeaderEpochClient.OffsetForEpochResult offsetsResult) {
                    Map<TopicPartition, OffsetAndMetadata> truncationWithoutResetPolicy = new HashMap<>();
                    if (!offsetsResult.partitionsToRetry().isEmpty()) {
                        subscriptions.setNextAllowedRetry(offsetsResult.partitionsToRetry(), time.milliseconds() + retryBackoffMs);
                        metadata.requestUpdate();
                    }

                    // For each OffsetsForLeader response, check if the end-offset is lower than our current offset
                    // for the partition. If so, it means we have experienced log truncation and need to reposition
                    // that partition's offset.
                    offsetsResult.endOffsets().forEach((respTopicPartition, respEndOffset) -> {
                        if (!subscriptions.isAssigned(respTopicPartition)) {
                            log.debug("Ignoring OffsetsForLeader response for partition {} which is not currently assigned.", respTopicPartition);
                            return;
                        }

                        if (subscriptions.awaitingValidation(respTopicPartition)) {
                            SubscriptionState.FetchPosition currentPosition = subscriptions.position(respTopicPartition);
                            Metadata.LeaderAndEpoch currentLeader = currentPosition.currentLeader;
                            if (!currentLeader.equals(cachedLeaderAndEpochs.get(respTopicPartition))) {
                                return;
                            }

                            if (respEndOffset.endOffset() < currentPosition.offset) {
                                if (subscriptions.hasDefaultOffsetResetPolicy()) {
                                    SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                                            respEndOffset.endOffset(), Optional.of(respEndOffset.leaderEpoch()), currentLeader);
                                    log.info("Truncation detected for partition {}, resetting offset to {}", respTopicPartition, newPosition);
                                    subscriptions.seek(respTopicPartition, newPosition);
                                } else {
                                    log.warn("Truncation detected for partition {}, but no reset policy is set", respTopicPartition);
                                    truncationWithoutResetPolicy.put(respTopicPartition, new OffsetAndMetadata(
                                            respEndOffset.endOffset(), Optional.of(respEndOffset.leaderEpoch()), null));
                                }
                            } else {
                                // Offset is fine, clear the validation state
                                subscriptions.validate(respTopicPartition);
                            }
                        }
                    });

                    if (!truncationWithoutResetPolicy.isEmpty()) {
                        throw new LogTruncationException(truncationWithoutResetPolicy);
                    }
                }

                @Override
                public void onFailure(RuntimeException e) {
                    subscriptions.requestFailed(dataMap.keySet(), time.milliseconds() + retryBackoffMs);
                    metadata.requestUpdate();

                    if (!(e instanceof RetriableException) && !cachedOffsetForLeaderException.compareAndSet(null, e)) {
                        log.error("Discarding error in OffsetsForLeaderEpoch because another error is pending", e);
                    }
                }
            });
        });
    }

    /**
     * Search the offsets by target times for the specified partitions.
     *
     * @param timestampsToSearch the mapping between partitions and target time
     * @param requireTimestamps true if we should fail with an UnsupportedVersionException if the broker does
     *                         not support fetching precise timestamps for offsets
     * @return A response which can be polled to obtain the corresponding timestamps and offsets.
     */
    private RequestFuture<ListOffsetsClient.ResultData> sendListOffsetsRequests(final Map<TopicPartition, Long> timestampsToSearch,
                                                                                final boolean requireTimestamps) {
        final Set<TopicPartition> partitionsToRetry = new HashSet<>();
        Map<Node, Map<TopicPartition, ListOffsetRequest.PartitionData>> timestampsToSearchByNode =
                groupListOffsetRequests(timestampsToSearch, partitionsToRetry);
        if (timestampsToSearchByNode.isEmpty())
            return RequestFuture.failure(new StaleMetadataException());

        final RequestFuture<ListOffsetsClient.ResultData> listOffsetRequestsFuture = new RequestFuture<>();
        final Map<TopicPartition, ListOffsetsClient.ListOffsetData> fetchedTimestampOffsets = new HashMap<>();
        final AtomicInteger remainingResponses = new AtomicInteger(timestampsToSearchByNode.size());

        for (Map.Entry<Node, Map<TopicPartition, ListOffsetRequest.PartitionData>> entry : timestampsToSearchByNode.entrySet()) {

            RequestFuture<ListOffsetsClient.ResultData> future = listOffsetsClient.sendAsyncRequest(
                    entry.getKey(), new ListOffsetsClient.RequestData(entry.getValue(), requireTimestamps, isolationLevel));
            future.addListener(new RequestFutureListener<ListOffsetsClient.ResultData>() {
                @Override
                public void onSuccess(ListOffsetsClient.ResultData partialResult) {
                    synchronized (listOffsetRequestsFuture) {
                        fetchedTimestampOffsets.putAll(partialResult.fetchedOffsets);
                        partitionsToRetry.addAll(partialResult.partitionsToRetry);

                        if (remainingResponses.decrementAndGet() == 0 && !listOffsetRequestsFuture.isDone()) {
                            ListOffsetsClient.ResultData result = new ListOffsetsClient.ResultData(
                                    fetchedTimestampOffsets, partitionsToRetry);
                            listOffsetRequestsFuture.complete(result);
                        }
                    }
                }

                @Override
                public void onFailure(RuntimeException e) {
                    synchronized (listOffsetRequestsFuture) {
                        if (!listOffsetRequestsFuture.isDone())
                            listOffsetRequestsFuture.raise(e);
                    }
                }
            });
        }
        return listOffsetRequestsFuture;
    }

    /**
     * Groups timestamps to search by node for topic partitions in `timestampsToSearch` that have
     * leaders available. Topic partitions from `timestampsToSearch` that do not have their leader
     * available are added to `partitionsToRetry`
     * @param timestampsToSearch The mapping from partitions ot the target timestamps
     * @param partitionsToRetry A set of topic partitions that will be extended with partitions
     *                          that need metadata update or re-connect to the leader.
     */
    private Map<Node, Map<TopicPartition, ListOffsetRequest.PartitionData>> groupListOffsetRequests(
            Map<TopicPartition, Long> timestampsToSearch,
            Set<TopicPartition> partitionsToRetry) {

        final Map<TopicPartition, ListOffsetRequest.PartitionData> partitionDataMap = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry: timestampsToSearch.entrySet()) {
            TopicPartition tp  = entry.getKey();
            Optional<MetadataCache.PartitionInfoAndEpoch> currentInfo = metadata.partitionInfoIfCurrent(tp);
            if (!currentInfo.isPresent()) {
                log.debug("Leader for partition {} is unknown for fetching offset", tp);
                metadata.requestUpdate();
                partitionsToRetry.add(tp);
            } else if (currentInfo.get().partitionInfo().leader() == null) {
                log.debug("Leader for partition {} is unavailable for fetching offset", tp);
                metadata.requestUpdate();
                partitionsToRetry.add(tp);
            } else if (client.isUnavailable(currentInfo.get().partitionInfo().leader())) {
                client.maybeThrowAuthFailure(currentInfo.get().partitionInfo().leader());

                // The connection has failed and we need to await the blackout period before we can
                // try again. No need to request a metadata update since the disconnect will have
                // done so already.
                log.debug("Leader {} for partition {} is unavailable for fetching offset until reconnect backoff expires",
                        currentInfo.get().partitionInfo().leader(), tp);
                partitionsToRetry.add(tp);
            } else {
                partitionDataMap.put(tp,
                        new ListOffsetRequest.PartitionData(entry.getValue(), Optional.of(currentInfo.get().epoch())));
            }
        }

        return regroupPartitionMapByNode(partitionDataMap, (tp, partitionData) -> metadata.fetch().leaderFor(tp));
    }

    private List<TopicPartition> fetchablePartitions() {
        Set<TopicPartition> exclude = new HashSet<>();
        if (nextInLineRecords != null && !nextInLineRecords.isFetched) {
            exclude.add(nextInLineRecords.partition);
        }
        for (CompletedFetch completedFetch : completedFetches) {
            exclude.add(completedFetch.partition);
        }
        return subscriptions.fetchablePartitions(tp -> !exclude.contains(tp));
    }

    /**
     * Create fetch requests for all nodes for which we have assigned partitions
     * that have no existing requests in flight.
     */
    private Map<Node, FetchSessionHandler.FetchRequestData> prepareFetchRequests() {
        Map<Node, FetchSessionHandler.Builder> fetchable = new LinkedHashMap<>();

        // Ensure the position has an up-to-date leader
        subscriptions.assignedPartitions().forEach(
            tp -> subscriptions.maybeValidatePosition(tp, metadata.leaderAndEpoch(tp)));

        for (TopicPartition partition : fetchablePartitions()) {
            SubscriptionState.FetchPosition position = this.subscriptions.position(partition);
            Metadata.LeaderAndEpoch leaderAndEpoch = position.currentLeader;
            Node node = leaderAndEpoch.leader;

            if (node == null || node.isEmpty()) {
                metadata.requestUpdate();
            } else if (client.isUnavailable(node)) {
                client.maybeThrowAuthFailure(node);

                // If we try to send during the reconnect blackout window, then the request is just
                // going to be failed anyway before being sent, so skip the send for now
                log.trace("Skipping fetch for partition {} because node {} is awaiting reconnect backoff", partition, node);
            } else if (client.hasPendingRequests(node)) {
                log.trace("Skipping fetch for partition {} because there is an in-flight request to {}", partition, node);
            } else {
                // if there is a leader and no in-flight requests, issue a new fetch
                FetchSessionHandler.Builder builder = fetchable.get(leaderAndEpoch.leader);
                if (builder == null) {
                    int id = leaderAndEpoch.leader.id();
                    FetchSessionHandler handler = sessionHandler(id);
                    if (handler == null) {
                        handler = new FetchSessionHandler(logContext, id);
                        sessionHandlers.put(id, handler);
                    }
                    builder = handler.newBuilder();
                    fetchable.put(leaderAndEpoch.leader, builder);
                }

                builder.add(partition, new FetchRequest.PartitionData(position.offset,
                        FetchRequest.INVALID_LOG_START_OFFSET, this.fetchSize, leaderAndEpoch.epoch));

                log.debug("Added {} fetch request for partition {} at position {} to node {}", isolationLevel,
                    partition, position, leaderAndEpoch.leader);
            }
        }

        Map<Node, FetchSessionHandler.FetchRequestData> reqs = new LinkedHashMap<>();
        for (Map.Entry<Node, FetchSessionHandler.Builder> entry : fetchable.entrySet()) {
            reqs.put(entry.getKey(), entry.getValue().build());
        }
        return reqs;
    }

    /**
     * Regroup a map consisting of {@link TopicPartition} keys and generic values to an outer map of {@link Node}. This
     * is generally used to group request data based on where it is going (the node).
     * @param partitionMap
     * @param groupingFunction
     * @param <T>
     * @return
     */
    private <T> Map<Node, Map<TopicPartition, T>> regroupPartitionMapByNode(
            Map<TopicPartition, T> partitionMap,
            BiFunction<TopicPartition, T, Node> groupingFunction) {
        return partitionMap.entrySet()
                .stream()
                .collect(Collectors.groupingBy(entry -> groupingFunction.apply(entry.getKey(), entry.getValue()),
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    /**
     * The callback for fetch completion
     */
    private PartitionRecords parseCompletedFetch(CompletedFetch completedFetch) {
        TopicPartition tp = completedFetch.partition;
        FetchResponse.PartitionData<Records> partition = completedFetch.partitionData;
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
                SubscriptionState.FetchPosition position = subscriptions.position(tp);
                if (position == null || position.offset != fetchOffset) {
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

                if (partition.logStartOffset >= 0) {
                    log.trace("Updating log start offset for partition {} to {}", tp, partition.logStartOffset);
                    subscriptions.updateLogStartOffset(tp, partition.logStartOffset);
                }

                if (partition.lastStableOffset >= 0) {
                    log.trace("Updating last stable offset for partition {} to {}", tp, partition.lastStableOffset);
                    subscriptions.updateLastStableOffset(tp, partition.lastStableOffset);
                }
            } else if (error == Errors.NOT_LEADER_FOR_PARTITION ||
                       error == Errors.REPLICA_NOT_AVAILABLE ||
                       error == Errors.KAFKA_STORAGE_ERROR ||
                       error == Errors.FENCED_LEADER_EPOCH) {
                log.debug("Error in fetch for partition {}: {}", tp, error.exceptionName());
                this.metadata.requestUpdate();
            } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                log.warn("Received unknown topic or partition error in fetch for partition {}", tp);
                this.metadata.requestUpdate();
            } else if (error == Errors.OFFSET_OUT_OF_RANGE) {
                if (fetchOffset != subscriptions.position(tp).offset) {
                    log.debug("Discarding stale fetch response for partition {} since the fetched offset {} " +
                            "does not match the current offset {}", tp, fetchOffset, subscriptions.position(tp));
                } else if (subscriptions.hasDefaultOffsetResetPolicy()) {
                    log.info("Fetch offset {} is out of range for partition {}, resetting offset", fetchOffset, tp);
                    subscriptions.requestOffsetReset(tp);
                } else {
                    throw new OffsetOutOfRangeException(Collections.singletonMap(tp, fetchOffset));
                }
            } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                //we log the actual partition and not just the topic to help with ACL propagation issues in large clusters
                log.warn("Not authorized to read from partition {}.", tp);
                throw new TopicAuthorizationException(Collections.singleton(tp.topic()));
            } else if (error == Errors.UNKNOWN_LEADER_EPOCH) {
                log.debug("Received unknown leader epoch error in fetch for partition {}", tp);
            } else if (error == Errors.UNKNOWN_SERVER_ERROR) {
                log.warn("Unknown error fetching data for topic-partition {}", tp);
            } else {
                throw new IllegalStateException("Unexpected error code " + error.code() + " while fetching from partition " + tp);
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
            Optional<Integer> leaderEpoch = maybeLeaderEpoch(batch.partitionLeaderEpoch());
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
                                        key, value, headers, leaderEpoch);
        } catch (RuntimeException e) {
            throw new SerializationException("Error deserializing key/value for partition " + partition +
                    " at offset " + record.offset() + ". If needed, please seek past the record to continue consumption.", e);
        }
    }

    private Optional<Integer> maybeLeaderEpoch(int leaderEpoch) {
        return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ? Optional.empty() : Optional.of(leaderEpoch);
    }

    /**
     * Clear the buffered data which are not a part of newly assigned partitions
     *
     * @param assignedPartitions  newly assigned {@link TopicPartition}
     */
    public void clearBufferedDataForUnassignedPartitions(Collection<TopicPartition> assignedPartitions) {
        Iterator<CompletedFetch> itr = completedFetches.iterator();
        while (itr.hasNext()) {
            TopicPartition tp = itr.next().partition;
            if (!assignedPartitions.contains(tp)) {
                itr.remove();
            }
        }
        if (nextInLineRecords != null && !assignedPartitions.contains(nextInLineRecords.partition)) {
            nextInLineRecords.drain();
            nextInLineRecords = null;
        }
    }

    /**
     * Clear the buffered data which are not a part of newly assigned topics
     *
     * @param assignedTopics  newly assigned topics
     */
    public void clearBufferedDataForUnassignedTopics(Collection<String> assignedTopics) {
        Set<TopicPartition> currentTopicPartitions = new HashSet<>();
        for (TopicPartition tp : subscriptions.assignedPartitions()) {
            if (assignedTopics.contains(tp.topic())) {
                currentTopicPartitions.add(tp);
            }
        }
        clearBufferedDataForUnassignedPartitions(currentTopicPartitions);
    }

    // Visibilty for testing
    protected FetchSessionHandler sessionHandler(int node) {
        return sessionHandlers.get(node);
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
        private Optional<Integer> lastEpoch;
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
            this.lastEpoch = Optional.empty();
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
                    lastEpoch = currentBatch.partitionLeaderEpoch() == RecordBatch.NO_PARTITION_LEADER_EPOCH ?
                            Optional.empty() : Optional.of(currentBatch.partitionLeaderEpoch());

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

        private PriorityQueue<FetchResponse.AbortedTransaction> abortedTransactions(FetchResponse.PartitionData<?> partition) {
            if (partition.abortedTransactions == null || partition.abortedTransactions.isEmpty())
                return null;

            PriorityQueue<FetchResponse.AbortedTransaction> abortedTransactions = new PriorityQueue<>(
                    partition.abortedTransactions.size(), Comparator.comparingLong(o -> o.firstOffset)
            );
            abortedTransactions.addAll(partition.abortedTransactions);
            return abortedTransactions;
        }

        private boolean containsAbortMarker(RecordBatch batch) {
            if (!batch.isControlBatch())
                return false;

            Iterator<Record> batchIterator = batch.iterator();
            if (!batchIterator.hasNext())
                return false;

            Record firstRecord = batchIterator.next();
            return ControlRecordType.ABORT == ControlRecordType.parse(firstRecord.key());
        }
    }

    private static class CompletedFetch {
        private final TopicPartition partition;
        private final long fetchedOffset;
        private final FetchResponse.PartitionData<Records> partitionData;
        private final FetchResponseMetricAggregator metricAggregator;
        private final short responseVersion;

        private CompletedFetch(TopicPartition partition,
                               long fetchedOffset,
                               FetchResponse.PartitionData<Records> partitionData,
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
                this.sensors.bytesFetched.record(this.fetchMetrics.fetchBytes);
                this.sensors.recordsFetched.record(this.fetchMetrics.fetchRecords);

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
        private final Sensor recordsFetchLead;

        private int assignmentId = 0;
        private Set<TopicPartition> assignedPartitions = Collections.emptySet();

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

            this.recordsFetchLead = metrics.sensor("records-lead");
            this.recordsFetchLead.add(metrics.metricInstance(metricsRegistry.recordsLeadMin), new Min());
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

        private void maybeUpdateAssignment(SubscriptionState subscription) {
            int newAssignmentId = subscription.assignmentId();
            if (this.assignmentId != newAssignmentId) {
                Set<TopicPartition> newAssignedPartitions = new HashSet<>(subscription.assignedPartitions());
                for (TopicPartition tp : this.assignedPartitions) {
                    if (!newAssignedPartitions.contains(tp)) {
                        metrics.removeSensor(partitionLagMetricName(tp));
                        metrics.removeSensor(partitionLeadMetricName(tp));
                    }
                }
                this.assignedPartitions = newAssignedPartitions;
                this.assignmentId = newAssignmentId;
            }
        }

        private void recordPartitionLead(TopicPartition tp, long lead) {
            this.recordsFetchLead.record(lead);

            String name = partitionLeadMetricName(tp);
            Sensor recordsLead = this.metrics.getSensor(name);
            if (recordsLead == null) {
                Map<String, String> metricTags = new HashMap<>(2);
                metricTags.put("topic", tp.topic().replace('.', '_'));
                metricTags.put("partition", String.valueOf(tp.partition()));

                recordsLead = this.metrics.sensor(name);

                recordsLead.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLead, metricTags), new Value());
                recordsLead.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLeadMin, metricTags), new Min());
                recordsLead.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLeadAvg, metricTags), new Avg());
            }
            recordsLead.record(lead);
        }

        private void recordPartitionLag(TopicPartition tp, long lag) {
            this.recordsFetchLag.record(lag);

            String name = partitionLagMetricName(tp);
            Sensor recordsLag = this.metrics.getSensor(name);
            if (recordsLag == null) {
                Map<String, String> metricTags = new HashMap<>(2);
                metricTags.put("topic", tp.topic().replace('.', '_'));
                metricTags.put("partition", String.valueOf(tp.partition()));

                recordsLag = this.metrics.sensor(name);

                recordsLag.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLag, metricTags), new Value());
                recordsLag.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLagMax, metricTags), new Max());
                recordsLag.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLagAvg, metricTags), new Avg());
            }
            recordsLag.record(lag);
        }

        private static String partitionLagMetricName(TopicPartition tp) {
            return tp + ".records-lag";
        }

        private static String partitionLeadMetricName(TopicPartition tp) {
            return tp + ".records-lead";
        }

    }

    @Override
    public void close() {
        if (nextInLineRecords != null)
            nextInLineRecords.drain();
        decompressionBufferSupplier.close();
    }

    private Set<String> topicsForPartitions(Collection<TopicPartition> partitions) {
        return partitions.stream().map(TopicPartition::topic).collect(Collectors.toSet());
    }

}
