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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
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
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.LogEntry;
import org.apache.kafka.common.record.MemoryRecords;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * This class manage the fetching process with the brokers.
 */
public class Fetcher<K, V> {

    private static final Logger log = LoggerFactory.getLogger(Fetcher.class);

    private final ConsumerNetworkClient client;
    private final Time time;
    private final int minBytes;
    private final int maxWaitMs;
    private final int fetchSize;
    private final long retryBackoffMs;
    private final int maxPollRecords;
    private final boolean checkCrcs;
    private final Metadata metadata;
    private final FetchManagerMetrics sensors;
    private final SubscriptionState subscriptions;
    private final List<PartitionRecords<K, V>> records;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    private final Map<TopicPartition, Long> offsetOutOfRangePartitions;
    private final Set<String> unauthorizedTopics;
    private final Map<TopicPartition, Long> recordTooLargePartitions;

    public Fetcher(ConsumerNetworkClient client,
                   int minBytes,
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
        this.maxWaitMs = maxWaitMs;
        this.fetchSize = fetchSize;
        this.maxPollRecords = maxPollRecords;
        this.checkCrcs = checkCrcs;

        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;

        this.records = new LinkedList<>();
        this.offsetOutOfRangePartitions = new HashMap<>();
        this.unauthorizedTopics = new HashSet<>();
        this.recordTooLargePartitions = new HashMap<>();

        this.sensors = new FetchManagerMetrics(metrics, metricGrpPrefix);
        this.retryBackoffMs = retryBackoffMs;
    }

    /**
     * Set-up a fetch request for any node that we have assigned partitions for which doesn't have one.
     *
     * @param cluster The current cluster metadata
     */
    public void sendFetches(Cluster cluster) {
        for (Map.Entry<Node, FetchRequest> fetchEntry: createFetchRequests(cluster).entrySet()) {
            final FetchRequest fetch = fetchEntry.getValue();
            client.send(fetchEntry.getKey(), ApiKeys.FETCH, fetch)
                    .addListener(new RequestFutureListener<ClientResponse>() {
                        @Override
                        public void onSuccess(ClientResponse response) {
                            handleFetchResponse(response, fetch);
                        }

                        @Override
                        public void onFailure(RuntimeException e) {
                            log.debug("Fetch failed", e);
                        }
                    });
        }
    }

    /**
     * Update the fetch positions for the provided partitions.
     * @param partitions
     * @throws NoOffsetForPartitionException If no offset is stored for a given partition and no reset policy is available
     */
    public void updateFetchPositions(Set<TopicPartition> partitions) {
        // reset the fetch position to the committed position
        for (TopicPartition tp : partitions) {
            if (!subscriptions.isAssigned(tp) || subscriptions.isFetchable(tp))
                continue;

            // TODO: If there are several offsets to reset, we could submit offset requests in parallel
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
        return getTopicMetadata(MetadataRequest.allTopics(), timeout);
    }

    /**
     * Get metadata for all topics present in Kafka cluster
     *
     * @param request The MetadataRequest to send
     * @param timeout time for which getting topic metadata is attempted
     * @return The map of topics with their partition information
     */
    public Map<String, List<PartitionInfo>> getTopicMetadata(MetadataRequest request, long timeout) {
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
                MetadataResponse response = new MetadataResponse(future.value().responseBody());
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
    private RequestFuture<ClientResponse> sendMetadataRequest(MetadataRequest request) {
        final Node node = client.leastLoadedNode();
        if (node == null)
            return RequestFuture.noBrokersAvailable();
        else
            return client.send(node, ApiKeys.METADATA, request);
    }

    /**
     * Reset offsets for the given partition using the offset reset strategy.
     *
     * @param partition The given partition that needs reset offset
     * @throws org.apache.kafka.clients.consumer.NoOffsetForPartitionException If no offset reset strategy is defined
     */
    private void resetOffset(TopicPartition partition) {
        OffsetResetStrategy strategy = subscriptions.resetStrategy(partition);
        final long timestamp;
        if (strategy == OffsetResetStrategy.EARLIEST)
            timestamp = ListOffsetRequest.EARLIEST_TIMESTAMP;
        else if (strategy == OffsetResetStrategy.LATEST)
            timestamp = ListOffsetRequest.LATEST_TIMESTAMP;
        else
            throw new NoOffsetForPartitionException(partition);

        log.debug("Resetting offset for partition {} to {} offset.", partition, strategy.name().toLowerCase(Locale.ROOT));
        long offset = listOffset(partition, timestamp);

        // we might lose the assignment while fetching the offset, so check it is still active
        if (subscriptions.isAssigned(partition))
            this.subscriptions.seek(partition, offset);
    }

    /**
     * Fetch a single offset before the given timestamp for the partition.
     *
     * @param partition The partition that needs fetching offset.
     * @param timestamp The timestamp for fetching offset.
     * @return The offset of the message that is published before the given timestamp
     */
    private long listOffset(TopicPartition partition, long timestamp) {
        while (true) {
            RequestFuture<Long> future = sendListOffsetRequest(partition, timestamp);
            client.poll(future);

            if (future.succeeded())
                return future.value();

            if (!future.isRetriable())
                throw future.exception();

            if (future.exception() instanceof InvalidMetadataException)
                client.awaitMetadataUpdate();
            else
                time.sleep(retryBackoffMs);
        }
    }

    /**
     * If any partition from previous fetchResponse contains OffsetOutOfRange error and
     * the defaultResetPolicy is NONE, throw OffsetOutOfRangeException
     *
     * @throws OffsetOutOfRangeException If there is OffsetOutOfRange error in fetchResponse
     */
    private void throwIfOffsetOutOfRange() throws OffsetOutOfRangeException {
        Map<TopicPartition, Long> currentOutOfRangePartitions = new HashMap<>();

        // filter offsetOutOfRangePartitions to retain only the fetchable partitions
        for (Map.Entry<TopicPartition, Long> entry: this.offsetOutOfRangePartitions.entrySet()) {
            if (!subscriptions.isFetchable(entry.getKey())) {
                log.debug("Ignoring fetched records for {} since it is no longer fetchable", entry.getKey());
                continue;
            }
            Long position = subscriptions.position(entry.getKey());
            // ignore partition if the current position != the offset in fetchResponse, e.g. after seek()
            if (position != null && entry.getValue().equals(position))
                currentOutOfRangePartitions.put(entry.getKey(), entry.getValue());
        }
        this.offsetOutOfRangePartitions.clear();
        if (!currentOutOfRangePartitions.isEmpty())
            throw new OffsetOutOfRangeException(currentOutOfRangePartitions);
    }

    /**
     * If any topic from previous fetchResponse contains an Authorization error, raise an exception
     * @throws TopicAuthorizationException
     */
    private void throwIfUnauthorizedTopics() throws TopicAuthorizationException {
        if (!unauthorizedTopics.isEmpty()) {
            Set<String> topics = new HashSet<>(unauthorizedTopics);
            unauthorizedTopics.clear();
            throw new TopicAuthorizationException(topics);
        }
    }

    /**
     * If any partition from previous fetchResponse gets a RecordTooLarge error, throw RecordTooLargeException
     *
     * @throws RecordTooLargeException If there is a message larger than fetch size and hence cannot be ever returned
     */
    private void throwIfRecordTooLarge() throws RecordTooLargeException {
        Map<TopicPartition, Long> copiedRecordTooLargePartitions = new HashMap<>(this.recordTooLargePartitions);
        this.recordTooLargePartitions.clear();

        if (!copiedRecordTooLargePartitions.isEmpty())
            throw new RecordTooLargeException("There are some messages at [Partition=Offset]: "
                + copiedRecordTooLargePartitions
                + " whose size is larger than the fetch size "
                + this.fetchSize
                + " and hence cannot be ever returned."
                + " Increase the fetch size, or decrease the maximum message size the broker will allow.",
                copiedRecordTooLargePartitions);
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
        if (this.subscriptions.partitionAssignmentNeeded()) {
            return Collections.emptyMap();
        } else {
            Map<TopicPartition, List<ConsumerRecord<K, V>>> drained = new HashMap<>();
            throwIfOffsetOutOfRange();
            throwIfUnauthorizedTopics();
            throwIfRecordTooLarge();

            int maxRecords = maxPollRecords;
            Iterator<PartitionRecords<K, V>> iterator = records.iterator();
            while (iterator.hasNext() && maxRecords > 0) {
                PartitionRecords<K, V> part = iterator.next();
                maxRecords -= append(drained, part, maxRecords);
                if (part.isConsumed())
                    iterator.remove();
            }
            return drained;
        }
    }

    private int append(Map<TopicPartition, List<ConsumerRecord<K, V>>> drained,
                       PartitionRecords<K, V> part,
                       int maxRecords) {
        if (!subscriptions.isAssigned(part.partition)) {
            // this can happen when a rebalance happened before fetched records are returned to the consumer's poll call
            log.debug("Not returning fetched records for partition {} since it is no longer assigned", part.partition);
        } else {
            // note that the consumed position should always be available as long as the partition is still assigned
            long position = subscriptions.position(part.partition);
            if (!subscriptions.isFetchable(part.partition)) {
                // this can happen when a partition is paused before fetched records are returned to the consumer's poll call
                log.debug("Not returning fetched records for assigned partition {} since it is no longer fetchable", part.partition);
            } else if (part.fetchOffset == position) {
                List<ConsumerRecord<K, V>> partRecords = part.take(maxRecords);
                long nextOffset = partRecords.get(partRecords.size() - 1).offset() + 1;

                log.trace("Returning fetched records at offset {} for assigned partition {} and update " +
                        "position to {}", position, part.partition, nextOffset);

                List<ConsumerRecord<K, V>> records = drained.get(part.partition);
                if (records == null) {
                    records = partRecords;
                    drained.put(part.partition, records);
                } else {
                    records.addAll(partRecords);
                }

                subscriptions.position(part.partition, nextOffset);
                return partRecords.size();
            } else {
                // these records aren't next in line based on the last consumed position, ignore them
                // they must be from an obsolete request
                log.debug("Ignoring fetched records for {} at offset {} since the current position is {}",
                        part.partition, part.fetchOffset, position);
            }
        }

        part.discard();
        return 0;
    }

    /**
     * Fetch a single offset before the given timestamp for the partition.
     *
     * @param topicPartition The partition that needs fetching offset.
     * @param timestamp The timestamp for fetching offset.
     * @return A response which can be polled to obtain the corresponding offset.
     */
    private RequestFuture<Long> sendListOffsetRequest(final TopicPartition topicPartition, long timestamp) {
        Map<TopicPartition, ListOffsetRequest.PartitionData> partitions = new HashMap<>(1);
        partitions.put(topicPartition, new ListOffsetRequest.PartitionData(timestamp, 1));
        PartitionInfo info = metadata.fetch().partition(topicPartition);
        if (info == null) {
            metadata.add(topicPartition.topic());
            log.debug("Partition {} is unknown for fetching offset, wait for metadata refresh", topicPartition);
            return RequestFuture.staleMetadata();
        } else if (info.leader() == null) {
            log.debug("Leader for partition {} unavailable for fetching offset, wait for metadata refresh", topicPartition);
            return RequestFuture.leaderNotAvailable();
        } else {
            Node node = info.leader();
            ListOffsetRequest request = new ListOffsetRequest(-1, partitions);
            return client.send(node, ApiKeys.LIST_OFFSETS, request)
                    .compose(new RequestFutureAdapter<ClientResponse, Long>() {
                        @Override
                        public void onSuccess(ClientResponse response, RequestFuture<Long> future) {
                            handleListOffsetResponse(topicPartition, response, future);
                        }
                    });
        }
    }

    /**
     * Callback for the response of the list offset call above.
     * @param topicPartition The partition that was fetched
     * @param clientResponse The response from the server.
     */
    private void handleListOffsetResponse(TopicPartition topicPartition,
                                          ClientResponse clientResponse,
                                          RequestFuture<Long> future) {
        ListOffsetResponse lor = new ListOffsetResponse(clientResponse.responseBody());
        short errorCode = lor.responseData().get(topicPartition).errorCode;
        if (errorCode == Errors.NONE.code()) {
            List<Long> offsets = lor.responseData().get(topicPartition).offsets;
            if (offsets.size() != 1)
                throw new IllegalStateException("This should not happen.");
            long offset = offsets.get(0);
            log.debug("Fetched offset {} for partition {}", offset, topicPartition);

            future.complete(offset);
        } else if (errorCode == Errors.NOT_LEADER_FOR_PARTITION.code()
                || errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code()) {
            log.debug("Attempt to fetch offsets for partition {} failed due to obsolete leadership information, retrying.",
                    topicPartition);
            future.raise(Errors.forCode(errorCode));
        } else {
            log.warn("Attempt to fetch offsets for partition {} failed due to: {}",
                    topicPartition, Errors.forCode(errorCode).message());
            future.raise(new StaleMetadataException());
        }
    }

    private Set<TopicPartition> fetchablePartitions() {
        Set<TopicPartition> fetchable = subscriptions.fetchablePartitions();
        if (records.isEmpty())
            return fetchable;
        for (PartitionRecords<K, V> partitionRecords : records)
            fetchable.remove(partitionRecords.partition);
        return fetchable;
    }

    /**
     * Create fetch requests for all nodes for which we have assigned partitions
     * that have no existing requests in flight.
     */
    private Map<Node, FetchRequest> createFetchRequests(Cluster cluster) {
        // create the fetch info
        Map<Node, Map<TopicPartition, FetchRequest.PartitionData>> fetchable = new HashMap<>();
        for (TopicPartition partition : fetchablePartitions()) {
            Node node = cluster.leaderFor(partition);
            if (node == null) {
                metadata.requestUpdate();
            } else if (this.client.pendingRequestCount(node) == 0) {
                // if there is a leader and no in-flight requests, issue a new fetch
                Map<TopicPartition, FetchRequest.PartitionData> fetch = fetchable.get(node);
                if (fetch == null) {
                    fetch = new HashMap<>();
                    fetchable.put(node, fetch);
                }

                long position = this.subscriptions.position(partition);
                fetch.put(partition, new FetchRequest.PartitionData(position, this.fetchSize));
                log.trace("Added fetch request for partition {} at offset {}", partition, position);
            }
        }

        // create the fetches
        Map<Node, FetchRequest> requests = new HashMap<>();
        for (Map.Entry<Node, Map<TopicPartition, FetchRequest.PartitionData>> entry : fetchable.entrySet()) {
            Node node = entry.getKey();
            FetchRequest fetch = new FetchRequest(this.maxWaitMs, this.minBytes, entry.getValue());
            requests.put(node, fetch);
        }
        return requests;
    }

    /**
     * The callback for fetch completion
     */
    private void handleFetchResponse(ClientResponse resp, FetchRequest request) {
        int totalBytes = 0;
        int totalCount = 0;
        FetchResponse response = new FetchResponse(resp.responseBody());
        for (Map.Entry<TopicPartition, FetchResponse.PartitionData> entry : response.responseData().entrySet()) {
            TopicPartition tp = entry.getKey();
            FetchResponse.PartitionData partition = entry.getValue();
            if (!subscriptions.isFetchable(tp)) {
                // this can happen when a rebalance happened or a partition consumption paused
                // while fetch is still in-flight
                log.debug("Ignoring fetched records for partition {} since it is no longer fetchable", tp);
            } else if (partition.errorCode == Errors.NONE.code()) {
                long fetchOffset = request.fetchData().get(tp).offset;

                // we are interested in this fetch only if the beginning offset matches the
                // current consumed position
                Long position = subscriptions.position(tp);
                if (position == null || position != fetchOffset) {
                    log.debug("Discarding fetch response for partition {} since its offset {} does not match " +
                            "the expected offset {}", tp, fetchOffset, position);
                    continue;
                }

                int bytes = 0;
                ByteBuffer buffer = partition.recordSet;
                MemoryRecords records = MemoryRecords.readableRecords(buffer);
                List<ConsumerRecord<K, V>> parsed = new ArrayList<>();
                for (LogEntry logEntry : records) {
                    // Skip the messages earlier than current position.
                    if (logEntry.offset() >= position) {
                        parsed.add(parseRecord(tp, logEntry));
                        bytes += logEntry.size();
                    }
                }

                if (!parsed.isEmpty()) {
                    log.trace("Adding fetched record for partition {} with offset {} to buffered record list", tp, position);
                    ConsumerRecord<K, V> record = parsed.get(parsed.size() - 1);
                    this.records.add(new PartitionRecords<>(fetchOffset, tp, parsed));
                    this.sensors.recordsFetchLag.record(partition.highWatermark - record.offset());
                } else if (buffer.limit() > 0) {
                    // we did not read a single message from a non-empty buffer
                    // because that message's size is larger than fetch size, in this case
                    // record this exception
                    this.recordTooLargePartitions.put(tp, fetchOffset);
                }

                this.sensors.recordTopicFetchMetrics(tp.topic(), bytes, parsed.size());
                totalBytes += bytes;
                totalCount += parsed.size();
            } else if (partition.errorCode == Errors.NOT_LEADER_FOR_PARTITION.code()
                || partition.errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code()) {
                this.metadata.requestUpdate();
            } else if (partition.errorCode == Errors.OFFSET_OUT_OF_RANGE.code()) {
                long fetchOffset = request.fetchData().get(tp).offset;
                if (subscriptions.hasDefaultOffsetResetPolicy())
                    subscriptions.needOffsetReset(tp);
                else
                    this.offsetOutOfRangePartitions.put(tp, fetchOffset);
                log.info("Fetch offset {} is out of range, resetting offset", fetchOffset);
            } else if (partition.errorCode == Errors.TOPIC_AUTHORIZATION_FAILED.code()) {
                log.warn("Not authorized to read from topic {}.", tp.topic());
                unauthorizedTopics.add(tp.topic());
            } else if (partition.errorCode == Errors.UNKNOWN.code()) {
                log.warn("Unknown error fetching data for topic-partition {}", tp);
            } else {
                throw new IllegalStateException("Unexpected error code " + partition.errorCode + " while fetching data");
            }
        }
        this.sensors.bytesFetched.record(totalBytes);
        this.sensors.recordsFetched.record(totalCount);
        this.sensors.fetchThrottleTimeSensor.record(response.getThrottleTime());
        this.sensors.fetchLatency.record(resp.requestLatencyMs());
    }

    /**
     * Parse the record entry, deserializing the key / value fields if necessary
     */
    private ConsumerRecord<K, V> parseRecord(TopicPartition partition, LogEntry logEntry) {
        try {
            if (this.checkCrcs)
                logEntry.record().ensureValid();
            long offset = logEntry.offset();
            long timestamp = logEntry.record().timestamp();
            TimestampType timestampType = logEntry.record().timestampType();
            ByteBuffer keyBytes = logEntry.record().key();
            byte[] keyByteArray = keyBytes == null ? null : Utils.toArray(keyBytes);
            K key = keyBytes == null ? null : this.keyDeserializer.deserialize(partition.topic(), keyByteArray);
            ByteBuffer valueBytes = logEntry.record().value();
            byte[] valueByteArray = valueBytes == null ? null : Utils.toArray(valueBytes);
            V value = valueBytes == null ? null : this.valueDeserializer.deserialize(partition.topic(), valueByteArray);

            return new ConsumerRecord<>(partition.topic(), partition.partition(), offset,
                                        timestamp, timestampType, logEntry.record().checksum(),
                                        keyByteArray == null ? ConsumerRecord.NULL_SIZE : keyByteArray.length,
                                        valueByteArray == null ? ConsumerRecord.NULL_SIZE : valueByteArray.length,
                                        key, value);
        } catch (KafkaException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new KafkaException("Error deserializing key/value for partition " + partition + " at offset " + logEntry.offset(), e);
        }
    }

    private static class PartitionRecords<K, V> {
        public long fetchOffset;
        public TopicPartition partition;
        public List<ConsumerRecord<K, V>> records;

        public PartitionRecords(long fetchOffset, TopicPartition partition, List<ConsumerRecord<K, V>> records) {
            this.fetchOffset = fetchOffset;
            this.partition = partition;
            this.records = records;
        }

        private boolean isConsumed() {
            return records == null || records.isEmpty();
        }

        private void discard() {
            this.records = null;
        }

        private List<ConsumerRecord<K, V>> take(int n) {
            if (records == null)
                return Collections.emptyList();

            if (n >= records.size()) {
                List<ConsumerRecord<K, V>> res = this.records;
                this.records = null;
                return res;
            }

            List<ConsumerRecord<K, V>> res = new ArrayList<>(n);
            Iterator<ConsumerRecord<K, V>> iterator = records.iterator();
            for (int i = 0; i < n; i++) {
                res.add(iterator.next());
                iterator.remove();
            }

            if (iterator.hasNext())
                this.fetchOffset = iterator.next().offset();

            return res;
        }
    }

    private class FetchManagerMetrics {
        public final Metrics metrics;
        public final String metricGrpName;

        public final Sensor bytesFetched;
        public final Sensor recordsFetched;
        public final Sensor fetchLatency;
        public final Sensor recordsFetchLag;
        public final Sensor fetchThrottleTimeSensor;


        public FetchManagerMetrics(Metrics metrics, String metricGrpPrefix) {
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

        public void recordTopicFetchMetrics(String topic, int bytes, int records) {
            Map<String, String> metricTags = new HashMap<>();
            metricTags.put("topic", topic.replace(".", "_"));

            // record bytes fetched
            String name = "topic." + topic + ".bytes-fetched";
            Sensor bytesFetched = this.metrics.getSensor(name);
            if (bytesFetched == null) {
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
    }
}
