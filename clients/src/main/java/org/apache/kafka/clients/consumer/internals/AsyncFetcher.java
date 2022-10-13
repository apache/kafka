package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.FetchResponseBackgroundEvent;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
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
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Contains three subclasses, the {@code FetchSender}, the {@code
 * FetchHandler}, and the {@code FetchBuffer}. The FetchSender is responsible
 * for constructing the fetch requests based on the {@code FetchBuffer}, and
 * enqueue the request to the network client on the background thread. The
 * {@code FetchHandler} handles fetch responses on the polling thread, and
 * update the {@code FetchBuffer}.
 *
 * The {@code FetchBuffer} consist of a {@code completedFetches} queue, which
 * holds completed sends, and a {@code inlineNextFetch}, which is the next
 * fetch to be processed.
 *
 * @param <K> Key type of the consumer record
 * @param <V> Value type of the consumer record
 */
public class AsyncFetcher<K, V> {
    public FetchBuffer createFetchBuffer() {
        return new FetchBuffer();
    }

    public FetchSender createFetchSender(
            Time time,
            LogContext logContext,
            SubscriptionState subscriptions,
            ConsumerMetadata metadata,
            ConsumerNetworkClient client,
            Metrics metrics,
            FetcherMetricsRegistry metricsRegistry,
            ApiVersions apiVersions,
            IsolationLevel isolationLevel,
            int minBytes, int maxBytes, int maxWaitMs, int fetchSize,
            String clientRackId,
            boolean checkCrcs,
            FetchBuffer fetchBuffer,
            BlockingQueue<BackgroundEvent> backgroundEventQueue) {
        return new FetchSender(time, logContext, subscriptions, metadata,
                client, metrics, metricsRegistry, apiVersions, isolationLevel
                , minBytes, maxBytes, maxWaitMs, fetchSize, clientRackId,
                checkCrcs, fetchBuffer, backgroundEventQueue);
    }

    public FetchHandler<K, V> createFetchHandler(LogContext logContext,
                                                 int maxPollRecords,
                                                 IsolationLevel isolationLevel,
                                                 FetchManagerMetrics sensors,
                                                 String fetchSize,
                                                 Deserializer<K> keyDeserializer,
                                                 Deserializer<V> valueDeserializer,
                                                 ConsumerMetadata metadata,
                                                 FetchBuffer fetchBuffer) {
        return new FetchHandler<>(logContext, maxPollRecords,
                isolationLevel, sensors, fetchSize,
                keyDeserializer, valueDeserializer, metadata, fetchBuffer);

    }

    class FetchBuffer {
        public final BlockingQueue<CompletedFetch> completedFetches;
        public Optional<CompletedFetch> nextInLineFetch;

        public FetchBuffer() {
            this.completedFetches = new LinkedBlockingQueue<>();
            this.nextInLineFetch = Optional.empty();
        }

        public synchronized void updateNextInlineFetch(CompletedFetch fetch) {
            this.nextInLineFetch = Optional.ofNullable(fetch);
        }

        public synchronized Set<TopicPartition> getPartitionsToExclude() {
            Set<TopicPartition> exclude = new HashSet<>();
            if (nextInLineFetch.isPresent() && nextInLineFetch.get().isConsumed) {
                exclude.add(nextInLineFetch.get().partition);
            }
            for (CompletedFetch completedFetch : completedFetches) {
                exclude.add(completedFetch.partition);
            }
            return exclude;
        }
    }

    class FetchSender implements Closeable {
        private final FetchManagerMetrics sensors;
        private final SubscriptionState subscriptions;
        private final ConsumerMetadata metadata;
        private final ConsumerNetworkClient client;
        private final Set<Integer> nodesWithPendingFetchRequests;
        private final Map<Integer, FetchSessionHandler> sessionHandlers;
        private final AtomicInteger metadataUpdateVersion = new AtomicInteger(-1);
        private final ApiVersions apiVersions;
        private final IsolationLevel isolationLevel;
        private final Time time;
        private final LogContext logContext;
        private final Logger log;
        private final int minBytes;
        private final int maxBytes;
        private final int maxWaitMs;
        private final int fetchSize;
        private final String clientRackId;

        private final Queue<RequestFuture<ClientResponse>> futureResults;
        private final FetchBuffer fetchBuffer;
        private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
        private final boolean chechCrcs;

        public FetchSender(
                Time time,
                LogContext logContext,
                SubscriptionState subscriptions,
                ConsumerMetadata metadata,
                ConsumerNetworkClient client,
                Metrics metrics,
                FetcherMetricsRegistry metricsRegistry,
                ApiVersions apiVersions,
                IsolationLevel isolationLevel,
                int minBytes, int maxBytes, int maxWaitMs, int fetchSize,
                String clientRackId,
                boolean checkCrcs,
                FetchBuffer fetchBuffer,
                BlockingQueue<BackgroundEvent> backgroundEventQueue) {
            this.time = time;
            this.logContext = logContext;
            this.log = logContext.logger(getClass());
            this.subscriptions = subscriptions;
            this.metadata = metadata;
            this.client = client;
            this.apiVersions = apiVersions;
            this.isolationLevel = isolationLevel;

            this.nodesWithPendingFetchRequests = new HashSet<>();
            this.sessionHandlers = new HashMap<>();
            this.sensors = new FetchManagerMetrics(metrics, metricsRegistry);
            this.futureResults = new ArrayDeque<>();

            this.minBytes = minBytes;
            this.maxWaitMs = maxWaitMs;
            this.maxBytes = maxBytes;
            this.fetchSize = fetchSize;
            this.chechCrcs = checkCrcs;
            this.clientRackId = clientRackId;
            this.fetchBuffer = fetchBuffer;
            this.backgroundEventQueue = backgroundEventQueue;
        }

        /**
         * Set-up a fetch request for any node that we have assigned partitions for which doesn't already have
         * an in-flight fetch or pending fetch data.
         *
         * @return number of fetches sent
         */
        public Queue<RequestFuture<ClientResponse>> sendFetches() {
            // Update metrics in case there was an assignment change
            sensors.maybeUpdateAssignment(subscriptions);

            Map<Node, FetchSessionHandler.FetchRequestData> fetchRequestMap = prepareFetchRequests();
            for (Map.Entry<Node, FetchSessionHandler.FetchRequestData> entry : fetchRequestMap.entrySet()) {
                final Node fetchTarget = entry.getKey();
                final FetchSessionHandler.FetchRequestData data = entry.getValue();
                final short maxVersion;
                if (!data.canUseTopicIds()) {
                    maxVersion = (short) 12;
                } else {
                    maxVersion = ApiKeys.FETCH.latestVersion();
                }
                final FetchRequest.Builder request = FetchRequest.Builder
                        .forConsumer(maxVersion, this.maxWaitMs, this.minBytes, data.toSend())
                        .isolationLevel(isolationLevel)
                        .setMaxBytes(this.maxBytes)
                        .metadata(data.metadata())
                        .removed(data.toForget())
                        .replaced(data.toReplace())
                        .rackId(clientRackId);

                if (log.isDebugEnabled()) {
                    log.debug("Sending {} {} to broker {}", isolationLevel, data.toString(), fetchTarget);
                }
                RequestFuture<ClientResponse> future = client.send(fetchTarget, request);
                futureResults.add(future);
                // We add the node to the set of nodes with pending fetch requests before adding the
                // listener because the future may have been fulfilled on another thread (e.g. during a
                // disconnection being handled by the heartbeat thread) which will mean the listener
                // will be invoked synchronously.
                this.nodesWithPendingFetchRequests.add(entry.getKey().id());
                future.addListener(new RequestFutureListener<ClientResponse>() {
                    @Override
                    public void onSuccess(ClientResponse resp) {
                        synchronized (this) {
                            try {
                                FetchResponse response = (FetchResponse) resp.responseBody();
                                FetchSessionHandler handler = sessionHandler(fetchTarget.id());
                                if (handler == null) {
                                    log.error("Unable to find FetchSessionHandler for node {}. Ignoring fetch response.",
                                            fetchTarget.id());
                                    return;
                                }
                                if (!handler.handleResponse(response, resp.requestHeader().apiVersion())) {
                                    if (response.error() == Errors.FETCH_SESSION_TOPIC_ID_ERROR) {
                                        metadata.requestUpdate();
                                    }
                                    return;
                                }

                                Map<TopicPartition, FetchResponseData.PartitionData> responseData = response.responseData(handler.sessionTopicNames(), resp.requestHeader().apiVersion());
                                Set<TopicPartition> partitions = new HashSet<>(responseData.keySet());
                                FetchResponseMetricAggregator metricAggregator = new FetchResponseMetricAggregator(sensors, partitions);

                                for (Map.Entry<TopicPartition, FetchResponseData.PartitionData> entry : responseData.entrySet()) {
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
                                                    "Response for missing session request partition: partition={}; metadata={}; toSend={}; toForget={}; toReplace={}",
                                                    new Object[]{partition, data.metadata(), data.toSend(), data.toForget(), data.toReplace()}).getMessage();
                                        }

                                        // Received fetch response for missing session partition
                                        throw new IllegalStateException(message);
                                    } else {
                                        long fetchOffset = requestData.fetchOffset;
                                        FetchResponseData.PartitionData partitionData = entry.getValue();

                                        log.debug("Fetch {} at offset {} for partition {} returned fetch data {}",
                                                isolationLevel, fetchOffset, partition, partitionData);

                                        Iterator<? extends RecordBatch> batches = FetchResponse.recordsOrFail(partitionData).batches().iterator();
                                        short responseVersion = resp.requestHeader().apiVersion();

                                        fetchBuffer.completedFetches.add(new CompletedFetch(partition, partitionData,
                                                metricAggregator, batches,
                                                fetchOffset, responseVersion,
                                                checkCrcs));
                                        // TODO: to be implemented
                                        BackgroundEvent backgroundEvent =
                                                new FetchResponseBackgroundEvent(response);
                                        backgroundEventQueue.add(backgroundEvent);
                                    }
                                }


                                sensors.fetchLatency.record(resp.requestLatencyMs());
                            } finally {
                                nodesWithPendingFetchRequests.remove(fetchTarget.id());
                            }
                        }
                    }

                    @Override
                    public void onFailure(RuntimeException e) {
                        synchronized (this) {
                            try {
                                FetchSessionHandler handler = sessionHandler(fetchTarget.id());
                                if (handler != null) {
                                    handler.handleError(e);
                                }
                            } finally {
                                nodesWithPendingFetchRequests.remove(fetchTarget.id());
                            }
                        }
                    }
                });
            }
            return futureResults;
        }

        private Map<Node, FetchSessionHandler.FetchRequestData> prepareFetchRequests() {
            Map<Node, FetchSessionHandler.Builder> fetchable = new LinkedHashMap<>();

            validatePositionsOnMetadataChange();

            long currentTimeMs = time.milliseconds();
            Map<String, Uuid> topicIds = metadata.topicIds();

            for (TopicPartition partition : fetchablePartitions()) {
                SubscriptionState.FetchPosition position = this.subscriptions.position(partition);
                if (position == null) {
                    throw new IllegalStateException("Missing position for fetchable partition " + partition);
                }

                Optional<Node> leaderOpt = position.currentLeader.leader;
                if (!leaderOpt.isPresent()) {
                    log.debug("Requesting metadata update for partition {} since the position {} is missing the current leader node", partition, position);
                    metadata.requestUpdate();
                    continue;
                }

                // Use the preferred read replica if set, otherwise the partition's leader
                Node node = selectReadReplica(partition, leaderOpt.get(), currentTimeMs);
                if (client.isUnavailable(node)) {
                    client.maybeThrowAuthFailure(node);

                    // If we try to send during the reconnect backoff window, then the request is just
                    // going to be failed anyway before being sent, so skip the send for now
                    log.trace("Skipping fetch for partition {} because node {} is awaiting reconnect backoff", partition, node);
                } else if (this.nodesWithPendingFetchRequests.contains(node.id())) {
                    log.trace("Skipping fetch for partition {} because previous request to {} has not been processed", partition, node);
                } else {
                    // if there is a leader and no in-flight requests, issue a new fetch
                    FetchSessionHandler.Builder builder = fetchable.get(node);
                    if (builder == null) {
                        int id = node.id();
                        FetchSessionHandler handler = sessionHandler(id);
                        if (handler == null) {
                            handler = new FetchSessionHandler(logContext, id);
                            sessionHandlers.put(id, handler);
                        }
                        builder = handler.newBuilder();
                        fetchable.put(node, builder);
                    }
                    builder.add(partition, new FetchRequest.PartitionData(
                            topicIds.getOrDefault(partition.topic(), Uuid.ZERO_UUID),
                            position.offset, FetchRequest.INVALID_LOG_START_OFFSET, this.fetchSize,
                            position.currentLeader.epoch, Optional.empty()));

                    log.debug("Added {} fetch request for partition {} at position {} to node {}", isolationLevel,
                            partition, position, node);
                }
            }

            Map<Node, FetchSessionHandler.FetchRequestData> reqs = new LinkedHashMap<>();
            for (Map.Entry<Node, FetchSessionHandler.Builder> entry : fetchable.entrySet()) {
                reqs.put(entry.getKey(), entry.getValue().build());
            }
            return reqs;
        }

        // Visible for testing
        protected FetchSessionHandler sessionHandler(int node) {
            return sessionHandlers.get(node);
        }

        /**
         * If we have seen new metadata (as tracked by {@link org.apache.kafka.clients.Metadata#updateVersion()}), then
         * we should check that all of the assignments have a valid position.
         */
        private void validatePositionsOnMetadataChange() {
            int newMetadataUpdateVersion = metadata.updateVersion();
            if (metadataUpdateVersion.getAndSet(newMetadataUpdateVersion) != newMetadataUpdateVersion) {
                subscriptions.assignedPartitions().forEach(topicPartition -> {
                    ConsumerMetadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(topicPartition);
                    subscriptions.maybeValidatePositionForCurrentLeader(apiVersions, topicPartition, leaderAndEpoch);
                });
            }
        }

        private List<TopicPartition> fetchablePartitions() {
            Set<TopicPartition> partitionsToExclude =
                    fetchBuffer.getPartitionsToExclude();
            return subscriptions.fetchablePartitions(tp -> !partitionsToExclude.contains(tp));
        }

        /**
         * Determine which replica to read from.
         */
        Node selectReadReplica(TopicPartition partition, Node leaderReplica, long currentTimeMs) {
            Optional<Integer> nodeId = subscriptions.preferredReadReplica(partition, currentTimeMs);
            if (nodeId.isPresent()) {
                Optional<Node> node = nodeId.flatMap(id -> metadata.fetch().nodeIfOnline(partition, id));
                if (node.isPresent()) {
                    return node.get();
                } else {
                    log.trace("Not fetching from {} for partition {} since it is marked offline or is missing from our metadata," +
                            " using the leader instead.", nodeId, partition);
                    subscriptions.clearPreferredReadReplica(partition);
                    return leaderReplica;
                }
            } else {
                return leaderReplica;
            }
        }

        @Override
        public void close() {
            log.info("closing fetch handler");
        }
    }

    class FetchHandler<K, V> {
        private Time time;
        private final LogContext logContext;
        private Logger log;
        SubscriptionState subscriptions;
        private final int maxPollRecords;
        private IsolationLevel isolationLevel;
        private FetchManagerMetrics sensors;
        private String fetchSize;

        private final Deserializer<K> keyDeserializer;
        private final Deserializer<V> valueDeserializer;
        private final ConsumerMetadata metadata;
        private final FetchBuffer fetchBuffer;

        public FetchHandler(LogContext logContext,
                            int maxPollRecords,
                            IsolationLevel isolationLevel,
                            FetchManagerMetrics sensors,
                            String fetchSize,
                            Deserializer<K> keyDeserializer,
                            Deserializer<V> valueDeserializer,
                            ConsumerMetadata metadata,
                            FetchBuffer fetchBuffer) {
            this.logContext = logContext;
            this.log = logContext.logger(getClass());
            this.maxPollRecords = maxPollRecords;
            this.isolationLevel = isolationLevel;
            this.sensors = sensors;
            this.fetchSize = fetchSize;
            this.keyDeserializer = keyDeserializer;
            this.valueDeserializer = valueDeserializer;
            this.metadata = metadata;
            this.fetchBuffer = fetchBuffer;
        }
        /**
         * Return the fetched records, empty the record buffer and update the consumed position.
         *
         * NOTE: returning an {@link Fetch#isEmpty empty} fetch guarantees the consumed position is not updated.
         *
         * @return A {@link Fetch} for the requested partitions
         * @throws OffsetOutOfRangeException If there is OffsetOutOfRange error in fetchResponse and
         *         the defaultResetPolicy is NONE
         * @throws TopicAuthorizationException If there is TopicAuthorization error in fetchResponse.
         */
        public Fetch<K, V> collectFetch() {
            Fetch<K, V> fetch = Fetch.empty();
            Queue<CompletedFetch> pausedCompletedFetches = new ArrayDeque<>();
            int recordsRemaining = maxPollRecords;

            try {
                while (recordsRemaining > 0) {
                    Optional<CompletedFetch> nextInlineFetch =
                            fetchBuffer.nextInLineFetch;
                    if (!nextInlineFetch.isPresent() ||
                            nextInlineFetch.get().isConsumed) {
                        CompletedFetch records = fetchBuffer.completedFetches.peek();
                        if (records == null) break;

                        if (records.notInitialized()) {
                            try {
                                fetchBuffer.nextInLineFetch =
                                        Optional.ofNullable(initializeCompletedFetch(records));
                            } catch (Exception e) {
                                // Remove a completedFetch upon a parse with exception if (1) it contains no records, and
                                // (2) there are no fetched records with actual content preceding this exception.
                                // The first condition ensures that the completedFetches is not stuck with the same completedFetch
                                // in cases such as the TopicAuthorizationException, and the second condition ensures that no
                                // potential data loss due to an exception in a following record.
                                FetchResponseData.PartitionData partition = records.partitionData;
                                if (fetch.isEmpty() && FetchResponse.recordsOrFail(partition).sizeInBytes() == 0) {
                                    fetchBuffer.completedFetches.poll();
                                }
                                throw e;
                            }
                        } else {
                            fetchBuffer.nextInLineFetch =
                                    Optional.of(records);
                        }
                        fetchBuffer.completedFetches.poll();
                    } else if (subscriptions.isPaused(nextInlineFetch.get().partition)) {
                        // when the partition is paused we add the records back to the completedFetches queue instead of draining
                        // them so that they can be returned on a subsequent poll if the partition is resumed at that time
                        CompletedFetch nextInLineFetch =
                                nextInlineFetch.get();
                        log.debug("Skipping fetching records for assigned " +
                                "partition {} because it is paused",
                                nextInLineFetch.partition);
                        pausedCompletedFetches.add(nextInLineFetch);
                        fetchBuffer.nextInLineFetch = Optional.empty();
                    } else {
                        CompletedFetch nextInLineFetch = nextInlineFetch.get();
                        Fetch<K, V> nextFetch = fetchRecords(nextInLineFetch,
                                recordsRemaining);
                        recordsRemaining -= nextFetch.numRecords();
                        fetch.add(nextFetch);
                    }
                }
            } catch (KafkaException e) {
                if (fetch.isEmpty())
                    throw e;
            } finally {
                // add any polled completed fetches for paused partitions back to the completed fetches queue to be
                // re-evaluated in the next poll
                fetchBuffer.completedFetches.addAll(pausedCompletedFetches);
            }

            return fetch;
        }

        private Fetch<K, V> fetchRecords(CompletedFetch completedFetch, int maxRecords) {
            if (!subscriptions.isAssigned(completedFetch.partition)) {
                // this can happen when a rebalance happened before fetched records are returned to the consumer's poll call
                log.debug("Not returning fetched records for partition {} since it is no longer assigned",
                        completedFetch.partition);
            } else if (!subscriptions.isFetchable(completedFetch.partition)) {
                // this can happen when a partition is paused before fetched records are returned to the consumer's
                // poll call or if the offset is being reset
                log.debug("Not returning fetched records for assigned partition {} since it is no longer fetchable",
                        completedFetch.partition);
            } else {
                SubscriptionState.FetchPosition position = subscriptions.position(completedFetch.partition);
                if (position == null) {
                    throw new IllegalStateException("Missing position for fetchable partition " + completedFetch.partition);
                }

                if (completedFetch.nextFetchOffset == position.offset) {
                    List<ConsumerRecord<K, V>> partRecords =
                            completedFetch.fetchRecords(logContext,
                                    subscriptions,
                                    isolationLevel,
                                    null, // buffer supplier
                                    keyDeserializer,
                                    valueDeserializer,
                                    maxRecords);

                    log.trace("Returning {} fetched records at offset {} for assigned partition {}",
                            partRecords.size(), position, completedFetch.partition);

                    boolean positionAdvanced = false;

                    if (completedFetch.nextFetchOffset > position.offset) {
                        SubscriptionState.FetchPosition nextPosition = new SubscriptionState.FetchPosition(
                                completedFetch.nextFetchOffset,
                                completedFetch.lastEpoch,
                                position.currentLeader);
                        log.trace("Updating fetch position from {} to {} for partition {} and returning {} records from `poll()`",
                                position, nextPosition, completedFetch.partition, partRecords.size());
                        subscriptions.position(completedFetch.partition, nextPosition);
                        positionAdvanced = true;
                    }

                    Long partitionLag = subscriptions.partitionLag(completedFetch.partition, isolationLevel);
                    if (partitionLag != null)
                        this.sensors.recordPartitionLag(completedFetch.partition, partitionLag);

                    Long lead = subscriptions.partitionLead(completedFetch.partition);
                    if (lead != null) {
                        this.sensors.recordPartitionLead(completedFetch.partition, lead);
                    }

                    return Fetch.forPartition(completedFetch.partition, partRecords, positionAdvanced);
                } else {
                    // these records aren't next in line based on the last consumed position, ignore them
                    // they must be from an obsolete request
                    log.debug("Ignoring fetched records for {} at offset {} since the current position is {}",
                            completedFetch.partition, completedFetch.nextFetchOffset, position);
                }
            }

            log.trace("Draining fetched records for partition {}", completedFetch.partition);
            completedFetch.drain(subscriptions);

            return Fetch.empty();
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
                byte[] keyByteArray = keyBytes == null ? null : org.apache.kafka.common.utils.Utils.toArray(keyBytes);
                K key = keyBytes == null ? null : this.keyDeserializer.deserialize(partition.topic(), headers, keyByteArray);
                ByteBuffer valueBytes = record.value();
                byte[] valueByteArray = valueBytes == null ? null : Utils.toArray(valueBytes);
                V value = valueBytes == null ? null : this.valueDeserializer.deserialize(partition.topic(), headers, valueByteArray);
                return new ConsumerRecord<>(partition.topic(), partition.partition(), offset,
                        timestamp, timestampType,
                        keyByteArray == null ? ConsumerRecord.NULL_SIZE : keyByteArray.length,
                        valueByteArray == null ? ConsumerRecord.NULL_SIZE : valueByteArray.length,
                        key, value, headers, leaderEpoch);
            } catch (RuntimeException e) {
                throw new RecordDeserializationException(partition, record.offset(),
                        "Error deserializing key/value for partition " + partition +
                                " at offset " + record.offset() + ". If needed, please seek past the record to continue consumption.", e);
            }
        }



        /**
         * Initialize a CompletedFetch object.
         */
        private CompletedFetch initializeCompletedFetch(CompletedFetch nextCompletedFetch) {
            TopicPartition tp = nextCompletedFetch.partition;
            FetchResponseData.PartitionData partition = nextCompletedFetch.partitionData;
            long fetchOffset = nextCompletedFetch.nextFetchOffset;
            CompletedFetch completedFetch = null;
            Errors error = Errors.forCode(partition.errorCode());

            try {
                if (!subscriptions.hasValidPosition(tp)) {
                    // this can happen when a rebalance happened while fetch is still in-flight
                    log.debug("Ignoring fetched records for partition {} since it no longer has valid position", tp);
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
                            FetchResponse.recordsSize(partition), tp, position);
                    Iterator<? extends RecordBatch> batches = FetchResponse.recordsOrFail(partition).batches().iterator();
                    completedFetch = nextCompletedFetch;

                    if (!batches.hasNext() && FetchResponse.recordsSize(partition) > 0) {
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

                    if (partition.highWatermark() >= 0) {
                        log.trace("Updating high watermark for partition {} to {}", tp, partition.highWatermark());
                        subscriptions.updateHighWatermark(tp, partition.highWatermark());
                    }

                    if (partition.logStartOffset() >= 0) {
                        log.trace("Updating log start offset for partition {} to {}", tp, partition.logStartOffset());
                        subscriptions.updateLogStartOffset(tp, partition.logStartOffset());
                    }

                    if (partition.lastStableOffset() >= 0) {
                        log.trace("Updating last stable offset for partition {} to {}", tp, partition.lastStableOffset());
                        subscriptions.updateLastStableOffset(tp, partition.lastStableOffset());
                    }

                    if (FetchResponse.isPreferredReplica(partition)) {
                        subscriptions.updatePreferredReadReplica(completedFetch.partition, partition.preferredReadReplica(), () -> {
                            long expireTimeMs = time.milliseconds() + metadata.metadataExpireMs();
                            log.debug("Updating preferred read replica for partition {} to {}, set to expire at {}",
                                    tp, partition.preferredReadReplica(), expireTimeMs);
                            return expireTimeMs;
                        });
                    }

                    nextCompletedFetch.initialized = true;
                } else if (error == Errors.NOT_LEADER_OR_FOLLOWER ||
                        error == Errors.REPLICA_NOT_AVAILABLE ||
                        error == Errors.KAFKA_STORAGE_ERROR ||
                        error == Errors.FENCED_LEADER_EPOCH ||
                        error == Errors.OFFSET_NOT_AVAILABLE) {
                    log.debug("Error in fetch for partition {}: {}", tp, error.exceptionName());
                    this.metadata.requestUpdate();
                } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                    log.warn("Received unknown topic or partition error in fetch for partition {}", tp);
                    this.metadata.requestUpdate();
                } else if (error == Errors.UNKNOWN_TOPIC_ID) {
                    log.warn("Received unknown topic ID error in fetch for partition {}", tp);
                    this.metadata.requestUpdate();
                } else if (error == Errors.INCONSISTENT_TOPIC_ID) {
                    log.warn("Received inconsistent topic ID error in fetch for partition {}", tp);
                    this.metadata.requestUpdate();
                } else if (error == Errors.OFFSET_OUT_OF_RANGE) {
                    Optional<Integer> clearedReplicaId = subscriptions.clearPreferredReadReplica(tp);
                    if (!clearedReplicaId.isPresent()) {
                        // If there's no preferred replica to clear, we're fetching from the leader so handle this error normally
                        SubscriptionState.FetchPosition position = subscriptions.position(tp);
                        if (position == null || fetchOffset != position.offset) {
                            log.debug("Discarding stale fetch response for partition {} since the fetched offset {} " +
                                    "does not match the current offset {}", tp, fetchOffset, position);
                        } else {
                            handleOffsetOutOfRange(position, tp);
                        }
                    } else {
                        log.debug("Unset the preferred read replica {} for partition {} since we got {} when fetching {}",
                                clearedReplicaId.get(), tp, error, fetchOffset);
                    }
                } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                    //we log the actual partition and not just the topic to help with ACL propagation issues in large clusters
                    log.warn("Not authorized to read from partition {}.", tp);
                    throw new TopicAuthorizationException(Collections.singleton(tp.topic()));
                } else if (error == Errors.UNKNOWN_LEADER_EPOCH) {
                    log.debug("Received unknown leader epoch error in fetch for partition {}", tp);
                } else if (error == Errors.UNKNOWN_SERVER_ERROR) {
                    log.warn("Unknown server error while fetching offset {} for topic-partition {}",
                            fetchOffset, tp);
                } else if (error == Errors.CORRUPT_MESSAGE) {
                    throw new KafkaException("Encountered corrupt message when fetching offset "
                            + fetchOffset
                            + " for topic-partition "
                            + tp);
                } else {
                    throw new IllegalStateException("Unexpected error code "
                            + error.code()
                            + " while fetching at offset "
                            + fetchOffset
                            + " from topic-partition " + tp);
                }
            } finally {
                if (completedFetch == null)
                    nextCompletedFetch.metricAggregator.record(tp, 0, 0);

                if (error != Errors.NONE)
                    // we move the partition to the end if there was an error. This way, it's more likely that partitions for
                    // the same topic can remain together (allowing for more efficient serialization).
                    subscriptions.movePartitionToEnd(tp);
            }

            return completedFetch;
        }

        private void handleOffsetOutOfRange(SubscriptionState.FetchPosition fetchPosition, TopicPartition topicPartition) {
            String errorMessage = "Fetch position " + fetchPosition + " is out of range for partition " + topicPartition;
            if (subscriptions.hasDefaultOffsetResetPolicy()) {
                log.info("{}, resetting offset", errorMessage);
                subscriptions.requestOffsetReset(topicPartition);
            } else {
                log.info("{}, raising error to the application since no reset policy is configured", errorMessage);
                throw new OffsetOutOfRangeException(errorMessage,
                        Collections.singletonMap(topicPartition, fetchPosition.offset));
            }
        }

        /**
         * Parse the record entry, deserializing the key / value fields if necessary
         */
        private ConsumerRecord<K, V> parseRecord(TopicPartition partition,
                                                 RecordBatch batch,
                                                 Record record,
                                                 Deserializer<K> keyDeserializer,
                                                 Deserializer<V> valueDeserializer) {
            try {
                long offset = record.offset();
                long timestamp = record.timestamp();
                Optional<Integer> leaderEpoch = maybeLeaderEpoch(batch.partitionLeaderEpoch());
                TimestampType timestampType = batch.timestampType();
                Headers headers = new RecordHeaders(record.headers());
                ByteBuffer keyBytes = record.key();
                byte[] keyByteArray = keyBytes == null ? null : Utils.toArray(keyBytes);
                K key = keyBytes == null ? null : keyDeserializer.deserialize(partition.topic(),
                        headers, keyByteArray);
                ByteBuffer valueBytes = record.value();
                byte[] valueByteArray = valueBytes == null ? null : Utils.toArray(valueBytes);
                V value = valueBytes == null ? null : valueDeserializer.deserialize(partition.topic(), headers, valueByteArray);
                return new ConsumerRecord<>(partition.topic(), partition.partition(), offset,
                        timestamp, timestampType,
                        keyByteArray == null ? ConsumerRecord.NULL_SIZE : keyByteArray.length,
                        valueByteArray == null ? ConsumerRecord.NULL_SIZE : valueByteArray.length,
                        key, value, headers, leaderEpoch);
            } catch (RuntimeException e) {
                throw new RecordDeserializationException(partition, record.offset(),
                        "Error deserializing key/value for partition " + partition +
                                " at offset " + record.offset() + ". If needed, please seek past the record to continue consumption.", e);
            }
        }

        private Optional<Integer> maybeLeaderEpoch(int leaderEpoch) {
            return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ? Optional.empty() : Optional.of(leaderEpoch);
        }
    }

    static class FetchManagerMetrics {
        private final Metrics metrics;
        private FetcherMetricsRegistry metricsRegistry;
        private final Sensor bytesFetched;
        private final Sensor recordsFetched;
        final Sensor fetchLatency;
        private final Sensor recordsFetchLag;
        private final Sensor recordsFetchLead;

        private int assignmentId = 0;
        private Set<TopicPartition> assignedPartitions = Collections.emptySet();

        FetchManagerMetrics(Metrics metrics, FetcherMetricsRegistry metricsRegistry) {
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
            this.fetchLatency.add(new Meter(new WindowedCount(), metrics.metricInstance(metricsRegistry.fetchRequestRate),
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

        void maybeUpdateAssignment(SubscriptionState subscription) {
            int newAssignmentId = subscription.assignmentId();
            if (this.assignmentId != newAssignmentId) {
                Set<TopicPartition> newAssignedPartitions = subscription.assignedPartitions();
                for (TopicPartition tp : this.assignedPartitions) {
                    if (!newAssignedPartitions.contains(tp)) {
                        metrics.removeSensor(partitionLagMetricName(tp));
                        metrics.removeSensor(partitionLeadMetricName(tp));
                        metrics.removeMetric(partitionPreferredReadReplicaMetricName(tp));
                    }
                }

                for (TopicPartition tp : newAssignedPartitions) {
                    if (!this.assignedPartitions.contains(tp)) {
                        MetricName metricName = partitionPreferredReadReplicaMetricName(tp);
                        metrics.addMetricIfAbsent(
                                metricName,
                                null,
                                (Gauge<Integer>) (config, now) -> subscription.preferredReadReplica(tp, 0L).orElse(-1)
                        );
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
                Map<String, String> metricTags = topicPartitionTags(tp);

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
                Map<String, String> metricTags = topicPartitionTags(tp);
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

        private MetricName partitionPreferredReadReplicaMetricName(TopicPartition tp) {
            Map<String, String> metricTags = topicPartitionTags(tp);
            return this.metrics.metricInstance(metricsRegistry.partitionPreferredReadReplica, metricTags);
        }

        private Map<String, String> topicPartitionTags(TopicPartition tp) {
            Map<String, String> metricTags = new HashMap<>(2);
            metricTags.put("topic", tp.topic().replace('.', '_'));
            metricTags.put("partition", String.valueOf(tp.partition()));
            return metricTags;
        }
    }

    /**
     * Since we parse the message data for each partition from each fetch response lazily, fetch-level
     * metrics need to be aggregated as the messages from each partition are parsed. This class is used
     * to facilitate this incremental aggregation.
     */
    static class FetchResponseMetricAggregator {
        private final FetchManagerMetrics sensors;
        private final Set<TopicPartition> unrecordedPartitions;

        private final FetchResponseMetricAggregator.FetchMetrics fetchMetrics = new FetchResponseMetricAggregator.FetchMetrics();
        private final Map<String, FetchResponseMetricAggregator.FetchMetrics> topicFetchMetrics = new HashMap<>();

        FetchResponseMetricAggregator(FetchManagerMetrics sensors,
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
            FetchResponseMetricAggregator.FetchMetrics topicFetchMetric = this.topicFetchMetrics.get(topic);
            if (topicFetchMetric == null) {
                topicFetchMetric = new FetchResponseMetricAggregator.FetchMetrics();
                this.topicFetchMetrics.put(topic, topicFetchMetric);
            }
            topicFetchMetric.increment(bytes, records);

            if (this.unrecordedPartitions.isEmpty()) {
                // once all expected partitions from the fetch have reported in, record the metrics
                this.sensors.bytesFetched.record(this.fetchMetrics.fetchBytes);
                this.sensors.recordsFetched.record(this.fetchMetrics.fetchRecords);

                // also record per-topic metrics
                for (Map.Entry<String, FetchResponseMetricAggregator.FetchMetrics> entry: this.topicFetchMetrics.entrySet()) {
                    FetchResponseMetricAggregator.FetchMetrics metric = entry.getValue();
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



     private static class CompletedFetch<K, V> {
        private final TopicPartition partition;
        private final Iterator<? extends RecordBatch> batches;
        private final Set<Long> abortedProducerIds;
        private final PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions;
        private final FetchResponseData.PartitionData partitionData;
        private final FetchResponseMetricAggregator metricAggregator;
        private final short responseVersion;

        private int recordsRead;
        private int bytesRead;
        private RecordBatch currentBatch;
        private Record lastRecord;
        private CloseableIterator<Record> records;
        private long nextFetchOffset;
        private Optional<Integer> lastEpoch;
        private boolean isConsumed = false;
        private Exception cachedRecordException = null;
        private boolean corruptLastRecord = false;
        private boolean initialized = false;
        private boolean checkCrcs;

        CompletedFetch(TopicPartition partition,
                       FetchResponseData.PartitionData partitionData,
                       FetchResponseMetricAggregator metricAggregator,
                       Iterator<? extends RecordBatch> batches,
                       Long fetchOffset,
                       short responseVersion,
                       boolean checkCrcs) {
            this.partition = partition;
            this.partitionData = partitionData;
            this.metricAggregator = metricAggregator;
            this.batches = batches;
            this.nextFetchOffset = fetchOffset;
            this.responseVersion = responseVersion;
            this.lastEpoch = Optional.empty();
            this.abortedProducerIds = new HashSet<>();
            this.abortedTransactions = abortedTransactions(partitionData);
            this.checkCrcs = checkCrcs;
        }

        private boolean notInitialized() {
            return !this.initialized;
        }

        private PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions(FetchResponseData.PartitionData partition) {
            if (partition.abortedTransactions() == null || partition.abortedTransactions().isEmpty())
                return null;

            PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions = new PriorityQueue<>(
                    partition.abortedTransactions().size(), Comparator.comparingLong(FetchResponseData.AbortedTransaction::firstOffset)
            );
            abortedTransactions.addAll(partition.abortedTransactions());
            return abortedTransactions;
        }

         private Optional<Integer> maybeLeaderEpoch(int leaderEpoch) {
             return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ? Optional.empty() : Optional.of(leaderEpoch);
         }

         private void maybeCloseRecordStream() {
             if (records != null) {
                 records.close();
                 records = null;
             }
         }

         private void maybeEnsureValid(RecordBatch batch) {
             if (checkCrcs && currentBatch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                 try {
                     batch.ensureValid();
                 } catch (CorruptRecordException e) {
                     throw new KafkaException("Record batch for partition " + partition + " at offset " +
                             batch.baseOffset() + " is invalid, cause: " + e.getMessage());
                 }
             }
         }

         private void maybeEnsureValid(Record record) {
             if (checkCrcs) {
                 try {
                     record.ensureValid();
                 } catch (CorruptRecordException e) {
                     throw new KafkaException("Record for partition " + partition + " at offset " + record.offset()
                             + " is invalid, cause: " + e.getMessage());
                 }
             }
         }

         private void consumeAbortedTransactionsUpTo(long offset) {
             if (abortedTransactions == null)
                 return;

             while (!abortedTransactions.isEmpty() && abortedTransactions.peek().firstOffset() <= offset) {
                 FetchResponseData.AbortedTransaction abortedTransaction = abortedTransactions.poll();
                 abortedProducerIds.add(abortedTransaction.producerId());
             }
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

         private boolean isBatchAborted(RecordBatch batch) {
             return batch.isTransactional() && abortedProducerIds.contains(batch.producerId());
         }

         private ConsumerRecord<K, V> parseRecord(TopicPartition partition,
                                                  RecordBatch batch,
                                                  Record record,
                                                  Deserializer<K> keyDeserializer,
                                                  Deserializer<V> valueDeserializer) {
             try {
                 long offset = record.offset();
                 long timestamp = record.timestamp();
                 Optional<Integer> leaderEpoch = maybeLeaderEpoch(batch.partitionLeaderEpoch());
                 TimestampType timestampType = batch.timestampType();
                 Headers headers = new RecordHeaders(record.headers());
                 ByteBuffer keyBytes = record.key();
                 byte[] keyByteArray = keyBytes == null ? null : Utils.toArray(keyBytes);
                 K key = keyBytes == null ? null : keyDeserializer.deserialize(partition.topic(), headers, keyByteArray);
                 ByteBuffer valueBytes = record.value();
                 byte[] valueByteArray = valueBytes == null ? null : Utils.toArray(valueBytes);
                 V value = valueBytes == null ? null : valueDeserializer.deserialize(partition.topic(), headers, valueByteArray);
                 return new ConsumerRecord<>(partition.topic(), partition.partition(), offset,
                         timestamp, timestampType,
                         keyByteArray == null ? ConsumerRecord.NULL_SIZE : keyByteArray.length,
                         valueByteArray == null ? ConsumerRecord.NULL_SIZE : valueByteArray.length,
                         key, value, headers, leaderEpoch);
             } catch (RuntimeException e) {
                 throw new RecordDeserializationException(partition, record.offset(),
                         "Error deserializing key/value for partition " + partition +
                                 " at offset " + record.offset() + ". If needed, please seek past the record to continue consumption.", e);
             }
         }

         private void drain(SubscriptionState subscriptions) {
             if (!isConsumed) {
                 maybeCloseRecordStream();
                 cachedRecordException = null;
                 isConsumed = true;
                 metricAggregator.record(partition, bytesRead, recordsRead);

                 // we move the partition to the end if we received some bytes. This way, it's more likely that partitions
                 // for the same topic can remain together (allowing for more efficient serialization).
                 if (bytesRead > 0)
                     subscriptions.movePartitionToEnd(partition);
             }
         }

         private List<ConsumerRecord<K, V>> fetchRecords(LogContext logContext,
                                                         SubscriptionState subscriptions,
                                                         IsolationLevel isolationLevel,
                                                         BufferSupplier decompressionBufferSupplier,
                                                         Deserializer<K> keyDeserializer,
                                                         Deserializer<V> valueDeserializer,
                                                         int maxRecords) {
             // Error when fetching the next record before deserialization.
             if (corruptLastRecord)
                 throw new KafkaException("Received exception when fetching the next record from " + partition
                         + ". If needed, please seek past the record to "
                         + "continue consumption.", cachedRecordException);

             if (isConsumed)
                 return Collections.emptyList();

             List<ConsumerRecord<K, V>> records = new ArrayList<>();
             try {
                 for (int i = 0; i < maxRecords; i++) {
                     // Only move to next record if there was no exception in the last fetch. Otherwise we should
                     // use the last record to do deserialization again.
                     if (cachedRecordException == null) {
                         corruptLastRecord = true;
                         lastRecord = nextFetchedRecord(subscriptions,
                                 isolationLevel, decompressionBufferSupplier,
                                 logContext);
                         corruptLastRecord = false;
                     }
                     if (lastRecord == null)
                         break;
                     records.add(parseRecord(partition, currentBatch,
                             lastRecord, keyDeserializer, valueDeserializer));
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


         private Record nextFetchedRecord(SubscriptionState subscriptions,
                                                    IsolationLevel isolationLevel,
                                                    BufferSupplier decompressionBufferSupplier,
                                                    LogContext logContext) {
             Logger log = logContext.logger(getClass());
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
                         drain(subscriptions);
                         return null;
                     }

                     currentBatch = batches.next();
                     lastEpoch =
                             currentBatch.partitionLeaderEpoch() == RecordBatch.NO_PARTITION_LEADER_EPOCH ?
                                     Optional.empty() :
                                     Optional.of(currentBatch.partitionLeaderEpoch());

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
    }
}
