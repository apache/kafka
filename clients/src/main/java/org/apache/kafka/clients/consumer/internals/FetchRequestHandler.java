package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
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
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class FetchRequestHandler implements Closeable {
    private final Time time;
    private final LogContext logContext;
    private final Logger log;
    private final SubscriptionState subscriptions;
    private final ConsumerMetadata metadata;
    private final ConsumerNetworkClient client;
    private final Set<Integer> nodesWithPendingFetchRequests;
    private final Map<Integer, FetchSessionHandler> sessionHandlers;
    private final AtomicInteger metadataUpdateVersion = new AtomicInteger(-1);
    private final ApiVersions apiVersions;
    private final IsolationLevel isolationLevel;

    private final int minBytes;
    private final int maxBytes;
    private final int maxWaitMs;
    private final int fetchSize;
    private final String clientRackId;
    private final AsyncFetcher.FetchManagerMetrics sensors;
    private final AsyncFetcher.FetchBuffer fetchBuffer;

    private final Queue<RequestFuture<ClientResponse>> futureResults;

    public FetchRequestHandler(
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
            AsyncFetcher.FetchBuffer fetchBuffer) {
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
        this.sensors = new AsyncFetcher.FetchManagerMetrics(metrics, metricsRegistry);
        this.futureResults = new ArrayDeque<>();

        this.minBytes = minBytes;
        this.maxWaitMs = maxWaitMs;
        this.maxBytes = maxBytes;
        this.fetchSize = fetchSize;
        this.clientRackId = clientRackId;
        this.fetchBuffer = fetchBuffer;
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
                            AsyncFetcher.FetchResponseMetricAggregator metricAggregator = new AsyncFetcher.FetchResponseMetricAggregator(sensors, partitions);

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

                                    fetchBuffer.completedFetches.add(new AsyncFetcher.CompletedFetch(partition, partitionData,
                                            metricAggregator, batches,
                                            fetchOffset, responseVersion,
                                            false));
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
        Set<TopicPartition> exclusion = fetchBuffer.getPartitionsToExclude();
        return subscriptions.fetchablePartitions(tp -> !exclusion.contains(tp));
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
