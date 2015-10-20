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
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerWakeupException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Subscription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class manages the coordination process with the consumer coordinator.
 */
public final class ConsumerCoordinator extends AbstractCoordinator implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ConsumerCoordinator.class);

    private final Map<String, PartitionAssignor> protocolMap;
    private final org.apache.kafka.clients.Metadata metadata;
    private final MetadataSnapshot metadataSnapshot;
    private final ConsumerCoordinatorMetrics sensors;
    private final SubscriptionState subscriptions;
    private final OffsetCommitCallback defaultOffsetCommitCallback;
    private final boolean autoCommitEnabled;

    /**
     * Initialize the coordination manager.
     */
    public ConsumerCoordinator(ConsumerNetworkClient client,
                               String groupId,
                               int sessionTimeoutMs,
                               int heartbeatIntervalMs,
                               List<PartitionAssignor> assignors,
                               Metadata metadata,
                               SubscriptionState subscriptions,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Map<String, String> metricTags,
                               Time time,
                               long requestTimeoutMs,
                               long retryBackoffMs,
                               OffsetCommitCallback defaultOffsetCommitCallback,
                               boolean autoCommitEnabled,
                               long autoCommitIntervalMs) {
        super(client,
                groupId,
                sessionTimeoutMs,
                heartbeatIntervalMs,
                metrics,
                metricGrpPrefix,
                metricTags,
                time,
                requestTimeoutMs,
                retryBackoffMs);
        this.metadata = metadata;

        this.metadata.requestUpdate();
        this.metadataSnapshot = new MetadataSnapshot();
        this.subscriptions = subscriptions;
        this.defaultOffsetCommitCallback = defaultOffsetCommitCallback;
        this.autoCommitEnabled = autoCommitEnabled;

        this.protocolMap = new HashMap<>();
        for (PartitionAssignor assignor : assignors)
            this.protocolMap.put(assignor.name(), assignor);

        addMetadataListener();

        if (autoCommitEnabled)
            scheduleAutoCommitTask(autoCommitIntervalMs);

        this.sensors = new ConsumerCoordinatorMetrics(metrics, metricGrpPrefix, metricTags);
    }

    @Override
    public String protocolType() {
        return "consumer";
    }

    @Override
    public LinkedHashMap<String, ByteBuffer> metadata() {
        LinkedHashMap<String, ByteBuffer> metadata = new LinkedHashMap<>();
        for (PartitionAssignor assignor : protocolMap.values()) {
            Subscription subscription = assignor.subscription(subscriptions.subscription());
            metadata.put(assignor.name(), ConsumerProtocol.serializeSubscription(subscription));
        }
        return metadata;
    }

    private void addMetadataListener() {
        this.metadata.addListener(new Metadata.Listener() {
            @Override
            public void onMetadataUpdate(Cluster cluster) {
                if (subscriptions.hasPatternSubscription()) {
                    final List<String> topicsToSubscribe = new ArrayList<>();

                    for (String topic : cluster.topics())
                        if (subscriptions.getSubscribedPattern().matcher(topic).matches())
                            topicsToSubscribe.add(topic);

                    subscriptions.changeSubscription(topicsToSubscribe);
                    metadata.setTopics(subscriptions.groupSubscription());
                }

                // check if there are any changes to the metadata which should trigger a rebalance
                if (metadataSnapshot.update(subscriptions, cluster) && subscriptions.partitionsAutoAssigned())
                    subscriptions.needReassignment();
            }
        });
    }

    @Override
    protected void onJoin(int generation,
                          String memberId,
                          String assignmentStrategy,
                          ByteBuffer assignmentBuffer) {
        PartitionAssignor assignor = protocolMap.get(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

        Assignment assignment = ConsumerProtocol.deserializeAssignment(assignmentBuffer);

        // set the flag to refresh last committed offsets
        subscriptions.needRefreshCommits();

        // update partition assignment
        subscriptions.changePartitionAssignment(assignment.partitions());

        // give the assignor a chance to update internal state based on the received assignment
        assignor.onAssignment(assignment);

        // execute the user's callback after rebalance
        ConsumerRebalanceListener listener = subscriptions.listener();
        log.debug("Setting newly assigned partitions {}", subscriptions.assignedPartitions());
        try {
            Set<TopicPartition> assigned = new HashSet<>(subscriptions.assignedPartitions());
            listener.onPartitionsAssigned(assigned);
        } catch (Exception e) {
            log.error("User provided listener " + listener.getClass().getName()
                    + " failed on partition assignment: ", e);
        }
    }

    @Override
    protected Map<String, ByteBuffer> doSync(String leaderId,
                                             String assignmentStrategy,
                                             Map<String, ByteBuffer> allSubscriptions) {
        PartitionAssignor assignor = protocolMap.get(protocol);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

        Set<String> allSubscribedTopics = new HashSet<>();
        Map<String, Subscription> subscriptions = new HashMap<>();
        for (Map.Entry<String, ByteBuffer> subscriptionEntry : allSubscriptions.entrySet()) {
            Subscription subscription = ConsumerProtocol.deserializeSubscription(subscriptionEntry.getValue());
            subscriptions.put(subscriptionEntry.getKey(), subscription);
            allSubscribedTopics.addAll(subscription.topics());
        }

        // the leader will begin watching for changes to any of the topics the group is interested in,
        // which ensures that all metadata changes will eventually be seen
        this.subscriptions.groupSubscribe(allSubscribedTopics);
        metadata.setTopics(this.subscriptions.groupSubscription());
        client.ensureFreshMetadata();

        log.debug("Performing {} assignment for subscriptions {}", assignor.name(), subscriptions);

        Map<String, Assignment> assignment = assignor.assign(metadata.fetch(), subscriptions);

        log.debug("Finished assignment: {}", assignment);

        Map<String, ByteBuffer> groupAssignment = new HashMap<>();
        for (Map.Entry<String, Assignment> assignmentEntry : assignment.entrySet()) {
            ByteBuffer buffer = ConsumerProtocol.serializeAssignment(assignmentEntry.getValue());
            groupAssignment.put(assignmentEntry.getKey(), buffer);
        }

        return groupAssignment;
    }

    @Override
    protected void onLeave(int generation, String memberId) {
        // commit offsets prior to rebalance if auto-commit enabled
        maybeAutoCommitOffsetsSync();

        // execute the user's callback before rebalance
        ConsumerRebalanceListener listener = subscriptions.listener();
        log.debug("Revoking previously assigned partitions {}", subscriptions.assignedPartitions());
        try {
            Set<TopicPartition> revoked = new HashSet<>(subscriptions.assignedPartitions());
            listener.onPartitionsRevoked(revoked);
        } catch (Exception e) {
            log.error("User provided listener " + listener.getClass().getName()
                    + " failed on partition revocation: ", e);
        }

        subscriptions.needReassignment();
    }

    @Override
    public boolean needRejoin() {
        return subscriptions.partitionsAutoAssigned() &&
                (super.needRejoin() || subscriptions.partitionAssignmentNeeded());
    }

    /**
     * Refresh the committed offsets for provided partitions.
     */
    public void refreshCommittedOffsetsIfNeeded() {
        if (subscriptions.refreshCommitsNeeded()) {
            Map<TopicPartition, OffsetAndMetadata> offsets = fetchCommittedOffsets(subscriptions.assignedPartitions());
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                // verify assignment is still active
                if (subscriptions.isAssigned(tp))
                    this.subscriptions.committed(tp, entry.getValue());
            }
            this.subscriptions.commitsRefreshed();
        }
    }

    /**
     * Fetch the current committed offsets from the coordinator for a set of partitions.
     * @param partitions The partitions to fetch offsets for
     * @return A map from partition to the committed offset
     */
    public Map<TopicPartition, OffsetAndMetadata> fetchCommittedOffsets(Set<TopicPartition> partitions) {
        while (true) {
            ensureCoordinatorKnown();

            // contact coordinator to fetch committed offsets
            RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future = sendOffsetFetchRequest(partitions);
            client.poll(future);

            if (future.succeeded())
                return future.value();

            if (!future.isRetriable())
                throw future.exception();

            Utils.sleep(retryBackoffMs);
        }
    }

    /**
     * Ensure that we have a valid partition assignment from the coordinator.
     */
    public void ensurePartitionAssignment() {
        if (subscriptions.partitionsAutoAssigned())
            ensureActiveGroup();
    }

    @Override
    public void close() {
        // commit offsets prior to closing if auto-commit enabled
        while (true) {
            try {
                maybeAutoCommitOffsetsSync();
                return;
            } catch (ConsumerWakeupException e) {
                // ignore wakeups while closing to ensure we have a chance to commit
                continue;
            }
        }
    }

    public void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        this.subscriptions.needRefreshCommits();
        RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
        final OffsetCommitCallback cb = callback == null ? defaultOffsetCommitCallback : callback;
        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                cb.onComplete(offsets, null);
            }

            @Override
            public void onFailure(RuntimeException e) {
                cb.onComplete(offsets, e);
            }
        });
    }

    public void commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets.isEmpty())
            return;

        while (true) {
            ensureCoordinatorKnown();

            RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
            client.poll(future);

            if (future.succeeded()) {
                return;
            }

            if (!future.isRetriable()) {
                throw future.exception();
            }

            Utils.sleep(retryBackoffMs);
        }
    }

    private void scheduleAutoCommitTask(final long interval) {
        DelayedTask task = new DelayedTask() {
            public void run(long now) {
                commitOffsetsAsync(subscriptions.allConsumed(), new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if (exception != null)
                            log.error("Auto offset commit failed.", exception);
                    }
                });
                client.schedule(this, now + interval);
            }
        };
        client.schedule(task, time.milliseconds() + interval);
    }

    private void maybeAutoCommitOffsetsSync() {
        if (autoCommitEnabled) {
            try {
                commitOffsetsSync(subscriptions.allConsumed());
            } catch (ConsumerWakeupException e) {
                // rethrow wakeups since they are triggered by the user
                throw e;
            } catch (Exception e) {
                // consistent with async auto-commit failures, we do not propagate the exception
                log.error("Auto offset commit failed.", e);
            }
        }
    }

    /**
     * Commit offsets for the specified list of topics and partitions. This is a non-blocking call
     * which returns a request future that can be polled in the case of a synchronous commit or ignored in the
     * asynchronous case.
     *
     * @param offsets The list of offsets per partition that should be committed.
     * @return A request future whose value indicates whether the commit was successful or not
     */
    private RequestFuture<Void> sendOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

        if (offsets.isEmpty())
            return RequestFuture.voidSuccess();

        // create the offset commit request
        Map<TopicPartition, OffsetCommitRequest.PartitionData> offsetData = new HashMap<>(offsets.size());
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            offsetData.put(entry.getKey(), new OffsetCommitRequest.PartitionData(
                    offsetAndMetadata.offset(), offsetAndMetadata.metadata()));
        }

        OffsetCommitRequest req = new OffsetCommitRequest(this.groupId,
                this.generation,
                this.memberId,
                OffsetCommitRequest.DEFAULT_RETENTION_TIME,
                offsetData);

        return client.send(coordinator, ApiKeys.OFFSET_COMMIT, req)
                .compose(new OffsetCommitResponseHandler(offsets));
    }

    public static class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit failed.", exception);
        }
    }

    private class OffsetCommitResponseHandler extends CoordinatorResponseHandler<OffsetCommitResponse, Void> {

        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        public OffsetCommitResponseHandler(Map<TopicPartition, OffsetAndMetadata> offsets) {
            this.offsets = offsets;
        }

        @Override
        public OffsetCommitResponse parse(ClientResponse response) {
            return new OffsetCommitResponse(response.responseBody());
        }

        @Override
        public void handle(OffsetCommitResponse commitResponse, RequestFuture<Void> future) {
            sensors.commitLatency.record(response.requestLatencyMs());
            for (Map.Entry<TopicPartition, Short> entry : commitResponse.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetAndMetadata offsetAndMetadata = this.offsets.get(tp);
                long offset = offsetAndMetadata.offset();

                short errorCode = entry.getValue();
                if (errorCode == Errors.NONE.code()) {
                    log.debug("Committed offset {} for partition {}", offset, tp);
                    if (subscriptions.isAssigned(tp))
                        // update the local cache only if the partition is still assigned
                        subscriptions.committed(tp, offsetAndMetadata);
                } else {
                    if (errorCode == Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code()
                            || errorCode == Errors.NOT_COORDINATOR_FOR_GROUP.code()) {
                        coordinatorDead();
                    } else if (errorCode == Errors.UNKNOWN_MEMBER_ID.code()
                            || errorCode == Errors.ILLEGAL_GENERATION.code()) {
                        // need to re-join group
                        subscriptions.needReassignment();
                    }

                    log.error("Error committing partition {} at offset {}: {}",
                            tp,
                            offset,
                            Errors.forCode(errorCode).exception().getMessage());

                    future.raise(Errors.forCode(errorCode));
                    return;
                }
            }

            future.complete(null);
        }
    }

    /**
     * Fetch the committed offsets for a set of partitions. This is a non-blocking call. The
     * returned future can be polled to get the actual offsets returned from the broker.
     *
     * @param partitions The set of partitions to get offsets for.
     * @return A request future containing the committed offsets.
     */
    private RequestFuture<Map<TopicPartition, OffsetAndMetadata>> sendOffsetFetchRequest(Set<TopicPartition> partitions) {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

        log.debug("Fetching committed offsets for partitions: {}",  Utils.join(partitions, ", "));
        // construct the request
        OffsetFetchRequest request = new OffsetFetchRequest(this.groupId, new ArrayList<TopicPartition>(partitions));

        // send the request with a callback
        return client.send(coordinator, ApiKeys.OFFSET_FETCH, request)
                .compose(new OffsetFetchResponseHandler());
    }

    private class OffsetFetchResponseHandler extends CoordinatorResponseHandler<OffsetFetchResponse, Map<TopicPartition, OffsetAndMetadata>> {

        @Override
        public OffsetFetchResponse parse(ClientResponse response) {
            return new OffsetFetchResponse(response.responseBody());
        }

        @Override
        public void handle(OffsetFetchResponse response, RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(response.responseData().size());
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData data = entry.getValue();
                if (data.hasError()) {
                    log.debug("Error fetching offset for topic-partition {}: {}", tp, Errors.forCode(data.errorCode)
                            .exception()
                            .getMessage());
                    if (data.errorCode == Errors.OFFSET_LOAD_IN_PROGRESS.code()) {
                        // just retry
                        future.raise(Errors.OFFSET_LOAD_IN_PROGRESS);
                    } else if (data.errorCode == Errors.NOT_COORDINATOR_FOR_GROUP.code()) {
                        // re-discover the coordinator and retry
                        coordinatorDead();
                        future.raise(Errors.NOT_COORDINATOR_FOR_GROUP);
                    } else if (data.errorCode == Errors.UNKNOWN_MEMBER_ID.code()
                            || data.errorCode == Errors.ILLEGAL_GENERATION.code()) {
                        // need to re-join group
                        subscriptions.needReassignment();
                        future.raise(Errors.forCode(data.errorCode));
                    } else {
                        future.raise(new KafkaException("Unexpected error in fetch offset response: "
                                + Errors.forCode(data.errorCode).exception().getMessage()));
                    }
                    return;
                } else if (data.offset >= 0) {
                    // record the position with the offset (-1 indicates no committed offset to fetch)
                    offsets.put(tp, new OffsetAndMetadata(data.offset, data.metadata));
                } else {
                    log.debug("No committed offset for partition " + tp);
                }
            }

            future.complete(offsets);
        }
    }

    private class ConsumerCoordinatorMetrics {
        public final Metrics metrics;
        public final String metricGrpName;

        public final Sensor commitLatency;

        public ConsumerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix, Map<String, String> tags) {
            this.metrics = metrics;
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.commitLatency = metrics.sensor("commit-latency");
            this.commitLatency.add(new MetricName("commit-latency-avg",
                this.metricGrpName,
                "The average time taken for a commit request",
                tags), new Avg());
            this.commitLatency.add(new MetricName("commit-latency-max",
                this.metricGrpName,
                "The max time taken for a commit request",
                tags), new Max());
            this.commitLatency.add(new MetricName("commit-rate",
                this.metricGrpName,
                "The number of commit calls per second",
                tags), new Rate(new Count()));

            Measurable numParts =
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        return subscriptions.assignedPartitions().size();
                    }
                };
            metrics.addMetric(new MetricName("assigned-partitions",
                this.metricGrpName,
                "The number of partitions currently assigned to this consumer",
                tags),
                numParts);
        }
    }

    private static class MetadataSnapshot {
        private Map<String, Integer> partitionsPerTopic = new HashMap<>();

        public boolean update(SubscriptionState subscription, Cluster cluster) {
            Map<String, Integer> partitionsPerTopic = new HashMap<>();
            for (String topic : subscription.groupSubscription())
                partitionsPerTopic.put(topic, cluster.partitionCountForTopic(topic));

            if (!partitionsPerTopic.equals(this.partitionsPerTopic)) {
                this.partitionsPerTopic = partitionsPerTopic;
                return true;
            }

            return false;
        }
    }


}
