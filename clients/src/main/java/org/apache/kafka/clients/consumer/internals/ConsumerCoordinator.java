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
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Subscription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.Topics;
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
import org.apache.kafka.common.requests.JoinGroupRequest.ProtocolMetadata;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class manages the coordination process with the consumer coordinator.
 */
public final class ConsumerCoordinator extends AbstractCoordinator {

    private static final Logger log = LoggerFactory.getLogger(ConsumerCoordinator.class);

    private final List<PartitionAssignor> assignors;
    private final org.apache.kafka.clients.Metadata metadata;
    private final MetadataSnapshot metadataSnapshot;
    private final ConsumerCoordinatorMetrics sensors;
    private final SubscriptionState subscriptions;
    private final OffsetCommitCallback defaultOffsetCommitCallback;
    private final boolean autoCommitEnabled;
    private final AutoCommitTask autoCommitTask;
    private final ConsumerInterceptors<?, ?> interceptors;
    private final boolean excludeInternalTopics;

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
                               Time time,
                               long retryBackoffMs,
                               OffsetCommitCallback defaultOffsetCommitCallback,
                               boolean autoCommitEnabled,
                               long autoCommitIntervalMs,
                               ConsumerInterceptors<?, ?> interceptors,
                               boolean excludeInternalTopics) {
        super(client,
                groupId,
                sessionTimeoutMs,
                heartbeatIntervalMs,
                metrics,
                metricGrpPrefix,
                time,
                retryBackoffMs);
        this.metadata = metadata;

        this.metadata.requestUpdate();
        this.metadataSnapshot = new MetadataSnapshot();
        this.subscriptions = subscriptions;
        this.defaultOffsetCommitCallback = defaultOffsetCommitCallback;
        this.autoCommitEnabled = autoCommitEnabled;
        this.assignors = assignors;

        addMetadataListener();

        this.autoCommitTask = autoCommitEnabled ? new AutoCommitTask(autoCommitIntervalMs) : null;
        this.sensors = new ConsumerCoordinatorMetrics(metrics, metricGrpPrefix);
        this.interceptors = interceptors;
        this.excludeInternalTopics = excludeInternalTopics;
    }

    @Override
    public String protocolType() {
        return ConsumerProtocol.PROTOCOL_TYPE;
    }

    @Override
    public List<ProtocolMetadata> metadata() {
        List<ProtocolMetadata> metadataList = new ArrayList<>();
        for (PartitionAssignor assignor : assignors) {
            Subscription subscription = assignor.subscription(subscriptions.subscription());
            ByteBuffer metadata = ConsumerProtocol.serializeSubscription(subscription);
            metadataList.add(new ProtocolMetadata(assignor.name(), metadata));
        }
        return metadataList;
    }

    private void addMetadataListener() {
        this.metadata.addListener(new Metadata.Listener() {
            @Override
            public void onMetadataUpdate(Cluster cluster) {
                // if we encounter any unauthorized topics, raise an exception to the user
                if (!cluster.unauthorizedTopics().isEmpty())
                    throw new TopicAuthorizationException(new HashSet<>(cluster.unauthorizedTopics()));

                if (subscriptions.hasPatternSubscription()) {
                    final List<String> topicsToSubscribe = new ArrayList<>();

                    for (String topic : cluster.topics())
                        if (subscriptions.getSubscribedPattern().matcher(topic).matches() &&
                                !(excludeInternalTopics && Topics.INTERNAL_TOPICS.contains(topic)))
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

    private PartitionAssignor lookupAssignor(String name) {
        for (PartitionAssignor assignor : this.assignors) {
            if (assignor.name().equals(name))
                return assignor;
        }
        return null;
    }

    @Override
    protected void onJoinComplete(int generation,
                                  String memberId,
                                  String assignmentStrategy,
                                  ByteBuffer assignmentBuffer) {
        PartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

        Assignment assignment = ConsumerProtocol.deserializeAssignment(assignmentBuffer);

        // set the flag to refresh last committed offsets
        subscriptions.needRefreshCommits();

        // update partition assignment
        subscriptions.assignFromSubscribed(assignment.partitions());

        // give the assignor a chance to update internal state based on the received assignment
        assignor.onAssignment(assignment);

        // restart the autocommit task if needed
        if (autoCommitEnabled)
            autoCommitTask.enable();

        // execute the user's callback after rebalance
        ConsumerRebalanceListener listener = subscriptions.listener();
        log.info("Setting newly assigned partitions {} for group {}", subscriptions.assignedPartitions(), groupId);
        try {
            Set<TopicPartition> assigned = new HashSet<>(subscriptions.assignedPartitions());
            listener.onPartitionsAssigned(assigned);
        } catch (WakeupException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} for group {} failed on partition assignment",
                    listener.getClass().getName(), groupId, e);
        }
    }

    @Override
    protected Map<String, ByteBuffer> performAssignment(String leaderId,
                                                        String assignmentStrategy,
                                                        Map<String, ByteBuffer> allSubscriptions) {
        PartitionAssignor assignor = lookupAssignor(assignmentStrategy);
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

        log.debug("Performing assignment for group {} using strategy {} with subscriptions {}",
                groupId, assignor.name(), subscriptions);

        Map<String, Assignment> assignment = assignor.assign(metadata.fetch(), subscriptions);

        log.debug("Finished assignment for group {}: {}", groupId, assignment);

        Map<String, ByteBuffer> groupAssignment = new HashMap<>();
        for (Map.Entry<String, Assignment> assignmentEntry : assignment.entrySet()) {
            ByteBuffer buffer = ConsumerProtocol.serializeAssignment(assignmentEntry.getValue());
            groupAssignment.put(assignmentEntry.getKey(), buffer);
        }

        return groupAssignment;
    }

    @Override
    protected void onJoinPrepare(int generation, String memberId) {
        // commit offsets prior to rebalance if auto-commit enabled
        maybeAutoCommitOffsetsSync();

        // execute the user's callback before rebalance
        ConsumerRebalanceListener listener = subscriptions.listener();
        log.info("Revoking previously assigned partitions {} for group {}", subscriptions.assignedPartitions(), groupId);
        try {
            Set<TopicPartition> revoked = new HashSet<>(subscriptions.assignedPartitions());
            listener.onPartitionsRevoked(revoked);
        } catch (WakeupException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} for group {} failed on partition revocation",
                    listener.getClass().getName(), groupId, e);
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

            time.sleep(retryBackoffMs);
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
        // we do not need to re-enable wakeups since we are closing already
        client.disableWakeups();
        try {
            maybeAutoCommitOffsetsSync();
        } finally {
            super.close();
        }
    }

    public void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        this.subscriptions.needRefreshCommits();
        RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
        final OffsetCommitCallback cb = callback == null ? defaultOffsetCommitCallback : callback;
        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                if (interceptors != null)
                    interceptors.onCommit(offsets);
                cb.onComplete(offsets, null);
            }

            @Override
            public void onFailure(RuntimeException e) {
                cb.onComplete(offsets, e);
            }
        });
    }

    /**
     * Commit offsets synchronously. This method will retry until the commit completes successfully
     * or an unrecoverable error is encountered.
     * @param offsets The offsets to be committed
     * @throws org.apache.kafka.common.errors.AuthorizationException if the consumer is not authorized to the group
     *             or to any of the specified partitions
     * @throws CommitFailedException if an unrecoverable error occurs before the commit can be completed
     */
    public void commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets.isEmpty())
            return;

        while (true) {
            ensureCoordinatorKnown();

            RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
            client.poll(future);

            if (future.succeeded()) {
                if (interceptors != null)
                    interceptors.onCommit(offsets);
                return;
            }

            if (!future.isRetriable())
                throw future.exception();

            time.sleep(retryBackoffMs);
        }
    }

    private class AutoCommitTask implements DelayedTask {
        private final long interval;
        private boolean enabled = false;
        private boolean requestInFlight = false;

        public AutoCommitTask(long interval) {
            this.interval = interval;
        }

        public void enable() {
            if (!enabled) {
                // there shouldn't be any instances scheduled, but call unschedule anyway to ensure
                // that this task is only ever scheduled once
                client.unschedule(this);
                this.enabled = true;

                if (!requestInFlight) {
                    long now = time.milliseconds();
                    client.schedule(this, interval + now);
                }
            }
        }

        public void disable() {
            this.enabled = false;
            client.unschedule(this);
        }

        private void reschedule(long at) {
            if (enabled)
                client.schedule(this, at);
        }

        public void run(final long now) {
            if (!enabled)
                return;

            if (coordinatorUnknown()) {
                log.debug("Cannot auto-commit offsets for group {} since the coordinator is unknown", groupId);
                client.schedule(this, now + retryBackoffMs);
                return;
            }

            requestInFlight = true;
            commitOffsetsAsync(subscriptions.allConsumed(), new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    requestInFlight = false;
                    if (exception == null) {
                        reschedule(now + interval);
                    } else if (exception instanceof SendFailedException) {
                        log.debug("Failed to send automatic offset commit for group {}", groupId);
                        reschedule(now);
                    } else {
                        log.warn("Auto offset commit failed for group {}: {}", groupId, exception.getMessage());
                        reschedule(now + interval);
                    }
                }
            });
        }
    }

    private void maybeAutoCommitOffsetsSync() {
        if (autoCommitEnabled) {
            // disable periodic commits prior to committing synchronously. note that they will
            // be re-enabled after a rebalance completes
            autoCommitTask.disable();

            try {
                commitOffsetsSync(subscriptions.allConsumed());
            } catch (WakeupException e) {
                // rethrow wakeups since they are triggered by the user
                throw e;
            } catch (Exception e) {
                // consistent with async auto-commit failures, we do not propagate the exception
                log.warn("Auto offset commit failed for group {}: {}", groupId, e.getMessage());
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

        log.trace("Sending offset-commit request with {} to coordinator {} for group {}", offsets, coordinator, groupId);

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
            Set<String> unauthorizedTopics = new HashSet<>();

            for (Map.Entry<TopicPartition, Short> entry : commitResponse.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetAndMetadata offsetAndMetadata = this.offsets.get(tp);
                long offset = offsetAndMetadata.offset();

                Errors error = Errors.forCode(entry.getValue());
                if (error == Errors.NONE) {
                    log.debug("Group {} committed offset {} for partition {}", groupId, offset, tp);
                    if (subscriptions.isAssigned(tp))
                        // update the local cache only if the partition is still assigned
                        subscriptions.committed(tp, offsetAndMetadata);
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    log.error("Not authorized to commit offsets for group {}", groupId);
                    future.raise(new GroupAuthorizationException(groupId));
                    return;
                } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                    unauthorizedTopics.add(tp.topic());
                } else if (error == Errors.OFFSET_METADATA_TOO_LARGE
                        || error == Errors.INVALID_COMMIT_OFFSET_SIZE) {
                    // raise the error to the user
                    log.debug("Offset commit for group {} failed on partition {}: {}", groupId, tp, error.message());
                    future.raise(error);
                    return;
                } else if (error == Errors.GROUP_LOAD_IN_PROGRESS) {
                    // just retry
                    log.debug("Offset commit for group {} failed: {}", groupId, error.message());
                    future.raise(error);
                    return;
                } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR_FOR_GROUP
                        || error == Errors.REQUEST_TIMED_OUT) {
                    log.debug("Offset commit for group {} failed: {}", groupId, error.message());
                    coordinatorDead();
                    future.raise(error);
                    return;
                } else if (error == Errors.UNKNOWN_MEMBER_ID
                        || error == Errors.ILLEGAL_GENERATION
                        || error == Errors.REBALANCE_IN_PROGRESS) {
                    // need to re-join group
                    log.debug("Offset commit for group {} failed: {}", groupId, error.message());
                    subscriptions.needReassignment();
                    future.raise(new CommitFailedException("Commit cannot be completed since the group has already " +
                            "rebalanced and assigned the partitions to another member. This means that the time " +
                            "between subsequent calls to poll() was longer than the configured session.timeout.ms, " +
                            "which typically implies that the poll loop is spending too much time message processing. " +
                            "You can address this either by increasing the session timeout or by reducing the maximum " +
                            "size of batches returned in poll() with max.poll.records."));
                    return;
                } else {
                    log.error("Group {} failed to commit partition {} at offset {}: {}", groupId, tp, offset, error.message());
                    future.raise(new KafkaException("Unexpected error in commit: " + error.message()));
                    return;
                }
            }

            if (!unauthorizedTopics.isEmpty()) {
                log.error("Not authorized to commit to topics {} for group {}", unauthorizedTopics, groupId);
                future.raise(new TopicAuthorizationException(unauthorizedTopics));
            } else {
                future.complete(null);
            }
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

        log.debug("Group {} fetching committed offsets for partitions: {}", groupId, partitions);
        // construct the request
        OffsetFetchRequest request = new OffsetFetchRequest(this.groupId, new ArrayList<>(partitions));

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
                    Errors error = Errors.forCode(data.errorCode);
                    log.debug("Group {} failed to fetch offset for partition {}: {}", groupId, tp, error.message());

                    if (error == Errors.GROUP_LOAD_IN_PROGRESS) {
                        // just retry
                        future.raise(error);
                    } else if (error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                        // re-discover the coordinator and retry
                        coordinatorDead();
                        future.raise(error);
                    } else if (error == Errors.UNKNOWN_MEMBER_ID
                            || error == Errors.ILLEGAL_GENERATION) {
                        // need to re-join group
                        subscriptions.needReassignment();
                        future.raise(error);
                    } else {
                        future.raise(new KafkaException("Unexpected error in fetch offset response: " + error.message()));
                    }
                    return;
                } else if (data.offset >= 0) {
                    // record the position with the offset (-1 indicates no committed offset to fetch)
                    offsets.put(tp, new OffsetAndMetadata(data.offset, data.metadata));
                } else {
                    log.debug("Group {} has no committed offset for partition {}", groupId, tp);
                }
            }

            future.complete(offsets);
        }
    }

    private class ConsumerCoordinatorMetrics {
        public final Metrics metrics;
        public final String metricGrpName;

        public final Sensor commitLatency;

        public ConsumerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metrics = metrics;
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.commitLatency = metrics.sensor("commit-latency");
            this.commitLatency.add(metrics.metricName("commit-latency-avg",
                this.metricGrpName,
                "The average time taken for a commit request"), new Avg());
            this.commitLatency.add(metrics.metricName("commit-latency-max",
                this.metricGrpName,
                "The max time taken for a commit request"), new Max());
            this.commitLatency.add(metrics.metricName("commit-rate",
                this.metricGrpName,
                "The number of commit calls per second"), new Rate(new Count()));

            Measurable numParts =
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        return subscriptions.assignedPartitions().size();
                    }
                };
            metrics.addMetric(metrics.metricName("assigned-partitions",
                this.metricGrpName,
                "The number of partitions currently assigned to this consumer"), numParts);
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
