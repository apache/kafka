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
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerWakeupException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.UnknownConsumerIdException;
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
import org.apache.kafka.common.requests.ConsumerMetadataRequest;
import org.apache.kafka.common.requests.ConsumerMetadataResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This class manages the coordination process with the consumer coordinator.
 */
public final class Coordinator implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(Coordinator.class);

    private final ConsumerNetworkClient client;
    private final Time time;
    private final String groupId;
    private final Heartbeat heartbeat;
    private final HeartbeatTask heartbeatTask;
    private final int sessionTimeoutMs;
    private final String assignmentStrategy;
    private final SubscriptionState subscriptions;
    private final CoordinatorMetrics sensors;
    private final long requestTimeoutMs;
    private final long retryBackoffMs;
    private final OffsetCommitCallback defaultOffsetCommitCallback;
    private final boolean autoCommitEnabled;

    private Node consumerCoordinator;
    private String consumerId;
    private int generation;

    /**
     * Initialize the coordination manager.
     */
    public Coordinator(ConsumerNetworkClient client,
                       String groupId,
                       int sessionTimeoutMs,
                       int heartbeatIntervalMs,
                       String assignmentStrategy,
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
        this.client = client;
        this.time = time;
        this.generation = OffsetCommitRequest.DEFAULT_GENERATION_ID;
        this.consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID;
        this.groupId = groupId;
        this.consumerCoordinator = null;
        this.subscriptions = subscriptions;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.assignmentStrategy = assignmentStrategy;
        this.heartbeat = new Heartbeat(this.sessionTimeoutMs, heartbeatIntervalMs, time.milliseconds());
        this.heartbeatTask = new HeartbeatTask();
        this.sensors = new CoordinatorMetrics(metrics, metricGrpPrefix, metricTags);
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
        this.defaultOffsetCommitCallback = defaultOffsetCommitCallback;
        this.autoCommitEnabled = autoCommitEnabled;

        if (autoCommitEnabled)
            scheduleAutoCommitTask(autoCommitIntervalMs);
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
        if (!subscriptions.partitionAssignmentNeeded())
            return;

        // commit offsets prior to rebalance if auto-commit enabled
        maybeAutoCommitOffsetsSync();

        ConsumerRebalanceListener listener = subscriptions.listener();

        // execute the user's listener before rebalance
        log.debug("Revoking previously assigned partitions {}", this.subscriptions.assignedPartitions());
        try {
            Set<TopicPartition> revoked = new HashSet<>(subscriptions.assignedPartitions());
            listener.onPartitionsRevoked(revoked);
        } catch (Exception e) {
            log.error("User provided listener " + listener.getClass().getName()
                    + " failed on partition revocation: ", e);
        }

        reassignPartitions();

        // execute the user's listener after rebalance
        log.debug("Setting newly assigned partitions {}", this.subscriptions.assignedPartitions());
        try {
            Set<TopicPartition> assigned = new HashSet<>(subscriptions.assignedPartitions());
            listener.onPartitionsAssigned(assigned);
        } catch (Exception e) {
            log.error("User provided listener " + listener.getClass().getName()
                    + " failed on partition assignment: ", e);
        }
    }

    private void reassignPartitions() {
        while (subscriptions.partitionAssignmentNeeded()) {
            ensureCoordinatorKnown();

            // ensure that there are no pending requests to the coordinator. This is important
            // in particular to avoid resending a pending JoinGroup request.
            if (client.pendingRequestCount(this.consumerCoordinator) > 0) {
                client.awaitPendingRequests(this.consumerCoordinator);
                continue;
            }

            RequestFuture<Void> future = sendJoinGroupRequest();
            client.poll(future);

            if (future.failed()) {
                if (future.exception() instanceof UnknownConsumerIdException)
                    continue;
                else if (!future.isRetriable())
                    throw future.exception();
                Utils.sleep(retryBackoffMs);
            }
        }
    }

    /**
     * Block until the coordinator for this group is known.
     */
    public void ensureCoordinatorKnown() {
        while (coordinatorUnknown()) {
            RequestFuture<Void> future = sendConsumerMetadataRequest();
            client.poll(future, requestTimeoutMs);

            if (future.failed())
                client.awaitMetadataUpdate();
        }
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

    private class HeartbeatTask implements DelayedTask {

        public void reset() {
            // start or restart the heartbeat task to be executed at the next chance
            long now = time.milliseconds();
            heartbeat.resetSessionTimeout(now);
            client.unschedule(this);
            client.schedule(this, now);
        }

        @Override
        public void run(final long now) {
            if (!subscriptions.partitionsAutoAssigned() ||
                    subscriptions.partitionAssignmentNeeded() ||
                    coordinatorUnknown())
                // no need to send if we're not using auto-assignment or if we are
                // awaiting a rebalance
                return;

            if (heartbeat.sessionTimeoutExpired(now)) {
                // we haven't received a successful heartbeat in one session interval
                // so mark the coordinator dead
                coordinatorDead();
                return;
            }

            if (!heartbeat.shouldHeartbeat(now)) {
                // we don't need to heartbeat now, so reschedule for when we do
                client.schedule(this, now + heartbeat.timeToNextHeartbeat(now));
            } else {
                heartbeat.sentHeartbeat(now);
                RequestFuture<Void> future = sendHeartbeatRequest();
                future.addListener(new RequestFutureListener<Void>() {
                    @Override
                    public void onSuccess(Void value) {
                        long now = time.milliseconds();
                        heartbeat.receiveHeartbeat(now);
                        long nextHeartbeatTime = now + heartbeat.timeToNextHeartbeat(now);
                        client.schedule(HeartbeatTask.this, nextHeartbeatTime);
                    }

                    @Override
                    public void onFailure(RuntimeException e) {
                        client.schedule(HeartbeatTask.this, time.milliseconds() + retryBackoffMs);
                    }
                });
            }
        }
    }

    /**
     * Send a request to get a new partition assignment. This is a non-blocking call which sends
     * a JoinGroup request to the coordinator (if it is available). The returned future must
     * be polled to see if the request completed successfully.
     * @return A request future whose completion indicates the result of the JoinGroup request.
     */
    private RequestFuture<Void> sendJoinGroupRequest() {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

        // send a join group request to the coordinator
        List<String> subscribedTopics = new ArrayList<String>(subscriptions.subscription());
        log.debug("(Re-)joining group {} with subscribed topics {}", groupId, subscribedTopics);

        JoinGroupRequest request = new JoinGroupRequest(groupId,
                this.sessionTimeoutMs,
                subscribedTopics,
                this.consumerId,
                this.assignmentStrategy);

        // create the request for the coordinator
        log.debug("Issuing request ({}: {}) to coordinator {}", ApiKeys.JOIN_GROUP, request, this.consumerCoordinator.id());
        return client.send(consumerCoordinator, ApiKeys.JOIN_GROUP, request)
                .compose(new JoinGroupResponseHandler());
    }

    private class JoinGroupResponseHandler extends CoordinatorResponseHandler<JoinGroupResponse, Void> {

        @Override
        public JoinGroupResponse parse(ClientResponse response) {
            return new JoinGroupResponse(response.responseBody());
        }

        @Override
        public void handle(JoinGroupResponse joinResponse, RequestFuture<Void> future) {
            // process the response
            short errorCode = joinResponse.errorCode();

            if (errorCode == Errors.NONE.code()) {
                Coordinator.this.consumerId = joinResponse.consumerId();
                Coordinator.this.generation = joinResponse.generationId();

                // set the flag to refresh last committed offsets
                subscriptions.needRefreshCommits();

                log.debug("Joined group: {}", joinResponse.toStruct());

                // record re-assignment time
                sensors.partitionReassignments.record(response.requestLatencyMs());

                // update partition assignment
                subscriptions.changePartitionAssignment(joinResponse.assignedPartitions());
                heartbeatTask.reset();
                future.complete(null);
            } else if (errorCode == Errors.UNKNOWN_CONSUMER_ID.code()) {
                // reset the consumer id and retry immediately
                Coordinator.this.consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID;
                log.info("Attempt to join group {} failed due to unknown consumer id, resetting and retrying.",
                        groupId);
                future.raise(Errors.UNKNOWN_CONSUMER_ID);
            } else if (errorCode == Errors.CONSUMER_COORDINATOR_NOT_AVAILABLE.code()
                    || errorCode == Errors.NOT_COORDINATOR_FOR_CONSUMER.code()) {
                // re-discover the coordinator and retry with backoff
                coordinatorDead();
                log.info("Attempt to join group {} failed due to obsolete coordinator information, retrying.",
                        groupId);
                future.raise(Errors.forCode(errorCode));
            } else if (errorCode == Errors.UNKNOWN_PARTITION_ASSIGNMENT_STRATEGY.code()
                    || errorCode == Errors.INCONSISTENT_PARTITION_ASSIGNMENT_STRATEGY.code()
                    || errorCode == Errors.INVALID_SESSION_TIMEOUT.code()) {
                // log the error and re-throw the exception
                Errors error = Errors.forCode(errorCode);
                log.error("Attempt to join group {} failed due to: {}",
                        groupId, error.exception().getMessage());
                future.raise(error);
            } else {
                // unexpected error, throw the exception
                future.raise(new KafkaException("Unexpected error in join group response: "
                        + Errors.forCode(joinResponse.errorCode()).exception().getMessage()));
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
     * Reset the generation/consumerId tracked by this consumer.
     */
    public void resetGeneration() {
        this.generation = OffsetCommitRequest.DEFAULT_GENERATION_ID;
        this.consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID;
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
                this.consumerId,
                OffsetCommitRequest.DEFAULT_RETENTION_TIME,
                offsetData);

        return client.send(consumerCoordinator, ApiKeys.OFFSET_COMMIT, req)
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
                    if (errorCode == Errors.CONSUMER_COORDINATOR_NOT_AVAILABLE.code()
                            || errorCode == Errors.NOT_COORDINATOR_FOR_CONSUMER.code()) {
                        coordinatorDead();
                    } else if (errorCode == Errors.UNKNOWN_CONSUMER_ID.code()
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
        return client.send(consumerCoordinator, ApiKeys.OFFSET_FETCH, request)
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
                    } else if (data.errorCode == Errors.NOT_COORDINATOR_FOR_CONSUMER.code()) {
                        // re-discover the coordinator and retry
                        coordinatorDead();
                        future.raise(Errors.NOT_COORDINATOR_FOR_CONSUMER);
                    } else if (data.errorCode == Errors.UNKNOWN_CONSUMER_ID.code()
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

    /**
     * Send a heartbeat request now (visible only for testing).
     */
    public RequestFuture<Void> sendHeartbeatRequest() {
        HeartbeatRequest req = new HeartbeatRequest(this.groupId, this.generation, this.consumerId);
        return client.send(consumerCoordinator, ApiKeys.HEARTBEAT, req)
                .compose(new HeartbeatCompletionHandler());
    }

    public boolean coordinatorUnknown() {
        return this.consumerCoordinator == null;
    }

    /**
     * Discover the current coordinator for the consumer group. Sends a ConsumerMetadata request to
     * one of the brokers. The returned future should be polled to get the result of the request.
     * @return A request future which indicates the completion of the metadata request
     */
    private RequestFuture<Void> sendConsumerMetadataRequest() {
        // initiate the consumer metadata request
        // find a node to ask about the coordinator
        Node node = this.client.leastLoadedNode();
        if (node == null) {
            // TODO: If there are no brokers left, perhaps we should use the bootstrap set
            // from configuration?
            return RequestFuture.noBrokersAvailable();
        } else {
            // create a consumer metadata request
            log.debug("Issuing consumer metadata request to broker {}", node.id());
            ConsumerMetadataRequest metadataRequest = new ConsumerMetadataRequest(this.groupId);
            return client.send(node, ApiKeys.CONSUMER_METADATA, metadataRequest)
                    .compose(new RequestFutureAdapter<ClientResponse, Void>() {
                        @Override
                        public void onSuccess(ClientResponse response, RequestFuture<Void> future) {
                            handleConsumerMetadataResponse(response, future);
                        }
                    });
        }
    }

    private void handleConsumerMetadataResponse(ClientResponse resp, RequestFuture<Void> future) {
        log.debug("Consumer metadata response {}", resp);

        // parse the response to get the coordinator info if it is not disconnected,
        // otherwise we need to request metadata update
        if (resp.wasDisconnected()) {
            future.raise(new DisconnectException());
        } else if (!coordinatorUnknown()) {
            // We already found the coordinator, so ignore the request
            future.complete(null);
        } else {
            ConsumerMetadataResponse consumerMetadataResponse = new ConsumerMetadataResponse(resp.responseBody());
            // use MAX_VALUE - node.id as the coordinator id to mimic separate connections
            // for the coordinator in the underlying network client layer
            // TODO: this needs to be better handled in KAFKA-1935
            if (consumerMetadataResponse.errorCode() == Errors.NONE.code()) {
                this.consumerCoordinator = new Node(Integer.MAX_VALUE - consumerMetadataResponse.node().id(),
                        consumerMetadataResponse.node().host(),
                        consumerMetadataResponse.node().port());
                heartbeatTask.reset();
                future.complete(null);
            } else {
                future.raise(Errors.forCode(consumerMetadataResponse.errorCode()));
            }
        }
    }

    /**
     * Mark the current coordinator as dead.
     */
    private void coordinatorDead() {
        if (this.consumerCoordinator != null) {
            log.info("Marking the coordinator {} dead.", this.consumerCoordinator.id());
            this.consumerCoordinator = null;
        }
    }

    private class HeartbeatCompletionHandler extends CoordinatorResponseHandler<HeartbeatResponse, Void> {
        @Override
        public HeartbeatResponse parse(ClientResponse response) {
            return new HeartbeatResponse(response.responseBody());
        }

        @Override
        public void handle(HeartbeatResponse heartbeatResponse, RequestFuture<Void> future) {
            sensors.heartbeatLatency.record(response.requestLatencyMs());
            short error = heartbeatResponse.errorCode();
            if (error == Errors.NONE.code()) {
                log.debug("Received successful heartbeat response.");
                future.complete(null);
            } else if (error == Errors.CONSUMER_COORDINATOR_NOT_AVAILABLE.code()
                    || error == Errors.NOT_COORDINATOR_FOR_CONSUMER.code()) {
                log.info("Attempt to heart beat failed since coordinator is either not started or not valid, marking it as dead.");
                coordinatorDead();
                future.raise(Errors.forCode(error));
            } else if (error == Errors.REBALANCE_IN_PROGRESS.code()) {
                log.info("Attempt to heart beat failed since the group is rebalancing, try to re-join group.");
                subscriptions.needReassignment();
                future.raise(Errors.REBALANCE_IN_PROGRESS);
            } else if (error == Errors.ILLEGAL_GENERATION.code()) {
                log.info("Attempt to heart beat failed since generation id is not legal, try to re-join group.");
                subscriptions.needReassignment();
                future.raise(Errors.ILLEGAL_GENERATION);
            } else if (error == Errors.UNKNOWN_CONSUMER_ID.code()) {
                log.info("Attempt to heart beat failed since consumer id is not valid, reset it and try to re-join group.");
                consumerId = JoinGroupRequest.UNKNOWN_CONSUMER_ID;
                subscriptions.needReassignment();
                future.raise(Errors.UNKNOWN_CONSUMER_ID);
            } else {
                future.raise(new KafkaException("Unexpected error in heartbeat response: "
                        + Errors.forCode(error).exception().getMessage()));
            }
        }
    }

    private abstract class CoordinatorResponseHandler<R, T>
            extends RequestFutureAdapter<ClientResponse, T> {
        protected ClientResponse response;

        public abstract R parse(ClientResponse response);

        public abstract void handle(R response, RequestFuture<T> future);

        @Override
        public void onSuccess(ClientResponse clientResponse, RequestFuture<T> future) {
            this.response = clientResponse;

            if (clientResponse.wasDisconnected()) {
                int correlation = response.request().request().header().correlationId();
                log.debug("Cancelled request {} with correlation id {} due to coordinator {} being disconnected",
                        response.request(),
                        correlation,
                        response.request().request().destination());

                // mark the coordinator as dead
                coordinatorDead();
                future.raise(new DisconnectException());
                return;
            }

            R response = parse(clientResponse);
            handle(response, future);
        }

        @Override
        public void onFailure(RuntimeException e, RequestFuture<T> future) {
            if (e instanceof DisconnectException) {
                log.debug("Coordinator request failed", e);
                coordinatorDead();
            }
            future.raise(e);
        }
    }


    private class CoordinatorMetrics {
        public final Metrics metrics;
        public final String metricGrpName;

        public final Sensor commitLatency;
        public final Sensor heartbeatLatency;
        public final Sensor partitionReassignments;

        public CoordinatorMetrics(Metrics metrics, String metricGrpPrefix, Map<String, String> tags) {
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

            this.heartbeatLatency = metrics.sensor("heartbeat-latency");
            this.heartbeatLatency.add(new MetricName("heartbeat-response-time-max",
                this.metricGrpName,
                "The max time taken to receive a response to a hearbeat request",
                tags), new Max());
            this.heartbeatLatency.add(new MetricName("heartbeat-rate",
                this.metricGrpName,
                "The average number of heartbeats per second",
                tags), new Rate(new Count()));

            this.partitionReassignments = metrics.sensor("reassignment-latency");
            this.partitionReassignments.add(new MetricName("reassignment-time-avg",
                this.metricGrpName,
                "The average time taken for a partition reassignment",
                tags), new Avg());
            this.partitionReassignments.add(new MetricName("reassignment-time-max",
                this.metricGrpName,
                "The max time taken for a partition reassignment",
                tags), new Avg());
            this.partitionReassignments.add(new MetricName("reassignment-rate",
                this.metricGrpName,
                "The number of partition reassignments per second",
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

            Measurable lastHeartbeat =
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        return TimeUnit.SECONDS.convert(now - heartbeat.lastHeartbeatSend(), TimeUnit.MILLISECONDS);
                    }
                };
            metrics.addMetric(new MetricName("last-heartbeat-seconds-ago",
                this.metricGrpName,
                "The number of seconds since the last controller heartbeat",
                tags),
                lastHeartbeat);
        }
    }
}
