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

import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitBackgroundEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultAsyncCoordinator extends AbstractAsyncCoordinator {

    private final Logger log;
    private final SubscriptionState subscriptions;
    private final AtomicInteger pendingAsyncCommits;
    private final ConsumerCoordinatorMetrics sensors;
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;

    public DefaultAsyncCoordinator(
            final Time time,
            final LogContext logContext,
            final GroupRebalanceConfig rebalanceConfig,
            final ConsumerNetworkClient networkClient,
            final SubscriptionState subscriptionState,
            final BlockingQueue<BackgroundEvent> backgroundEventQueue,
            final Metrics metrics,
            final String metricGrpPrefix) {
        super(time, logContext, rebalanceConfig, networkClient, metrics, metricGrpPrefix);
        this.log = logContext.logger(getClass());
        this.subscriptions = subscriptionState;
        this.pendingAsyncCommits = new AtomicInteger();
        this.sensors = new ConsumerCoordinatorMetrics(metrics, metricGrpPrefix);
        this.backgroundEventQueue = backgroundEventQueue;
    }

    public void commitOffsets(
            final Map<TopicPartition, OffsetAndMetadata> offsets,
            final Optional<OffsetCommitCallback> callback,
            final RequestFuture<Void> future) {
        if (offsets.isEmpty()) {
            // No need to check coordinator if offsets is empty since commit of empty offsets is completed locally.
            doCommitOffsets(offsets, callback).chain(future);
        } else if (!coordinatorUnknownAndUnreadyAsync()) {
            // If the coordinator is ready, then we send the offset commit to the network client
            doCommitOffsets(offsets, callback).chain(future);
        } else {
            // we don't know the current coordinator, we will send a FindCoordinator request and
            // handle the commit offsets based on the FindCoordinator response. If the call
            // succeed, commit the offsets, otherwise, raise RetriableCommitFailedException and
            // handle it on the polling thread.
            pendingAsyncCommits.incrementAndGet();
            lookupCoordinator().addListener(new RequestFutureListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                    pendingAsyncCommits.decrementAndGet();
                    doCommitOffsets(offsets, callback).chain(future);
                    networkClient.pollNoWakeup();
                }

                @Override
                public void onFailure(RuntimeException e) {
                    pendingAsyncCommits.decrementAndGet();
                    backgroundEventQueue.add(new CommitBackgroundEvent(
                            offsets,
                            callback.orElse(new DefaultOffsetCommitCallback()),
                            Optional.of(new RetriableCommitFailedException(e))));
                }
            });
        }
        networkClient.pollNoWakeup();
    }

    private boolean coordinatorUnknownAndUnreadyAsync() {
        return coordinatorUnknown() && !ensureCoordinatorReadyAsync();
    }

    protected RequestFuture<Void> doCommitOffsets(
            final Map<TopicPartition, OffsetAndMetadata> offsets,
            final Optional<OffsetCommitCallback> callback) {
        RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                backgroundEventQueue.add(new CommitBackgroundEvent(
                        offsets,
                        callback.orElse(new DefaultOffsetCommitCallback()),
                        Optional.empty()));
            }
            @Override
            public void onFailure(RuntimeException e) {
                Exception commitException = e;

                if (e instanceof RetriableException) {
                    commitException = new RetriableCommitFailedException(e);
                } else if (e instanceof FencedInstanceIdException) {
                    commitException = new FencedInstanceIdException("Get fenced exception for group.instance.id "
                            + rebalanceConfig.groupInstanceId.orElse("unset_instance_id")
                            + ", current member.id is " + memberId());
                }
                backgroundEventQueue.add(
                        new CommitBackgroundEvent(
                                offsets,
                                callback.orElse(new DefaultOffsetCommitCallback()),
                                Optional.of(commitException)));
            }
        });
        return future;
    }

    /**
     * Commit offsets for the specified list of topics and partitions. This is a non-blocking call
     * which returns a request future that can be polled in the case of a synchronous commit or ignored in the
     * asynchronous case.
     *
     * NOTE: This is visible only for testing
     *
     * @param offsets The list of offsets per partition that should be committed.
     * @return A request future whose value indicates whether the commit was successful or not
     */
    RequestFuture<Void> sendOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets.isEmpty())
            return RequestFuture.voidSuccess();

        Node coordinator = checkAndGetCoordinator();
        if (coordinator == null)
            return RequestFuture.coordinatorNotAvailable();

        // create the offset commit request
        Map<String, OffsetCommitRequestData.OffsetCommitRequestTopic> requestTopicDataMap = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            if (offsetAndMetadata.offset() < 0) {
                return RequestFuture.failure(new IllegalArgumentException("Invalid offset: " + offsetAndMetadata.offset()));
            }

            OffsetCommitRequestData.OffsetCommitRequestTopic topic = requestTopicDataMap
                    .getOrDefault(topicPartition.topic(),
                            new OffsetCommitRequestData.OffsetCommitRequestTopic()
                                    .setName(topicPartition.topic())
                    );

            topic.partitions().add(new OffsetCommitRequestData.OffsetCommitRequestPartition()
                    .setPartitionIndex(topicPartition.partition())
                    .setCommittedOffset(offsetAndMetadata.offset())
                    .setCommittedLeaderEpoch(offsetAndMetadata.leaderEpoch().orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                    .setCommittedMetadata(offsetAndMetadata.metadata())
            );
            requestTopicDataMap.put(topicPartition.topic(), topic);
        }

        final Generation generation;
        final String groupInstanceId;
        if (subscriptions.hasAutoAssignedPartitions()) {
            generation = generationIfStable();
            groupInstanceId = rebalanceConfig.groupInstanceId.orElse(null);
            // if the generation is null, we are not part of an active group (and we expect to be).
            // the only thing we can do is fail the commit and let the user rejoin the group in poll().
            if (generation == null) {
                log.info("Failing OffsetCommit request since the consumer is not part of an active group");

                if (rebalanceInProgress()) {
                    // if the client knows it is already rebalancing, we can use RebalanceInProgressException instead of
                    // CommitFailedException to indicate this is not a fatal error
                    return RequestFuture.failure(new RebalanceInProgressException("Offset commit cannot be completed since the " +
                            "consumer is undergoing a rebalance for auto partition assignment. You can try completing the rebalance " +
                            "by calling poll() and then retry the operation."));
                } else {
                    return RequestFuture.failure(new CommitFailedException("Offset commit cannot be completed since the " +
                            "consumer is not part of an active group for auto partition assignment; it is likely that the consumer " +
                            "was kicked out of the group."));
                }
            }
        } else {
            generation = Generation.NO_GENERATION;
            groupInstanceId = null;
        }

        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(
                new OffsetCommitRequestData()
                        .setGroupId(this.rebalanceConfig.groupId)
                        .setGenerationId(generation.generationId)
                        .setMemberId(generation.memberId)
                        .setGroupInstanceId(groupInstanceId)
                        .setTopics(new ArrayList<>(requestTopicDataMap.values()))
        );

        log.trace("Sending OffsetCommit request with {} to coordinator {}", offsets, coordinator);

        return networkClient.send(coordinator, builder)
                .compose(new OffsetCommitResponseHandler(offsets, generation));
    }

    /**
     * Get the current generation state if the group is stable, otherwise return null
     *
     * @return the current generation or null
     */
    protected Generation generationIfStable() {
        if (!hasGroup())
            return null;
        return this.generation;
    }

    public void close(final Timer timer) {
        // we do not need to re-enable wakeups since we are closing already
        networkClient.disableWakeups();
        try {
            // TODO: Add auto-commit logic
            while (pendingAsyncCommits.get() > 0 && timer.notExpired()) {
                ensureCoordinatorReady(timer);
                networkClient.poll(timer);
            }
        } finally {
            super.close();
        }
    }

    private class OffsetCommitResponseHandler extends CoordinatorResponseHandler<OffsetCommitResponse, Void> {
        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        private OffsetCommitResponseHandler(Map<TopicPartition, OffsetAndMetadata> offsets, Generation generation) {
            super(generation);
            this.offsets = offsets;
        }

        @Override
        public void handle(OffsetCommitResponse commitResponse, RequestFuture<Void> future) {
            sensors.commitSensor.record(response.requestLatencyMs());
            Set<String> unauthorizedTopics = new HashSet<>();

            for (OffsetCommitResponseData.OffsetCommitResponseTopic topic : commitResponse.data().topics()) {
                for (OffsetCommitResponseData.OffsetCommitResponsePartition partition : topic.partitions()) {
                    TopicPartition tp = new TopicPartition(topic.name(), partition.partitionIndex());
                    OffsetAndMetadata offsetAndMetadata = this.offsets.get(tp);

                    long offset = offsetAndMetadata.offset();

                    Errors error = Errors.forCode(partition.errorCode());
                    if (error == Errors.NONE) {
                        log.debug("Committed offset {} for partition {}", offset, tp);
                    } else {
                        if (error.exception() instanceof RetriableException) {
                            log.warn("Offset commit failed on partition {} at offset {}: {}", tp, offset, error.message());
                        } else {
                            log.error("Offset commit failed on partition {} at offset {}: {}", tp, offset, error.message());
                        }

                        if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                            future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                            return;
                        } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                            unauthorizedTopics.add(tp.topic());
                        } else if (error == Errors.OFFSET_METADATA_TOO_LARGE
                                || error == Errors.INVALID_COMMIT_OFFSET_SIZE) {
                            // raise the error to the user
                            future.raise(error);
                            return;
                        } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS
                                || error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                            // just retry
                            future.raise(error);
                            return;
                        } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                                || error == Errors.NOT_COORDINATOR
                                || error == Errors.REQUEST_TIMED_OUT) {
                            markCoordinatorUnknown(error);
                            future.raise(error);
                            return;
                        } else if (error == Errors.FENCED_INSTANCE_ID) {
                            log.info("OffsetCommit failed with {} due to group instance id {} fenced", sentGeneration, rebalanceConfig.groupInstanceId);
                            future.raise(error);
                            // TODO: Generation change will be implemented after the protocol is
                            //  completed. Here we raised the error for testing purpose
                        } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                            // TODO: handle rebalance in progress
                            return;
                        } else if (error == Errors.UNKNOWN_MEMBER_ID
                                || error == Errors.ILLEGAL_GENERATION) {
                            // TODO: handle invalid generation
                            return;
                        } else {
                            future.raise(new KafkaException("Unexpected error in commit: " + error.message()));
                            return;
                        }
                    }
                }

                if (!unauthorizedTopics.isEmpty()) {
                    log.error("Not authorized to commit to topics {}", unauthorizedTopics);
                    future.raise(new TopicAuthorizationException(unauthorizedTopics));
                } else {
                    future.complete(null);
                }
            }
        }
    }

    private class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit with offsets {} failed", offsets, exception);
        }
    }

    private class ConsumerCoordinatorMetrics {
        private final String metricGrpName;
        private final Sensor commitSensor;
        private final Sensor revokeCallbackSensor;
        private final Sensor assignCallbackSensor;
        private final Sensor loseCallbackSensor;

        private ConsumerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.commitSensor = metrics.sensor("commit-latency");
            this.commitSensor.add(metrics.metricName("commit-latency-avg",
                    this.metricGrpName,
                    "The average time taken for a commit request"), new Avg());
            this.commitSensor.add(metrics.metricName("commit-latency-max",
                    this.metricGrpName,
                    "The max time taken for a commit request"), new Max());
            this.commitSensor.add(createMeter(metrics, metricGrpName, "commit", "commit calls"));

            this.revokeCallbackSensor = metrics.sensor("partition-revoked-latency");
            this.revokeCallbackSensor.add(metrics.metricName("partition-revoked-latency-avg",
                    this.metricGrpName,
                    "The average time taken for a partition-revoked rebalance listener callback"), new Avg());
            this.revokeCallbackSensor.add(metrics.metricName("partition-revoked-latency-max",
                    this.metricGrpName,
                    "The max time taken for a partition-revoked rebalance listener callback"), new Max());

            this.assignCallbackSensor = metrics.sensor("partition-assigned-latency");
            this.assignCallbackSensor.add(metrics.metricName("partition-assigned-latency-avg",
                    this.metricGrpName,
                    "The average time taken for a partition-assigned rebalance listener callback"), new Avg());
            this.assignCallbackSensor.add(metrics.metricName("partition-assigned-latency-max",
                    this.metricGrpName,
                    "The max time taken for a partition-assigned rebalance listener callback"), new Max());

            this.loseCallbackSensor = metrics.sensor("partition-lost-latency");
            this.loseCallbackSensor.add(metrics.metricName("partition-lost-latency-avg",
                    this.metricGrpName,
                    "The average time taken for a partition-lost rebalance listener callback"), new Avg());
            this.loseCallbackSensor.add(metrics.metricName("partition-lost-latency-max",
                    this.metricGrpName,
                    "The max time taken for a partition-lost rebalance listener callback"), new Max());

            Measurable numParts = (config, now) -> subscriptions.numAssignedPartitions();
            metrics.addMetric(metrics.metricName("assigned-partitions",
                    this.metricGrpName,
                    "The number of partitions currently assigned to this consumer"), numParts);
        }
    }
}
