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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnstableOffsetCommitException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class manages the offset with the consumer coordinator.
 */
public class OffsetManageCoordinator {

    private final Logger log;
    private final GroupRebalanceConfig rebalanceConfig;
    private AtomicBoolean asyncCommitFenced;
    private final AtomicInteger pendingAsyncCommits;
    private ConsumerNetworkClient client;
    private final OffsetCommitCallback defaultOffsetCommitCallback;
    private final ConsumerInterceptors<?, ?> interceptors;
    private final SubscriptionState subscriptions;
    private final boolean autoCommitEnabled;
    private Timer nextAutoCommitTimer;
    private final int autoCommitIntervalMs;
    private final boolean throwOnFetchStableOffsetsUnsupported;
    // hold onto request&future for committed offset requests to enable async calls.
    private PendingCommittedOffsetRequest pendingCommittedOffsetRequest = null;
    private final ConsumerMetadata metadata;
    private AbstractCoordinator consumerCoordinator;
    private final Sensor commitSensor;

    private static class PendingCommittedOffsetRequest {
        private final Set<TopicPartition> requestedPartitions;
        private final AbstractCoordinator.Generation requestedGeneration;
        private final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> response;

        private PendingCommittedOffsetRequest(final Set<TopicPartition> requestedPartitions,
                                              final AbstractCoordinator.Generation generationAtRequestTime,
                                              final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> response) {
            this.requestedPartitions = Objects.requireNonNull(requestedPartitions);
            this.response = Objects.requireNonNull(response);
            this.requestedGeneration = generationAtRequestTime;
        }

        private boolean sameRequest(final Set<TopicPartition> currentRequest, final AbstractCoordinator.Generation currentGeneration) {
            return Objects.equals(requestedGeneration, currentGeneration) && requestedPartitions.equals(currentRequest);
        }
    }

    // this collection must be thread-safe because it is modified from the response handler
    // of offset commit requests, which may be invoked from the heartbeat thread
    private final ConcurrentLinkedQueue<OffsetCommitCompletion> completedOffsetCommits;


    public OffsetManageCoordinator(GroupRebalanceConfig rebalanceConfig,
                                   LogContext logContext,
                                   ConsumerNetworkClient client,
                                   ConsumerMetadata metadata,
                                   SubscriptionState subscriptions,
                                   Time time,
                                   boolean autoCommitEnabled,
                                   int autoCommitIntervalMs,
                                   ConsumerInterceptors<?, ?> interceptors,
                                   boolean throwOnFetchStableOffsetsUnsupported,
                                   Sensor commitSensor,
                                   AbstractCoordinator consumerCoordinator) {

        this.client = client;
        this.rebalanceConfig = rebalanceConfig;
        this.log = logContext.logger(OffsetManageCoordinator.class);
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.defaultOffsetCommitCallback = new DefaultOffsetCommitCallback();
        this.autoCommitEnabled = autoCommitEnabled;
        this.autoCommitIntervalMs = autoCommitIntervalMs;
        this.completedOffsetCommits = new ConcurrentLinkedQueue<>();
        this.interceptors = interceptors;
        this.pendingAsyncCommits = new AtomicInteger();
        this.asyncCommitFenced = new AtomicBoolean(false);
        this.throwOnFetchStableOffsetsUnsupported = throwOnFetchStableOffsetsUnsupported;
        this.consumerCoordinator = consumerCoordinator;
        this.commitSensor = commitSensor;

        if (autoCommitEnabled)
            this.nextAutoCommitTimer = time.timer(autoCommitIntervalMs);
    }

    //------------------------------------------------------
    // methods used by ConsumerCoordinator when assigning partitions
    //------------------------------------------------------

    public boolean isAutoCommitEnabled() {
        return autoCommitEnabled;
    }

    public void updateAndResetTimer() {
        nextAutoCommitTimer.updateAndReset(autoCommitIntervalMs);
    }

    public long timeToNextPoll(long now) {
        return nextAutoCommitTimer.remainingMs();
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    public void close(final Timer timer) {

        maybeAutoCommitOffsetsSync(timer);
        while (pendingAsyncCommits.get() > 0 && timer.notExpired()) {
            consumerCoordinator.ensureCoordinatorReady(timer);
            client.poll(timer);
            invokeCompletedOffsetCommitCallbacks();
        }
    }

    //------------------------------------------------------
    // methods for offset management
    //------------------------------------------------------

    // visible for testing
    void invokeCompletedOffsetCommitCallbacks() {
        if (asyncCommitFenced.get()) {
            throw new FencedInstanceIdException("Get fenced exception for group.instance.id "
                    + rebalanceConfig.groupInstanceId.orElse("unset_instance_id")
                    + ", current member.id is " + consumerCoordinator.memberId());
        }
        while (true) {
            OffsetCommitCompletion completion = completedOffsetCommits.poll();
            if (completion == null) {
                break;
            }
            completion.invoke();
        }
    }

    protected void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        invokeCompletedOffsetCommitCallbacks();

        if (!consumerCoordinator.coordinatorUnknown()) {
            doCommitOffsetsAsync(offsets, callback);
        } else {
            // we don't know the current coordinator, so try to find it and then send the commit
            // or fail (we don't want recursive retries which can cause offset commits to arrive
            // out of order). Note that there may be multiple offset commits chained to the same
            // coordinator lookup request. This is fine because the listeners will be invoked in
            // the same order that they were added. Note also that AbstractCoordinator prevents
            // multiple concurrent coordinator lookup requests.
            pendingAsyncCommits.incrementAndGet();
            consumerCoordinator.lookupCoordinator().addListener(new RequestFutureListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                    pendingAsyncCommits.decrementAndGet();
                    doCommitOffsetsAsync(offsets, callback);
                    client.pollNoWakeup();
                }

                @Override
                public void onFailure(RuntimeException e) {
                    pendingAsyncCommits.decrementAndGet();
                    completedOffsetCommits.add(new OffsetCommitCompletion(callback, offsets,
                            new RetriableCommitFailedException(e)));
                }
            });
        }

        // ensure the commit has a chance to be transmitted (without blocking on its completion).
        // Note that commits are treated as heartbeats by the coordinator, so there is no need to
        // explicitly allow heartbeats through delayed task execution.
        client.pollNoWakeup();
    }

    private void doCommitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
        final OffsetCommitCallback cb = callback == null ? defaultOffsetCommitCallback : callback;
        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                if (interceptors != null)
                    interceptors.onCommit(offsets);
                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, null));
            }

            @Override
            public void onFailure(RuntimeException e) {
                Exception commitException = e;

                if (e instanceof RetriableException) {
                    commitException = new RetriableCommitFailedException(e);
                }
                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, commitException));
                if (commitException instanceof FencedInstanceIdException) {
                    asyncCommitFenced.set(true);
                }
            }
        });
    }

    /**
     * Commit offsets for the specified list of topics and partitions. This is a non-blocking call
     * which returns a request future that can be polled in the case of a synchronous commit or ignored in the
     * asynchronous case.
     * <p>
     * NOTE: This is visible only for testing
     *
     * @param offsets The list of offsets per partition that should be committed.
     * @return A request future whose value indicates whether the commit was successful or not
     */
    RequestFuture<Void> sendOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets.isEmpty())
            return RequestFuture.voidSuccess();

        Node coordinator = consumerCoordinator.checkAndGetCoordinator();
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

        final AbstractCoordinator.Generation generation;
        if (subscriptions.hasAutoAssignedPartitions()) {
            generation = consumerCoordinator.generationIfStable();
            // if the generation is null, we are not part of an active group (and we expect to be).
            // the only thing we can do is fail the commit and let the user rejoin the group in poll().
            if (generation == null) {
                log.info("Failing OffsetCommit request since the consumer is not part of an active group");

                if (consumerCoordinator.rebalanceInProgress()) {
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
            generation = AbstractCoordinator.Generation.NO_GENERATION;
        }

        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(
                new OffsetCommitRequestData()
                        .setGroupId(this.rebalanceConfig.groupId)
                        .setGenerationId(generation.generationId)
                        .setMemberId(generation.memberId)
                        .setGroupInstanceId(rebalanceConfig.groupInstanceId.orElse(null))
                        .setTopics(new ArrayList<>(requestTopicDataMap.values()))
        );

        log.trace("Sending OffsetCommit request with {} to coordinator {}", offsets, coordinator);

        return client.send(coordinator, builder)
                .compose(new OffsetCommitResponseHandler(offsets, generation, consumerCoordinator));
    }


    private class OffsetCommitResponseHandler extends AbstractCoordinator.CoordinatorResponseHandler<OffsetCommitResponse, Void> {
        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        private OffsetCommitResponseHandler(Map<TopicPartition, OffsetAndMetadata> offsets, AbstractCoordinator.Generation generation, AbstractCoordinator coordinator) {
            coordinator.super(generation);
            this.offsets = offsets;
        }

        @Override
        public void handle(OffsetCommitResponse commitResponse, RequestFuture<Void> future) {

            commitSensor.record(response.requestLatencyMs());

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
                            consumerCoordinator.markCoordinatorUnknown();
                            future.raise(error);
                            return;
                        } else if (error == Errors.FENCED_INSTANCE_ID) {
                            log.info("OffsetCommit failed with {} due to group instance id {} fenced", sentGeneration, rebalanceConfig.groupInstanceId);

                            // if the generation has changed or we are not in rebalancing, do not raise the fatal error but rebalance-in-progress
                            if (generationUnchanged()) {
                                future.raise(error);
                            } else {
                                if (consumerCoordinator.state == AbstractCoordinator.MemberState.REBALANCING) {
                                    future.raise(new RebalanceInProgressException("Offset commit cannot be completed since the " +
                                            "consumer member's old generation is fenced by its group instance id, it is possible that " +
                                            "this consumer has already participated another rebalance and got a new generation"));
                                } else {
                                    future.raise(new CommitFailedException());
                                }
                            }
                            return;
                        } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                            /* Consumer should not try to commit offset in between join-group and sync-group,
                             * and hence on broker-side it is not expected to see a commit offset request
                             * during CompletingRebalance phase; if it ever happens then broker would return
                             * this error to indicate that we are still in the middle of a rebalance.
                             * In this case we would throw a RebalanceInProgressException,
                             * request re-join but do not reset generations. If the callers decide to retry they
                             * can go ahead and call poll to finish up the rebalance first, and then try commit again.
                             */
                            consumerCoordinator.requestRejoin();
                            future.raise(new RebalanceInProgressException("Offset commit cannot be completed since the " +
                                    "consumer group is executing a rebalance at the moment. You can try completing the rebalance " +
                                    "by calling poll() and then retry commit again"));
                            return;
                        } else if (error == Errors.UNKNOWN_MEMBER_ID
                                || error == Errors.ILLEGAL_GENERATION) {
                            log.info("OffsetCommit failed with {}: {}", sentGeneration, error.message());

                            // only need to reset generation and re-join group if generation has not changed or we are not in rebalancing;
                            // otherwise only raise rebalance-in-progress error
                            if (!generationUnchanged() && consumerCoordinator.state == AbstractCoordinator.MemberState.REBALANCING) {
                                future.raise(new RebalanceInProgressException("Offset commit cannot be completed since the " +
                                        "consumer member's generation is already stale, meaning it has already participated another rebalance and " +
                                        "got a new generation. You can try completing the rebalance by calling poll() and then retry commit again"));
                            } else {
                                consumerCoordinator.resetGenerationOnResponseError(ApiKeys.OFFSET_COMMIT, error);
                                future.raise(new CommitFailedException());
                            }
                            return;
                        } else {
                            future.raise(new KafkaException("Unexpected error in commit: " + error.message()));
                            return;
                        }
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


    /**
     * Commit offsets synchronously. This method will retry until the commit completes successfully
     * or an unrecoverable error is encountered.
     *
     * @param offsets The offsets to be committed
     * @return If the offset commit was successfully sent and a successful response was received from
     * the coordinator
     * @throws org.apache.kafka.common.errors.AuthorizationException if the consumer is not authorized to the group
     *                                                               or to any of the specified partitions. See the exception for more details
     * @throws CommitFailedException                                 if an unrecoverable error occurs before the commit can be completed
     * @throws FencedInstanceIdException                             if a static member gets fenced
     */
    public boolean commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets, Timer timer) {
        invokeCompletedOffsetCommitCallbacks();

        if (offsets.isEmpty())
            return true;

        do {
            if (consumerCoordinator.coordinatorUnknown() && !consumerCoordinator.ensureCoordinatorReady(timer)) {
                return false;
            }

            RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
            client.poll(future, timer);

            // We may have had in-flight offset commits when the synchronous commit began. If so, ensure that
            // the corresponding callbacks are invoked prior to returning in order to preserve the order that
            // the offset commits were applied.
            invokeCompletedOffsetCommitCallbacks();

            if (future.succeeded()) {
                if (interceptors != null)
                    interceptors.onCommit(offsets);
                return true;
            }

            if (future.failed() && !future.isRetriable())
                throw future.exception();

            timer.sleep(rebalanceConfig.retryBackoffMs);
        } while (timer.notExpired());

        return false;
    }


    public void maybeAutoCommitOffsetsAsync(long now) {
        if (autoCommitEnabled) {
            nextAutoCommitTimer.update(now);
            if (nextAutoCommitTimer.isExpired()) {
                nextAutoCommitTimer.reset(autoCommitIntervalMs);
                doAutoCommitOffsetsAsync();
            }
        }
    }

    private void doAutoCommitOffsetsAsync() {
        Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
        log.debug("Sending asynchronous auto-commit of offsets {}", allConsumedOffsets);

        commitOffsetsAsync(allConsumedOffsets, (offsets, exception) -> {
            if (exception != null) {
                if (exception instanceof RetriableCommitFailedException) {
                    log.debug("Asynchronous auto-commit of offsets {} failed due to retriable error: {}", offsets,
                            exception);
                    nextAutoCommitTimer.updateAndReset(rebalanceConfig.retryBackoffMs);
                } else {
                    log.warn("Asynchronous auto-commit of offsets {} failed: {}", offsets, exception.getMessage());
                }
            } else {
                log.debug("Completed asynchronous auto-commit of offsets {}", offsets);
            }
        });
    }

    protected void maybeAutoCommitOffsetsSync(Timer timer) {
        if (autoCommitEnabled) {
            Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
            try {
                log.debug("Sending synchronous auto-commit of offsets {}", allConsumedOffsets);
                if (!commitOffsetsSync(allConsumedOffsets, timer))
                    log.debug("Auto-commit of offsets {} timed out before completion", allConsumedOffsets);
            } catch (WakeupException | InterruptException e) {
                log.debug("Auto-commit of offsets {} was interrupted before completion", allConsumedOffsets);
                // rethrow wakeups since they are triggered by the user
                throw e;
            } catch (Exception e) {
                // consistent with async auto-commit failures, we do not propagate the exception
                log.warn("Synchronous auto-commit of offsets {} failed: {}", allConsumedOffsets, e.getMessage());
            }
        }
    }

    /**
     * Fetch the current committed offsets from the coordinator for a set of partitions.
     *
     * @param partitions The partitions to fetch offsets for
     * @return A map from partition to the committed offset or null if the operation timed out
     */
    public Map<TopicPartition, OffsetAndMetadata> fetchCommittedOffsets(final Set<TopicPartition> partitions,
                                                                        final Timer timer) {
        if (partitions.isEmpty()) return Collections.emptyMap();

        final AbstractCoordinator.Generation generationForOffsetRequest = consumerCoordinator.generationIfStable();
        if (pendingCommittedOffsetRequest != null &&
                !pendingCommittedOffsetRequest.sameRequest(partitions, generationForOffsetRequest)) {
            // if we were waiting for a different request, then just clear it.
            pendingCommittedOffsetRequest = null;
        }

        do {
            if (!consumerCoordinator.ensureCoordinatorReady(timer)) return null;

            // contact coordinator to fetch committed offsets
            final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future;
            if (pendingCommittedOffsetRequest != null) {
                future = pendingCommittedOffsetRequest.response;
            } else {
                future = sendOffsetFetchRequest(partitions);
                pendingCommittedOffsetRequest = new PendingCommittedOffsetRequest(partitions, generationForOffsetRequest, future);
            }
            client.poll(future, timer);

            if (future.isDone()) {
                pendingCommittedOffsetRequest = null;

                if (future.succeeded()) {
                    return future.value();
                } else if (!future.isRetriable()) {
                    throw future.exception();
                } else {
                    timer.sleep(rebalanceConfig.retryBackoffMs);
                }
            } else {
                return null;
            }
        } while (timer.notExpired());
        return null;
    }

    /**
     * Refresh the committed offsets for provided partitions.
     *
     * @param timer Timer bounding how long this method can block
     * @return true iff the operation completed within the timeout
     */
    public boolean refreshCommittedOffsetsIfNeeded(Timer timer) {
        final Set<TopicPartition> initializingPartitions = subscriptions.initializingPartitions();

        final Map<TopicPartition, OffsetAndMetadata> offsets = fetchCommittedOffsets(initializingPartitions, timer);
        if (offsets == null) return false;

        for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            final TopicPartition tp = entry.getKey();
            final OffsetAndMetadata offsetAndMetadata = entry.getValue();
            if (offsetAndMetadata != null) {
                // first update the epoch if necessary
                entry.getValue().leaderEpoch().ifPresent(epoch -> this.metadata.updateLastSeenEpochIfNewer(entry.getKey(), epoch));

                // it's possible that the partition is no longer assigned when the response is received,
                // so we need to ignore seeking if that's the case
                if (this.subscriptions.isAssigned(tp)) {
                    final ConsumerMetadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(tp);
                    final SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(
                            offsetAndMetadata.offset(), offsetAndMetadata.leaderEpoch(),
                            leaderAndEpoch);

                    this.subscriptions.seekUnvalidated(tp, position);

                    log.info("Setting offset for partition {} to the committed offset {}", tp, position);
                } else {
                    log.info("Ignoring the returned {} since its partition {} is no longer assigned",
                            offsetAndMetadata, tp);
                }
            }
        }
        return true;
    }

    /**
     * Fetch the committed offsets for a set of partitions. This is a non-blocking call. The
     * returned future can be polled to get the actual offsets returned from the broker.
     *
     * @param partitions The set of partitions to get offsets for.
     * @return A request future containing the committed offsets.
     */
    protected RequestFuture<Map<TopicPartition, OffsetAndMetadata>> sendOffsetFetchRequest(Set<TopicPartition> partitions) {
        Node coordinator = consumerCoordinator.checkAndGetCoordinator();
        if (coordinator == null)
            return RequestFuture.coordinatorNotAvailable();

        log.debug("Fetching committed offsets for partitions: {}", partitions);
        // construct the request
        OffsetFetchRequest.Builder requestBuilder =
                new OffsetFetchRequest.Builder(this.rebalanceConfig.groupId, true, new ArrayList<>(partitions), throwOnFetchStableOffsetsUnsupported);

        // send the request with a callback
        return client.send(coordinator, requestBuilder)
                .compose(new OffsetFetchResponseHandler(consumerCoordinator));
    }

    private class OffsetFetchResponseHandler extends AbstractCoordinator.CoordinatorResponseHandler<OffsetFetchResponse, Map<TopicPartition, OffsetAndMetadata>> {
        private OffsetFetchResponseHandler(AbstractCoordinator coordinator) {
            coordinator.super(AbstractCoordinator.Generation.NO_GENERATION);
        }

        @Override
        public void handle(OffsetFetchResponse response, RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
            if (response.hasError()) {
                Errors error = response.error();
                log.debug("Offset fetch failed: {}", error.message());

                if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                    // just retry
                    future.raise(error);
                } else if (error == Errors.NOT_COORDINATOR) {
                    // re-discover the coordinator and retry
                    consumerCoordinator.markCoordinatorUnknown();
                    future.raise(error);
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                } else {
                    future.raise(new KafkaException("Unexpected error in fetch offset response: " + error.message()));
                }
                return;
            }

            Set<String> unauthorizedTopics = null;
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(response.responseData().size());
            Set<TopicPartition> unstableTxnOffsetTopicPartitions = new HashSet<>();
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData partitionData = entry.getValue();
                if (partitionData.hasError()) {
                    Errors error = partitionData.error;
                    log.debug("Failed to fetch offset for partition {}: {}", tp, error.message());

                    if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        future.raise(new KafkaException("Topic or Partition " + tp + " does not exist"));
                        return;
                    } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                        if (unauthorizedTopics == null) {
                            unauthorizedTopics = new HashSet<>();
                        }
                        unauthorizedTopics.add(tp.topic());
                    } else if (error == Errors.UNSTABLE_OFFSET_COMMIT) {
                        unstableTxnOffsetTopicPartitions.add(tp);
                    } else {
                        future.raise(new KafkaException("Unexpected error in fetch offset response for partition " +
                                tp + ": " + error.message()));
                        return;
                    }
                } else if (partitionData.offset >= 0) {
                    // record the position with the offset (-1 indicates no committed offset to fetch);
                    // if there's no committed offset, record as null
                    offsets.put(tp, new OffsetAndMetadata(partitionData.offset, partitionData.leaderEpoch, partitionData.metadata));
                } else {
                    log.info("Found no committed offset for partition {}", tp);
                    offsets.put(tp, null);
                }
            }

            if (unauthorizedTopics != null) {
                future.raise(new TopicAuthorizationException(unauthorizedTopics));
            } else if (!unstableTxnOffsetTopicPartitions.isEmpty()) {
                // just retry
                log.info("The following partitions still have unstable offsets " +
                        "which are not cleared on the broker side: {}" +
                        ", this could be either " +
                        "transactional offsets waiting for completion, or " +
                        "normal offsets waiting for replication after appending to local log", unstableTxnOffsetTopicPartitions);
                future.raise(new UnstableOffsetCommitException("There are unstable offsets for the requested topic partitions"));
            } else {
                future.complete(offsets);
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

    private static class OffsetCommitCompletion {
        private final OffsetCommitCallback callback;
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final Exception exception;

        private OffsetCommitCompletion(OffsetCommitCallback callback, Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            this.callback = callback;
            this.offsets = offsets;
            this.exception = exception;
        }

        public void invoke() {
            if (callback != null)
                callback.onComplete(offsets, exception);
        }
    }
}
