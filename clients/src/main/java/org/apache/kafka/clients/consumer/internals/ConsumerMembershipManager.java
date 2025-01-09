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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CompletableBackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackNeededEvent;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.metrics.ConsumerRebalanceMetricsManager;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceMetricsManager;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED;
import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_LOST;
import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_REVOKED;

/**
 * Group manager for a single consumer that has a group id defined in the config
 * {@link ConsumerConfig#GROUP_ID_CONFIG}, to use the Kafka-based offset management capability,
 * and the consumer group protocol to get automatically assigned partitions when calling the
 * subscribe API.
 *
 * <p/>
 *
 * While the subscribe API hasn't been called (or if the consumer called unsubscribe), this manager
 * will only be responsible for keeping the member in the {@link MemberState#UNSUBSCRIBED} state,
 * where it can commit offsets to the group identified by the {@link #groupId()}, without joining
 * the group.
 *
 * <p/>
 *
 * If the consumer subscribe API is called, this manager will use the {@link #groupId()} to join the
 * consumer group, and based on the consumer group protocol heartbeats, will handle the full
 * lifecycle of the member as it joins the group, reconciles assignments, handles fencing and
 * fatal errors, and leaves the group.
 *
 * <p/>
 *
 * Reconciliation process:<p/>
 * The member accepts all assignments received from the broker, resolves topic names from
 * metadata, reconciles the resolved assignments, and keeps the unresolved to be reconciled when
 * discovered with a metadata update. Reconciliations of resolved assignments are executed
 * sequentially and acknowledged to the server as they complete. The reconciliation process
 * involves multiple async operations, so the member will continue to heartbeat while these
 * operations complete, to make sure that the member stays in the group while reconciling.
 *
 * <p/>
 *
 * Reconciliation steps:
 * <ol>
 *     <li>Resolve topic names for all topic IDs received in the target assignment. Topic names
 *     found in metadata are then ready to be reconciled. Topic IDs not found are kept as
 *     unresolved, and the member request metadata updates until it resolves them (or the broker
 *     removes it from the target assignment.</li>
 *     <li>Commit offsets if auto-commit is enabled.</li>
 *     <li>Invoke the user-defined onPartitionsRevoked listener.</li>
 *     <li>Invoke the user-defined onPartitionsAssigned listener.</li>
 *     <li>When the above steps complete, the member acknowledges the reconciled assignment,
 *     which is the subset of the target that was resolved from metadata and actually reconciled.
 *     The ack is performed by sending a heartbeat request back to the broker, including the
 *     reconciled assignment.</li>
 * </ol>
 *
 * Note that user-defined callbacks are triggered from this manager that runs in the
 * BackgroundThread, but executed in the Application Thread, where a failure will be returned to
 * the user if the callbacks fail. This manager is only concerned about the callbacks completion to
 * know that it can proceed with the reconciliation.
 */
public class ConsumerMembershipManager extends AbstractMembershipManager<ConsumerGroupHeartbeatResponse> {

    /**
     * Group instance ID to be used by the member, provided when creating the current membership manager.
     */
    protected final Optional<String> groupInstanceId;

    /**
     * Rebalance timeout. To be used as time limit for the commit request issued
     * when a new assignment is received, that is retried until it succeeds, fails with a
     * non-retriable error, it the time limit expires.
     */
    private final int rebalanceTimeoutMs;

    /**
     * Name of the server-side assignor this member has configured to use. It will be sent
     * out to the server on the {@link ConsumerGroupHeartbeatRequest}. If not defined, the server
     * will select the assignor implementation to use.
     */
    private final Optional<String> serverAssignor;

    /**
     * Manager to perform commit requests needed before revoking partitions (if auto-commit is
     * enabled)
     */
    private final CommitRequestManager commitRequestManager;

    /**
     * Serves as the conduit by which we can report events to the application thread. This is needed as we send
     * {@link ConsumerRebalanceListenerCallbackNeededEvent callbacks} and, if needed,
     * {@link ErrorEvent errors} to the application thread.
     */
    private final BackgroundEventHandler backgroundEventHandler;

    public ConsumerMembershipManager(String groupId,
                                     Optional<String> groupInstanceId,
                                     int rebalanceTimeoutMs,
                                     Optional<String> serverAssignor,
                                     SubscriptionState subscriptions,
                                     CommitRequestManager commitRequestManager,
                                     ConsumerMetadata metadata,
                                     LogContext logContext,
                                     BackgroundEventHandler backgroundEventHandler,
                                     Time time,
                                     Metrics metrics) {
        this(groupId,
            groupInstanceId,
            rebalanceTimeoutMs,
            serverAssignor,
            subscriptions,
            commitRequestManager,
            metadata,
            logContext,
            backgroundEventHandler,
            time,
            new ConsumerRebalanceMetricsManager(metrics));
    }

    // Visible for testing
    ConsumerMembershipManager(String groupId,
                              Optional<String> groupInstanceId,
                              int rebalanceTimeoutMs,
                              Optional<String> serverAssignor,
                              SubscriptionState subscriptions,
                              CommitRequestManager commitRequestManager,
                              ConsumerMetadata metadata,
                              LogContext logContext,
                              BackgroundEventHandler backgroundEventHandler,
                              Time time,
                              RebalanceMetricsManager metricsManager) {
        super(groupId,
            subscriptions,
            metadata,
            logContext.logger(ConsumerMembershipManager.class),
            time,
            metricsManager);
        this.groupInstanceId = groupInstanceId;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.serverAssignor = serverAssignor;
        this.commitRequestManager = commitRequestManager;
        this.backgroundEventHandler = backgroundEventHandler;
    }

    /**
     * @return Instance ID used by the member when joining the group. If non-empty, it will indicate that
     * this is a static member.
     */
    public Optional<String> groupInstanceId() {
        return groupInstanceId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onHeartbeatSuccess(ConsumerGroupHeartbeatResponse response) {
        ConsumerGroupHeartbeatResponseData responseData = response.data();
        if (responseData.errorCode() != Errors.NONE.code()) {
            String errorMessage = String.format(
                    "Unexpected error in Heartbeat response. Expected no error, but received: %s",
                    Errors.forCode(responseData.errorCode())
            );
            throw new IllegalArgumentException(errorMessage);
        }
        MemberState state = state();
        if (state == MemberState.LEAVING) {
            log.debug("Ignoring heartbeat response received from broker. Member {} with epoch {} is " +
                    "already leaving the group.", memberId, memberEpoch);
            return;
        }
        if (state == MemberState.UNSUBSCRIBED && maybeCompleteLeaveInProgress()) {
            log.debug("Member {} with epoch {} received a successful response to the heartbeat " +
                    "to leave the group and completed the leave operation. ", memberId, memberEpoch);
            return;
        }
        if (isNotInGroup()) {
            log.debug("Ignoring heartbeat response received from broker. Member {} is in {} state" +
                    " so it's not a member of the group. ", memberId, state);
            return;
        }

        updateMemberEpoch(responseData.memberEpoch());

        ConsumerGroupHeartbeatResponseData.Assignment assignment = responseData.assignment();

        if (assignment != null) {
            if (!state.canHandleNewAssignment()) {
                // New assignment received but member is in a state where it cannot take new
                // assignments (ex. preparing to leave the group)
                log.debug("Ignoring new assignment {} received from server because member is in {} state.",
                        assignment, state);
                return;
            }

            Map<Uuid, SortedSet<Integer>> newAssignment = new HashMap<>();
            assignment.topicPartitions().forEach(topicPartition ->
                newAssignment.put(topicPartition.topicId(), new TreeSet<>(topicPartition.partitions())));
            processAssignmentReceived(newAssignment);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CompletableFuture<Void> signalReconciliationStarted() {
        // Issue a commit request that will be retried until it succeeds, fails with a
        // non-retriable error, or the time limit expires. Retry on stale member epoch error, in a
        // best effort to commit the offsets in the case where the epoch might have changed while
        // the current reconciliation is in process. Note this is using the rebalance timeout as
        // it is the limit enforced by the broker to complete the reconciliation process.
        return commitRequestManager.maybeAutoCommitSyncBeforeRevocation(getDeadlineMsForTimeout(rebalanceTimeoutMs));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void signalReconciliationCompleting() {
        // Reschedule the auto commit starting from now that the member has a new assignment.
        commitRequestManager.resetAutoCommitTimer();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CompletableFuture<Void> signalMemberLeavingGroup() {
        return invokeOnPartitionsRevokedOrLostToReleaseAssignment();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CompletableFuture<Void> signalPartitionsLost(Set<TopicPartition> partitionsLost) {
        return invokeOnPartitionsLostCallback(partitionsLost);
    }

    /**
     * Release member assignment by calling the user defined callbacks for onPartitionsRevoked or
     * onPartitionsLost.
     * <ul>
     *     <li>If the member is part of the group (epoch > 0), this will invoke onPartitionsRevoked.
     *     This will be the case when releasing assignment because the member is intentionally
     *     leaving the group (after a call to unsubscribe)</li>
     *
     *     <li>If the member is not part of the group (epoch <=0), this will invoke onPartitionsLost.
     *     This will be the case when releasing assignment after being fenced .</li>
     * </ul>
     *
     * @return Future that will complete when the callback execution completes.
     */
    private CompletableFuture<Void> invokeOnPartitionsRevokedOrLostToReleaseAssignment() {
        SortedSet<TopicPartition> droppedPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        droppedPartitions.addAll(subscriptions.assignedPartitions());

        log.info("Member {} is triggering callbacks to release assignment {} and leave group",
                memberId, droppedPartitions);

        CompletableFuture<Void> callbackResult;
        if (droppedPartitions.isEmpty()) {
            // No assignment to release.
            callbackResult = CompletableFuture.completedFuture(null);
        } else {
            // Release assignment.
            if (memberEpoch > 0) {
                // Member is part of the group. Invoke onPartitionsRevoked.
                callbackResult = revokePartitions(droppedPartitions);
            } else {
                // Member is not part of the group anymore. Invoke onPartitionsLost.
                callbackResult = invokeOnPartitionsLostCallback(droppedPartitions);
            }
        }
        return callbackResult;
    }

    /**
     * @return Server-side assignor implementation configured for the member, that will be sent
     * out to the server to be used. If empty, then the server will select the assignor.
     */
    public Optional<String> serverAssignor() {
        return this.serverAssignor;
    }

    private CompletableFuture<Void> invokeOnPartitionsRevokedCallback(Set<TopicPartition> partitionsRevoked) {
        // This should not trigger the callback if partitionsRevoked is empty, to keep the
        // current behaviour.
        Optional<ConsumerRebalanceListener> listener = subscriptions.rebalanceListener();
        if (!partitionsRevoked.isEmpty() && listener.isPresent()) {
            return enqueueConsumerRebalanceListenerCallback(ON_PARTITIONS_REVOKED, partitionsRevoked);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Void> invokeOnPartitionsAssignedCallback(Set<TopicPartition> partitionsAssigned) {
        // This should always trigger the callback, even if partitionsAssigned is empty, to keep
        // the current behaviour.
        Optional<ConsumerRebalanceListener> listener = subscriptions.rebalanceListener();
        if (listener.isPresent()) {
            return enqueueConsumerRebalanceListenerCallback(ON_PARTITIONS_ASSIGNED, partitionsAssigned);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Void> invokeOnPartitionsLostCallback(Set<TopicPartition> partitionsLost) {
        // This should not trigger the callback if partitionsLost is empty, to keep the current
        // behaviour.
        Optional<ConsumerRebalanceListener> listener = subscriptions.rebalanceListener();
        if (!partitionsLost.isEmpty() && listener.isPresent()) {
            return enqueueConsumerRebalanceListenerCallback(ON_PARTITIONS_LOST, partitionsLost);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Void> signalPartitionsAssigned(Set<TopicPartition> partitionsAssigned) {
        return invokeOnPartitionsAssignedCallback(partitionsAssigned);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void signalPartitionsBeingRevoked(Set<TopicPartition> partitionsToRevoke) {
        logPausedPartitionsBeingRevoked(partitionsToRevoke);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Void> signalPartitionsRevoked(Set<TopicPartition> partitionsRevoked) {
        return invokeOnPartitionsRevokedCallback(partitionsRevoked);
    }

    /**
     * Log partitions being revoked that were already paused, since the pause flag will be
     * effectively lost.
     */
    private void logPausedPartitionsBeingRevoked(Set<TopicPartition> partitionsToRevoke) {
        Set<TopicPartition> revokePausedPartitions = subscriptions.pausedPartitions();
        revokePausedPartitions.retainAll(partitionsToRevoke);
        if (!revokePausedPartitions.isEmpty()) {
            log.info("The pause flag in partitions [{}] will be removed due to revocation.", revokePausedPartitions.stream().map(TopicPartition::toString).collect(Collectors.joining(", ")));
        }
    }

    /**
     * Enqueue a {@link ConsumerRebalanceListenerCallbackNeededEvent} to trigger the execution of the
     * appropriate {@link ConsumerRebalanceListener} {@link ConsumerRebalanceListenerMethodName method} on the
     * application thread.
     *
     * <p/>
     *
     * Because the reconciliation process (run in the background thread) will be blocked by the application thread
     * until it completes this, we need to provide a {@link CompletableFuture} by which to remember where we left off.
     *
     * @param methodName Callback method that needs to be executed on the application thread
     * @param partitions Partitions to supply to the callback method
     * @return Future that will be chained within the rest of the reconciliation logic
     */
    private CompletableFuture<Void> enqueueConsumerRebalanceListenerCallback(ConsumerRebalanceListenerMethodName methodName,
                                                                             Set<TopicPartition> partitions) {
        SortedSet<TopicPartition> sortedPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        sortedPartitions.addAll(partitions);

        CompletableBackgroundEvent<Void> event = new ConsumerRebalanceListenerCallbackNeededEvent(methodName, sortedPartitions);
        backgroundEventHandler.add(event);
        log.debug("The event to trigger the {} method execution was enqueued successfully", methodName.fullyQualifiedMethodName());
        return event.future();
    }

    /**
     * Signals that a {@link ConsumerRebalanceListener} callback has completed. This is invoked when the
     * application thread has completed the callback and has submitted a
     * {@link ConsumerRebalanceListenerCallbackCompletedEvent} to the network I/O thread. At this point, we
     * notify the state machine that it's complete so that it can move to the next appropriate step of the
     * rebalance process.
     *
     * @param event Event with details about the callback that was executed
     */
    public void consumerRebalanceListenerCallbackCompleted(ConsumerRebalanceListenerCallbackCompletedEvent event) {
        ConsumerRebalanceListenerMethodName methodName = event.methodName();
        Optional<KafkaException> error = event.error();
        CompletableFuture<Void> future = event.future();

        if (error.isPresent()) {
            Exception e = error.get();
            log.warn(
                    "The {} method completed with an error ({}); signaling to continue to the next phase of rebalance",
                    methodName.fullyQualifiedMethodName(),
                    e.getMessage()
            );

            future.completeExceptionally(e);
        } else {
            log.debug(
                    "The {} method completed successfully; signaling to continue to the next phase of rebalance",
                    methodName.fullyQualifiedMethodName()
            );

            future.complete(null);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int joinGroupEpoch() {
        return ConsumerGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int leaveGroupEpoch() {
        return groupInstanceId.isPresent() ?
                ConsumerGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH :
                ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
    }
}
