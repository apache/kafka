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

import org.apache.kafka.clients.consumer.internals.events.StreamsOnAllTasksLostCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksAssignedCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksRevokedCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.metrics.ConsumerRebalanceMetricsManager;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceMetricsManager;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tracks the state of a single member in relationship to a group:
 * <p/>
 * Responsible for:
 * <ul>
 *   <li>Keeping member state</li>
 *   <li>Keeping assignment for the member</li>
 *   <li>Reconciling assignment, for example, if tasks need to be revoked before other tasks can be assigned</li>
 *   <li>Calling the assignment and revocation callbacks on the Streams client</li>
 * </ul>
 */
public class StreamsMembershipManager implements RequestManager {

    /**
     * A data structure to represent the current task assignment, and target task assignment of a member in a
     * streams group.
     * <p/>
     * Besides the assigned tasks, it contains a local epoch that is bumped whenever the assignment changes, to ensure
     * that two assignments with the same tasks but different local epochs are not considered equal.
     */
    private static class LocalAssignment {
        public static final long NONE_EPOCH = -1;
        public static final LocalAssignment NONE = new LocalAssignment(
            NONE_EPOCH,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        public final long localEpoch;
        public final Map<String, SortedSet<Integer>> activeTasks;
        public final Map<String, SortedSet<Integer>> standbyTasks;
        public final Map<String, SortedSet<Integer>> warmupTasks;

        public LocalAssignment(final long localEpoch,
                               final Map<String, SortedSet<Integer>> activeTasks,
                               final Map<String, SortedSet<Integer>> standbyTasks,
                               final Map<String, SortedSet<Integer>> warmupTasks) {
            this.localEpoch = localEpoch;
            this.activeTasks = activeTasks;
            this.standbyTasks = standbyTasks;
            this.warmupTasks = warmupTasks;
            if (localEpoch == NONE_EPOCH &&
                    (!activeTasks.isEmpty() || !standbyTasks.isEmpty() || !warmupTasks.isEmpty())) {
                throw new IllegalArgumentException("Local epoch must be set if tasks are assigned.");
            }
        }

        Optional<LocalAssignment> updateWith(final Map<String, SortedSet<Integer>> activeTasks,
                                             final Map<String, SortedSet<Integer>> standbyTasks,
                                             final Map<String, SortedSet<Integer>> warmupTasks) {
            if (localEpoch != NONE_EPOCH &&
                    activeTasks.equals(this.activeTasks) &&
                    standbyTasks.equals(this.standbyTasks) &&
                    warmupTasks.equals(this.warmupTasks)) {
                return Optional.empty();
            }

            long nextLocalEpoch = localEpoch + 1;
            return Optional.of(new LocalAssignment(nextLocalEpoch, activeTasks, standbyTasks, warmupTasks));
        }

        @Override
        public String toString() {
            return "LocalAssignment{" +
                "localEpoch=" + localEpoch +
                ", activeTasks=" + activeTasks +
                ", standbyTasks=" + standbyTasks +
                ", warmupTasks=" + warmupTasks +
                '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LocalAssignment that = (LocalAssignment) o;
            return localEpoch == that.localEpoch &&
                Objects.equals(activeTasks, that.activeTasks) &&
                Objects.equals(standbyTasks, that.standbyTasks) &&
                Objects.equals(warmupTasks, that.warmupTasks);
        }

        @Override
        public int hashCode() {
            return Objects.hash(localEpoch, activeTasks, standbyTasks, warmupTasks);
        }
    }

    /**
     * TopicPartition comparator based on topic name and partition.
     */
    static final Utils.TopicPartitionComparator TOPIC_PARTITION_COMPARATOR = new Utils.TopicPartitionComparator();

    private final Logger log;

    /**
     * The processor that handles events of the Streams rebalance protocol.
     * For example, requests for invocation of assignment/revocation callbacks.
     */
    private final StreamsRebalanceEventsProcessor streamsRebalanceEventsProcessor;

    /**
     * Data needed to participate in the Streams rebalance protocol.
     */
    private final StreamsRebalanceData streamsRebalanceData;

    /**
     * Subscription state object holding the current assignment the member has for the topology
     * of the Streams application.
     */
    private final SubscriptionState subscriptionState;

    /**
     * Current state of this member as part of the consumer group, as defined in {@link MemberState}.
     */
    private MemberState state;

    /**
     * Group ID of the Streams group the member will be part of, provided when creating the current
     * membership manager.
     */
    private final String groupId;

    /**
     * Member ID generated by the consumer at startup, which is unique within the group and remains consistent
     * for the entire lifetime of the process. This ID acts as an incarnation identifier for the consumer process
     * and does not reset or change, even if the consumer leaves and rejoins the group.
     * The Member ID remains the same until the process is completely stopped or terminated.
     */
    private final String memberId = Uuid.randomUuid().toString();

    /**
     * Group instance ID to be used by a static member, provided when creating the current membership manager.
     */
    private final Optional<String> groupInstanceId = Optional.empty();

    /**
     * Current epoch of the member. It will be set to 0 by the member, and provided to the server
     * on the heartbeat request, to join the group. It will be then maintained by the server,
     * incremented as the member reconciles and acknowledges the assignments it receives. It will
     * be reset to 0 if the member gets fenced.
     */
    private int memberEpoch = 0;

    /**
     * If the member is currently leaving the group after a call to {@link #leaveGroup()} or
     * {@link #leaveGroupOnClose()}, this will have a future that will complete when the ongoing leave operation
     * completes (callbacks executed and heartbeat request to leave is sent out). This will be empty if the
     * member is not leaving.
     */
    private Optional<CompletableFuture<Void>> leaveGroupInProgress = Optional.empty();

    /**
     * Future that will complete when a stale member completes releasing its assignment after
     * leaving the group due to poll timer expired. Used to make sure that the member rejoins
     * when the timer is reset, only when it completes releasing its assignment.
     */
    private CompletableFuture<Void> staleMemberAssignmentRelease;

    /**
     * If there is a reconciliation running (callbacks).
     * This will be true if {@link #maybeReconcile()} has been triggered
     * after receiving a heartbeat response, or a metadata update.
     */
    private boolean reconciliationInProgress;

    /**
     * True if a reconciliation is in progress and the member rejoined the group since the start
     * of the reconciliation. Used to know that the reconciliation in progress should be
     * interrupted and not be applied.
     */
    private boolean rejoinedWhileReconciliationInProgress;

    /**
     * Registered listeners that will be notified whenever the member epoch gets updated
     * (valid values received from the broker, or values cleared due to member leaving
     * the group, getting fenced or failing).
     */
    private final List<MemberStateListener> stateUpdatesListeners = new ArrayList<>();

    /**
     * Tasks received in the last target assignment, together with its local epoch.
     *
     * This member variable is reassigned every time a new assignment is received.
     * It is equal to LocalAssignment.NONE whenever we are not in a group.
     */
    private LocalAssignment targetAssignment = LocalAssignment.NONE;

    /**
     * Assignment that the member received from the server and successfully processed, together with
     * its local epoch.
     *
     * This is equal to LocalAssignment.NONE when we are not in a group, or we haven't reconciled any assignment yet.
     */
    private LocalAssignment currentAssignment = LocalAssignment.NONE;

    /**
     * AtomicBoolean to track whether the subscription is updated.
     * If it's true and subscription state is UNSUBSCRIBED, the next {@link #onConsumerPoll()} will change member state to JOINING.
     */
    private final AtomicBoolean subscriptionUpdated = new AtomicBoolean(false);

    /**
     * Measures successful rebalance latency and number of failed rebalances.
     */
    private final RebalanceMetricsManager metricsManager;

    private final Time time;

    /**
     * True if the poll timer has expired, signaled by a call to
     * {@link #transitionToSendingLeaveGroup(boolean)} with dueToExpiredPollTimer param true. This
     * will be used to determine that the member should transition to STALE after leaving the
     * group, to release its assignment and wait for the timer to be reset.
     */
    private boolean isPollTimerExpired;

    /**
     * Constructs the Streams membership manager.
     *
     * @param groupId                           The ID of the group.
     * @param streamsRebalanceEventsProcessor   The processor that handles Streams rebalance events like requests for
     *                                          invocation of assignment/revocation callbacks.
     * @param streamsRebalanceData              Data needed to participate in the Streams rebalance protocol.
     * @param subscriptionState                 The subscription state of the member.
     * @param logContext                        The log context.
     * @param time                              The time.
     * @param metrics                           The metrics.
     */
    public StreamsMembershipManager(final String groupId,
                                    final StreamsRebalanceEventsProcessor streamsRebalanceEventsProcessor,
                                    final StreamsRebalanceData streamsRebalanceData,
                                    final SubscriptionState subscriptionState,
                                    final LogContext logContext,
                                    final Time time,
                                    final Metrics metrics) {
        log = logContext.logger(StreamsMembershipManager.class);
        this.state = MemberState.UNSUBSCRIBED;
        this.groupId = groupId;
        this.streamsRebalanceEventsProcessor = streamsRebalanceEventsProcessor;
        this.streamsRebalanceData = streamsRebalanceData;
        this.subscriptionState = subscriptionState;
        metricsManager = new ConsumerRebalanceMetricsManager(metrics);
        this.time = time;
    }

    /**
     * @return Group ID of the group the member is part of (or wants to be part of).
     */
    public String groupId() {
        return groupId;
    }

    /**
     * @return Member ID that is generated at startup and remains unchanged for the entire lifetime of the process.
     */
    public String memberId() {
        return memberId;
    }

    /**
     * @return Instance ID used by the member when joining the group. If non-empty, it will indicate that
     * this is a static member.
     */
    public Optional<String> groupInstanceId() {
        return groupInstanceId;
    }

    /**
     * @return Current epoch of the member, maintained by the server.
     */
    public int memberEpoch() {
        return memberEpoch;
    }

    /**
     * @return Current state of this member in relationship to a group, as defined in
     * {@link MemberState}.
     */
    public MemberState state() {
        return state;
    }

    /**
     * @return True if the member is preparing to leave the group (waiting for callbacks), or
     * leaving (sending last heartbeat). This is used to skip proactively leaving the group when
     * the poll timer expires.
     */
    public boolean isLeavingGroup() {
        return state == MemberState.PREPARE_LEAVING || state == MemberState.LEAVING;
    }

    private boolean isNotInGroup() {
        return state == MemberState.UNSUBSCRIBED ||
            state == MemberState.FENCED ||
            state == MemberState.FATAL ||
            state == MemberState.STALE;
    }

    /**
     * Register a new listener that will be invoked whenever the member state changes, or a new
     * member ID or epoch is received.
     *
     * @param listener Listener to invoke.
     */
    public void registerStateListener(MemberStateListener listener) {
        Objects.requireNonNull(listener, "State updates listener cannot be null");
        for (MemberStateListener registeredListener : stateUpdatesListeners) {
            if (registeredListener == listener) {
                throw new IllegalArgumentException("Listener is already registered.");
            }
        }
        stateUpdatesListeners.add(listener);
    }

    /**
     * Call all listeners that are registered to get notified when the member epoch is updated.
     * This also includes the member ID in the notification. If the member fails or leaves
     * the group, this will be invoked with empty epoch.
     */
    private void notifyEpochChange(Optional<Integer> epoch) {
        stateUpdatesListeners.forEach(stateListener -> stateListener.onMemberEpochUpdated(epoch, memberId));
    }

    /**
     * Invokes the {@link MemberStateListener#onGroupAssignmentUpdated(java.util.Set)} callback for each listener when the
     * set of assigned partitions changes. This includes on assignment changes, unsubscribe, and when leaving
     * the group.
     */
    void notifyAssignmentChange(Set<TopicPartition> partitions) {
        stateUpdatesListeners.forEach(stateListener -> stateListener.onGroupAssignmentUpdated(partitions));
    }

    /**
     * Transition to the {@link MemberState#JOINING} state, indicating that the member will
     * try to join the group on the next heartbeat request. This is expected to be invoked when
     * the user calls the subscribe API, or when the member wants to rejoin after getting fenced.
     * Visible for testing.
     */
    private void transitionToJoining() {
        if (state == MemberState.FATAL) {
            log.warn("No action taken to join the group with the updated subscription because " +
                "the member is in FATAL state");
            return;
        }
        if (reconciliationInProgress) {
            rejoinedWhileReconciliationInProgress = true;
        }
        resetEpoch();
        transitionTo(MemberState.JOINING);
        clearCurrentTaskAssignment();
    }

    /**
     * Reset member epoch to the value required for the leave the group heartbeat request, and
     * transition to the {@link MemberState#LEAVING} state so that a heartbeat request is sent
     * out with it.
     *
     * @param dueToExpiredPollTimer True if the leave group is due to an expired poll timer. This
     *                              will indicate that the member must remain STALE after leaving,
     *                              until it releases its assignment and the timer is reset.
     */
    private void transitionToSendingLeaveGroup(boolean dueToExpiredPollTimer) {
        if (state == MemberState.FATAL) {
            log.warn("Member {} with epoch {} won't send leave group request because it is in " +
                "FATAL state", memberId, memberEpoch);
            return;
        }
        if (state == MemberState.UNSUBSCRIBED) {
            log.warn("Member {} won't send leave group request because it is already out of the group.",
                memberId);
            return;
        }

        if (dueToExpiredPollTimer) {
            isPollTimerExpired = true;
            // Briefly transition through prepare leaving. The member does not have to release
            // any assignment before sending the leave group given that is stale. It will invoke
            // onAllTasksLost after sending the leave group on the STALE state.
            transitionTo(MemberState.PREPARE_LEAVING);
        }
        finalizeLeaving();
        transitionTo(MemberState.LEAVING);
    }

    private void finalizeLeaving() {
        updateMemberEpoch(StreamsGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH);
        clearCurrentTaskAssignment();
    }

    /**
     * Transition to STALE to release assignments because the member has left the group due to
     * expired poll timer. This will trigger the onAllTasksLost callback. Once the callback
     * completes, the member will remain stale until the poll timer is reset by an application
     * poll event. See {@link #maybeRejoinStaleMember()}.
     */
    private void transitionToStale() {
        transitionTo(MemberState.STALE);

        final CompletableFuture<Void> onAllTasksLostCallbackExecution =
            streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation();
        staleMemberAssignmentRelease = onAllTasksLostCallbackExecution.whenComplete((result, error) -> {
            if (error != null) {
                log.error("Task revocation callback invocation failed " +
                    "after member left group due to expired poll timer.", error);
            }
            clearTaskAndPartitionAssignment();
            log.debug("Member {} sent leave group heartbeat and released its assignment. It will remain " +
                    "in {} state until the poll timer is reset, and it will then rejoin the group",
                memberId, MemberState.STALE);
        });
    }

    /**
     * Transition the member to the FATAL state and update the member info as required. This is
     * invoked when un-recoverable errors occur (ex. when the heartbeat returns a non-retriable
     * error)
     */
    public void transitionToFatal() {
        MemberState previousState = state;
        transitionTo(MemberState.FATAL);
        log.error("Member {} with epoch {} transitioned to fatal state", memberId, memberEpoch);
        notifyEpochChange(Optional.empty());

        if (previousState == MemberState.UNSUBSCRIBED) {
            log.debug("Member {} with epoch {} got fatal error from the broker but it already " +
                "left the group, so onAllTasksLost callback won't be triggered.", memberId, memberEpoch);
            return;
        }

        if (previousState == MemberState.LEAVING || previousState == MemberState.PREPARE_LEAVING) {
            log.info("Member {} with epoch {} was leaving the group with state {} when it got a " +
                "fatal error from the broker. It will discard the ongoing leave and remain in " +
                "fatal state.", memberId, memberEpoch, previousState);
            maybeCompleteLeaveInProgress();
            return;
        }

        CompletableFuture<Void> onAllTasksLostCallbackExecuted = streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation();
        onAllTasksLostCallbackExecuted.whenComplete((result, error) -> {
            if (error != null) {
                log.error("onAllTasksLost callback invocation failed while releasing assignment " +
                    "after member failed with fatal error.", error);
            }
            clearTaskAndPartitionAssignment();
        });
    }

    /**
     * Notify when the heartbeat request is skipped.
     * Transition out of the {@link MemberState#LEAVING} state even if the heartbeat was not sent.
     * This will ensure that the member is not blocked on {@link MemberState#LEAVING} (best
     * effort to send the request, without any response handling or retry logic)
     */
    public void onHeartbeatRequestSkipped() {
        if (state == MemberState.LEAVING) {
            log.warn("Heartbeat to leave group cannot be sent (most probably due to coordinator " +
                    "not known/available). Member {} with epoch {} will transition to {}.",
                memberId, memberEpoch, MemberState.UNSUBSCRIBED);
            transitionTo(MemberState.UNSUBSCRIBED);
            maybeCompleteLeaveInProgress();
        }
    }

    /**
     * Update the member state, setting it to the nextState only if it is a valid transition.
     *
     * @throws IllegalStateException If transitioning from the member {@link #state} to the
     *                               nextState is not allowed as defined in {@link MemberState}.
     */
    private void transitionTo(MemberState nextState) {
        if (!state.equals(nextState) && !nextState.getPreviousValidStates().contains(state)) {
            throw new IllegalStateException(String.format("Invalid state transition from %s to %s",
                state, nextState));
        }

        if (isCompletingRebalance(state, nextState)) {
            metricsManager.recordRebalanceEnded(time.milliseconds());
        }
        if (isStartingRebalance(state, nextState)) {
            metricsManager.recordRebalanceStarted(time.milliseconds());
        }

        log.info("Member {} with epoch {} transitioned from {} to {}.", memberId, memberEpoch, state, nextState);
        this.state = nextState;
    }

    private static boolean isCompletingRebalance(MemberState currentState, MemberState nextState) {
        return currentState == MemberState.RECONCILING &&
            (nextState == MemberState.STABLE || nextState == MemberState.ACKNOWLEDGING);
    }

    private static boolean isStartingRebalance(MemberState currentState, MemberState nextState) {
        return currentState != MemberState.RECONCILING && nextState == MemberState.RECONCILING;
    }

    private void resetEpoch() {
        updateMemberEpoch(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH);
    }

    private void updateMemberEpoch(int newEpoch) {
        boolean newEpochReceived = this.memberEpoch != newEpoch;
        this.memberEpoch = newEpoch;
        if (newEpochReceived) {
            if (memberEpoch > 0) {
                notifyEpochChange(Optional.of(memberEpoch));
            } else {
                notifyEpochChange(Optional.empty());
            }
        }
    }

    /**
     * Discard assignments received that have not been reconciled yet (waiting for metadata
     * or the next reconciliation loop).
     */
    private void clearCurrentTaskAssignment() {
        currentAssignment = LocalAssignment.NONE;
    }

    /**
     * Clear the assigned partitions in the member subscription, pending assignments and metadata cache.
     */
    private void clearTaskAndPartitionAssignment() {
        subscriptionState.assignFromSubscribed(Collections.emptySet());
        notifyAssignmentChange(Collections.emptySet());
        currentAssignment = LocalAssignment.NONE;
        targetAssignment = LocalAssignment.NONE;
    }

    /**
     * @return True if the member should not send heartbeats, which is the case when it is in a
     * state where it is not an active member of the group.
     */
    public boolean shouldSkipHeartbeat() {
        return isNotInGroup();
    }

    /**
     * @return True if the member should send heartbeat to the coordinator without waiting for
     * the interval.
     */
    public boolean shouldHeartbeatNow() {
        return state == MemberState.ACKNOWLEDGING || state == MemberState.LEAVING || state == MemberState.JOINING;
    }

    /**
     * Set {@link #subscriptionUpdated} to true to indicate that the subscription has been updated.
     * The next {@link #onConsumerPoll()} will join the group with the updated subscription, if the member is not part of it yet.
     * If the member is already part of the group, this will only ensure that the updated subscription
     * is included in the next heartbeat request.
     * <p/>
     * Note that the list of topics in the subscription is taken from the shared subscription state.
     */
    public void onSubscriptionUpdated() {
        subscriptionUpdated.compareAndSet(false, true);
    }

    /**
     * Join the group if the member is not part of it yet. This function separates {@link #transitionToJoining}
     * from the {@link #onSubscriptionUpdated} to fulfill the requirement of the "rebalances will only occur during an
     * active call to {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(java.time.Duration)}"
     */
    public void onConsumerPoll() {
        if (subscriptionUpdated.compareAndSet(true, false) && state == MemberState.UNSUBSCRIBED) {
            transitionToJoining();
        }
    }

    /**
     * Update state when a heartbeat is generated. This will transition out of the states that end
     * when a heartbeat request is sent, without waiting for a response (ex.
     * {@link MemberState#ACKNOWLEDGING} and {@link MemberState#LEAVING}).
     */
    public void onHeartbeatRequestGenerated() {
        if (state == MemberState.ACKNOWLEDGING) {
            if (targetAssignmentReconciled()) {
                transitionTo(MemberState.STABLE);
            } else {
                log.debug("Member {} with epoch {} transitioned to {} after a heartbeat was sent " +
                    "to ack a previous reconciliation. New assignments are ready to " +
                    "be reconciled.", memberId, memberEpoch, MemberState.RECONCILING);
                transitionTo(MemberState.RECONCILING);
            }
        } else if (state == MemberState.LEAVING) {
            if (isPollTimerExpired) {
                log.debug("Member {} with epoch {} generated the heartbeat to leave due to expired poll timer. It will " +
                    "remain stale (no heartbeat) until it rejoins the group on the next consumer " +
                    "poll.", memberId, memberEpoch);
                transitionToStale();
            } else {
                log.debug("Member {} with epoch {} generated the heartbeat to leave the group.", memberId, memberEpoch);
                transitionTo(MemberState.UNSUBSCRIBED);
            }
        }
    }

    /**
     * Notify about a successful heartbeat response.
     *
     * @param response Heartbeat response to extract member info and errors from.
     */
    public void onHeartbeatSuccess(StreamsGroupHeartbeatResponse response) {
        StreamsGroupHeartbeatResponseData responseData = response.data();
        throwIfUnexpectedError(responseData);
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

        final List<StreamsGroupHeartbeatResponseData.TaskIds> activeTasks = responseData.activeTasks();
        final List<StreamsGroupHeartbeatResponseData.TaskIds> standbyTasks = responseData.standbyTasks();
        final List<StreamsGroupHeartbeatResponseData.TaskIds> warmupTasks = responseData.warmupTasks();

        if (activeTasks != null && standbyTasks != null && warmupTasks != null) {

            if (!state.canHandleNewAssignment()) {
                log.debug("Ignoring new assignment: active tasks {}, standby tasks {}, and warm-up tasks {} received " +
                        "from server because member is in {} state.",
                    activeTasks, standbyTasks, warmupTasks, state);
                return;
            }

            processAssignmentReceived(
                toTasksAssignment(activeTasks),
                toTasksAssignment(standbyTasks),
                toTasksAssignment(warmupTasks)
            );
        } else {
            if (responseData.activeTasks() != null ||
                responseData.standbyTasks() != null ||
                responseData.warmupTasks() != null) {

                throw new IllegalStateException("Invalid response data, task collections must be all null or all non-null: "
                    + responseData);
            }
        }
    }

    /**
     * Notify the member that an error heartbeat response was received.
     *
     * @param retriable True if the request failed with a retriable error.
     */
    public void onHeartbeatFailure(boolean retriable) {
        if (!retriable) {
            metricsManager.maybeRecordRebalanceFailed();
        }
        // The leave group request is sent out once (not retried), so we should complete the leave
        // operation once the request completes, regardless of the response.
        if (state == MemberState.UNSUBSCRIBED && maybeCompleteLeaveInProgress()) {
            log.warn("Member {} with epoch {} received a failed response to the heartbeat to " +
                "leave the group and completed the leave operation. ", memberId, memberEpoch);
        }
    }

    /**
     * Notify when the poll timer expired.
     */
    public void onPollTimerExpired() {
        transitionToSendingLeaveGroup(true);
    }

    /**
     * Notify when member is fenced.
     */
    public void onFenced() {
        if (state == MemberState.PREPARE_LEAVING) {
            log.debug("Member {} with epoch {} got fenced but it is already preparing to leave " +
                "the group, so it will stop sending heartbeat and won't attempt to send the " +
                "leave request or rejoin.", memberId, memberEpoch);
            finalizeLeaving();
            transitionTo(MemberState.UNSUBSCRIBED);
            maybeCompleteLeaveInProgress();
            return;
        }

        if (state == MemberState.LEAVING) {
            log.debug("Member {} with epoch {} got fenced before sending leave group heartbeat. " +
                "It will not send the leave request and won't attempt to rejoin.", memberId, memberEpoch);
            transitionTo(MemberState.UNSUBSCRIBED);
            maybeCompleteLeaveInProgress();
            return;
        }
        if (state == MemberState.UNSUBSCRIBED) {
            log.debug("Member {} with epoch {} got fenced but it already left the group, so it " +
                "won't attempt to rejoin.", memberId, memberEpoch);
            return;
        }
        transitionTo(MemberState.FENCED);
        resetEpoch();
        log.debug("Member {} with epoch {} transitioned to {} state. It will release its " +
            "assignment and rejoin the group.", memberId, memberEpoch, MemberState.FENCED);

        CompletableFuture<Void> callbackResult = streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation();
        callbackResult.whenComplete((result, error) -> {
            if (error != null) {
                log.error("onAllTasksLost callback invocation failed while releasing assignment" +
                    " after member got fenced. Member will rejoin the group anyways.", error);
            }
            clearTaskAndPartitionAssignment();
            if (state == MemberState.FENCED) {
                transitionToJoining();
            } else {
                log.debug("Fenced member onAllTasksLost callback completed but the state has " +
                    "already changed to {}, so the member won't rejoin the group", state);
            }
        });
    }

    private void throwIfUnexpectedError(StreamsGroupHeartbeatResponseData responseData) {
        if (responseData.errorCode() != Errors.NONE.code()) {
            String errorMessage = String.format(
                "Unexpected error in Heartbeat response. Expected no error, but received: %s with message: '%s'",
                Errors.forCode(responseData.errorCode()), responseData.errorMessage()
            );
            throw new IllegalArgumentException(errorMessage);
        }
    }

    /**
     * Transition a {@link MemberState#STALE} member to {@link MemberState#JOINING} when it completes
     * releasing its assignment. This is expected to be used when the poll timer is reset.
     */
    public void maybeRejoinStaleMember() {
        isPollTimerExpired = false;
        if (state == MemberState.STALE) {
            log.debug("Expired poll timer has been reset so stale member {} will rejoin the group " +
                "when it completes releasing its previous assignment.", memberId);
            staleMemberAssignmentRelease.whenComplete((__, error) -> transitionToJoining());
        }
    }

    /**
     * Complete the leave in progress (if any). This is expected to be used to complete the leave
     * in progress when a member receives the response to the leave heartbeat.
     */
    private boolean maybeCompleteLeaveInProgress() {
        if (leaveGroupInProgress.isPresent()) {
            leaveGroupInProgress.get().complete(null);
            leaveGroupInProgress = Optional.empty();
            return true;
        }
        return false;
    }

    private static SortedSet<StreamsRebalanceData.TaskId> toTaskIdSet(final Map<String, SortedSet<Integer>> tasks) {
        SortedSet<StreamsRebalanceData.TaskId> taskIdSet = new TreeSet<>();
        for (final Map.Entry<String, SortedSet<Integer>> task : tasks.entrySet()) {
            final String subtopologyId = task.getKey();
            final SortedSet<Integer> partitions = task.getValue();
            for (final int partition : partitions) {
                taskIdSet.add(new StreamsRebalanceData.TaskId(subtopologyId, partition));
            }
        }
        return taskIdSet;
    }

    private static Map<String, SortedSet<Integer>> toTasksAssignment(final List<StreamsGroupHeartbeatResponseData.TaskIds> taskIds) {
        return taskIds.stream()
            .collect(Collectors.toMap(StreamsGroupHeartbeatResponseData.TaskIds::subtopologyId, taskId -> new TreeSet<>(taskId.partitions())));
    }

    /**
     * Leaves the group when the member closes.
     *
     * <p>
     * This method does the following:
     * <ol>
     *     <li>Transitions member state to {@link MemberState#PREPARE_LEAVING}.</li>
     *     <li>Skips the invocation of the revocation callback or lost callback.</li>
     *     <li>Clears the current and target assignment, unsubscribes from all topics and
     *     transitions the member state to {@link MemberState#LEAVING}.</li>
     * </ol>
     * States {@link MemberState#PREPARE_LEAVING} and {@link MemberState#LEAVING} cause the heartbeat request manager
     * to send a leave group heartbeat.
     * </p>
     *
     * @return future that will complete when the heartbeat to leave the group has been sent out.
     */
    public CompletableFuture<Void> leaveGroupOnClose() {
        return leaveGroup(true);
    }

    /**
     * Leaves the group.
     *
     * <p>
     * This method does the following:
     * <ol>
     *     <li>Transitions member state to {@link MemberState#PREPARE_LEAVING}.</li>
     *     <li>Requests the invocation of the revocation callback or lost callback.</li>
     *     <li>Once the callback completes, it clears the current and target assignment, unsubscribes from
     *     all topics and transitions the member state to {@link MemberState#LEAVING}.</li>
     * </ol>
     * States {@link MemberState#PREPARE_LEAVING} and {@link MemberState#LEAVING} cause the heartbeat request manager
     * to send a leave group heartbeat.
     * </p>
     *
     * @return future that will complete when the revocation callback execution completes and the heartbeat
     *         to leave the group has been sent out.
     */
    public CompletableFuture<Void> leaveGroup() {
        return leaveGroup(false);
    }

    private CompletableFuture<Void> leaveGroup(final boolean isOnClose) {
        if (isNotInGroup()) {
            if (state == MemberState.FENCED) {
                clearTaskAndPartitionAssignment();
                transitionTo(MemberState.UNSUBSCRIBED);
            }
            subscriptionState.unsubscribe();
            notifyAssignmentChange(Collections.emptySet());
            return CompletableFuture.completedFuture(null);
        }

        if (state == MemberState.PREPARE_LEAVING || state == MemberState.LEAVING) {
            log.debug("Leave group operation already in progress for member {}", memberId);
            return leaveGroupInProgress.get();
        }

        transitionTo(MemberState.PREPARE_LEAVING);
        CompletableFuture<Void> onGroupLeft = new CompletableFuture<>();
        leaveGroupInProgress = Optional.of(onGroupLeft);
        if (isOnClose) {
            leaving();
        } else {
            CompletableFuture<Void> onAllActiveTasksReleasedCallbackExecuted = releaseActiveTasks();
            onAllActiveTasksReleasedCallbackExecuted
                .whenComplete((__, callbackError) -> leavingAfterReleasingActiveTasks(callbackError));
        }

        return onGroupLeft;
    }

    private CompletableFuture<Void> releaseActiveTasks() {
        if (memberEpoch > 0) {
            return revokeActiveTasks(toTaskIdSet(currentAssignment.activeTasks));
        } else {
            return releaseLostActiveTasks();
        }
    }

    private void leavingAfterReleasingActiveTasks(Throwable callbackError) {
        if (callbackError != null) {
            log.error("Member {} callback to revoke task assignment failed. It will proceed " +
                    "to clear its assignment and send a leave group heartbeat",
                memberId, callbackError);
        } else {
            log.info("Member {} completed callback to revoke task assignment. It will proceed " +
                    "to clear its assignment and send a leave group heartbeat",
                memberId);
        }
        leaving();
    }

    private void leaving() {
        clearTaskAndPartitionAssignment();
        subscriptionState.unsubscribe();
        transitionToSendingLeaveGroup(false);
    }

    /**
     * This will process the assignment received if it is different from the member's current
     * assignment. If a new assignment is received, this will make sure reconciliation is attempted
     * on the next call of `poll`. If another reconciliation is currently in process, the first `poll`
     * after that reconciliation will trigger the new reconciliation.
     *
     * @param activeTasks Target active tasks assignment received from the broker.
     * @param standbyTasks Target standby tasks assignment received from the broker.
     * @param warmupTasks Target warm-up tasks assignment received from the broker.
     */
    private void processAssignmentReceived(Map<String, SortedSet<Integer>> activeTasks,
                                           Map<String, SortedSet<Integer>> standbyTasks,
                                           Map<String, SortedSet<Integer>> warmupTasks) {
        replaceTargetAssignmentWithNewAssignment(activeTasks, standbyTasks, warmupTasks);
        if (!targetAssignmentReconciled()) {
            transitionTo(MemberState.RECONCILING);
        } else {
            log.debug("Target assignment {} received from the broker is equals to the member " +
                    "current assignment {}. Nothing to reconcile.",
                targetAssignment, currentAssignment);
            if (state == MemberState.RECONCILING || state == MemberState.JOINING) {
                transitionTo(MemberState.STABLE);
            }
        }
    }

    private boolean targetAssignmentReconciled() {
        return currentAssignment.equals(targetAssignment);
    }

    private void replaceTargetAssignmentWithNewAssignment(Map<String, SortedSet<Integer>> activeTasks,
                                                          Map<String, SortedSet<Integer>> standbyTasks,
                                                          Map<String, SortedSet<Integer>> warmupTasks) {
        targetAssignment.updateWith(activeTasks, standbyTasks, warmupTasks)
            .ifPresent(updatedAssignment -> {
                log.debug("Target assignment updated from {} to {}. Member will reconcile it on the next poll.",
                    targetAssignment, updatedAssignment);
                targetAssignment = updatedAssignment;
            });
    }

    /**
     * Called by the network thread to reconcile the current and target assignment.
     */
    @Override
    public NetworkClientDelegate.PollResult poll(long currentTimeMs) {
        if (state == MemberState.RECONCILING) {
            maybeReconcile();
        }
        return NetworkClientDelegate.PollResult.EMPTY;
    }

    /**
     * Reconcile the assignment that has been received from the server. Reconciliation will trigger the
     * callbacks and update the subscription state.
     *
     * There are two conditions under which no reconciliation will be triggered:
     *  - We have already reconciled the assignment (the target assignment is the same as the current assignment).
     *  - Another reconciliation is already in progress.
     */
    private void maybeReconcile() {
        if (targetAssignmentReconciled()) {
            log.trace("Ignoring reconciliation attempt. Target assignment is equal to the " +
                "current assignment.");
            return;
        }
        if (reconciliationInProgress) {
            log.trace("Ignoring reconciliation attempt. Another reconciliation is already in progress. Assignment " +
                targetAssignment + " will be handled in the next reconciliation loop.");
            return;
        }

        markReconciliationInProgress();

        SortedSet<StreamsRebalanceData.TaskId> assignedActiveTasks = toTaskIdSet(targetAssignment.activeTasks);
        SortedSet<StreamsRebalanceData.TaskId> ownedActiveTasks = toTaskIdSet(currentAssignment.activeTasks);
        SortedSet<StreamsRebalanceData.TaskId> activeTasksToRevoke = new TreeSet<>(ownedActiveTasks);
        activeTasksToRevoke.removeAll(assignedActiveTasks);
        SortedSet<StreamsRebalanceData.TaskId> assignedStandbyTasks = toTaskIdSet(targetAssignment.standbyTasks);
        SortedSet<StreamsRebalanceData.TaskId> ownedStandbyTasks = toTaskIdSet(currentAssignment.standbyTasks);
        SortedSet<StreamsRebalanceData.TaskId> assignedWarmupTasks = toTaskIdSet(targetAssignment.warmupTasks);
        SortedSet<StreamsRebalanceData.TaskId> ownedWarmupTasks = toTaskIdSet(currentAssignment.warmupTasks);

        log.info("Assigned tasks with local epoch {}\n" +
                "\tMember:                        {}\n" +
                "\tAssigned active tasks:         {}\n" +
                "\tOwned active tasks:            {}\n" +
                "\tActive tasks to revoke:        {}\n" +
                "\tAssigned standby tasks:        {}\n" +
                "\tOwned standby tasks:           {}\n" +
                "\tAssigned warm-up tasks:        {}\n" +
                "\tOwned warm-up tasks:           {}\n",
            targetAssignment.localEpoch,
            memberId,
            assignedActiveTasks,
            ownedActiveTasks,
            activeTasksToRevoke,
            assignedStandbyTasks,
            ownedStandbyTasks,
            assignedWarmupTasks,
            ownedWarmupTasks
        );

        SortedSet<TopicPartition> ownedTopicPartitionsFromSubscriptionState = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        ownedTopicPartitionsFromSubscriptionState.addAll(subscriptionState.assignedPartitions());
        SortedSet<TopicPartition> ownedTopicPartitionsFromAssignedTasks =
            topicPartitionsForActiveTasks(currentAssignment.activeTasks);
        if (!ownedTopicPartitionsFromAssignedTasks.equals(ownedTopicPartitionsFromSubscriptionState)) {
            throw new IllegalStateException("Owned partitions from subscription state and owned partitions from " +
                "assigned active tasks are not equal. " +
                "Owned partitions from subscription state: " + ownedTopicPartitionsFromSubscriptionState + ", " +
                "Owned partitions from assigned active tasks: " + ownedTopicPartitionsFromAssignedTasks);
        }
        SortedSet<TopicPartition> assignedTopicPartitions = topicPartitionsForActiveTasks(targetAssignment.activeTasks);
        SortedSet<TopicPartition> partitionsToRevoke = new TreeSet<>(ownedTopicPartitionsFromSubscriptionState);
        partitionsToRevoke.removeAll(assignedTopicPartitions);

        final CompletableFuture<Void> tasksRevoked = revokeActiveTasks(activeTasksToRevoke);

        final CompletableFuture<Void> tasksRevokedAndAssigned = tasksRevoked.thenCompose(__ -> {
            if (!maybeAbortReconciliation()) {
                return assignTasks(assignedActiveTasks, ownedActiveTasks, assignedStandbyTasks, assignedWarmupTasks);
            }
            return CompletableFuture.completedFuture(null);
        });

        // The current target assignment is captured to ensure that acknowledging the current assignment is done with
        // the same target assignment that was used when this reconciliation was initiated.
        LocalAssignment currentTargetAssignment = targetAssignment;
        tasksRevokedAndAssigned.whenComplete((__, callbackError) -> {
            if (callbackError != null) {
                log.error("Reconciliation failed: callback invocation failed for tasks {}",
                    currentTargetAssignment, callbackError);
                markReconciliationCompleted();
            } else {
                if (reconciliationInProgress && !maybeAbortReconciliation()) {
                    currentAssignment = currentTargetAssignment;
                    transitionTo(MemberState.ACKNOWLEDGING);
                    markReconciliationCompleted();
                }
            }
        });
    }

    private CompletableFuture<Void> revokeActiveTasks(final SortedSet<StreamsRebalanceData.TaskId> activeTasksToRevoke) {
        if (activeTasksToRevoke.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        log.info("Revoking previously assigned active tasks {}", activeTasksToRevoke.stream()
            .map(StreamsRebalanceData.TaskId::toString)
            .collect(Collectors.joining(", ")));

        final SortedSet<TopicPartition> partitionsToRevoke = topicPartitionsForActiveTasks(activeTasksToRevoke);
        log.debug("Marking partitions pending for revocation: {}", partitionsToRevoke);
        subscriptionState.markPendingRevocation(partitionsToRevoke);

        CompletableFuture<Void> tasksRevoked = new CompletableFuture<>();
        CompletableFuture<Void> onTasksRevokedCallbackExecuted =
            streamsRebalanceEventsProcessor.requestOnTasksRevokedCallbackInvocation(activeTasksToRevoke);
        onTasksRevokedCallbackExecuted.whenComplete((__, callbackError) -> {
            if (callbackError != null) {
                log.error("onTasksRevoked callback invocation failed for tasks {}",
                    activeTasksToRevoke, callbackError);
                tasksRevoked.completeExceptionally(callbackError);
            } else {
                tasksRevoked.complete(null);
            }
        });
        return tasksRevoked;
    }

    private CompletableFuture<Void> assignTasks(final SortedSet<StreamsRebalanceData.TaskId> activeTasksToAssign,
                                                final SortedSet<StreamsRebalanceData.TaskId> ownedActiveTasks,
                                                final SortedSet<StreamsRebalanceData.TaskId> standbyTasksToAssign,
                                                final SortedSet<StreamsRebalanceData.TaskId> warmupTasksToAssign) {
        log.info("Assigning active tasks {{}}, standby tasks {{}}, and warm-up tasks {{}} to the member.",
            activeTasksToAssign.stream()
                .map(StreamsRebalanceData.TaskId::toString)
                .collect(Collectors.joining(", ")),
            standbyTasksToAssign.stream()
                .map(StreamsRebalanceData.TaskId::toString)
                .collect(Collectors.joining(", ")),
            warmupTasksToAssign.stream()
                .map(StreamsRebalanceData.TaskId::toString)
                .collect(Collectors.joining(", "))
        );

        final SortedSet<TopicPartition> partitionsToAssign = topicPartitionsForActiveTasks(activeTasksToAssign);
        final SortedSet<TopicPartition> partitionsToAssigneNotPreviouslyOwned =
            partitionsToAssignNotPreviouslyOwned(partitionsToAssign, topicPartitionsForActiveTasks(ownedActiveTasks));

        subscriptionState.assignFromSubscribedAwaitingCallback(
            partitionsToAssign,
            partitionsToAssigneNotPreviouslyOwned
        );
        notifyAssignmentChange(partitionsToAssign);

        CompletableFuture<Void> onTasksAssignedCallbackExecuted =
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                new StreamsRebalanceData.Assignment(
                    activeTasksToAssign,
                    standbyTasksToAssign,
                    warmupTasksToAssign
                )
            );
        onTasksAssignedCallbackExecuted.whenComplete((__, callbackError) -> {
            if (callbackError == null) {
                subscriptionState.enablePartitionsAwaitingCallback(partitionsToAssign);
            } else {
                if (!partitionsToAssigneNotPreviouslyOwned.isEmpty()) {
                    log.warn("Leaving newly assigned partitions {} marked as non-fetchable and not " +
                            "requiring initializing positions after onTasksAssigned callback failed.",
                        partitionsToAssigneNotPreviouslyOwned, callbackError);
                }
            }
        });

        return onTasksAssignedCallbackExecuted;
    }

    private CompletableFuture<Void> releaseLostActiveTasks() {
        final SortedSet<StreamsRebalanceData.TaskId> activeTasksToRelease = toTaskIdSet(currentAssignment.activeTasks);
        log.info("Revoking previously assigned and now lost active tasks {}", activeTasksToRelease.stream()
            .map(StreamsRebalanceData.TaskId::toString)
            .collect(Collectors.joining(", ")));

        final SortedSet<TopicPartition> partitionsToRelease = topicPartitionsForActiveTasks(activeTasksToRelease);
        log.debug("Marking lost partitions pending for revocation: {}", partitionsToRelease);
        subscriptionState.markPendingRevocation(partitionsToRelease);

        return streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation();
    }

    private SortedSet<TopicPartition> partitionsToAssignNotPreviouslyOwned(final SortedSet<TopicPartition> assignedTopicPartitions,
                                                                           final SortedSet<TopicPartition> ownedTopicPartitions) {
        SortedSet<TopicPartition> assignedPartitionsNotPreviouslyOwned = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        assignedPartitionsNotPreviouslyOwned.addAll(assignedTopicPartitions);
        assignedPartitionsNotPreviouslyOwned.removeAll(ownedTopicPartitions);
        return assignedPartitionsNotPreviouslyOwned;
    }

    private SortedSet<TopicPartition> topicPartitionsForActiveTasks(final Map<String, SortedSet<Integer>> activeTasks) {
        final SortedSet<TopicPartition> topicPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        activeTasks.forEach((subtopologyId, partitionIds) ->
            Stream.concat(
                streamsRebalanceData.subtopologies().get(subtopologyId).sourceTopics().stream(),
                streamsRebalanceData.subtopologies().get(subtopologyId).repartitionSourceTopics().keySet().stream()
            ).forEach(topic -> {
                for (final int partitionId : partitionIds) {
                    topicPartitions.add(new TopicPartition(topic, partitionId));
                }
            })
        );
        return topicPartitions;
    }

    private SortedSet<TopicPartition> topicPartitionsForActiveTasks(final SortedSet<StreamsRebalanceData.TaskId> activeTasks) {
        final SortedSet<TopicPartition> topicPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        activeTasks.forEach(task ->
            Stream.concat(
                streamsRebalanceData.subtopologies().get(task.subtopologyId()).sourceTopics().stream(),
                streamsRebalanceData.subtopologies().get(task.subtopologyId()).repartitionSourceTopics().keySet().stream()
            ).forEach(topic -> {
                topicPartitions.add(new TopicPartition(topic, task.partitionId()));
            })
        );
        return topicPartitions;
    }

    private void markReconciliationCompleted() {
        reconciliationInProgress = false;
        rejoinedWhileReconciliationInProgress = false;
    }

    private boolean maybeAbortReconciliation() {
        boolean shouldAbort = state != MemberState.RECONCILING || rejoinedWhileReconciliationInProgress;
        if (shouldAbort) {
            String reason = rejoinedWhileReconciliationInProgress ?
                "the member has re-joined the group" :
                "the member already transitioned out of the reconciling state into " + state;
            log.info("Interrupting reconciliation that is not relevant anymore because " + reason);
            markReconciliationCompleted();
        }
        return shouldAbort;
    }

    private void markReconciliationInProgress() {
        reconciliationInProgress = true;
        rejoinedWhileReconciliationInProgress = false;
    }

    /**
     * Completes the future that marks the completed execution of the onTasksRevoked callback.

     * @param event The event containing the future sent from the application thread to the network thread to
     *              confirm the execution of the callback.
     */
    public void onTasksRevokedCallbackCompleted(final StreamsOnTasksRevokedCallbackCompletedEvent event) {
        Optional<KafkaException> error = event.error();
        CompletableFuture<Void> future = event.future();

        if (error.isPresent()) {
            Exception e = error.get();
            log.warn("The onTasksRevoked callback completed with an error ({}); " +
                "signaling to continue to the next phase of rebalance", e.getMessage());
            future.completeExceptionally(e);
        } else {
            log.debug("The onTasksRevoked callback completed successfully; signaling to continue to the next phase of rebalance");
            future.complete(null);
        }
    }

    /**
     * Completes the future that marks the completed execution of the onTasksAssigned callback.

     * @param event The event containing the future sent from the application thread to the network thread to
     *              confirm the execution of the callback.
     */
    public void onTasksAssignedCallbackCompleted(final StreamsOnTasksAssignedCallbackCompletedEvent event) {
        Optional<KafkaException> error = event.error();
        CompletableFuture<Void> future = event.future();

        if (error.isPresent()) {
            Exception e = error.get();
            log.warn("The onTasksAssigned callback completed with an error ({}); " +
                "signaling to continue to the next phase of rebalance", e.getMessage());
            future.completeExceptionally(e);
        } else {
            log.debug("The onTasksAssigned callback completed successfully; signaling to continue to the next phase of rebalance");
            future.complete(null);
        }
    }

    /**
     * Completes the future that marks the completed execution of the onAllTasksLost callback.

     * @param event The event containing the future sent from the application thread to the network thread to
     *              confirm the execution of the callback.
     */
    public void onAllTasksLostCallbackCompleted(final StreamsOnAllTasksLostCallbackCompletedEvent event) {
        Optional<KafkaException> error = event.error();
        CompletableFuture<Void> future = event.future();

        if (error.isPresent()) {
            Exception e = error.get();
            log.warn("The onAllTasksLost callback completed with an error ({}); " +
                "signaling to continue to the next phase of rebalance", e.getMessage());
            future.completeExceptionally(e);
        } else {
            log.debug("The onAllTasksLost callback completed successfully; signaling to continue to the next phase of rebalance");
            future.complete(null);
        }
    }
}
