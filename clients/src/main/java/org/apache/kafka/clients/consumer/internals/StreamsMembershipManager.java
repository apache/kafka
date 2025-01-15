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
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tracks state the state of a single member in relationship to a group:
 * <p/>
 * Responsible for:
 * <ul>
 *   <li>Keeping member state</li>
 *   <li>Keeping assignment for the member</li>
 *   <li>Computing assignment for the group if the member is required to do so</li>
 *   <li>Calling the assignment and revocation callbacks on the Streams client</li>
 * </ul>
 */
public class StreamsMembershipManager implements RequestManager {

    /**
     * A data structure to represent the current task assignment, and current target task assignment of a member in a
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
            if (localEpoch != NONE_EPOCH) {
                if (activeTasks.equals(this.activeTasks) &&
                    standbyTasks.equals(this.standbyTasks) &&
                    warmupTasks.equals(this.warmupTasks)) {

                    return Optional.empty();
                }
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

    static final Utils.TopicPartitionComparator TOPIC_PARTITION_COMPARATOR = new Utils.TopicPartitionComparator();

    private final Logger log;

    private final StreamsRebalanceEventsProcessor streamsRebalanceEventsProcessor;

    private final StreamsRebalanceData streamsRebalanceData;

    private final SubscriptionState subscriptionState;

    private MemberState state;

    private final String groupId;

    private final String memberId = Uuid.randomUuid().toString();

    private final Optional<String> groupInstanceId = Optional.empty();

    private int memberEpoch = 0;

    private Optional<CompletableFuture<Void>> leaveGroupInProgress = Optional.empty();

    private CompletableFuture<Void> staleMemberAssignmentRelease;

    private boolean reconciliationInProgress;

    private boolean rejoinedWhileReconciliationInProgress;

    private final List<MemberStateListener> stateUpdatesListeners = new ArrayList<>();

    private LocalAssignment targetAssignment = LocalAssignment.NONE;

    private LocalAssignment currentAssignment = LocalAssignment.NONE;

    private final AtomicBoolean subscriptionUpdated = new AtomicBoolean(false);

    private final RebalanceMetricsManager metricsManager;

    private final Time time;

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

    public String groupId() {
        return groupId;
    }

    public String memberId() {
        return memberId;
    }

    public Optional<String> groupInstanceId() {
        return groupInstanceId;
    }

    public int memberEpoch() {
        return memberEpoch;
    }

    public MemberState state() {
        return state;
    }

    public boolean isLeavingGroup() {
        MemberState state = state();
        return state == MemberState.PREPARE_LEAVING || state == MemberState.LEAVING;
    }

    private boolean isNotInGroup() {
        MemberState state = state();
        return state == MemberState.UNSUBSCRIBED ||
            state == MemberState.FENCED ||
            state == MemberState.FATAL ||
            state == MemberState.STALE;
    }

    public void registerStateListener(MemberStateListener listener) {
        stateUpdatesListeners.add(Objects.requireNonNull(listener, "State updates listener cannot be null"));
    }

    void notifyEpochChange(Optional<Integer> epoch) {
        stateUpdatesListeners.forEach(stateListener -> stateListener.onMemberEpochUpdated(epoch, memberId));
    }

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
            // onTaskAssignment with empty assignment after sending the leave group on the STALE state.
            transitionTo(MemberState.PREPARE_LEAVING);
        }
        finalizeLeaving();
        transitionTo(MemberState.LEAVING);
    }

    private void finalizeLeaving() {
        updateMemberEpoch(StreamsGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH);
        clearCurrentTaskAssignment();
    }

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

    public void transitionToFatal() {
        MemberState previousState = state;
        transitionTo(MemberState.FATAL);
        log.error("Member {} with epoch {} transitioned to fatal state", memberId, memberEpoch);
        notifyEpochChange(Optional.empty());

        if (previousState == MemberState.UNSUBSCRIBED) {
            log.debug("Member {} with epoch {} got fatal error from the broker but it already " +
                "left the group, so onTaskAssignment callback won't be triggered.", memberId, memberEpoch);
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
                log.error("onTaskAssignment callback invocation failed while releasing assignment" +
                    "after member failed with fatal error.", error);
            }
            clearTaskAndPartitionAssignment();
        });
    }

    public void transitionToUnsubscribeIfLeaving() {
        if (state == MemberState.LEAVING) {
            log.warn("Heartbeat to leave group cannot be sent (most probably due to coordinator " +
                    "not known/available). Member {} with epoch {} will transition to {}.",
                memberId, memberEpoch, MemberState.UNSUBSCRIBED);
            transitionTo(MemberState.UNSUBSCRIBED);
            maybeCompleteLeaveInProgress();
        }
    }

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

    private void clearCurrentTaskAssignment() {
        currentAssignment = LocalAssignment.NONE;
    }

    private void clearTaskAndPartitionAssignment() {
        subscriptionState.assignFromSubscribed(Collections.emptySet());
        currentAssignment = LocalAssignment.NONE;
        targetAssignment = LocalAssignment.NONE;
    }

    public boolean shouldSkipHeartbeat() {
        return isNotInGroup();
    }

    public boolean shouldHeartbeatNow() {
        MemberState state = state();
        return state == MemberState.ACKNOWLEDGING || state == MemberState.LEAVING || state == MemberState.JOINING;
    }

    public void onSubscriptionUpdated() {
        subscriptionUpdated.compareAndSet(false, true);
    }

    public void onConsumerPoll() {
        if (subscriptionUpdated.compareAndSet(true, false) && state == MemberState.UNSUBSCRIBED) {
            transitionToJoining();
        }
    }

    public void onHeartbeatRequestGenerated() {
        MemberState state = state();
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

    public void onPollTimerExpired() {
        transitionToSendingLeaveGroup(true);
    }

    public void onFenced() {
        if (state == MemberState.PREPARE_LEAVING) {
            log.info("Member {} with epoch {} got fenced but it is already preparing to leave " +
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

    public void maybeRejoinStaleMember() {
        isPollTimerExpired = false;
        if (state == MemberState.STALE) {
            log.debug("Expired poll timer has been reset so stale member {} will rejoin the group " +
                "when it completes releasing its previous assignment.", memberId);
            staleMemberAssignmentRelease.whenComplete((__, error) -> transitionToJoining());
        }
    }

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
            return CompletableFuture.completedFuture(null);
        }

        if (state == MemberState.PREPARE_LEAVING || state == MemberState.LEAVING) {
            log.debug("Leave group operation already in progress for member {}", memberId);
            return leaveGroupInProgress.get();
        }

        transitionTo(MemberState.PREPARE_LEAVING);
        CompletableFuture<Void> onGroupLeft = new CompletableFuture<>();
        leaveGroupInProgress = Optional.of(onGroupLeft);
        if (!isOnClose) {
            CompletableFuture<Void> onAllActiveTasksReleasedCallbackExecuted = releaseActiveTasks();
            onAllActiveTasksReleasedCallbackExecuted
                .whenComplete((__, callbackError) -> leavingAfterReleasingActiveTasks(callbackError));
        } else {
            leaving();
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
            // Make sure we transition the member back to STABLE if it was RECONCILING (ex.
            // member was RECONCILING unresolved assignments that were just removed by the
            // broker), or JOINING (member joining received empty assignment).
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

    @Override
    public NetworkClientDelegate.PollResult poll(long currentTimeMs) {
        if (state == MemberState.RECONCILING) {
            maybeReconcile();
        }
        return NetworkClientDelegate.PollResult.EMPTY;
    }

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
        SortedSet<TopicPartition> assignedTopicPartitionsNotPreviouslyOwned =
            partitionsToAssignNotPreviouslyOwned(assignedTopicPartitions, ownedTopicPartitionsFromSubscriptionState);
        SortedSet<TopicPartition> partitionsToRevoke = new TreeSet<>(ownedTopicPartitionsFromSubscriptionState);
        partitionsToRevoke.removeAll(assignedTopicPartitions);

        final CompletableFuture<Void> onTasksRevokedCallbackExecuted = revokeActiveTasks(activeTasksToRevoke);

        final CompletableFuture<Void> onTasksRevokedAndAssignedCallbacksExecuted = onTasksRevokedCallbackExecuted.thenCompose(__ -> {
            if (!maybeAbortReconciliation()) {
                return assignTasks(assignedActiveTasks, ownedActiveTasks, assignedStandbyTasks, assignedWarmupTasks);
            }
            return CompletableFuture.completedFuture(null);
        });

        // The current target assignment is captured to ensure that acknowledging the current assignment is done with
        // the same target assignment that was used when this reconciliation was initiated.
        LocalAssignment currentTargetAssignment = targetAssignment;
        onTasksRevokedAndAssignedCallbacksExecuted.whenComplete((__, callbackError) -> {
            if (callbackError != null) {
                log.error("Reconciliation failed: callback invocation failed for tasks {}",
                    currentTargetAssignment, callbackError);
                markReconciliationCompleted();
            } else {
                if (reconciliationInProgress && !maybeAbortReconciliation()) {
                    subscriptionState.enablePartitionsAwaitingCallback(assignedTopicPartitionsNotPreviouslyOwned);
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

        return streamsRebalanceEventsProcessor.requestOnTasksRevokedCallbackInvocation(activeTasksToRevoke);
    }

    private CompletableFuture<Void> assignTasks(final SortedSet<StreamsRebalanceData.TaskId> activeTasksToAssign,
                                                final SortedSet<StreamsRebalanceData.TaskId> ownedActiveTasks,
                                                final SortedSet<StreamsRebalanceData.TaskId> standbyTasksToAssign,
                                                final SortedSet<StreamsRebalanceData.TaskId> warmupTasksToAssign) {
        log.info("Assigning " +
                (activeTasksToAssign.isEmpty() ? "no active tasks, " : "active tasks {}, ") +
                (standbyTasksToAssign.isEmpty() ? "no standby tasks, " : "standby tasks {}, and ") +
                (warmupTasksToAssign.isEmpty() ? "no warm-up tasks. " : "warm-up tasks {}.") +
                "to the member.",
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

        return streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
            new StreamsRebalanceData.Assignment(
                activeTasksToAssign,
                standbyTasksToAssign,
                warmupTasksToAssign
            )
        );
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
                streamsRebalanceData.subtopologies().get(subtopologyId).sourceTopics.stream(),
                streamsRebalanceData.subtopologies().get(subtopologyId).repartitionSourceTopics.keySet().stream()
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
                streamsRebalanceData.subtopologies().get(task.subtopologyId()).sourceTopics.stream(),
                streamsRebalanceData.subtopologies().get(task.subtopologyId()).repartitionSourceTopics.keySet().stream()
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
