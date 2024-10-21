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

import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnAssignmentCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnAssignmentCallbackNeededEvent;
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
import org.apache.kafka.common.telemetry.internals.ClientTelemetryProvider;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
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

public class StreamsMembershipManager implements RequestManager {

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

    private final StreamsAssignmentInterface streamsAssignmentInterface;

    private final SubscriptionState subscriptionState;

    private final ConsumerMetadata consumerMetadata;

    private final BackgroundEventHandler backgroundEventHandler;

    private final Map<String, Uuid> assignedTopicIdCache = new HashMap<>();

    private MemberState state;

    private final String groupId;

    private String memberId = "";

    private final Optional<String> groupInstanceId = Optional.empty();

    private int memberEpoch = 0;

    private Optional<CompletableFuture<Void>> leaveGroupInProgress = Optional.empty();

    private CompletableFuture<Void> staleMemberAssignmentRelease;

    private boolean reconciliationInProgress;

    private boolean rejoinedWhileReconciliationInProgress;

    private final Optional<ClientTelemetryReporter> clientTelemetryReporter;

    private LocalAssignment targetAssignment = LocalAssignment.NONE;

    private LocalAssignment currentAssignment = LocalAssignment.NONE;

    private final AtomicBoolean subscriptionUpdated = new AtomicBoolean(false);

    private final RebalanceMetricsManager metricsManager;

    private final Time time;

    private boolean isPollTimerExpired;

    public StreamsMembershipManager(final String groupId,
                                    final StreamsAssignmentInterface streamsAssignmentInterface,
                                    final ConsumerMetadata metadata,
                                    final SubscriptionState subscriptionState,
                                    final LogContext logContext,
                                    final Optional<ClientTelemetryReporter> clientTelemetryReporter,
                                    final BackgroundEventHandler backgroundEventHandler,
                                    final Time time,
                                    final Metrics metrics) {
        log = logContext.logger(StreamsMembershipManager.class);
        this.state = MemberState.UNSUBSCRIBED;
        this.groupId = groupId;
        this.streamsAssignmentInterface = streamsAssignmentInterface;
        this.consumerMetadata = metadata;
        this.subscriptionState = subscriptionState;
        this.clientTelemetryReporter = clientTelemetryReporter;
        this.backgroundEventHandler = backgroundEventHandler;
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
        clearPendingTaskAssignment();
    }

    private void transitionToSendingLeaveGroup(boolean dueToExpiredPollTimer) {
        if (state == MemberState.FATAL) {
            log.warn("Member {} with epoch {} won't send leave group request because it is in " +
                "FATAL state", memberIdInfoForLog(), memberEpoch);
            return;
        }
        if (state == MemberState.UNSUBSCRIBED) {
            log.warn("Member {} won't send leave group request because it is already out of the group.",
                memberIdInfoForLog());
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
        clearPendingTaskAssignment();
    }

    private void transitionToStale() {
        transitionTo(MemberState.STALE);

        CompletableFuture<Void> onAllTasksRevokedDone = invokeRevokingAllTasksCallback();
        staleMemberAssignmentRelease = onAllTasksRevokedDone.whenComplete((result, error) -> {
            if (error != null) {
                log.error("Task revocation callback invocation failed " +
                    "after member left group due to expired poll timer.", error);
            }
            clearTaskAndPartitionAssignment();
            log.debug("Member {} sent leave group heartbeat and released its assignment. It will remain " +
                    "in {} state until the poll timer is reset, and it will then rejoin the group",
                memberIdInfoForLog(), MemberState.STALE);
        });
    }

    public void transitionToFatal() {
        MemberState previousState = state;
        transitionTo(MemberState.FATAL);
        log.error("Member {} with epoch {} transitioned to fatal state", memberIdInfoForLog(), memberEpoch);

        if (previousState == MemberState.UNSUBSCRIBED) {
            log.debug("Member {} with epoch {} got fatal error from the broker but it already " +
                "left the group, so onTaskAssignment callback won't be triggered.", memberIdInfoForLog(), memberEpoch);
            return;
        }

        if (previousState == MemberState.LEAVING || previousState == MemberState.PREPARE_LEAVING) {
            log.info("Member {} with epoch {} was leaving the group with state {} when it got a " +
                "fatal error from the broker. It will discard the ongoing leave and remain in " +
                "fatal state.", memberIdInfoForLog(), memberEpoch, previousState);
            maybeCompleteLeaveInProgress();
            return;
        }

        CompletableFuture<Void> callbackResult = invokeRevokingAllTasksCallback();
        callbackResult.whenComplete((result, error) -> {
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
                memberIdInfoForLog(), memberEpoch, MemberState.UNSUBSCRIBED);
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

        log.info("Member {} with epoch {} transitioned from {} to {}.", memberIdInfoForLog(), memberEpoch, state, nextState);
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
        memberEpoch = newEpoch;
    }

    private void clearPendingTaskAssignment() {
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
                    "be reconciled.", memberIdInfoForLog(), memberEpoch, MemberState.RECONCILING);
                transitionTo(MemberState.RECONCILING);
            }
        } else if (state == MemberState.LEAVING) {
            if (isPollTimerExpired) {
                log.debug("Member {} with epoch {} generated the heartbeat to leave due to expired poll timer. It will " +
                    "remain stale (no heartbeat) until it rejoins the group on the next consumer " +
                    "poll.", memberIdInfoForLog(), memberEpoch);
                transitionToStale();
            } else {
                log.debug("Member {} with epoch {} generated the heartbeat to leave the group.", memberIdInfoForLog(), memberEpoch);
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

        // Update the group member id label in the client telemetry reporter if the member id has
        // changed. Initially the member id is empty, and it is updated when the member joins the
        // group. This is done here to avoid updating the label on every heartbeat response. Also
        // check if the member id is null, as the schema defines it as nullable.
        if (responseData.memberId() != null && !responseData.memberId().equals(memberId)) {
            clientTelemetryReporter.ifPresent(reporter -> reporter.updateMetricsLabels(
                Collections.singletonMap(ClientTelemetryProvider.GROUP_MEMBER_ID, responseData.memberId())));
        }

        memberId = responseData.memberId();
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
                "leave the group and completed the leave operation. ", memberIdInfoForLog(), memberEpoch);
        }
    }

    public void onPollTimerExpired() {
        transitionToSendingLeaveGroup(true);
    }

    public void onFenced() {
        if (state == MemberState.PREPARE_LEAVING) {
            log.info("Member {} with epoch {} got fenced but it is already preparing to leave " +
                "the group, so it will stop sending heartbeat and won't attempt to send the " +
                "leave request or rejoin.", memberIdInfoForLog(), memberEpoch);
            finalizeLeaving();
            transitionTo(MemberState.UNSUBSCRIBED);
            maybeCompleteLeaveInProgress();
            return;
        }

        if (state == MemberState.LEAVING) {
            log.debug("Member {} with epoch {} got fenced before sending leave group heartbeat. " +
                "It will not send the leave request and won't attempt to rejoin.", memberIdInfoForLog(), memberEpoch);
            transitionTo(MemberState.UNSUBSCRIBED);
            maybeCompleteLeaveInProgress();
            return;
        }
        if (state == MemberState.UNSUBSCRIBED) {
            log.debug("Member {} with epoch {} got fenced but it already left the group, so it " +
                "won't attempt to rejoin.", memberIdInfoForLog(), memberEpoch);
            return;
        }
        transitionTo(MemberState.FENCED);
        resetEpoch();
        log.debug("Member {} with epoch {} transitioned to {} state. It will release its " +
            "assignment and rejoin the group.", memberIdInfoForLog(), memberEpoch, MemberState.FENCED);

        CompletableFuture<Void> callbackResult = invokeRevokingAllTasksCallback();
        callbackResult.whenComplete((result, error) -> {
            if (error != null) {
                log.error("onTaskAssignment callback invocation failed while releasing assignment" +
                    " after member got fenced. Member will rejoin the group anyways.", error);
            }
            clearTaskAndPartitionAssignment();
            if (state == MemberState.FENCED) {
                transitionToJoining();
            } else {
                log.debug("Fenced member onTaskAssignment callback completed but the state has " +
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
                "when it completes releasing its previous assignment.", memberIdInfoForLog());
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

    private static SortedSet<StreamsAssignmentInterface.TaskId> toTaskIdSet(final Map<String, SortedSet<Integer>> tasks) {
        SortedSet<StreamsAssignmentInterface.TaskId> taskIdSet = new TreeSet<>();
        for (final Map.Entry<String, SortedSet<Integer>> task : tasks.entrySet()) {
            final String subtopologyId = task.getKey();
            final SortedSet<Integer> partitions = task.getValue();
            for (final int partition : partitions) {
                taskIdSet.add(new StreamsAssignmentInterface.TaskId(subtopologyId, partition));
            }
        }
        return taskIdSet;
    }

    private static Map<String, SortedSet<Integer>> toTasksAssignment(final List<StreamsGroupHeartbeatResponseData.TaskIds> taskIds) {
        return taskIds.stream()
            .collect(Collectors.toMap(StreamsGroupHeartbeatResponseData.TaskIds::subtopologyId, taskId -> new TreeSet<>(taskId.partitions())));
    }

    /**
     * Leaves the group.
     *
     * <p>
     * This method does the following:
     * <ol>
     *     <li>Transitions member state to {@link MemberState#PREPARE_LEAVING}.</li>
     *     <li>Requests the invocation of the revocation callback.</li>
     *     <li>Once the revocation callback completes, it clears the current and target assignment, unsubscribes from
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
        if (isNotInGroup()) {
            if (state == MemberState.FENCED) {
                clearTaskAndPartitionAssignment();
                transitionTo(MemberState.UNSUBSCRIBED);
            }
            subscriptionState.unsubscribe();
            return CompletableFuture.completedFuture(null);
        }

        if (state == MemberState.PREPARE_LEAVING || state == MemberState.LEAVING) {
            log.debug("Leave group operation already in progress for member {}", memberIdInfoForLog());
            return leaveGroupInProgress.get();
        }

        CompletableFuture<Void> onGroupLeft = new CompletableFuture<>();
        leaveGroupInProgress = Optional.of(onGroupLeft);
        CompletableFuture<Void> onAllTasksRevokedDone = prepareLeaving();
        onAllTasksRevokedDone.whenComplete((__, callbackError) -> leaving(callbackError));

        return onGroupLeft;
    }

    private CompletableFuture<Void> prepareLeaving() {
        transitionTo(MemberState.PREPARE_LEAVING);
        return invokeRevokingAllTasksCallback();
    }

    private void leaving(Throwable callbackError) {
        if (callbackError != null) {
            log.error("Member {} callback to revoke task assignment failed. It will proceed " +
                    "to clear its assignment and send a leave group heartbeat",
                memberIdInfoForLog(), callbackError);
        } else {
            log.info("Member {} completed callback to revoke task assignment. It will proceed " +
                    "to clear its assignment and send a leave group heartbeat",
                memberIdInfoForLog());
        }
        subscriptionState.unsubscribe();
        clearTaskAndPartitionAssignment();
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

        // ToDo: add standby and warmup tasks
        SortedSet<StreamsAssignmentInterface.TaskId> assignedActiveTasks = toTaskIdSet(targetAssignment.activeTasks);

        log.info("Assigned tasks with local epoch {}\n" +
                "\tMember:                        {}\n" +
                "\tActive tasks:                  {}\n",
            targetAssignment.localEpoch,
            memberIdInfoForLog(),
            assignedActiveTasks
        );

        SortedSet<TopicPartition> ownedTopicPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        ownedTopicPartitions.addAll(subscriptionState.assignedPartitions());
        SortedSet<TopicPartition> assignedTopicPartitions = topicPartitionsForActiveTasks(targetAssignment.activeTasks);
        SortedSet<TopicPartition> assignedTopicPartitionsNotPreviouslyOwned =
            assignedTopicPartitionsNotPreviouslyOwned(assignedTopicPartitions, ownedTopicPartitions);

        subscriptionState.assignFromSubscribedAwaitingCallback(
            assignedTopicPartitions,
            assignedTopicPartitionsNotPreviouslyOwned
        );

        final StreamsOnAssignmentCallbackNeededEvent onAssignmentCallbackNeededEvent =
            new StreamsOnAssignmentCallbackNeededEvent(new StreamsAssignmentInterface.Assignment(
                assignedActiveTasks,
                Collections.emptySet(),
                Collections.emptySet()
            ));
        CompletableFuture<Void> onTasksAssignmentDone = onAssignmentCallbackNeededEvent.future();
        backgroundEventHandler.add(onAssignmentCallbackNeededEvent);
        // The current target assignment is captured to ensure that acknowledging the current assignment is done with
        // the same target assignment that was used when this reconciliation was initiated.
        LocalAssignment currentTargetAssignment = targetAssignment;
        onTasksAssignmentDone.whenComplete((__, callbackError) -> {
            if (callbackError != null) {
                log.error("Reconciliation failed: onTasksAssignment callback invocation failed for tasks {}",
                    targetAssignment, callbackError);
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

    private SortedSet<TopicPartition> assignedTopicPartitionsNotPreviouslyOwned(final SortedSet<TopicPartition> assignedTopicPartitions,
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
                streamsAssignmentInterface.subtopologyMap().get(subtopologyId).sourceTopics.stream(),
                streamsAssignmentInterface.subtopologyMap().get(subtopologyId).repartitionSourceTopics.keySet().stream()
            ).forEach(topic -> {
                for (final int partitionId : partitionIds) {
                    topicPartitions.add(new TopicPartition(topic, partitionId));
                }
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

    private CompletableFuture<Void> invokeRevokingAllTasksCallback() {
        final StreamsOnAssignmentCallbackNeededEvent onAssignmentCallbackNeededEvent =
            new StreamsOnAssignmentCallbackNeededEvent(new StreamsAssignmentInterface.Assignment(
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet()
            ));
        backgroundEventHandler.add(onAssignmentCallbackNeededEvent);
        return onAssignmentCallbackNeededEvent.future();
    }

    public void onTaskAssignmentCallbackCompleted(StreamsOnAssignmentCallbackCompletedEvent event) {
        Optional<KafkaException> error = event.error();
        CompletableFuture<Void> future = event.future();

        if (error.isPresent()) {
            Exception e = error.get();
            log.warn("The onTaskAssignment callback completed with an error ({}); " +
                "signaling to continue to the next phase of rebalance", e.getMessage());
            future.completeExceptionally(e);
        } else {
            log.debug("The onTaskAssignment callback completed successfully; signaling to continue to the next phase of rebalance");
            future.complete(null);
        }
    }

    private String memberIdInfoForLog() {
        return (memberId == null || memberId.isEmpty()) ? "<no ID>" : memberId;
    }
}
