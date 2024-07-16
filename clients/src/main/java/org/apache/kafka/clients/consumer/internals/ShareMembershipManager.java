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
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.Utils.TopicIdPartitionComparator;
import org.apache.kafka.clients.consumer.internals.Utils.TopicPartitionComparator;
import org.apache.kafka.clients.consumer.internals.metrics.ShareRebalanceMetricsManager;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareGroupHeartbeatRequest;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryProvider;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Group manager for a single consumer that has a group id defined in the config
 * {@link ConsumerConfig#GROUP_ID_CONFIG} and the share group protocol to get automatically
 * assigned partitions when calling the subscribe API.
 * <p/>
 *
 * While the subscribe API hasn't been called (or if the consumer called unsubscribe), this manager
 * will only be responsible for keeping the member in the {@link MemberState#UNSUBSCRIBED} state,
 * without joining the group.
 * <p/>
 *
 * If the consumer subscribe API is called, this manager will use the {@link #groupId()} to join the
 * share group, and based on the share group protocol heartbeats, will handle the full
 * lifecycle of the member as it joins the group, reconciles assignments, handles fatal errors,
 * and leaves the group.
 * <p/>
 *
 * Reconciliation process:<p/>
 * The member accepts all assignments received from the broker, resolves topic names from
 * metadata, reconciles the resolved assignments, and keeps the unresolved to be reconciled when
 * discovered with a metadata update. Reconciliations of resolved assignments are executed
 * sequentially and acknowledged to the server as they complete. The reconciliation process
 * involves multiple async operations, so the member will continue to heartbeat while these
 * operations complete, to make sure that the member stays in the group while reconciling.
 * <p/>
 *
 * Reconciliation steps:
 * <ol>
 *     <li>Resolve topic names for all topic IDs received in the target assignment. Topic names
 *     found in metadata are then ready to be reconciled. Topic IDs not found are kept as
 *     unresolved, and the member request metadata updates until it resolves them (or the broker
 *     removes it from the target assignment).</li>
 *     <li>When the above steps complete, the member acknowledges the reconciled assignment,
 *     which is the subset of the target that was resolved from metadata and actually reconciled.
 *     The ack is performed by sending a heartbeat request back to the broker.</li>
 * </ol>
 *
 */
public class ShareMembershipManager implements RequestManager {

    /**
     * Logger.
     */
    private final Logger log;

    /**
     * TopicPartition comparator based on topic name and partition id.
     */
    static final TopicPartitionComparator TOPIC_PARTITION_COMPARATOR = new TopicPartitionComparator();

    /**
     * TopicIdPartition comparator based on topic name and partition id (ignoring ID while sorting,
     * as this is sorted mainly for logging purposes).
     */
    static final TopicIdPartitionComparator TOPIC_ID_PARTITION_COMPARATOR = new TopicIdPartitionComparator();

    /**
     * Group ID of the share group the member will be part of, provided when creating the current
     * membership manager.
     */
    private final String groupId;

    /**
     * Member ID assigned by the server to the member, received in a heartbeat response when
     * joining the group specified in {@link #groupId}
     */
    private String memberId = "";

    /**
     * Current epoch of the member. It will be set to 0 by the member, and provided to the server
     * on the heartbeat request, to join the group. It will be then maintained by the server,
     * incremented as the member reconciles and acknowledges the assignments it receives. It will
     * be reset to 0 if the member gets fenced.
     */
    private int memberEpoch = 0;

    /**
     * Rack ID of the member, if specified.
     */
    private final String rackId;

    /**
     * Current state of this member as part of the share group, as defined in {@link MemberState}
     */
    private MemberState state;

    /**
     * Assignment that the member received from the server and successfully processed.
     */
    private Map<Uuid, SortedSet<Integer>> currentAssignment;

    /**
     * Subscription state object holding the current assignment the member has for the topics it
     * subscribed to.
     */
    private final SubscriptionState subscriptions;

    /**
     * Consumer metadata that we use to check cluster state and drive refresh when required.
     */
    private final ConsumerMetadata metadata;

    /**
     * Local cache of assigned topic IDs and names. Topics are added here when received in a
     * target assignment, as we discover their names in the Metadata cache, and removed when the
     * topic is not in the subscription anymore. The purpose of this cache is to avoid metadata
     * requests in cases where a currently assigned topic is in the target assignment (new
     * partition assigned, or revoked), but it is not present the Metadata cache at that moment.
     * The cache is cleared when the subscription changes ({@link #transitionToJoining()}, the
     * member fails ({@link #transitionToFatal()} or leaves the group ({@link #leaveGroup()}).
     */
    private final Map<Uuid, String> assignedTopicNamesCache;

    /**
     * Topic IDs and partitions received in the last target assignment. Items are added to this set
     * every time a target assignment is received. This is where the member collects the assignment
     * received from the broker, even though it may not be ready to fully reconcile due to missing
     * metadata.
     */
    private final Map<Uuid, SortedSet<Integer>> currentTargetAssignment;

    /**
     * If there is a reconciliation running. This will be true if {@link #maybeReconcile()} has been triggered
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
     * If the member is currently leaving the group after a call to {@link #leaveGroup()}}, this
     * will have a future that will complete when the ongoing leave operation completes
     * (heartbeat request to leave is sent out). This will be empty if the member is not leaving.
     */
    private Optional<CompletableFuture<Void>> leaveGroupInProgress = Optional.empty();

    /**
     * Registered listeners that will be notified whenever the memberID/epoch gets updated (valid
     * values received from the broker, or values cleared due to member leaving the group, getting
     * fenced or failing).
     */
    private final List<MemberStateListener> stateUpdatesListeners;

    /**
     * Optional client telemetry reporter which sends client telemetry data to the broker. This
     * will be empty if the client telemetry feature is not enabled. This is provided to update
     * the group member id label when the member joins the group.
     */
    private final Optional<ClientTelemetryReporter> clientTelemetryReporter;

    /**
     * Measures successful rebalance latency and number of failed rebalances.
     */
    private final ShareRebalanceMetricsManager metricsManager;

    private final Time time;

    /**
     * True if the poll timer has expired, signaled by a call to
     * {@link #transitionToSendingLeaveGroup(boolean)} with dueToExpiredPollTimer param true. This
     * will be used to determine that the member should transition to STALE after leaving the
     * group, to release its assignment and wait for the timer to be reset.
     */
    private boolean isPollTimerExpired;

    public ShareMembershipManager(LogContext logContext,
                                  String groupId,
                                  String rackId,
                                  SubscriptionState subscriptions,
                                  ConsumerMetadata metadata,
                                  Optional<ClientTelemetryReporter> clientTelemetryReporter,
                                  Time time,
                                  Metrics metrics) {
        this(logContext,
                groupId,
                rackId,
                subscriptions,
                metadata,
                clientTelemetryReporter,
                time,
                new ShareRebalanceMetricsManager(metrics));
    }

    // Visible for testing
    ShareMembershipManager(LogContext logContext,
                           String groupId,
                           String rackId,
                           SubscriptionState subscriptions,
                           ConsumerMetadata metadata,
                           Optional<ClientTelemetryReporter> clientTelemetryReporter,
                           Time time,
                           ShareRebalanceMetricsManager metricsManager) {
        this.log = logContext.logger(ShareMembershipManager.class);
        this.groupId = groupId;
        this.rackId = rackId;
        this.state = MemberState.UNSUBSCRIBED;
        this.subscriptions = subscriptions;
        this.metadata = metadata;
        this.assignedTopicNamesCache = new HashMap<>();
        this.currentTargetAssignment = new HashMap<>();
        this.currentAssignment = new HashMap<>();
        this.stateUpdatesListeners = new ArrayList<>();
        this.clientTelemetryReporter = clientTelemetryReporter;
        this.time = time;
        this.metricsManager = metricsManager;
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

        log.trace("Member {} with epoch {} transitioned from {} to {}.", memberId, memberEpoch, state, nextState);
        this.state = nextState;
    }

    private static boolean isCompletingRebalance(MemberState currentState, MemberState nextState) {
        return currentState == MemberState.RECONCILING &&
                (nextState == MemberState.STABLE || nextState == MemberState.ACKNOWLEDGING);
    }

    private static boolean isStartingRebalance(MemberState currentState, MemberState nextState) {
        return currentState != MemberState.RECONCILING && nextState == MemberState.RECONCILING;
    }

    /**
     * @return Group ID of the share group the member is part of (or wants to be part of).
     */
    public String groupId() {
        return groupId;
    }

    /**
     * @return Member ID assigned by the server to this member when it joins the share group.
     */
    public String memberId() {
        return memberId;
    }

    /**
     * @return Current epoch of the member, maintained by the server.
     */
    public int memberEpoch() {
        return memberEpoch;
    }

    /**
     * @return The rack ID of the member, if specified.
     */
    public String rackId() {
        return rackId;
    }

    /**
     * Update member info and transition member state based on a successful heartbeat response.
     *
     * @param response Heartbeat response to extract member info and errors from.
     */
    public void onHeartbeatSuccess(ShareGroupHeartbeatResponseData response) {
        if (response.errorCode() != Errors.NONE.code()) {
            String errorMessage = String.format(
                    "Unexpected error in Heartbeat response. Expected no error, but received: %s",
                    Errors.forCode(response.errorCode())
            );
            throw new IllegalArgumentException(errorMessage);
        }
        MemberState state = state();
        if (state == MemberState.LEAVING) {
            log.debug("Ignoring heartbeat response received from broker. Member {} with epoch {} is " +
                    "already leaving the group.", memberId, memberEpoch);
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
        if (response.memberId() != null && !response.memberId().equals(memberId)) {
            clientTelemetryReporter.ifPresent(reporter -> reporter.updateMetricsLabels(
                    Collections.singletonMap(ClientTelemetryProvider.GROUP_MEMBER_ID, response.memberId())));
        }

        this.memberId = response.memberId();
        updateMemberEpoch(response.memberEpoch());

        ShareGroupHeartbeatResponseData.Assignment assignment = response.assignment();

        if (assignment != null) {
            if (!state.canHandleNewAssignment()) {
                // New assignment received but member is in a state where it cannot take new
                // assignments (ex. preparing to leave the group)
                log.debug("Ignoring new assignment {} received from server because member is in {} state.",
                        assignment, state);
                return;
            }
            processAssignmentReceived(assignment);
        }
    }

    /**
     * @return True if the consumer is not a member of the group.
     */
    private boolean isNotInGroup() {
        return state == MemberState.UNSUBSCRIBED ||
                state == MemberState.FENCED ||
                state == MemberState.FATAL ||
                state == MemberState.STALE;
    }

    /**
     * This will process the assignment received if it is different from the member's current
     * assignment. If a new assignment is received, this will make sure reconciliation is attempted
     * on the next call to `poll`. If another reconciliation is currently in process, the first `poll`
     * after that reconciliation will trigger the new reconciliation.
     *
     * @param assignment Assignment received from the broker.
     */
    private void processAssignmentReceived(ShareGroupHeartbeatResponseData.Assignment assignment) {
        replaceTargetAssignmentWithNewAssignment(assignment);
        if (!targetAssignmentReconciled()) {
            // Transition the member to RECONCILING when receiving a new target
            // assignment from the broker, different from the current assignment. Note that the
            // reconciliation might not be triggered just yet because of missing metadata.
            transitionTo(MemberState.RECONCILING);
        } else {
            // Same assignment received, nothing to reconcile.
            log.debug("Target assignment {} received from the broker is equals to the member " +
                            "current assignment {}. Nothing to reconcile.",
                    currentTargetAssignment, currentAssignment);
            // Make sure we transition the member back to STABLE if it was RECONCILING (ex.
            // member was RECONCILING unresolved assignments that were just removed by the
            // broker), or JOINING (member joining received empty assignment).
            if (state == MemberState.RECONCILING || state == MemberState.JOINING) {
                transitionTo(MemberState.STABLE);
            }
        }
    }

    /**
     * Overwrite the target assignment with the new target assignment.
     *
     * @param assignment Target assignment received from the broker.
     */
    private void replaceTargetAssignmentWithNewAssignment(
            ShareGroupHeartbeatResponseData.Assignment assignment) {
        currentTargetAssignment.clear();
        assignment.topicPartitions().forEach(topicPartitions ->
                currentTargetAssignment.put(topicPartitions.topicId(), new TreeSet<>(topicPartitions.partitions())));
    }

    /**
     * Transition the member to the FENCED state, where the member will release the assignment and transition
     * to {@link MemberState#JOINING} to rejoin the group. This is expected to be invoked when
     * the heartbeat returns a FENCED_MEMBER_EPOCH or UNKNOWN_MEMBER_ID error.
     */
    public void transitionToFenced() {
        if (state == MemberState.PREPARE_LEAVING) {
            log.debug("Member {} with epoch {} got fenced but it is already preparing to leave " +
                            "the group, so it will stop sending heartbeat and won't attempt to rejoin.",
                    memberId, memberEpoch);
            // Transition to UNSUBSCRIBED, ensuring that the member (that is not part of the
            // group anymore from the broker point of view) will stop sending heartbeats while it
            // completes the ongoing leaving operation.
            transitionTo(MemberState.UNSUBSCRIBED);
            return;
        }

        if (state == MemberState.LEAVING) {
            log.debug("Member {} with epoch {} got fenced but it is already leaving the group " +
                    "with state {}, so it won't attempt to rejoin.", memberId, memberEpoch, state);
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

        // Release assignment
        clearSubscription();
        transitionToJoining();
    }

    /**
     * Transition the member to the FAILED state and update the member info as required. This is
     * invoked when un-recoverable errors occur (ex. when the heartbeat returns a non-retriable
     * error)
     */
    public void transitionToFatal() {
        MemberState previousState = state;
        transitionTo(MemberState.FATAL);
        if (memberId.isEmpty()) {
            log.error("Member {} with epoch {} transitioned to {} state", memberId, memberEpoch, MemberState.FATAL);
        } else {
            log.error("Non-member transitioned to {} state", MemberState.FATAL);
        }
        notifyEpochChange(Optional.empty(), Optional.empty());

        if (previousState == MemberState.UNSUBSCRIBED) {
            log.debug("Member {} with epoch {} got fatal error from the broker but it already " +
                    "left the group.", memberId, memberEpoch);
            return;
        }

        // Release assignment
        clearSubscription();
    }

    /**
     * Join the group with the updated subscription, if the member is not part of it yet. If the
     * member is already part of the group, this will only ensure that the updated subscription
     * is included in the next heartbeat request.
     * <p/>
     * Note that list of topics of the subscription is taken from the shared subscription state.
     */
    public void onSubscriptionUpdated() {
        if (state == MemberState.UNSUBSCRIBED) {
            transitionToJoining();
        }
    }

    /**
     * Clear the assigned partitions in the member subscription, pending assignments and metadata cache.
     */
    private void clearSubscription() {
        if (subscriptions.hasAutoAssignedPartitions()) {
            subscriptions.assignFromSubscribed(Collections.emptySet());
        }
        updateAssignment(Collections.emptySet());
        clearPendingAssignmentsAndLocalNamesCache();
    }

    /**
     * Update a new assignment by setting the assigned partitions in the member subscription.
     *
     * @param assignedPartitions Topic partitions to take as the new subscription assignment
     */
    private void updateSubscription(SortedSet<TopicIdPartition> assignedPartitions) {
        Collection<TopicPartition> assignedTopicPartitions = toTopicPartitionSet(assignedPartitions);
        if (subscriptions.hasAutoAssignedPartitions()) {
            subscriptions.assignFromSubscribed(assignedTopicPartitions);
        }
        updateAssignment(assignedPartitions);
    }

    /**
     * Transition to the {@link MemberState#JOINING} state, indicating that the member will
     * try to join the group on the next heartbeat request. This is expected to be invoked when
     * the user calls the subscribe API.
     * Visible for testing.
     */
    public void transitionToJoining() {
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
        clearPendingAssignmentsAndLocalNamesCache();
    }

    /**
     * Release assignment and transition to {@link MemberState#PREPARE_LEAVING} so that a heartbeat
     * request is sent indicating the broker that the member wants to leave the group. This is
     * expected to be invoked when the user calls the unsubscribe API.
     */
    public CompletableFuture<Void> leaveGroup() {
        if (isNotInGroup()) {
            if (state == MemberState.FENCED) {
                clearSubscription();
                transitionTo(MemberState.UNSUBSCRIBED);
            }
            return CompletableFuture.completedFuture(null);
        }

        if (state == MemberState.PREPARE_LEAVING || state == MemberState.LEAVING) {
            // Member already leaving. No-op and return existing leave group future that will
            // complete when the ongoing leave operation completes.
            return leaveGroupInProgress.get();
        }

        transitionTo(MemberState.PREPARE_LEAVING);
        CompletableFuture<Void> leaveResult = new CompletableFuture<>();
        leaveGroupInProgress = Optional.of(leaveResult);

        clearSubscription();

        // Transition to ensure that a heartbeat request is sent out to effectively leave the
        // group (even in the case where the member had no assignment to release)
        transitionToSendingLeaveGroup(false);

        // Return future to indicate that the leave group is done when the transition to send
        // the heartbeat has been made.
        return leaveResult;
    }

    /**
     * Reset member epoch to the value required for the leave the group heartbeat request, and
     * transition to the {@link MemberState#LEAVING} state so that a heartbeat
     * request is sent out with it.
     *
     * @param dueToExpiredPollTimer True if the leave group is due to an expired poll timer. This
     *                              will indicate that the member must remain STALE after leaving,
     *                              until it releases its assignment and the timer is reset.
     */
    public void transitionToSendingLeaveGroup(boolean dueToExpiredPollTimer) {
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
            this.isPollTimerExpired = true;

            // Briefly transition through prepare leaving. The member does not have to release
            // any assignment before sending the leave group given that is stale. It will invoke
            // onPartitionsLost after sending the leave group on the STALE state.
            transitionTo(MemberState.PREPARE_LEAVING);
        }
        updateMemberEpoch(ShareGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH);
        currentAssignment = new HashMap<>();
        transitionTo(MemberState.LEAVING);
    }

    /**
     * Call all listeners that are registered to get notified when the member epoch is updated.
     * This also includes the latest member ID in the notification. If the member fails or leaves
     * the group, this will be invoked with empty epoch and member ID.
     */
    private void notifyEpochChange(Optional<Integer> epoch, Optional<String> memberId) {
        stateUpdatesListeners.forEach(stateListener -> stateListener.onMemberEpochUpdated(epoch, memberId));
    }

    /**
     * @return True if the member should send heartbeat to the coordinator without waiting for
     * the interval.
     */
    public boolean shouldHeartbeatNow() {
        MemberState state = state();
        return state == MemberState.ACKNOWLEDGING || state == MemberState.LEAVING || state == MemberState.JOINING;
    }

    /**
     * Update state when a heartbeat is sent out. This will transition out of the states that end
     * when a heartbeat request is sent, without waiting for a response (ex.
     * {@link MemberState#ACKNOWLEDGING} and {@link MemberState#LEAVING}).
     */
    public void onHeartbeatRequestSent() {
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
                transitionToStale();
            } else {
                transitionToUnsubscribed();
            }
        }
    }

    /**
     * Transition out of the {@link MemberState#LEAVING} state even if the heartbeat was not sent.
     * This will ensure that the member is not blocked in {@link MemberState#LEAVING} (it's the best
     * effort to send the request, without any response handling or retry logic).
     */
    public void onHeartbeatRequestSkipped() {
        if (state == MemberState.LEAVING) {
            log.debug("Heartbeat for leaving group could not be sent. Member {} with epoch {} will transition to {}.",
                    memberId, memberEpoch, MemberState.UNSUBSCRIBED);
            transitionToUnsubscribed();
        }
    }

    private void transitionToUnsubscribed() {
        transitionTo(MemberState.UNSUBSCRIBED);
        leaveGroupInProgress.get().complete(null);
        leaveGroupInProgress = Optional.empty();
    }

    /**
     * @return True if there are no assignments waiting to be resolved from metadata or reconciled.
     */
    private boolean targetAssignmentReconciled() {
        return currentAssignment.equals(currentTargetAssignment);
    }

    /**
     * @return True if the member should not send heartbeats, which would be one of the following
     * cases:
     * <ul>
     * <li>Member is not subscribed to any topics</li>
     * <li>Member has received a fatal error in a previous heartbeat response</li>
     * <li>Member is stale, meaning that it has left the group due to expired poll timer</li>
     * </ul>
     */
    public boolean shouldSkipHeartbeat() {
        MemberState state = state();
        return state == MemberState.UNSUBSCRIBED ||
                state == MemberState.FATAL ||
                state == MemberState.STALE ||
                state == MemberState.FENCED;
    }

    /**
     * @return True if the member is preparing to leave the group (waiting for callbacks), or
     * leaving (sending last heartbeat). This is used to skip proactively leaving the group when
     * the consumer poll timer expires.
     */
    public boolean isLeavingGroup() {
        MemberState state = state();
        return state == MemberState.PREPARE_LEAVING || state == MemberState.LEAVING;
    }

    public void maybeRejoinStaleMember() {
        isPollTimerExpired = false;
        if (state == MemberState.STALE) {
            log.debug("Expired poll timer has been reset so stale member {} will rejoin the group" +
                    "when it completes releasing its previous assignment.", memberId);
            transitionToJoining();
        }
    }

    /**
     * Sets the epoch to the leave group epoch and clears the assignments. The member will rejoin with
     * the existing subscriptions after the next application poll event.
     */
    public void transitionToStale() {
        transitionTo(MemberState.STALE);

        // Release assignment
        clearSubscription();
        log.debug("Member {} sent leave group heartbeat and released its assignment. It will remain " +
                        "in {} state until the poll timer is reset, and it will then rejoin the group",
                memberId, MemberState.STALE);
    }

    /**
     * Reconcile the assignment that has been received from the server. If for some topics, the
     * topic ID cannot be matched to a topic name, a metadata update will be triggered and only
     * the subset of topics that are resolvable will be reconciled. Reconciliation will update
     * the subscription state.
     *
     * <p>There are three conditions under which no reconciliation will be triggered:
     *  - We have already reconciled the assignment (the target assignment is the same as the current assignment).
     *  - Another reconciliation is already in progress.
     *  - There are topics that haven't been added to the current assignment yet, but all their topic IDs
     *    are missing from the target assignment.
     */
    void maybeReconcile() {
        if (targetAssignmentReconciled()) {
            log.trace("Ignoring reconciliation attempt. Target assignment is equal to the " +
                    "current assignment.");
            return;
        }
        if (reconciliationInProgress) {
            log.trace("Ignoring reconciliation attempt. Another reconciliation is already in progress. Assignment {}" +
                    " will be handled in the next reconciliation loop.", currentTargetAssignment);
            return;
        }

        // Find the subset of the target assignment that can be resolved to topic names, and trigger a metadata update
        // if some topic IDs are not resolvable.
        SortedSet<TopicIdPartition> assignedTopicIdPartitions = findResolvableAssignmentAndTriggerMetadataUpdate();

        SortedSet<TopicPartition> ownedPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        ownedPartitions.addAll(subscriptions.assignedPartitions());

        // Keep copy of assigned TopicPartitions created from the TopicIdPartitions that are
        // being reconciled. Needed for interactions with the centralized subscription state that
        // does not support topic IDs yet.
        SortedSet<TopicPartition> assignedTopicPartitions = toTopicPartitionSet(assignedTopicIdPartitions);

        // Check same assignment. Based on topic names for now, until topic IDs are properly
        // supported in the centralized subscription state object. Note that this check is
        // required to make sure that reconciliation is not triggered if the assignment ready to
        // be reconciled is the same as the current one (even though the member may remain
        // in RECONCILING state if it has some unresolved assignments).
        boolean sameAssignmentReceived = assignedTopicPartitions.equals(ownedPartitions);
        if (sameAssignmentReceived) {
            log.debug("Ignoring reconciliation attempt. Target assignment ready to reconcile {} " +
                    "is equal to the member current assignment {}.", assignedTopicPartitions, ownedPartitions);
            return;
        }

        markReconciliationInProgress();

        // Partitions to assign (not previously owned)
        SortedSet<TopicPartition> addedPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        addedPartitions.addAll(assignedTopicPartitions);
        addedPartitions.removeAll(ownedPartitions);

        // Partitions to revoke
        SortedSet<TopicPartition> revokedPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        revokedPartitions.addAll(ownedPartitions);
        revokedPartitions.removeAll(assignedTopicPartitions);

        log.info("Updating assignment with\n" +
                        "\tAssigned partitions:                       {}\n" +
                        "\tCurrent owned partitions:                  {}\n" +
                        "\tAdded partitions (assigned - owned):       {}\n" +
                        "\tRevoked partitions (owned - assigned):     {}\n",
                assignedTopicIdPartitions,
                ownedPartitions,
                addedPartitions,
                revokedPartitions
        );

        if (!maybeAbortReconciliation()) {
            revokeAndAssign(assignedTopicIdPartitions, revokedPartitions);
        }
    }

    /**
     * Complete the reconciliation by updating the assignment and making the appropriate state
     * transition.
     */
    private void revokeAndAssign(SortedSet<TopicIdPartition> assignedTopicIdPartitions,
                                 SortedSet<TopicPartition> revokedPartitions) {
        if (!revokedPartitions.isEmpty()) {
            revokePartitions(revokedPartitions);
        }

        // Apply assignment
        if (!maybeAbortReconciliation()) {
            assignPartitions(assignedTopicIdPartitions);
        }

        // Make assignment effective on the broker by transitioning to send acknowledge.
        transitionTo(MemberState.ACKNOWLEDGING);
        markReconciliationCompleted();
    }

    /**
     * @return True if the reconciliation in progress should not continue. This could be because
     * the member is not in RECONCILING state anymore (member failed or is leaving the group), or
     * if it has rejoined the group (note that after rejoining the member could be RECONCILING
     * again, so checking the state is not enough)
     */
    boolean maybeAbortReconciliation() {
        boolean shouldAbort = state != MemberState.RECONCILING || rejoinedWhileReconciliationInProgress;
        if (shouldAbort) {
            String reason = rejoinedWhileReconciliationInProgress ?
                    "the member has re-joined the group" :
                    "the member already transitioned out of the reconciling state into " + state;
            log.info("Interrupting reconciliation that is not relevant anymore because {}.", reason);
            markReconciliationCompleted();
        }
        return shouldAbort;
    }

    // Visible for testing.
    void updateAssignment(Set<TopicIdPartition> assignedTopicIdPartitions) {
        currentAssignment.clear();
        assignedTopicIdPartitions.forEach(topicIdPartition -> {
            Uuid topicId = topicIdPartition.topicId();
            currentAssignment.computeIfAbsent(topicId, k -> new TreeSet<>()).add(topicIdPartition.partition());
        });
    }

    /**
     * Build set of {@link TopicPartition} from the given set of {@link TopicIdPartition}.
     */
    private SortedSet<TopicPartition> toTopicPartitionSet(SortedSet<TopicIdPartition> topicIdPartitions) {
        SortedSet<TopicPartition> result = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        topicIdPartitions.forEach(topicIdPartition -> result.add(topicIdPartition.topicPartition()));
        return result;
    }

    /**
     *  Visible for testing.
     */
    void markReconciliationInProgress() {
        reconciliationInProgress = true;
        rejoinedWhileReconciliationInProgress = false;
    }

    /**
     *  Visible for testing.
     */
    void markReconciliationCompleted() {
        reconciliationInProgress = false;
        rejoinedWhileReconciliationInProgress = false;
    }

    /**
     * Build set of TopicIdPartition (topic ID, topic name and partition id) from the target assignment
     * received from the broker (topic IDs and list of partitions).
     *
     * <p>
     * This will:
     *
     * <ol type="1">
     *     <li>Try to find topic names in the metadata cache</li>
     *     <li>For topics not found in metadata, try to find names in the local topic names cache
     *     (contains topic id and names currently assigned and resolved)</li>
     *     <li>If there are topics that are not in metadata cache or in the local cached
     *     of topic names assigned to this member, request a metadata update, and continue
     *     resolving names as the cache is updated.
     *     </li>
     * </ol>
     */
    private SortedSet<TopicIdPartition> findResolvableAssignmentAndTriggerMetadataUpdate() {
        final SortedSet<TopicIdPartition> assignmentReadyToReconcile = new TreeSet<>(TOPIC_ID_PARTITION_COMPARATOR);
        final HashMap<Uuid, SortedSet<Integer>> unresolved = new HashMap<>(currentTargetAssignment);

        // Try to resolve topic names from metadata cache or subscription cache, and move
        // assignments from the "unresolved" collection, to the "assignmentReadyToReconcile" one.
        Iterator<Map.Entry<Uuid, SortedSet<Integer>>> it = unresolved.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Uuid, SortedSet<Integer>> e = it.next();
            Uuid topicId = e.getKey();
            SortedSet<Integer> topicPartitions = e.getValue();

            Optional<String> nameFromMetadata = findTopicNameInGlobalOrLocalCache(topicId);
            nameFromMetadata.ifPresent(resolvedTopicName -> {
                // Name resolved, so assignment is ready for reconciliation.
                topicPartitions.forEach(tp ->
                        assignmentReadyToReconcile.add(new TopicIdPartition(topicId, tp, resolvedTopicName))
                );
                it.remove();
            });
        }

        if (!unresolved.isEmpty()) {
            log.debug("Topic Ids {} received in target assignment were not found in metadata and " +
                    "are not currently assigned. Requesting a metadata update now to resolve " +
                    "topic names.", unresolved.keySet());
            metadata.requestUpdate(true);
        }

        return assignmentReadyToReconcile;
    }

    /**
     * Look for topic in the global metadata cache. If found, add it to the local cache and
     * return it. If not found, look for it in the local metadata cache. Return empty if not
     * found in any of the two.
     */
    private Optional<String> findTopicNameInGlobalOrLocalCache(Uuid topicId) {
        String nameFromMetadataCache = metadata.topicNames().getOrDefault(topicId, null);
        if (nameFromMetadataCache != null) {
            // Add topic name to local cache, so it can be reused if included in a next target
            // assignment if metadata cache not available.
            assignedTopicNamesCache.put(topicId, nameFromMetadataCache);
            return Optional.of(nameFromMetadataCache);
        } else {
            // Topic ID was not found in metadata. Check if the topic name is in the local
            // cache of topics currently assigned. This will avoid a metadata request in the
            // case where the metadata cache may have been flushed right before the
            // revocation of a previously assigned topic.
            String nameFromSubscriptionCache = assignedTopicNamesCache.getOrDefault(topicId, null);
            return Optional.ofNullable(nameFromSubscriptionCache);
        }
    }

    /**
     * Revoke partitions.
     */
    void revokePartitions(Set<TopicPartition> revokedPartitions) {
        log.info("Revoking previously assigned partitions {}", revokedPartitions.stream().map(TopicPartition::toString).collect(Collectors.joining(", ")));

        // Mark partitions as pending revocation to stop fetching from the partitions.
        markPendingRevocationToPauseFetching(revokedPartitions);

        // At this point we expect to be in a middle of a revocation triggered from RECONCILING
        // or PREPARE_LEAVING, but it could be the case that the member received a fatal error
        // while waiting for the commit to complete.
        if (state == MemberState.FATAL) {
            String errorMsg = String.format("Member %s with epoch %s received a fatal error " +
                            "while waiting for a revocation commit to complete. Will abort revocation.",
                    memberId, memberEpoch);
            log.debug(errorMsg);
        }
    }


    /**
     * Make new assignment effective and update the local topic names cache, removing from it all topics that
     * are not assigned to the member anymore.
     *
     * @param assignedPartitions Full assignment that will be updated in the member subscription
     *                           state. This includes previously owned and newly added partitions.
     */
    private void assignPartitions(SortedSet<TopicIdPartition> assignedPartitions) {
        // Update assignment in the subscription state, and ensure that no fetching or positions
        // initialization happens for the newly added partitions.
        updateSubscription(assignedPartitions);

        // Clear topic names cache, removing topics that are not assigned to the member anymore.
        Set<String> assignedTopics = assignedPartitions.stream().map(TopicIdPartition::topic).collect(Collectors.toSet());
        assignedTopicNamesCache.values().retainAll(assignedTopics);
    }

    /**
     * Mark partitions as 'pending revocation', to effectively stop fetching.
     */
    private void markPendingRevocationToPauseFetching(Set<TopicPartition> partitionsToRevoke) {
        log.debug("Marking partitions pending for revocation: {}", partitionsToRevoke);
        subscriptions.markPendingRevocation(partitionsToRevoke);
    }

    /**
     * Discard assignments received that have not been reconciled yet (waiting for metadata
     * or the next reconciliation loop). Remove all elements from the topic names cache.
     */
    private void clearPendingAssignmentsAndLocalNamesCache() {
        currentTargetAssignment.clear();
        assignedTopicNamesCache.clear();
    }

    private void resetEpoch() {
        updateMemberEpoch(ShareGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH);
    }

    private void updateMemberEpoch(int newEpoch) {
        boolean newEpochReceived = this.memberEpoch != newEpoch;
        this.memberEpoch = newEpoch;
        // Simply notify based on epoch change only, given that the member will never receive a
        // new member ID without an epoch (member ID is only assigned when it joins the group).
        if (newEpochReceived) {
            if (memberEpoch > 0) {
                notifyEpochChange(Optional.of(memberEpoch), Optional.ofNullable(memberId));
            } else {
                notifyEpochChange(Optional.empty(), Optional.empty());
            }
        }
    }

    /**
     * @return Current state of this member in relationship to a group, as defined in
     * {@link MemberState}.
     */
    public MemberState state() {
        return state;
    }

    /**
     * @return Current assignment for the member as received from the broker (topic IDs and
     * partitions). This is the last assignment that the member has successfully reconciled.
     */
    public Map<Uuid, SortedSet<Integer>> currentAssignment() {
        return this.currentAssignment;
    }

    /**
     * @return Set of topic IDs received in a target assignment that have not been reconciled yet
     * because topic names are not in metadata or reconciliation hasn't finished. Reconciliation
     * hasn't finished for a topic if the currently active assignment has a different set of partitions
     * for the topic than the target assignment.
     *
     * Visible for testing.
     */
    Set<Uuid> topicsAwaitingReconciliation() {
        return topicPartitionsAwaitingReconciliation().keySet();
    }

    /**
     * @return Map of topics partitions received in a target assignment that have not been
     * reconciled yet because topic names are not in metadata or reconciliation hasn't finished.
     * The values in the map are the sets of partitions contained in the target assignment but
     * missing from the currently reconciled assignment, for each topic.
     *
     * Visible for testing.
     */
    Map<Uuid, SortedSet<Integer>> topicPartitionsAwaitingReconciliation() {
        final Map<Uuid, SortedSet<Integer>> topicPartitionMap = new HashMap<>();
        currentTargetAssignment.forEach((topicId, targetPartitions) -> {
            final SortedSet<Integer> reconciledPartitions = currentAssignment.get(topicId);
            if (!targetPartitions.equals(reconciledPartitions)) {
                final TreeSet<Integer> missingPartitions = new TreeSet<>(targetPartitions);
                if (reconciledPartitions != null) {
                    missingPartitions.removeAll(reconciledPartitions);
                }
                topicPartitionMap.put(topicId, missingPartitions);
            }
        });
        return Collections.unmodifiableMap(topicPartitionMap);
    }

    /**
     * @return If there is a reconciliation in process now. Note that reconciliation is triggered
     * by a call to {@link #maybeReconcile()}. Visible for testing.
     */
    boolean reconciliationInProgress() {
        return reconciliationInProgress;
    }

    /**
     * Register a new listener that will be invoked whenever the member state changes, or a new
     * member ID or epoch is received.
     *
     * @param listener Listener to invoke.
     */
    public void registerStateListener(MemberStateListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("State updates listener cannot be null");
        }
        this.stateUpdatesListeners.add(listener);
    }

    @Override
    public PollResult poll(long currentTimeMs) {
        if (state == MemberState.RECONCILING) {
            maybeReconcile();
        }
        return PollResult.EMPTY;
    }
}
