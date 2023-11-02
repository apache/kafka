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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.Utils.TopicPartitionComparator;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

/**
 * Membership manager that maintains group membership for a single member, following the new
 * consumer group protocol.
 * <p/>
 * This is responsible for:
 * <li>Keeping member info (ex. member id, member epoch, assignment, etc.)</li>
 * <li>Keeping member state as defined in {@link MemberState}.</li>
 * <p/>
 * Member info and state are updated based on the heartbeat responses the member receives.
 */
public class MembershipManagerImpl implements MembershipManager {

    /**
     * Group ID of the consumer group the member will be part of, provided when creating the current
     * membership manager.
     */
    private final String groupId;

    /**
     * Group instance ID to be used by the member, provided when creating the current membership manager.
     */
    private final Optional<String> groupInstanceId;

    /**
     * Member ID assigned by the server to the member, received in a heartbeat response when
     * joining the group specified in {@link #groupId}
     */
    private String memberId;

    /**
     * Current epoch of the member. It will be set to 0 by the member, and provided to the server
     * on the heartbeat request, to join the group. It will be then maintained by the server,
     * incremented as the member reconciles and acknowledges the assignments it receives. It will
     * be reset to 0 if the member gets fenced.
     */
    private int memberEpoch;

    /**
     * Current state of this member as part of the consumer group, as defined in {@link MemberState}
     */
    private MemberState state;

    /**
     * Name of the server-side assignor this member has configured to use. It will be sent
     * out to the server on the {@link ConsumerGroupHeartbeatRequest}. If not defined, the server
     * will select the assignor implementation to use.
     */
    private final Optional<String> serverAssignor;

    /**
     * Assignment that the member received from the server and successfully processed.
     */
    private Set<TopicPartition> currentAssignment;

    /**
     * Assignment that the member received from the server but hasn't completely processed
     * yet.
     */
    private Optional<ConsumerGroupHeartbeatResponseData.Assignment> targetAssignment;

    /**
     * Subscription state object holding the current assignment the member has for the topics it
     * subscribed to.
     */
    private final SubscriptionState subscriptions;

    /**
     * Metadata that allows us to create the partitions needed for {@link ConsumerRebalanceListener}.
     */
    private final ConsumerMetadata metadata;

    /**
     * TopicPartition comparator based on topic name and partition id.
     */
    private final static TopicPartitionComparator COMPARATOR = new TopicPartitionComparator();

    /**
     * Logger.
     */
    private final Logger log;

    /**
     * Manager to perform commit requests needed before revoking partitions (if auto-commit is
     * enabled)
     */
    private final CommitRequestManager commitRequestManager;

    /**
     * Manager to perform metadata requests. Used to get topic metadata when needed for resolving
     * topic names for topic IDs received in a target assignment.
     */
    private final TopicMetadataRequestManager metadataRequestManager;

    public MembershipManagerImpl(String groupId,
                                 SubscriptionState subscriptions,
                                 CommitRequestManager commitRequestManager,
                                 TopicMetadataRequestManager metadataRequestManager,
                                 ConsumerMetadata metadata,
                                 LogContext logContext) {
        this(groupId, null, null, subscriptions, commitRequestManager, metadataRequestManager,
                metadata, logContext);
    }

    public MembershipManagerImpl(String groupId,
                                 String groupInstanceId,
                                 String serverAssignor,
                                 SubscriptionState subscriptions,
                                 CommitRequestManager commitRequestManager,
                                 TopicMetadataRequestManager metadataRequestManager,
                                 ConsumerMetadata metadata,
                                 LogContext logContext) {
        this.groupId = groupId;
        this.state = MemberState.NOT_IN_GROUP;
        this.serverAssignor = Optional.ofNullable(serverAssignor);
        this.groupInstanceId = Optional.ofNullable(groupInstanceId);
        this.targetAssignment = Optional.empty();
        this.subscriptions = subscriptions;
        this.commitRequestManager = commitRequestManager;
        this.metadataRequestManager = metadataRequestManager;
        this.metadata = metadata;
        this.log = logContext.logger(MembershipManagerImpl.class);
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
        log.trace("Member {} transitioned from {} to {}.", memberId, state, nextState);
        this.state = nextState;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String groupId() {
        return groupId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> groupInstanceId() {
        return groupInstanceId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String memberId() {
        return memberId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int memberEpoch() {
        return memberEpoch;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onHeartbeatResponseReceived(ConsumerGroupHeartbeatResponseData response) {
        if (response.errorCode() != Errors.NONE.code()) {
            String errorMessage = String.format(
                    "Unexpected error in Heartbeat response. Expected no error, but received: %s",
                    Errors.forCode(response.errorCode())
            );
            throw new IllegalArgumentException(errorMessage);
        }
        this.memberId = response.memberId();
        this.memberEpoch = response.memberEpoch();
        ConsumerGroupHeartbeatResponseData.Assignment assignment = response.assignment();
        if (assignment != null) {
            setTargetAssignment(assignment);
            transitionTo(MemberState.RECONCILING);
            reconcile(targetAssignment.get());
        } else {
            transitionTo(MemberState.STABLE);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void transitionToFenced() {
        resetEpoch();
        transitionTo(MemberState.FENCED);

        // Release assignment
        CompletableFuture<Void> callbackResult = invokeOnPartitionsRevokedOrLostToReleaseAssignment();
        callbackResult.whenComplete((result, error) -> {
            if (error != null) {
                log.debug("OnPartitionsLost callback invocation failed while releasing assignment" +
                        "after member got fenced. Member will rejoin the group anyways.", error);
            }
            subscriptions.assignFromSubscribed(Collections.emptySet());
            transitionToJoinGroup();
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void transitionToFailed() {
        log.error("Member {} transitioned to {} state", memberId, MemberState.FATAL);

        // Update epoch to indicate that the member is not in the group anymore, so that the
        // onPartitionsLost is called to release assignment.
        memberEpoch = -1;
        invokeOnPartitionsRevokedOrLostToReleaseAssignment();

        transitionTo(MemberState.FATAL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void transitionToJoinGroup() {
        resetEpoch();
        transitionTo(MemberState.JOINING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Void> leaveGroup() {
        transitionTo(MemberState.LEAVING_GROUP);

        CompletableFuture<Void> callbackResult = invokeOnPartitionsRevokedOrLostToReleaseAssignment();
        callbackResult.whenComplete((result, error) -> {

            // Clear the subscription, no matter if the callback execution failed or succeeded.
            subscriptions.assignFromSubscribed(Collections.emptySet());

            // Transition to ensure that a heartbeat request is sent out to effectively leave the
            // group (even in the case where the member had no assignment to release or when the
            // callback execution failed.)
            transitionToSendingLeaveGroup();

        });

        // Return callback future to indicate that the leave group is done when the callbacks
        // complete, without waiting for the heartbeat to be sent out. (Best effort to send it
        // but do not hold the leave group operation for it)
        return callbackResult;
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
        SortedSet<TopicPartition> droppedPartitions = new TreeSet<>(COMPARATOR);
        droppedPartitions.addAll(subscriptions.assignedPartitions());

        CompletableFuture<Void> callbackResult;
        if (droppedPartitions.isEmpty()) {
            // No assignment to release
            callbackResult = CompletableFuture.completedFuture(null);
        } else {
            // Release assignment
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
     * Reset member epoch to the value required for the leave the group heartbeat request, and
     * transition to the {@link MemberState#SENDING_LEAVE_REQUEST} state so that a heartbeat
     * request is sent out with it.
     */
    private void transitionToSendingLeaveGroup() {
        memberEpoch = leaveGroupEpoch();
        currentAssignment = new HashSet<>();
        targetAssignment = Optional.empty();
        transitionTo(MemberState.SENDING_LEAVE_REQUEST);
    }

    /**
     * Return the epoch to use in the Heartbeat request to indicate that the member wants to
     * leave the group. Should be -2 if this is a static member, or -1 in any other case.
     */
    private int leaveGroupEpoch() {
        return groupInstanceId.isPresent() ? -2 : -1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean shouldHeartbeatNow() {
        return state() == MemberState.ACKNOWLEDGING_RECONCILED_ASSIGNMENT ||
                state() == MemberState.SENDING_LEAVE_REQUEST;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onHeartbeatRequestSent() {
        if (state() == MemberState.ACKNOWLEDGING_RECONCILED_ASSIGNMENT) {
            transitionTo(MemberState.STABLE);
        } else if (state() == MemberState.SENDING_LEAVE_REQUEST) {
            transitionTo(MemberState.NOT_IN_GROUP);
        }
    }

    @Override
    public boolean shouldSkipHeartbeat() {
        return state() == MemberState.NOT_IN_GROUP || state() == MemberState.FATAL;
    }

    void reconcile(ConsumerGroupHeartbeatResponseData.Assignment targetAssignment) {

        SortedSet<TopicPartition> ownedPartitions = new TreeSet<>(COMPARATOR);
        ownedPartitions.addAll(subscriptions.assignedPartitions());

        CompletableFuture<SortedSet<TopicPartition>> assignedPartitionsByNameResult = extractTopicPartitionsFromAssignment(targetAssignment);

        assignedPartitionsByNameResult.whenComplete((assignedPartitions, metadataError) -> {
            if (metadataError != null) {
                log.error("Reconciliation failed due to error getting metadata to resolve topic " +
                        "names for topic IDs {} in target assignment.", targetAssignment);
                // TODO: failing reconciliation (no ack sent to the broker), but leaving
                //  member in STABLE state. Double check if any other action should be taken
                //  here.
                transitionTo(MemberState.STABLE);
                return;
            }

            if (!subscriptions.checkAssignmentMatchedSubscription(assignedPartitions)) {
                log.debug("Target assignment {} does not match the current subscription {}; it is " +
                                "likely that the subscription has changed since we joined the group, " +
                                "will re-join with current subscription",
                        targetAssignment,
                        subscriptions.prettyString());
                transitionToJoinGroup();
                return;
            }

            // Partitions to assign (not previously owned)
            SortedSet<TopicPartition> addedPartitions = new TreeSet<>(COMPARATOR);
            addedPartitions.addAll(assignedPartitions);
            addedPartitions.removeAll(ownedPartitions);


            // Partitions to revoke
            SortedSet<TopicPartition> revokedPartitions = new TreeSet<>(COMPARATOR);
            revokedPartitions.addAll(ownedPartitions);
            revokedPartitions.removeAll(assignedPartitions);

            log.info("Updating assignment with\n" +
                            "\tAssigned partitions:                       {}\n" +
                            "\tCurrent owned partitions:                  {}\n" +
                            "\tAdded partitions (assigned - owned):       {}\n" +
                            "\tRevoked partitions (owned - assigned):     {}\n",
                    assignedPartitions,
                    ownedPartitions,
                    addedPartitions,
                    revokedPartitions
            );

            CompletableFuture<Void> revocationResult;
            if (!revokedPartitions.isEmpty()) {
                revocationResult = revokePartitions(revokedPartitions);
            } else {
                revocationResult = CompletableFuture.completedFuture(null);
                // Reschedule the auto commit starting from now (new assignment received without any
                // revocation).
                commitRequestManager.resetAutoCommitTimer();
            }

            // Future that will complete when the full reconciliation process completes (revocation
            // and assignment, executed sequentially)
            CompletableFuture<Void> reconciliationResult =
                    revocationResult.thenCompose(r -> {
                        if (state == MemberState.RECONCILING) {
                            // Make assignment effective on the client by updating the subscription state.
                            subscriptions.assignFromSubscribed(assignedPartitions);
                            // Invoke user call back
                            return invokeOnPartitionsAssignedCallback(addedPartitions);
                        } else {
                            // Revocation callback completed but member already moved out of the
                            // reconciling state.
                            CompletableFuture<Void> res = new CompletableFuture<>();
                            res.completeExceptionally(new KafkaException("Interrupting " +
                                    "reconciliation after revocation, as the member already " +
                                    "transitioned out of the reconciling state into " + state));
                            return res;
                        }
                    });

            reconciliationResult.whenComplete((result, error) -> {
                if (error != null) {
                    log.error("Reconciliation failed. ", error);
                } else {
                    if (state == MemberState.RECONCILING) {

                        // Make assignment effective on the broker by transitioning to send acknowledge.
                        transitionTo(MemberState.ACKNOWLEDGING_RECONCILED_ASSIGNMENT);

                        // Make assignment effective on the member group manager
                        this.currentAssignment = assignedPartitions;
                        this.targetAssignment = Optional.empty();

                    } else {
                        log.debug("New assignment processing completed but the member already " +
                                "transitioned out of the reconciliation state into {}. Interrupting " +
                                "reconciliation as it's not relevant anymore,", state);
                        // TODO: double check if subscription state changes needed. This is expected to be
                        //  the case where the member got fenced, failed or unsubscribed while the
                        //  reconciliation was in process. Transitions to those states update the
                        //  subscription state accordingly so it shouldn't be necessary to make any changes
                        //  to the subscription state at this point.
                    }
                }
            });
        });
    }

    /**
     * Build set of TopicPartition (topic name and partition id) from the assignment received
     * from the broker (topic IDs and list of partitions). For each topic ID this will attempt to
     * find the topic name in the metadata. If a topic ID is not found, this will request a
     * metadata update, and the reconciliation will resume the topic metadata is received.
     *
     * @param assignment Assignment received from the broker, containing partitions grouped by
     *                   topic id.
     * @return Set of {@link TopicPartition} containing topic name and partition id.
     */
    private CompletableFuture<SortedSet<TopicPartition>> extractTopicPartitionsFromAssignment(
            ConsumerGroupHeartbeatResponseData.Assignment assignment) {
        SortedSet<TopicPartition> assignedPartitions = new TreeSet<>(COMPARATOR);

        List<Uuid> topicsRequiringMetadata = new ArrayList<>();
        assignment.topicPartitions().forEach(topicPartitions -> {
            Uuid topicId = topicPartitions.topicId();
            if (!metadata.topicNames().containsKey(topicId)) {
                topicsRequiringMetadata.add(topicId);
            } else {
                String topicName = metadata.topicNames().get(topicId);
                topicPartitions.partitions().forEach(tp -> assignedPartitions.add(new TopicPartition(topicName, tp)));
            }

        });

        if (topicsRequiringMetadata.isEmpty()) {
            return CompletableFuture.completedFuture(assignedPartitions);
        } else {
            return resolveTopicNamesForTopicIds(topicsRequiringMetadata, assignedPartitions);
        }
    }

    /**
     * Perform a topic metadata request to discover topic names for the given topic ids.
     *
     * @param topicsRequiringMetadata List of topic Uuid for which topic names are needed.
     * @param resolvedTopicPartitions List of TopicPartitions for the topics with known names.
     *                                This list will be extended when the missing topic names are
     *                                received in metadata.
     *
     * @return Future that will complete when topic names are received for all
     * topicsRequiringMetadata. It will fail if a metadata response is received but does not
     * include all the topics that were requested.
     */
    private CompletableFuture<SortedSet<TopicPartition>> resolveTopicNamesForTopicIds(
            List<Uuid> topicsRequiringMetadata,
            SortedSet<TopicPartition> resolvedTopicPartitions) {
        CompletableFuture<SortedSet<TopicPartition>> result = new CompletableFuture<>();
        log.debug("Topic IDs {} received in assignment were not found in metadata. " +
                "Requesting metadata to resolve topic names and proceed with the " +
                "reconciliation.", topicsRequiringMetadata);
        // TODO: request metadata only for the topics that require it. Passing empty list to
        //  retrieve it for all topics until the TopicMetadataRequestManager supports a list
        //  of topics.
        CompletableFuture<Map<Topic, List<PartitionInfo>>> metadataResult = metadataRequestManager.requestTopicMetadata(Optional.empty());
        metadataResult.whenComplete((topicNameAndPartitionInfo, error) -> {
            if (error != null) {
                // Metadata request to get topic names failed. The TopicMetadataManager
                // handles retries on retriable errors, so at this point we consider this a
                // fatal error.
                log.error("Metadata request for topic IDs {} received in assignment failed.",
                        topicsRequiringMetadata, error);
                result.completeExceptionally(new KafkaException("Failed to get metadata for " +
                        "topic IDs received in target assignment.", error));
            } else {
                topicNameAndPartitionInfo.forEach((topic, partitionInfoList) -> {
                    if (topicsRequiringMetadata.contains(topic.topicId())) {
                        partitionInfoList.forEach(partitionInfo ->
                                resolvedTopicPartitions.add(new TopicPartition(topic.topicName(), partitionInfo.partition())));
                        topicsRequiringMetadata.remove(topic.topicId());
                    }
                });
                if (topicsRequiringMetadata.isEmpty()) {
                    result.complete(resolvedTopicPartitions);
                } else {
                    // TODO: check if this could happen. If so, we probably need to retry the
                    //  metadata request. Failing for now as simple initial approach.
                    result.completeExceptionally(new KafkaException("Failed to resolve topic " +
                            "names for all topic IDs received in target assignment."));
                }
            }
        });
        return result;
    }

    /**
     * Revoke partitions. This will:
     * <ul>
     *     <li>Trigger an async commit offsets request if auto-commit enabled.</li>
     *     <li>Invoke the onPartitionsRevoked callback if the user has registered it.</li>
     * </ul>
     *
     * This will wait on the commit request to finish before invoking the callback. If the commit
     * request fails, this will proceed to invoke the user callbacks anyway,
     * returning a future that will complete or fail depending on the callback execution only.
     *
     * @param revokedPartitions Partitions to revoke.
     * @return Future that will complete when the commit request and user callback completes.
     */
    private CompletableFuture<Void> revokePartitions(Set<TopicPartition> revokedPartitions) {
        log.info("Revoking previously assigned partitions {}", Utils.join(revokedPartitions, ", "));

        logPausedPartitionsBeingRevoked(revokedPartitions);

        // Mark partitions as pending revocation to stop fetching from the partitions (no new
        // fetches sent out, and no in-flight fetches responses processed).
        markPendingRevocationToPauseFetching(revokedPartitions);

        // Future that will complete when the revocation completes (including offset commit
        // request and user callback execution)
        CompletableFuture<Void> revocationResult = new CompletableFuture<>();

        // Commit offsets if auto-commit enabled.
        CompletableFuture<Void> commitResult;
        if (commitRequestManager.autoCommitEnabled()) {
            commitResult = commitRequestManager.maybeAutoCommitAllConsumed();
        } else {
            commitResult = CompletableFuture.completedFuture(null);
        }

        commitResult.whenComplete((result, error) -> {

            if (error != null) {
                // Commit request failed (commit request manager internally retries on
                // retriable errors, so at this point we assume this is non-retriable, but
                // proceed with the revocation anyway).
                log.debug("Commit request before revocation failed with non-retriable error. Will" +
                        " proceed with the revocation anyway.", error);
            }

            CompletableFuture<Void> userCallbackResult = invokeOnPartitionsRevokedCallback(revokedPartitions);
            userCallbackResult.whenComplete((callbackResult, callbackError) -> {
                if (callbackError != null) {
                    log.error("User provided callback failed on invocation of onPartitionsRevoked" +
                            " for partitions {}", revokedPartitions, callbackError);
                    revocationResult.completeExceptionally(callbackError);
                } else {
                    revocationResult.complete(null);
                }

            });
        });
        return revocationResult;
    }

    /**
     * Mark partitions as 'pending revocation', to effectively stop fetching while waiting for
     * the commit offsets request to complete, and ensure the application's position don't get
     * ahead of the committed positions. This mark will ensure that:
     * <ul>
     *     <li>No new fetches will be sent out for the partitions being revoked</li>
     *     <li>Previous in-flight fetch requests that may complete while the partitions are being revoked won't be processed.</li>
     * </ul>
     */
    private void markPendingRevocationToPauseFetching(Set<TopicPartition> partitionsToRevoke) {
        // When asynchronously committing offsets prior to the revocation of a set of partitions, there will be a
        // window of time between when the offset commit is sent and when it returns and revocation completes. It is
        // possible for pending fetches for these partitions to return during this time, which means the application's
        // position may get ahead of the committed position prior to revocation. This can cause duplicate consumption.
        // To prevent this, we mark the partitions as "pending revocation," which stops the Fetcher from sending new
        // fetches or returning data from previous fetches to the user.
        log.debug("Marking partitions pending for revocation: {}", partitionsToRevoke);
        subscriptions.markPendingRevocation(partitionsToRevoke);
    }

    private CompletableFuture<Void> invokeOnPartitionsRevokedCallback(Set<TopicPartition> partitionsRevoked) {
        Optional<ConsumerRebalanceListener> listener = subscriptions.rebalanceListener();
        if (listener.isPresent()) {
            throw new UnsupportedOperationException("User-defined callbacks not supported yet");
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Void> invokeOnPartitionsAssignedCallback(Set<TopicPartition> partitionsAssigned) {
        Optional<ConsumerRebalanceListener> listener = subscriptions.rebalanceListener();
        if (listener.isPresent()) {
            throw new UnsupportedOperationException("User-defined callbacks not supported yet");
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Void> invokeOnPartitionsLostCallback(Set<TopicPartition> partitionsLost) {
        Optional<ConsumerRebalanceListener> listener = subscriptions.rebalanceListener();
        if (listener.isPresent()) {
            throw new UnsupportedOperationException("User-defined callbacks not supported yet");
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Log partitions being revoked that were already paused, since the pause flag will be
     * effectively lost.
     */
    private void logPausedPartitionsBeingRevoked(Set<TopicPartition> partitionsToRevoke) {
        Set<TopicPartition> revokePausedPartitions = subscriptions.pausedPartitions();
        revokePausedPartitions.retainAll(partitionsToRevoke);
        if (!revokePausedPartitions.isEmpty()) {
            log.info("The pause flag in partitions [{}] will be removed due to revocation.", Utils.join(revokePausedPartitions, ", "));
        }
    }

    /**
     * Take new target assignment received from the server and set it as targetAssignment to be
     * processed. Following the consumer group protocol, the server won't send a new target
     * member while a previous one hasn't been acknowledged by the member, so this will fail
     * if a target assignment already exists.
     *
     * @throws IllegalStateException If a target assignment already exists.
     */
    private void setTargetAssignment(ConsumerGroupHeartbeatResponseData.Assignment newTargetAssignment) {
        if (!targetAssignment.isPresent()) {
            log.info("Member {} accepted new target assignment {} to reconcile", memberId, newTargetAssignment);
            targetAssignment = Optional.of(newTargetAssignment);
        } else {
            transitionToFailed();
            throw new IllegalStateException("Cannot set new target assignment because a " +
                    "previous one pending to be reconciled already exists.");
        }
    }

    private void resetEpoch() {
        this.memberEpoch = 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MemberState state() {
        return state;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> serverAssignor() {
        return this.serverAssignor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<TopicPartition> currentAssignment() {
        return this.currentAssignment;
    }


    /**
     * @return Assignment that the member received from the server but hasn't completely processed
     * yet.
     */
    public Optional<ConsumerGroupHeartbeatResponseData.Assignment> targetAssignment() {
        return targetAssignment;
    }
}
