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
import org.apache.kafka.clients.consumer.internals.Utils.TopicPartitionComparator;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

/**
 * Group manager for a single consumer that has configured a group id as defined in
 * {@link ConsumerConfig#GROUP_ID_CONFIG}, to use the Kafka-based offset management capability, and
 * the consumer group protocol to get automatically assigned partitions when calling the
 * subscribe API.
 *
 * <p/>
 *
 * While the subscribe API hasn't been called (or if the consumer called unsubscribe), this manager
 * will only be responsible for keeping the member in a {@link MemberState#NOT_IN_GROUP} state,
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
 * The reconciliation process is responsible for applying a new target assignment received from
 * the broker. It involves multiple async operations, so the member will continue to heartbeat
 * while these operations complete, to make sure that the member stays in the group while
 * reconciling.
 *
 * <p/>
 *
 * Reconciliation steps:
 * <ol>
 *     <li>Resolve topic names for all topic IDs received in the target assignment.</li>
 *     <li>Commit offsets if auto-commit is enabled.</li>
 *     <li>Invoke the user-defined onPartitionsRevoked listener.</li>
 *     <li>Invoke the user-defined onPartitionsAssigned listener.</li>
 *     <li>When the above steps complete, the member acknowledges the target assignment by
 *     sending a heartbeat request back to the broker, including the full target assignment
 *     that was just reconciled.</li>
 * </ol>
 *
 * Note that user-defined callbacks are triggered from this manager that runs in the
 * BackgroundThread, but executed in the Application Thread, where a failure will be returned to
 * the user if the callbacks fail. This manager is only concerned about the callbacks completion to
 * know that it can proceed with the reconciliation.
 */
public class MembershipManagerImpl implements MembershipManager, ClusterResourceListener {

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
     * Future that will complete when topic names have been found in metadata for all topic IDs
     * received in an assignment, indicating that they have all been added to the local cache.
     * Once this future completes, the reconciliation process resumes, to reconcile the assignment
     * now with known topic names.
     */
    private CompletableFuture<Void> allAssignedTopicNamesResolvedInLocalCache;

    /**
     * Local cache of assigned topic IDs and names. Topics are added here when received in a
     * target assignment, as we discover their names in the Metadata cache, and removed when the
     * topic is not in the subscription anymore. The purpose of this cache is to avoid metadata
     * requests in cases where a currently assigned topic is in the target assignment (new
     * partition assigned, or revoked), but it is not present the Metadata cache at that moment.
     * The cache is cleared when the subscription changes ({@link #transitionToJoining()}, the
     * member fails ({@link #transitionToFatal()} or leaves the group ({@link #leaveGroup()}).
     */
    private Map<Uuid, String> assignedTopicNamesCache;

    /**
     * Topic IDs received in a target assignment for which we haven't found topic names yet. This
     * is initially populated when a target assignment is received, and then is updated every
     * time a metadata update is received (items removed as topic names are discovered, and names
     * saved to the local {@link #assignedTopicNamesCache}). Once this set is empty (topic names have been
     * found for all assigned topic IDs), we can resume the reconciliation process.
     */
    private Set<Uuid> unknownTopicIdsFromTargetAssignment;

    /**
     * Epoch that a member must include a heartbeat request to indicate that it want to join or
     * re-join a group.
     */
    public static final int JOIN_GROUP_EPOCH = 0;

    /**
     * Epoch that a member (not static) must include in a heartbeat request to indicate that it
     * wants to leave the group. This is considered as a definitive leave.
     */
    public static final int LEAVE_GROUP_EPOCH = -1;

    /**
     * Epoch that a static member (member with group instance id) must include in a heartbeat
     * request to indicate that it wants to leave the group. This will be considered as a
     * potentially temporary leave.
     */
    public static final int LEAVE_GROUP_EPOCH_FOR_STATIC_MEMBER = -2;


    public MembershipManagerImpl(String groupId,
                                 SubscriptionState subscriptions,
                                 CommitRequestManager commitRequestManager,
                                 ConsumerMetadata metadata,
                                 LogContext logContext) {
        this(groupId, null, null, subscriptions, commitRequestManager, metadata, logContext);
    }

    public MembershipManagerImpl(String groupId,
                                 String groupInstanceId,
                                 String serverAssignor,
                                 SubscriptionState subscriptions,
                                 CommitRequestManager commitRequestManager,
                                 ConsumerMetadata metadata,
                                 LogContext logContext) {
        this.groupId = groupId;
        this.state = MemberState.NOT_IN_GROUP;
        this.serverAssignor = Optional.ofNullable(serverAssignor);
        this.groupInstanceId = Optional.ofNullable(groupInstanceId);
        this.targetAssignment = Optional.empty();
        this.subscriptions = subscriptions;
        this.commitRequestManager = commitRequestManager;
        this.metadata = metadata;
        this.assignedTopicNamesCache = new HashMap<>();
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
            transitionToJoining();
        });

        clearAssignedTopicNamesCache();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void transitionToFatal() {
        log.error("Member {} transitioned to {} state", memberId, MemberState.FATAL);

        // Update epoch to indicate that the member is not in the group anymore, so that the
        // onPartitionsLost is called to release assignment.
        memberEpoch = LEAVE_GROUP_EPOCH;
        invokeOnPartitionsRevokedOrLostToReleaseAssignment();

        clearAssignedTopicNamesCache();
        transitionTo(MemberState.FATAL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void transitionToJoining() {
        resetEpoch();
        transitionTo(MemberState.JOINING);
        clearAssignedTopicNamesCache();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Void> leaveGroup() {
        transitionTo(MemberState.LEAVING);

        CompletableFuture<Void> callbackResult = invokeOnPartitionsRevokedOrLostToReleaseAssignment();
        callbackResult.whenComplete((result, error) -> {

            // Clear the subscription, no matter if the callback execution failed or succeeded.
            subscriptions.assignFromSubscribed(Collections.emptySet());

            // Transition to ensure that a heartbeat request is sent out to effectively leave the
            // group (even in the case where the member had no assignment to release or when the
            // callback execution failed.)
            transitionToSendingLeaveGroup();

        });

        clearAssignedTopicNamesCache();

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
            // Remove all topic IDs and names from local cache
            callbackResult.whenComplete((result, error) -> clearAssignedTopicNamesCache());
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
        return groupInstanceId.isPresent() ? LEAVE_GROUP_EPOCH_FOR_STATIC_MEMBER : LEAVE_GROUP_EPOCH;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean shouldHeartbeatNow() {
        return state() == MemberState.SENDING_ACK_FOR_RECONCILED_ASSIGNMENT ||
                state() == MemberState.SENDING_LEAVE_REQUEST;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onHeartbeatRequestSent() {
        if (state() == MemberState.SENDING_ACK_FOR_RECONCILED_ASSIGNMENT) {
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
                transitionToJoining();
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

                            // Clear topic names cache only for topics that are not in the subscription anymore
                            for (TopicPartition tp : revokedPartitions) {
                                if (!subscriptions.subscription().contains(tp.topic())) {
                                    assignedTopicNamesCache.values().remove(tp.topic());
                                }
                            }

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
                    // Leaving member in RECONCILING state after callbacks fail. The member
                    // won't send the ack, and the expectation is that the broker will kick the
                    // member out of the group after the rebalance timeout expires, leading to a
                    // RECONCILING -> FENCED transition.
                    log.error("Reconciliation failed. ", error);
                } else {
                    if (state == MemberState.RECONCILING) {

                        // Make assignment effective on the broker by transitioning to send acknowledge.
                        transitionTo(MemberState.SENDING_ACK_FOR_RECONCILED_ASSIGNMENT);

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
     * Build set of TopicPartition (topic name and partition id) from the target assignment
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
     *
     * @param assignment Assignment received from the broker, containing partitions grouped by
     *                   topic id.
     * @return Set of {@link TopicPartition} containing topic name and partition id.
     */
    private CompletableFuture<SortedSet<TopicPartition>> extractTopicPartitionsFromAssignment(
            ConsumerGroupHeartbeatResponseData.Assignment assignment) {
        SortedSet<TopicPartition> assignedPartitions = new TreeSet<>(COMPARATOR);

        // Try to resolve topic names from metadata cache or subscription cache
        Map<Uuid, List<Integer>> topicsRequiringMetadata = new HashMap<>();

        assignment.topicPartitions().forEach(topicPartitions -> {

            Uuid topicId = topicPartitions.topicId();
            String nameFromMetadataCache = metadata.topicNames().getOrDefault(topicId, null);
            if (nameFromMetadataCache != null) {
                assignedPartitions.addAll(buildAssignedPartitionsWithTopicName(topicPartitions, nameFromMetadataCache));
                // Add topic name to local cache, so it can be reused if included in a next target
                // assignment if metadata cache not available.
                assignedTopicNamesCache.put(topicId, nameFromMetadataCache);
            } else {
                // Topic ID was not found in metadata. Check if the topic name is in the local
                // cache of topics currently assigned. This will avoid a metadata request in the
                // case where the metadata cache may have been flushed right before the
                // revocation of a previously assigned topic.
                String nameFromSubscriptionCache = assignedTopicNamesCache.getOrDefault(topicId, null);
                if (nameFromSubscriptionCache != null) {
                    assignedPartitions.addAll(buildAssignedPartitionsWithTopicName(topicPartitions, nameFromSubscriptionCache));
                } else {
                    topicsRequiringMetadata.put(topicId, topicPartitions.partitions());
                }
            }
        });

        final CompletableFuture<SortedSet<TopicPartition>> assignedPartitionsWithTopicName =
                new CompletableFuture<>();

        if (topicsRequiringMetadata.isEmpty()) {
            assignedPartitionsWithTopicName.complete(assignedPartitions);
        } else {
            log.debug("Topic Ids {} received in target assignment were not found in metadata and " +
                    "are not currently assigned. Requesting a metadata update now to resolve " +
                    "topic names.", topicsRequiringMetadata);
            unknownTopicIdsFromTargetAssignment = new HashSet<>(topicsRequiringMetadata.keySet());
            // Future that will complete only when/if all unknownTopicIdsAssigned are received in
            // metadata updates via the onUpdate callback.
            allAssignedTopicNamesResolvedInLocalCache = new CompletableFuture<>();
            allAssignedTopicNamesResolvedInLocalCache.whenComplete((result, error) -> {
                if (error != null) {
                    // This is unexpected, as this is an internal future that is only completed
                    // successfully on the onUpdate, when all unknown topics are resolved, or it
                    // is not completed at all.
                    assignedPartitionsWithTopicName.completeExceptionally(error);
                } else {
                    // All assigned topic IDs are resolved now in the local cache
                    topicsRequiringMetadata.forEach((topicId, partitions) -> {
                        String topicName = assignedTopicNamesCache.get(topicId);
                        partitions.forEach(tp -> assignedPartitions.add(new TopicPartition(topicName, tp)));
                    });
                    assignedPartitionsWithTopicName.complete(assignedPartitions);
                }
            });
            metadata.requestUpdate(true);
        }

        return assignedPartitionsWithTopicName;
    }

    /**
     * Build set of TopicPartition for the partitions included in the heartbeat topicPartitions,
     * and using the given topic name.
     */
    private SortedSet<TopicPartition> buildAssignedPartitionsWithTopicName(
            ConsumerGroupHeartbeatResponseData.TopicPartitions topicPartitions,
            String topicName) {
        SortedSet<TopicPartition> assignedPartitions = new TreeSet<>(COMPARATOR);
        topicPartitions.partitions().forEach(tp -> assignedPartitions.add(new TopicPartition(topicName,
                tp)));
        return assignedPartitions;
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
     * Remove all elements from the topic names cache.
     */
    private void clearAssignedTopicNamesCache() {
        assignedTopicNamesCache.clear();
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
            transitionToFatal();
            throw new IllegalStateException("Cannot set new target assignment because a " +
                    "previous one pending to be reconciled already exists.");
        }
    }

    private void resetEpoch() {
        this.memberEpoch = JOIN_GROUP_EPOCH;
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
     * yet. Visible for testing.
     */
    Optional<ConsumerGroupHeartbeatResponseData.Assignment> targetAssignment() {
        return targetAssignment;
    }

    /**
     * When cluster metadata is updated, try to resolve topic names for topic IDs received in
     * assignment, for which a topic name hasn't been resolved yet. Discovered topic named will
     * be added to the local topic names cached, to be used in the reconciliation process.
     *
     * <p/>
     *
     * This will incrementally resolve topic names for all unknown topic IDs received in the
     * target assignment that is trying to be reconciled. Once all topic names are resolved, it
     * will signal it so that the reconciliation process can resume. If some topic names are
     * still unknown, this will request another metadata update.
     */
    @Override
    public void onUpdate(ClusterResource clusterResource) {
        if (!unknownTopicIdsFromTargetAssignment.isEmpty()) {
            Iterator<Uuid> iter = unknownTopicIdsFromTargetAssignment.iterator();
            while (iter.hasNext()) {
                Uuid topicId = iter.next();
                if (metadata.topicNames().containsKey(topicId)) {
                    String topicName = metadata.topicNames().get(topicId);
                    assignedTopicNamesCache.put(topicId, topicName);
                    iter.remove();
                }
                if (unknownTopicIdsFromTargetAssignment.isEmpty()) {
                    log.debug("All topic names for topic IDs received in target " +
                            "assignment have been resolved after a metadata update.");
                    allAssignedTopicNamesResolvedInLocalCache.complete(null);
                    return;
                }
            }
            // Still some topic names not known for target assignment, request another metadata update
            log.debug("Topic names for topic IDs {} received in target assignment are still not " +
                    "resolved after a metadata update. Requesting another update now.", unknownTopicIdsFromTargetAssignment);
            metadata.requestUpdate(true);
        }
    }
}
