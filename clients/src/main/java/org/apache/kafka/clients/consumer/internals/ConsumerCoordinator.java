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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
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
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * This class manages the coordination process with the consumer coordinator.
 */
public final class ConsumerCoordinator extends AbstractCoordinator {
    private final GroupRebalanceConfig rebalanceConfig;
    private final Logger log;
    private final List<ConsumerPartitionAssignor> assignors;
    private final ConsumerMetadata metadata;
    private final ConsumerCoordinatorMetrics sensors;
    private final SubscriptionState subscriptions;
    private final OffsetCommitCallback defaultOffsetCommitCallback;
    private final boolean autoCommitEnabled;
    private final int autoCommitIntervalMs;
    private final ConsumerInterceptors<?, ?> interceptors;
    private final AtomicInteger pendingAsyncCommits;

    // this collection must be thread-safe because it is modified from the response handler
    // of offset commit requests, which may be invoked from the heartbeat thread
    private final ConcurrentLinkedQueue<OffsetCommitCompletion> completedOffsetCommits;

    private boolean isLeader = false;
    private Set<String> joinedSubscription;
    private MetadataSnapshot metadataSnapshot;
    private MetadataSnapshot assignmentSnapshot;
    private Timer nextAutoCommitTimer;
    private AtomicBoolean asyncCommitFenced;

    // hold onto request&future for committed offset requests to enable async calls.
    private PendingCommittedOffsetRequest pendingCommittedOffsetRequest = null;

    private static class PendingCommittedOffsetRequest {
        private final Set<TopicPartition> requestedPartitions;
        private final Generation requestedGeneration;
        private final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> response;

        private PendingCommittedOffsetRequest(final Set<TopicPartition> requestedPartitions,
                                              final Generation generationAtRequestTime,
                                              final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> response) {
            this.requestedPartitions = Objects.requireNonNull(requestedPartitions);
            this.response = Objects.requireNonNull(response);
            this.requestedGeneration = generationAtRequestTime;
        }

        private boolean sameRequest(final Set<TopicPartition> currentRequest, final Generation currentGeneration) {
            return Objects.equals(requestedGeneration, currentGeneration) && requestedPartitions.equals(currentRequest);
        }
    }

    private final RebalanceProtocol protocol;

    /**
     * Initialize the coordination manager.
     */
    public ConsumerCoordinator(GroupRebalanceConfig rebalanceConfig,
                               LogContext logContext,
                               ConsumerNetworkClient client,
                               List<ConsumerPartitionAssignor> assignors,
                               ConsumerMetadata metadata,
                               SubscriptionState subscriptions,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time,
                               boolean autoCommitEnabled,
                               int autoCommitIntervalMs,
                               ConsumerInterceptors<?, ?> interceptors) {
        super(rebalanceConfig,
              logContext,
              client,
              metrics,
              metricGrpPrefix,
              time);
        this.rebalanceConfig = rebalanceConfig;
        this.log = logContext.logger(ConsumerCoordinator.class);
        this.metadata = metadata;
        this.metadataSnapshot = new MetadataSnapshot(subscriptions, metadata.fetch(), metadata.updateVersion());
        this.subscriptions = subscriptions;
        this.defaultOffsetCommitCallback = new DefaultOffsetCommitCallback();
        this.autoCommitEnabled = autoCommitEnabled;
        this.autoCommitIntervalMs = autoCommitIntervalMs;
        this.assignors = assignors;
        this.completedOffsetCommits = new ConcurrentLinkedQueue<>();
        this.sensors = new ConsumerCoordinatorMetrics(metrics, metricGrpPrefix);
        this.interceptors = interceptors;
        this.pendingAsyncCommits = new AtomicInteger();
        this.asyncCommitFenced = new AtomicBoolean(false);

        if (autoCommitEnabled)
            this.nextAutoCommitTimer = time.timer(autoCommitIntervalMs);

        // select the rebalance protocol such that:
        //   1. only consider protocols that are supported by all the assignors. If there is no common protocols supported
        //      across all the assignors, throw an exception.
        //   2. if there are multiple protocols that are commonly supported, select the one with the highest id (i.e. the
        //      id number indicates how advanced the protocol is).
        // we know there are at least one assignor in the list, no need to double check for NPE
        if (!assignors.isEmpty()) {
            List<RebalanceProtocol> supportedProtocols = new ArrayList<>(assignors.get(0).supportedProtocols());

            for (ConsumerPartitionAssignor assignor : assignors) {
                supportedProtocols.retainAll(assignor.supportedProtocols());
            }

            if (supportedProtocols.isEmpty()) {
                throw new IllegalArgumentException("Specified assignors " +
                    assignors.stream().map(ConsumerPartitionAssignor::name).collect(Collectors.toSet()) +
                    " do not have commonly supported rebalance protocol");
            }

            Collections.sort(supportedProtocols);

            protocol = supportedProtocols.get(supportedProtocols.size() - 1);
        } else {
            protocol = null;
        }

        this.metadata.requestUpdate();
    }

    @Override
    public String protocolType() {
        return ConsumerProtocol.PROTOCOL_TYPE;
    }

    @Override
    protected JoinGroupRequestData.JoinGroupRequestProtocolCollection metadata() {
        log.debug("Joining group with current subscription: {}", subscriptions.subscription());
        this.joinedSubscription = subscriptions.subscription();
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocolSet = new JoinGroupRequestData.JoinGroupRequestProtocolCollection();

        List<String> topics = new ArrayList<>(joinedSubscription);
        for (ConsumerPartitionAssignor assignor : assignors) {
            Subscription subscription = new Subscription(topics,
                                                         assignor.subscriptionUserData(joinedSubscription),
                                                         subscriptions.assignedPartitionsList());
            ByteBuffer metadata = ConsumerProtocol.serializeSubscription(subscription);

            protocolSet.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
                    .setName(assignor.name())
                    .setMetadata(Utils.toArray(metadata)));
        }
        return protocolSet;
    }

    public void updatePatternSubscription(Cluster cluster) {
        final Set<String> topicsToSubscribe = cluster.topics().stream()
                .filter(subscriptions::matchesSubscribedPattern)
                .collect(Collectors.toSet());
        if (subscriptions.subscribeFromPattern(topicsToSubscribe))
            metadata.requestUpdateForNewTopics();
    }

    private ConsumerPartitionAssignor lookupAssignor(String name) {
        for (ConsumerPartitionAssignor assignor : this.assignors) {
            if (assignor.name().equals(name))
                return assignor;
        }
        return null;
    }

    private void maybeUpdateJoinedSubscription(Set<TopicPartition> assignedPartitions) {
        if (subscriptions.hasPatternSubscription()) {
            // Check if the assignment contains some topics that were not in the original
            // subscription, if yes we will obey what leader has decided and add these topics
            // into the subscriptions as long as they still match the subscribed pattern

            Set<String> addedTopics = new HashSet<>();
            // this is a copy because its handed to listener below
            for (TopicPartition tp : assignedPartitions) {
                if (!joinedSubscription.contains(tp.topic()))
                    addedTopics.add(tp.topic());
            }

            if (!addedTopics.isEmpty()) {
                Set<String> newSubscription = new HashSet<>(subscriptions.subscription());
                Set<String> newJoinedSubscription = new HashSet<>(joinedSubscription);
                newSubscription.addAll(addedTopics);
                newJoinedSubscription.addAll(addedTopics);

                if (this.subscriptions.subscribeFromPattern(newSubscription))
                    metadata.requestUpdateForNewTopics();
                this.joinedSubscription = newJoinedSubscription;
            }
        }
    }

    private Exception invokePartitionsAssigned(final Set<TopicPartition> assignedPartitions) {
        log.info("Adding newly assigned partitions: {}", Utils.join(assignedPartitions, ", "));

        ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
        try {
            listener.onPartitionsAssigned(assignedPartitions);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} failed on invocation of onPartitionsAssigned for partitions {}",
                listener.getClass().getName(), assignedPartitions, e);
            return e;
        }

        return null;
    }

    private Exception invokePartitionsRevoked(final Set<TopicPartition> revokedPartitions) {
        log.info("Revoke previously assigned partitions {}", Utils.join(revokedPartitions, ", "));

        ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
        try {
            listener.onPartitionsRevoked(revokedPartitions);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} failed on invocation of onPartitionsRevoked for partitions {}",
                listener.getClass().getName(), revokedPartitions, e);
            return e;
        }

        return null;
    }

    private Exception invokePartitionsLost(final Set<TopicPartition> lostPartitions) {
        log.info("Lost previously assigned partitions {}", Utils.join(lostPartitions, ", "));

        ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
        try {
            listener.onPartitionsLost(lostPartitions);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} failed on invocation of onPartitionsLost for partitions {}",
                listener.getClass().getName(), lostPartitions, e);
            return e;
        }

        return null;
    }

    @Override
    protected void onJoinComplete(int generation,
                                  String memberId,
                                  String assignmentStrategy,
                                  ByteBuffer assignmentBuffer) {
        log.debug("Executing onJoinComplete with generation {} and memberId {}", generation, memberId);

        // only the leader is responsible for monitoring for metadata changes (i.e. partition changes)
        if (!isLeader)
            assignmentSnapshot = null;

        ConsumerPartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

        Set<TopicPartition> ownedPartitions = new HashSet<>(subscriptions.assignedPartitions());

        Assignment assignment = ConsumerProtocol.deserializeAssignment(assignmentBuffer);

        Set<TopicPartition> assignedPartitions = new HashSet<>(assignment.partitions());

        if (!subscriptions.checkAssignmentMatchedSubscription(assignedPartitions)) {
            log.warn("We received an assignment {} that doesn't match our current subscription {}; it is likely " +
                "that the subscription has changed since we joined the group. Will try re-join the group with current subscription",
                assignment.partitions(), subscriptions.prettyString());

            requestRejoin();

            return;
        }

        // The leader may have assigned partitions which match our subscription pattern, but which
        // were not explicitly requested, so we update the joined subscription here.
        maybeUpdateJoinedSubscription(assignedPartitions);

        // give the assignor a chance to update internal state based on the received assignment
        ConsumerGroupMetadata metadata = new ConsumerGroupMetadata(rebalanceConfig.groupId, generation, memberId, rebalanceConfig.groupInstanceId);
        assignor.onAssignment(assignment, metadata);

        // reschedule the auto commit starting from now
        if (autoCommitEnabled)
            this.nextAutoCommitTimer.updateAndReset(autoCommitIntervalMs);

        // execute the user's callback after rebalance
        final AtomicReference<Exception> firstException = new AtomicReference<>(null);
        Set<TopicPartition> addedPartitions = new HashSet<>(assignedPartitions);
        addedPartitions.removeAll(ownedPartitions);

        switch (protocol) {
            case EAGER:
                // assign partitions that are not yet owned
                subscriptions.assignFromSubscribed(assignedPartitions);

                firstException.compareAndSet(null, invokePartitionsAssigned(addedPartitions));

                break;

            case COOPERATIVE:
                Set<TopicPartition> revokedPartitions = new HashSet<>(ownedPartitions);
                revokedPartitions.removeAll(assignedPartitions);

                log.info("Updating with newly assigned partitions: {}, compare with already owned partitions: {}, " +
                        "newly added partitions: {}, revoking partitions: {}",
                    Utils.join(assignedPartitions, ", "),
                    Utils.join(ownedPartitions, ", "),
                    Utils.join(addedPartitions, ", "),
                    Utils.join(revokedPartitions, ", "));

                // revoke partitions that was previously owned but no longer assigned;
                // note that we should only change the assignment AFTER we've triggered
                // the revoke callback
                if (!revokedPartitions.isEmpty()) {
                    firstException.compareAndSet(null, invokePartitionsRevoked(revokedPartitions));
                }

                subscriptions.assignFromSubscribed(assignedPartitions);

                // add partitions that were not previously owned but are now assigned
                firstException.compareAndSet(null, invokePartitionsAssigned(addedPartitions));

                // if revoked any partitions, need to re-join the group afterwards
                if (!revokedPartitions.isEmpty()) {
                    requestRejoin();
                }

                break;
        }

        if (firstException.get() != null)
            throw new KafkaException("User rebalance callback throws an error", firstException.get());
    }

    void maybeUpdateSubscriptionMetadata() {
        int version = metadata.updateVersion();
        if (version > metadataSnapshot.version) {
            Cluster cluster = metadata.fetch();

            if (subscriptions.hasPatternSubscription())
                updatePatternSubscription(cluster);

            // Update the current snapshot, which will be used to check for subscription
            // changes that would require a rebalance (e.g. new partitions).
            metadataSnapshot = new MetadataSnapshot(subscriptions, cluster, version);
        }
    }

    /**
     * Poll for coordinator events. This ensures that the coordinator is known and that the consumer
     * has joined the group (if it is using group management). This also handles periodic offset commits
     * if they are enabled.
     * <p>
     * Returns early if the timeout expires
     *
     * @param timer Timer bounding how long this method can block
     * @throws KafkaException if the rebalance callback throws an exception
     * @return true iff the operation succeeded
     */
    public boolean poll(Timer timer) {
        maybeUpdateSubscriptionMetadata();

        invokeCompletedOffsetCommitCallbacks();

        if (subscriptions.partitionsAutoAssigned()) {
            if (protocol == null) {
                throw new IllegalStateException("User configured " + ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG +
                    " to empty while trying to subscribe for group protocol to auto assign partitions");
            }
            // Always update the heartbeat last poll time so that the heartbeat thread does not leave the
            // group proactively due to application inactivity even if (say) the coordinator cannot be found.
            pollHeartbeat(timer.currentTimeMs());
            if (coordinatorUnknown() && !ensureCoordinatorReady(timer)) {
                return false;
            }

            if (rejoinNeededOrPending()) {
                // due to a race condition between the initial metadata fetch and the initial rebalance,
                // we need to ensure that the metadata is fresh before joining initially. This ensures
                // that we have matched the pattern against the cluster's topics at least once before joining.
                if (subscriptions.hasPatternSubscription()) {
                    // For consumer group that uses pattern-based subscription, after a topic is created,
                    // any consumer that discovers the topic after metadata refresh can trigger rebalance
                    // across the entire consumer group. Multiple rebalances can be triggered after one topic
                    // creation if consumers refresh metadata at vastly different times. We can significantly
                    // reduce the number of rebalances caused by single topic creation by asking consumer to
                    // refresh metadata before re-joining the group as long as the refresh backoff time has
                    // passed.
                    if (this.metadata.timeToAllowUpdate(timer.currentTimeMs()) == 0) {
                        this.metadata.requestUpdate();
                    }

                    if (!client.ensureFreshMetadata(timer)) {
                        return false;
                    }

                    maybeUpdateSubscriptionMetadata();
                }

                if (!ensureActiveGroup(timer)) {
                    return false;
                }
            }
        } else {
            // For manually assigned partitions, if there are no ready nodes, await metadata.
            // If connections to all nodes fail, wakeups triggered while attempting to send fetch
            // requests result in polls returning immediately, causing a tight loop of polls. Without
            // the wakeup, poll() with no channels would block for the timeout, delaying re-connection.
            // awaitMetadataUpdate() initiates new connections with configured backoff and avoids the busy loop.
            // When group management is used, metadata wait is already performed for this scenario as
            // coordinator is unknown, hence this check is not required.
            if (metadata.updateRequested() && !client.hasReadyNodes(timer.currentTimeMs())) {
                client.awaitMetadataUpdate(timer);
            }
        }

        maybeAutoCommitOffsetsAsync(timer.currentTimeMs());
        return true;
    }

    /**
     * Return the time to the next needed invocation of {@link #poll(Timer)}.
     * @param now current time in milliseconds
     * @return the maximum time in milliseconds the caller should wait before the next invocation of poll()
     */
    public long timeToNextPoll(long now) {
        if (!autoCommitEnabled)
            return timeToNextHeartbeat(now);

        return Math.min(nextAutoCommitTimer.remainingMs(), timeToNextHeartbeat(now));
    }

    private void updateGroupSubscription(Set<String> topics) {
        // the leader will begin watching for changes to any of the topics the group is interested in,
        // which ensures that all metadata changes will eventually be seen
        if (this.subscriptions.groupSubscribe(topics))
            metadata.requestUpdateForNewTopics();

        // update metadata (if needed) and keep track of the metadata used for assignment so that
        // we can check after rebalance completion whether anything has changed
        if (!client.ensureFreshMetadata(time.timer(Long.MAX_VALUE)))
            throw new TimeoutException();

        maybeUpdateSubscriptionMetadata();
    }

    @Override
    protected Map<String, ByteBuffer> performAssignment(String leaderId,
                                                        String assignmentStrategy,
                                                        List<JoinGroupResponseData.JoinGroupResponseMember> allSubscriptions) {
        ConsumerPartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

        Set<String> allSubscribedTopics = new HashSet<>();
        Map<String, Subscription> subscriptions = new HashMap<>();

        // collect all the owned partitions
        Map<String, List<TopicPartition>> ownedPartitions = new HashMap<>();

        for (JoinGroupResponseData.JoinGroupResponseMember memberSubscription : allSubscriptions) {
            Subscription subscription = ConsumerProtocol.deserializeSubscription(ByteBuffer.wrap(memberSubscription.metadata()));
            subscription.setGroupInstanceId(Optional.ofNullable(memberSubscription.groupInstanceId()));
            subscriptions.put(memberSubscription.memberId(), subscription);
            allSubscribedTopics.addAll(subscription.topics());
            ownedPartitions.put(memberSubscription.memberId(), subscription.ownedPartitions());
        }

        // the leader will begin watching for changes to any of the topics the group is interested in,
        // which ensures that all metadata changes will eventually be seen
        updateGroupSubscription(allSubscribedTopics);

        isLeader = true;

        log.debug("Performing assignment using strategy {} with subscriptions {}", assignor.name(), subscriptions);

        Map<String, Assignment> assignments = assignor.assign(metadata.fetch(), new GroupSubscription(subscriptions)).groupAssignment();

        if (protocol == RebalanceProtocol.COOPERATIVE) {
            validateCooperativeAssignment(ownedPartitions, assignments);
        }

        // user-customized assignor may have created some topics that are not in the subscription list
        // and assign their partitions to the members; in this case we would like to update the leader's
        // own metadata with the newly added topics so that it will not trigger a subsequent rebalance
        // when these topics gets updated from metadata refresh.
        //
        // TODO: this is a hack and not something we want to support long-term unless we push regex into the protocol
        //       we may need to modify the PartitionAssignor API to better support this case.
        Set<String> assignedTopics = new HashSet<>();
        for (Assignment assigned : assignments.values()) {
            for (TopicPartition tp : assigned.partitions())
                assignedTopics.add(tp.topic());
        }

        if (!assignedTopics.containsAll(allSubscribedTopics)) {
            Set<String> notAssignedTopics = new HashSet<>(allSubscribedTopics);
            notAssignedTopics.removeAll(assignedTopics);
            log.warn("The following subscribed topics are not assigned to any members: {} ", notAssignedTopics);
        }

        if (!allSubscribedTopics.containsAll(assignedTopics)) {
            Set<String> newlyAddedTopics = new HashSet<>(assignedTopics);
            newlyAddedTopics.removeAll(allSubscribedTopics);
            log.info("The following not-subscribed topics are assigned, and their metadata will be " +
                    "fetched from the brokers: {}", newlyAddedTopics);

            allSubscribedTopics.addAll(assignedTopics);
            updateGroupSubscription(allSubscribedTopics);
        }

        assignmentSnapshot = metadataSnapshot;

        log.debug("Finished assignment for group: {}", assignments);

        Map<String, ByteBuffer> groupAssignment = new HashMap<>();
        for (Map.Entry<String, Assignment> assignmentEntry : assignments.entrySet()) {
            ByteBuffer buffer = ConsumerProtocol.serializeAssignment(assignmentEntry.getValue());
            groupAssignment.put(assignmentEntry.getKey(), buffer);
        }

        return groupAssignment;
    }

    /**
     * Used by COOPERATIVE rebalance protocol only.
     *
     * Validate the assignments returned by the assignor such that no owned partitions are going to
     * be reassigned to a different consumer directly: if the assignor wants to reassign an owned partition,
     * it must first remove it from the new assignment of the current owner so that it is not assigned to any
     * member, and then in the next rebalance it can finally reassign those partitions not owned by anyone to consumers.
     */
    private void validateCooperativeAssignment(final Map<String, List<TopicPartition>> ownedPartitions,
                                               final Map<String, Assignment> assignments) {
        Set<TopicPartition> totalRevokedPartitions = new HashSet<>();
        Set<TopicPartition> totalAddedPartitions = new HashSet<>();
        for (final Map.Entry<String, Assignment> entry : assignments.entrySet()) {
            final Assignment assignment = entry.getValue();
            final Set<TopicPartition> addedPartitions = new HashSet<>(assignment.partitions());
            addedPartitions.removeAll(ownedPartitions.get(entry.getKey()));
            final Set<TopicPartition> revokedPartitions = new HashSet<>(ownedPartitions.get(entry.getKey()));
            revokedPartitions.removeAll(assignment.partitions());

            totalAddedPartitions.addAll(addedPartitions);
            totalRevokedPartitions.addAll(revokedPartitions);
        }

        // if there are overlap between revoked partitions and added partitions, it means some partitions
        // immediately gets re-assigned to another member while it is still claimed by some member
        totalAddedPartitions.retainAll(totalRevokedPartitions);
        if (!totalAddedPartitions.isEmpty()) {
            log.error("With the COOPERATIVE protocol, owned partitions cannot be " +
                "reassigned to other members; however the assignor has reassigned partitions {} which are still owned " +
                "by some members; return the error code to all members to let them stop", totalAddedPartitions);

            throw new IllegalStateException("Assignor supporting the COOPERATIVE protocol violates its requirements");
        }
    }

    @Override
    protected void onJoinPrepare(int generation, String memberId) {
        log.debug("Executing onJoinPrepare with generation {} and memberId {}", generation, memberId);
        // commit offsets prior to rebalance if auto-commit enabled
        maybeAutoCommitOffsetsSync(time.timer(rebalanceConfig.rebalanceTimeoutMs));

        // the generation / member-id can possibly be reset by the heartbeat thread
        // upon getting errors or heartbeat timeouts; in this case whatever is previously
        // owned partitions would be lost, we should trigger the callback and cleanup the assignment;
        // otherwise we can proceed normally and revoke the partitions depending on the protocol,
        // and in that case we should only change the assignment AFTER the revoke callback is triggered
        // so that users can still access the previously owned partitions to commit offsets etc.
        Exception exception = null;
        final Set<TopicPartition> revokedPartitions;
        if (generation == Generation.NO_GENERATION.generationId &&
            memberId.equals(Generation.NO_GENERATION.memberId)) {
            revokedPartitions = new HashSet<>(subscriptions.assignedPartitions());

            if (!revokedPartitions.isEmpty()) {
                log.info("Giving away all assigned partitions as lost since generation has been reset," +
                    "indicating that consumer is no longer part of the group");
                exception = invokePartitionsLost(revokedPartitions);

                subscriptions.assignFromSubscribed(Collections.emptySet());
            }
        } else {
            switch (protocol) {
                case EAGER:
                    // revoke all partitions
                    revokedPartitions = new HashSet<>(subscriptions.assignedPartitions());
                    exception = invokePartitionsRevoked(revokedPartitions);

                    subscriptions.assignFromSubscribed(Collections.emptySet());

                    break;

                case COOPERATIVE:
                    // only revoke those partitions that are not in the subscription any more.
                    Set<TopicPartition> ownedPartitions = new HashSet<>(subscriptions.assignedPartitions());
                    revokedPartitions = ownedPartitions.stream()
                        .filter(tp -> !subscriptions.subscription().contains(tp.topic()))
                        .collect(Collectors.toSet());

                    if (!revokedPartitions.isEmpty()) {
                        exception = invokePartitionsRevoked(revokedPartitions);

                        ownedPartitions.removeAll(revokedPartitions);
                        subscriptions.assignFromSubscribed(ownedPartitions);
                    }

                    break;
            }
        }


        isLeader = false;
        subscriptions.resetGroupSubscription();

        if (exception != null) {
            throw new KafkaException("User rebalance callback throws an error", exception);
        }
    }

    @Override
    public void onLeavePrepare() {
        // we should reset assignment and trigger the callback before leaving group
        Set<TopicPartition> droppedPartitions = new HashSet<>(subscriptions.assignedPartitions());

        if (subscriptions.partitionsAutoAssigned() && !droppedPartitions.isEmpty()) {
            final Exception e = invokePartitionsRevoked(droppedPartitions);

            subscriptions.assignFromSubscribed(Collections.emptySet());

            if (e != null) {
                throw new KafkaException("User rebalance callback throws an error", e);
            }
        }
    }

    /**
     * @throws KafkaException if the callback throws exception
     */
    @Override
    public boolean rejoinNeededOrPending() {
        if (!subscriptions.partitionsAutoAssigned())
            return false;

        // we need to rejoin if we performed the assignment and metadata has changed;
        // also for those owned-but-no-longer-existed partitions we should drop them as lost
        if (assignmentSnapshot != null && !assignmentSnapshot.matches(metadataSnapshot))
            return true;

        // we need to join if our subscription has changed since the last join
        if (joinedSubscription != null && !joinedSubscription.equals(subscriptions.subscription())) {
            return true;
        }

        return super.rejoinNeededOrPending();
    }

    /**
     * Refresh the committed offsets for provided partitions.
     *
     * @param timer Timer bounding how long this method can block
     * @return true iff the operation completed within the timeout
     */
    public boolean refreshCommittedOffsetsIfNeeded(Timer timer) {
        final Set<TopicPartition> missingFetchPositions = subscriptions.missingFetchPositions();

        final Map<TopicPartition, OffsetAndMetadata> offsets = fetchCommittedOffsets(missingFetchPositions, timer);
        if (offsets == null) return false;

        for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            final TopicPartition tp = entry.getKey();
            final OffsetAndMetadata offsetAndMetadata = entry.getValue();
            final ConsumerMetadata.LeaderAndEpoch leaderAndEpoch = metadata.leaderAndEpoch(tp);
            final SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(
                    offsetAndMetadata.offset(), offsetAndMetadata.leaderEpoch(),
                    leaderAndEpoch);

            log.info("Setting offset for partition {} to the committed offset {}", tp, position);
            entry.getValue().leaderEpoch().ifPresent(epoch -> this.metadata.updateLastSeenEpochIfNewer(entry.getKey(), epoch));
            this.subscriptions.seekUnvalidated(tp, position);
        }
        return true;
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

        final Generation generation = generation();
        if (pendingCommittedOffsetRequest != null && !pendingCommittedOffsetRequest.sameRequest(partitions, generation)) {
            // if we were waiting for a different request, then just clear it.
            pendingCommittedOffsetRequest = null;
        }

        do {
            if (!ensureCoordinatorReady(timer)) return null;

            // contact coordinator to fetch committed offsets
            final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future;
            if (pendingCommittedOffsetRequest != null) {
                future = pendingCommittedOffsetRequest.response;
            } else {
                future = sendOffsetFetchRequest(partitions);
                pendingCommittedOffsetRequest = new PendingCommittedOffsetRequest(partitions, generation, future);

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
     * @throws KafkaException if the rebalance callback throws exception
     */
    public void close(final Timer timer) {
        // we do not need to re-enable wakeups since we are closing already
        client.disableWakeups();
        try {
            maybeAutoCommitOffsetsSync(timer);
            while (pendingAsyncCommits.get() > 0 && timer.notExpired()) {
                ensureCoordinatorReady(timer);
                client.poll(timer);
                invokeCompletedOffsetCommitCallbacks();
            }
        } finally {
            super.close(timer);
        }
    }

    // visible for testing
    void invokeCompletedOffsetCommitCallbacks() {
        if (asyncCommitFenced.get()) {
            throw new FencedInstanceIdException("Get fenced exception for group.instance.id "
                + rebalanceConfig.groupInstanceId.orElse("unset_instance_id")
                + ", current member.id is " + memberId());
        }
        while (true) {
            OffsetCommitCompletion completion = completedOffsetCommits.poll();
            if (completion == null) {
                break;
            }
            completion.invoke();
        }
    }

    public void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        invokeCompletedOffsetCommitCallbacks();

        if (!coordinatorUnknown()) {
            doCommitOffsetsAsync(offsets, callback);
        } else {
            // we don't know the current coordinator, so try to find it and then send the commit
            // or fail (we don't want recursive retries which can cause offset commits to arrive
            // out of order). Note that there may be multiple offset commits chained to the same
            // coordinator lookup request. This is fine because the listeners will be invoked in
            // the same order that they were added. Note also that AbstractCoordinator prevents
            // multiple concurrent coordinator lookup requests.
            pendingAsyncCommits.incrementAndGet();
            lookupCoordinator().addListener(new RequestFutureListener<Void>() {
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
     * Commit offsets synchronously. This method will retry until the commit completes successfully
     * or an unrecoverable error is encountered.
     * @param offsets The offsets to be committed
     * @throws org.apache.kafka.common.errors.AuthorizationException if the consumer is not authorized to the group
     *             or to any of the specified partitions. See the exception for more details
     * @throws CommitFailedException if an unrecoverable error occurs before the commit can be completed
     * @throws FencedInstanceIdException if a static member gets fenced
     * @return If the offset commit was successfully sent and a successful response was received from
     *         the coordinator
     */
    public boolean commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets, Timer timer) {
        invokeCompletedOffsetCommitCallbacks();

        if (offsets.isEmpty())
            return true;

        do {
            if (coordinatorUnknown() && !ensureCoordinatorReady(timer)) {
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
                if (exception instanceof RetriableException) {
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

    private void maybeAutoCommitOffsetsSync(Timer timer) {
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

    private class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit with offsets {} failed", offsets, exception);
        }
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
        if (subscriptions.partitionsAutoAssigned()) {
            generation = generation();
            // if the generation is null, we are not part of an active group (and we expect to be).
            // the only thing we can do is fail the commit and let the user rejoin the group in poll()
            if (generation == null) {
                log.info("Failing OffsetCommit request since the consumer is not part of an active group");
                return RequestFuture.failure(new CommitFailedException());
            }
        } else
            generation = Generation.NO_GENERATION;

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
                .compose(new OffsetCommitResponseHandler(offsets));
    }

    private class OffsetCommitResponseHandler extends CoordinatorResponseHandler<OffsetCommitResponse, Void> {

        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        private OffsetCommitResponseHandler(Map<TopicPartition, OffsetAndMetadata> offsets) {
            this.offsets = offsets;
        }

        @Override
        public void handle(OffsetCommitResponse commitResponse, RequestFuture<Void> future) {
            sensors.commitLatency.record(response.requestLatencyMs());
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
                            markCoordinatorUnknown();
                            future.raise(error);
                            return;
                        } else if (error == Errors.FENCED_INSTANCE_ID) {
                            log.error("Received fatal exception: group.instance.id gets fenced");
                            future.raise(error);
                            return;
                        } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                            /* Consumer never tries to commit offset in between join-group and sync-group,
                             * and hence on broker-side it is not expected to see a commit offset request
                             * during CompletingRebalance phase; if it ever happens then broker would return
                             * this error. In this case we should just treat as a fatal CommitFailed exception.
                             * However, we do not need to reset generations and just request re-join, such that
                             * if the caller decides to proceed and poll, it would still try to proceed and re-join normally.
                             */
                            requestRejoin();
                            future.raise(new CommitFailedException());
                            return;
                        } else if (error == Errors.UNKNOWN_MEMBER_ID
                                || error == Errors.ILLEGAL_GENERATION) {
                            // need to reset generation and re-join group
                            resetGenerationOnResponseError(ApiKeys.OFFSET_COMMIT, error);
                            future.raise(new CommitFailedException());
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
     * Fetch the committed offsets for a set of partitions. This is a non-blocking call. The
     * returned future can be polled to get the actual offsets returned from the broker.
     *
     * @param partitions The set of partitions to get offsets for.
     * @return A request future containing the committed offsets.
     */
    private RequestFuture<Map<TopicPartition, OffsetAndMetadata>> sendOffsetFetchRequest(Set<TopicPartition> partitions) {
        Node coordinator = checkAndGetCoordinator();
        if (coordinator == null)
            return RequestFuture.coordinatorNotAvailable();

        log.debug("Fetching committed offsets for partitions: {}", partitions);
        // construct the request
        OffsetFetchRequest.Builder requestBuilder = new OffsetFetchRequest.Builder(this.rebalanceConfig.groupId,
                new ArrayList<>(partitions));

        // send the request with a callback
        return client.send(coordinator, requestBuilder)
                .compose(new OffsetFetchResponseHandler());
    }

    private class OffsetFetchResponseHandler extends CoordinatorResponseHandler<OffsetFetchResponse, Map<TopicPartition, OffsetAndMetadata>> {
        @Override
        public void handle(OffsetFetchResponse response, RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
            if (response.hasError()) {
                Errors error = response.error();
                log.debug("Offset fetch failed: {}", error.message());

                if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                    // just retry
                    future.raise(error);
                } else if (error == Errors.NOT_COORDINATOR || error == Errors.COORDINATOR_NOT_AVAILABLE) {
                    // re-discover the coordinator and retry
                    markCoordinatorUnknown();
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
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData data = entry.getValue();
                if (data.hasError()) {
                    Errors error = data.error;
                    log.debug("Failed to fetch offset for partition {}: {}", tp, error.message());

                    if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        future.raise(new KafkaException("Topic or Partition " + tp + " does not exist"));
                        return;
                    } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                        if (unauthorizedTopics == null) {
                            unauthorizedTopics = new HashSet<>();
                        }
                        unauthorizedTopics.add(tp.topic());
                    } else {
                        future.raise(new KafkaException("Unexpected error in fetch offset response for partition " +
                            tp + ": " + error.message()));
                        return;
                    }
                } else if (data.offset >= 0) {
                    // record the position with the offset (-1 indicates no committed offset to fetch)
                    offsets.put(tp, new OffsetAndMetadata(data.offset, data.leaderEpoch, data.metadata));
                } else {
                    log.info("Found no committed offset for partition {}", tp);
                }
            }

            if (unauthorizedTopics != null) {
                future.raise(new TopicAuthorizationException(unauthorizedTopics));
            } else {
                future.complete(offsets);
            }
        }
    }

    private class ConsumerCoordinatorMetrics {
        private final String metricGrpName;
        private final Sensor commitLatency;

        private ConsumerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.commitLatency = metrics.sensor("commit-latency");
            this.commitLatency.add(metrics.metricName("commit-latency-avg",
                this.metricGrpName,
                "The average time taken for a commit request"), new Avg());
            this.commitLatency.add(metrics.metricName("commit-latency-max",
                this.metricGrpName,
                "The max time taken for a commit request"), new Max());
            this.commitLatency.add(createMeter(metrics, metricGrpName, "commit", "commit calls"));

            Measurable numParts = (config, now) -> subscriptions.numAssignedPartitions();
            metrics.addMetric(metrics.metricName("assigned-partitions",
                this.metricGrpName,
                "The number of partitions currently assigned to this consumer"), numParts);
        }
    }

    private static class MetadataSnapshot {
        private final int version;
        private final Map<String, Integer> partitionsPerTopic;

        private MetadataSnapshot(SubscriptionState subscription, Cluster cluster, int version) {
            Map<String, Integer> partitionsPerTopic = new HashMap<>();
            for (String topic : subscription.groupSubscription()) {
                Integer numPartitions = cluster.partitionCountForTopic(topic);
                if (numPartitions != null)
                    partitionsPerTopic.put(topic, numPartitions);
            }
            this.partitionsPerTopic = partitionsPerTopic;
            this.version = version;
        }

        boolean matches(MetadataSnapshot other) {
            return version == other.version || partitionsPerTopic.equals(other.partitionsPerTopic);
        }

        Map<String, Integer> partitionsPerTopic() {
            return partitionsPerTopic;
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

    /* test-only classes below */
    RebalanceProtocol getProtocol() {
        return protocol;
    }
}
