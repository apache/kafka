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
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.consumer.internals.Utils.TopicPartitionComparator;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceCallbackMetricsManager;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnstableOffsetCommitException;
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
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ASSIGN_FROM_SUBSCRIBED_ASSIGNORS;
import static org.apache.kafka.clients.consumer.CooperativeStickyAssignor.COOPERATIVE_STICKY_ASSIGNOR_NAME;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.COORDINATOR_METRICS_SUFFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.refreshCommittedOffsets;

/**
 * This class manages the coordination process with the consumer coordinator.
 */
public final class ConsumerCoordinator extends AbstractCoordinator {
    private final static TopicPartitionComparator COMPARATOR = new TopicPartitionComparator();

    private final GroupRebalanceConfig rebalanceConfig;
    private final Logger log;
    private final List<ConsumerPartitionAssignor> assignors;
    private final ConsumerMetadata metadata;
    private final ConsumerCoordinatorMetrics coordinatorMetrics;
    private final SubscriptionState subscriptions;
    private final OffsetCommitCallback defaultOffsetCommitCallback;
    private final boolean autoCommitEnabled;
    private final int autoCommitIntervalMs;
    private final ConsumerInterceptors<?, ?> interceptors;
    // track number of async commits for which callback must be called
    // package private for testing
    final AtomicInteger inFlightAsyncCommits;
    // track the number of pending async commits waiting on the coordinator lookup to complete
    private final AtomicInteger pendingAsyncCommits;

    // this collection must be thread-safe because it is modified from the response handler
    // of offset commit requests, which may be invoked from the heartbeat thread
    private final ConcurrentLinkedQueue<OffsetCommitCompletion> completedOffsetCommits;
    private final AtomicBoolean asyncCommitFenced;
    private final boolean throwOnFetchStableOffsetsUnsupported;
    private final Optional<String> rackId;
    private boolean isLeader = false;
    private Set<String> joinedSubscription;
    private MetadataSnapshot metadataSnapshot;
    private MetadataSnapshot assignmentSnapshot;
    private Timer nextAutoCommitTimer;
    private ConsumerGroupMetadata groupMetadata;
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
    // Wraps the logic for invoking the ConsumerRebalanceListener methods
    private final ConsumerRebalanceListenerInvoker rebalanceListenerInvoker;
    // pending commit offset request in onJoinPrepare
    private RequestFuture<Void> autoCommitOffsetRequestFuture = null;
    // a timer for join prepare to know when to stop.
    // it'll set to rebalance timeout so that the member can join the group successfully
    // even though offset commit failed.
    private Timer joinPrepareTimer = null;

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
                               ConsumerInterceptors<?, ?> interceptors,
                               boolean throwOnFetchStableOffsetsUnsupported,
                               String rackId,
                               Optional<ClientTelemetryReporter> clientTelemetryReporter) {
        super(rebalanceConfig,
              logContext,
              client,
              metrics,
              metricGrpPrefix,
              time,
              clientTelemetryReporter);
        this.rebalanceConfig = rebalanceConfig;
        this.log = logContext.logger(ConsumerCoordinator.class);
        this.metadata = metadata;
        this.rackId = rackId == null || rackId.isEmpty() ? Optional.empty() : Optional.of(rackId);
        this.metadataSnapshot = new MetadataSnapshot(this.rackId, subscriptions, metadata.fetch(), metadata.updateVersion());
        this.subscriptions = subscriptions;
        this.defaultOffsetCommitCallback = new DefaultOffsetCommitCallback();
        this.autoCommitEnabled = autoCommitEnabled;
        this.autoCommitIntervalMs = autoCommitIntervalMs;
        this.assignors = assignors;
        this.completedOffsetCommits = new ConcurrentLinkedQueue<>();
        this.coordinatorMetrics = new ConsumerCoordinatorMetrics(metrics, metricGrpPrefix);
        this.interceptors = interceptors;
        this.inFlightAsyncCommits = new AtomicInteger();
        this.pendingAsyncCommits = new AtomicInteger();
        this.asyncCommitFenced = new AtomicBoolean(false);
        this.groupMetadata = new ConsumerGroupMetadata(rebalanceConfig.groupId,
            JoinGroupRequest.UNKNOWN_GENERATION_ID, JoinGroupRequest.UNKNOWN_MEMBER_ID, rebalanceConfig.groupInstanceId);
        this.throwOnFetchStableOffsetsUnsupported = throwOnFetchStableOffsetsUnsupported;

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

        this.rebalanceListenerInvoker = new ConsumerRebalanceListenerInvoker(
            logContext,
            subscriptions,
            time,
            new RebalanceCallbackMetricsManager(metrics, metricGrpPrefix)
        );
        this.metadata.requestUpdate(true);
    }

    // package private for testing
    boolean isLeader() {
        return this.isLeader;
    }

    // package private for testing
    SubscriptionState subscriptionState() {
        return this.subscriptions;
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
                                                         subscriptions.assignedPartitionsList(),
                                                         generation().generationId,
                                                         rackId);
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
            // this is a copy because it's handed to listener below
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

    private Exception invokeOnAssignment(final ConsumerPartitionAssignor assignor, final Assignment assignment) {
        log.info("Notifying assignor about the new {}", assignment);

        try {
            assignor.onAssignment(assignment, groupMetadata);
        } catch (Exception e) {
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

        // Only the leader is responsible for monitoring for metadata changes (i.e. partition changes)
        if (!isLeader)
            assignmentSnapshot = null;

        ConsumerPartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

        // Give the assignor a chance to update internal state based on the received assignment
        groupMetadata = new ConsumerGroupMetadata(rebalanceConfig.groupId, generation, memberId, rebalanceConfig.groupInstanceId);

        SortedSet<TopicPartition> ownedPartitions = new TreeSet<>(COMPARATOR);
        ownedPartitions.addAll(subscriptions.assignedPartitions());

        // should at least encode the short version
        if (assignmentBuffer.remaining() < 2)
            throw new IllegalStateException("There are insufficient bytes available to read assignment from the sync-group response (" +
                "actual byte size " + assignmentBuffer.remaining() + ") , this is not expected; " +
                "it is possible that the leader's assign function is buggy and did not return any assignment for this member, " +
                "or because static member is configured and the protocol is buggy hence did not get the assignment for this member");

        Assignment assignment = ConsumerProtocol.deserializeAssignment(assignmentBuffer);

        SortedSet<TopicPartition> assignedPartitions = new TreeSet<>(COMPARATOR);
        assignedPartitions.addAll(assignment.partitions());

        if (!subscriptions.checkAssignmentMatchedSubscription(assignedPartitions)) {
            final String fullReason = String.format("received assignment %s does not match the current subscription %s; " +
                    "it is likely that the subscription has changed since we joined the group, will re-join with current subscription",
                    assignment.partitions(), subscriptions.prettyString());
            requestRejoin("received assignment does not match the current subscription", fullReason);

            return;
        }

        final AtomicReference<Exception> firstException = new AtomicReference<>(null);
        SortedSet<TopicPartition> addedPartitions = new TreeSet<>(COMPARATOR);
        addedPartitions.addAll(assignedPartitions);
        addedPartitions.removeAll(ownedPartitions);

        if (protocol == RebalanceProtocol.COOPERATIVE) {
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

            if (!revokedPartitions.isEmpty()) {
                // Revoke partitions that were previously owned but no longer assigned;
                // note that we should only change the assignment (or update the assignor's state)
                // AFTER we've triggered  the revoke callback
                firstException.compareAndSet(null, rebalanceListenerInvoker.invokePartitionsRevoked(revokedPartitions));

                // If revoked any partitions, need to re-join the group afterwards
                final String fullReason = String.format("need to revoke partitions %s as indicated " +
                        "by the current assignment and re-join", revokedPartitions);
                requestRejoin("need to revoke partitions and re-join", fullReason);
            }
        }

        // The leader may have assigned partitions which match our subscription pattern, but which
        // were not explicitly requested, so we update the joined subscription here.
        maybeUpdateJoinedSubscription(assignedPartitions);

        // Catch any exception here to make sure we could complete the user callback.
        firstException.compareAndSet(null, invokeOnAssignment(assignor, assignment));

        // Reschedule the auto commit starting from now
        if (autoCommitEnabled)
            this.nextAutoCommitTimer.updateAndReset(autoCommitIntervalMs);

        subscriptions.assignFromSubscribed(assignedPartitions);

        // Add partitions that were not previously owned but are now assigned
        firstException.compareAndSet(null, rebalanceListenerInvoker.invokePartitionsAssigned(addedPartitions));

        if (firstException.get() != null) {
            if (firstException.get() instanceof KafkaException) {
                throw (KafkaException) firstException.get();
            } else {
                throw new KafkaException("User rebalance callback throws an error", firstException.get());
            }
        }
    }

    void maybeUpdateSubscriptionMetadata() {
        int version = metadata.updateVersion();
        if (version > metadataSnapshot.version) {
            Cluster cluster = metadata.fetch();

            if (subscriptions.hasPatternSubscription())
                updatePatternSubscription(cluster);

            // Update the current snapshot, which will be used to check for subscription
            // changes that would require a rebalance (e.g. new partitions).
            metadataSnapshot = new MetadataSnapshot(rackId, subscriptions, cluster, version);
        }
    }

    private boolean coordinatorUnknownAndUnreadySync(Timer timer) {
        return coordinatorUnknown() && !ensureCoordinatorReady(timer);
    }

    private boolean coordinatorUnknownAndUnreadyAsync() {
        return coordinatorUnknown() && !ensureCoordinatorReadyAsync();
    }

    /**
     * Poll for coordinator events. This ensures that the coordinator is known and that the consumer
     * has joined the group (if it is using group management). This also handles periodic offset commits
     * if they are enabled.
     * <p>
     * Returns early if the timeout expires or if waiting on rejoin is not required
     *
     * @param timer Timer bounding how long this method can block
     * @param waitForJoinGroup Boolean flag indicating if we should wait until re-join group completes
     * @throws KafkaException if the rebalance callback throws an exception
     * @return true iff the operation succeeded
     */
    public boolean poll(Timer timer, boolean waitForJoinGroup) {
        maybeUpdateSubscriptionMetadata();

        invokeCompletedOffsetCommitCallbacks();

        if (subscriptions.hasAutoAssignedPartitions()) {
            if (protocol == null) {
                throw new IllegalStateException("User configured " + ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG +
                    " to empty while trying to subscribe for group protocol to auto assign partitions");
            }
            // Always update the heartbeat last poll time so that the heartbeat thread does not leave the
            // group proactively due to application inactivity even if (say) the coordinator cannot be found.
            pollHeartbeat(timer.currentTimeMs());
            if (coordinatorUnknownAndUnreadySync(timer)) {
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
                        this.metadata.requestUpdate(true);
                    }

                    if (!client.ensureFreshMetadata(timer)) {
                        return false;
                    }

                    maybeUpdateSubscriptionMetadata();
                }

                // if not wait for join group, we would just use a timer of 0
                if (!ensureActiveGroup(waitForJoinGroup ? timer : time.timer(0L))) {
                    // since we may use a different timer in the callee, we'd still need
                    // to update the original timer's current time after the call
                    timer.update(time.milliseconds());

                    return false;
                }
            }
        } else {
            // For manually assigned partitions, we do not try to pro-actively lookup coordinator;
            // instead we only try to refresh metadata when necessary.
            // If connections to all nodes fail, wakeups triggered while attempting to send fetch
            // requests result in polls returning immediately, causing a tight loop of polls. Without
            // the wakeup, poll() with no channels would block for the timeout, delaying re-connection.
            // awaitMetadataUpdate() in ensureCoordinatorReady initiates new connections with configured backoff and avoids the busy loop.
            if (metadata.updateRequested() && !client.hasReadyNodes(timer.currentTimeMs())) {
                client.awaitMetadataUpdate(timer);
            }

            // if there is pending coordinator requests, ensure they have a chance to be transmitted.
            client.pollNoWakeup();
        }

        maybeAutoCommitOffsetsAsync(timer.currentTimeMs());
        return true;
    }

    /**
     * Return the time to the next needed invocation of {@link ConsumerNetworkClient#poll(Timer)}.
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

    private boolean isAssignFromSubscribedTopicsAssignor(String name) {
        return ASSIGN_FROM_SUBSCRIBED_ASSIGNORS.contains(name);
    }

    /**
     * user-customized assignor may have created some topics that are not in the subscription list
     * and assign their partitions to the members; in this case we would like to update the leader's
     * own metadata with the newly added topics so that it will not trigger a subsequent rebalance
     * when these topics gets updated from metadata refresh.
     *
     * We skip the check for in-product assignors since this will not happen in in-product assignors.
     *
     * TODO: this is a hack and not something we want to support long-term unless we push regex into the protocol
     *       we may need to modify the ConsumerPartitionAssignor API to better support this case.
     *
     * @param assignorName          the selected assignor name
     * @param assignments           the assignments after assignor assigned
     * @param allSubscribedTopics   all consumers' subscribed topics
     */
    private void maybeUpdateGroupSubscription(String assignorName,
                                              Map<String, Assignment> assignments,
                                              Set<String> allSubscribedTopics) {
        if (!isAssignFromSubscribedTopicsAssignor(assignorName)) {
            Set<String> assignedTopics = new HashSet<>();
            for (Assignment assigned : assignments.values()) {
                for (TopicPartition tp : assigned.partitions())
                    assignedTopics.add(tp.topic());
            }

            if (!assignedTopics.containsAll(allSubscribedTopics)) {
                SortedSet<String> notAssignedTopics = new TreeSet<>(allSubscribedTopics);
                notAssignedTopics.removeAll(assignedTopics);
                log.warn("The following subscribed topics are not assigned to any members: {} ", notAssignedTopics);
            }

            if (!allSubscribedTopics.containsAll(assignedTopics)) {
                SortedSet<String> newlyAddedTopics = new TreeSet<>(assignedTopics);
                newlyAddedTopics.removeAll(allSubscribedTopics);
                log.info("The following not-subscribed topics are assigned, and their metadata will be " +
                    "fetched from the brokers: {}", newlyAddedTopics);

                allSubscribedTopics.addAll(newlyAddedTopics);
                updateGroupSubscription(allSubscribedTopics);
            }
        }
    }

    @Override
    protected Map<String, ByteBuffer> onLeaderElected(String leaderId,
                                                      String assignmentStrategy,
                                                      List<JoinGroupResponseData.JoinGroupResponseMember> allSubscriptions,
                                                      boolean skipAssignment) {
        ConsumerPartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);
        String assignorName = assignor.name();

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

        if (skipAssignment) {
            log.info("Skipped assignment for returning static leader at generation {}. The static leader " +
                "will continue with its existing assignment.", generation().generationId);
            assignmentSnapshot = metadataSnapshot;
            return Collections.emptyMap();
        }

        log.debug("Performing assignment using strategy {} with subscriptions {}", assignorName, subscriptions);

        Map<String, Assignment> assignments = assignor.assign(metadata.fetch(), new GroupSubscription(subscriptions)).groupAssignment();

        // skip the validation for built-in cooperative sticky assignor since we've considered
        // the "generation" of ownedPartition inside the assignor
        if (protocol == RebalanceProtocol.COOPERATIVE && !assignorName.equals(COOPERATIVE_STICKY_ASSIGNOR_NAME)) {
            validateCooperativeAssignment(ownedPartitions, assignments);
        }

        maybeUpdateGroupSubscription(assignorName, assignments, allSubscribedTopics);

        // metadataSnapshot could be updated when the subscription is updated therefore
        // we must take the assignment snapshot after.
        assignmentSnapshot = metadataSnapshot;

        log.info("Finished assignment for group at generation {}: {}", generation().generationId, assignments);

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
        SortedSet<TopicPartition> totalAddedPartitions = new TreeSet<>(COMPARATOR);
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
                "by some members", totalAddedPartitions);

            throw new IllegalStateException("Assignor supporting the COOPERATIVE protocol violates its requirements");
        }
    }

    @Override
    protected boolean onJoinPrepare(Timer timer, int generation, String memberId) {
        log.debug("Executing onJoinPrepare with generation {} and memberId {}", generation, memberId);
        if (joinPrepareTimer == null) {
            // We should complete onJoinPrepare before rebalanceTimeoutMs,
            // and continue to join group to avoid member got kicked out from group
            joinPrepareTimer = time.timer(rebalanceConfig.rebalanceTimeoutMs);
        } else {
            joinPrepareTimer.update();
        }

        // async commit offsets prior to rebalance if auto-commit enabled
        // and there is no in-flight offset commit request
        if (autoCommitEnabled && autoCommitOffsetRequestFuture == null) {
            maybeMarkPartitionsPendingRevocation();
            autoCommitOffsetRequestFuture = maybeAutoCommitOffsetsAsync();
        }

        // wait for commit offset response before timer expired
        if (autoCommitOffsetRequestFuture != null) {
            Timer pollTimer = timer.remainingMs() < joinPrepareTimer.remainingMs() ?
                    timer : joinPrepareTimer;
            client.poll(autoCommitOffsetRequestFuture, pollTimer);
            joinPrepareTimer.update();

            // Keep retrying/waiting the offset commit when:
            // 1. offset commit haven't done (and joinPrepareTimer not expired)
            // 2. failed with retriable exception (and joinPrepareTimer not expired)
            // Otherwise, continue to revoke partitions, ex:
            // 1. if joinPrepareTimer has expired
            // 2. if offset commit failed with non-retriable exception
            // 3. if offset commit success
            boolean onJoinPrepareAsyncCommitCompleted = true;
            if (joinPrepareTimer.isExpired()) {
                log.error("Asynchronous auto-commit of offsets failed: joinPrepare timeout. Will continue to join group");
            } else if (!autoCommitOffsetRequestFuture.isDone()) {
                onJoinPrepareAsyncCommitCompleted = false;
            } else if (autoCommitOffsetRequestFuture.failed() && autoCommitOffsetRequestFuture.isRetriable()) {
                log.debug("Asynchronous auto-commit of offsets failed with retryable error: {}. Will retry it.",
                        autoCommitOffsetRequestFuture.exception().getMessage());
                onJoinPrepareAsyncCommitCompleted = false;
            } else if (autoCommitOffsetRequestFuture.failed() && !autoCommitOffsetRequestFuture.isRetriable()) {
                log.error("Asynchronous auto-commit of offsets failed: {}. Will continue to join group.",
                        autoCommitOffsetRequestFuture.exception().getMessage());
            }
            if (autoCommitOffsetRequestFuture.isDone()) {
                autoCommitOffsetRequestFuture = null;
            }
            if (!onJoinPrepareAsyncCommitCompleted) {
                pollTimer.sleep(Math.min(pollTimer.remainingMs(), rebalanceConfig.retryBackoffMs));
                timer.update();
                return false;
            }
        }

        // the generation / member-id can possibly be reset by the heartbeat thread
        // upon getting errors or heartbeat timeouts; in this case whatever is previously
        // owned partitions would be lost, we should trigger the callback and cleanup the assignment;
        // otherwise we can proceed normally and revoke the partitions depending on the protocol,
        // and in that case we should only change the assignment AFTER the revoke callback is triggered
        // so that users can still access the previously owned partitions to commit offsets etc.
        Exception exception = null;
        final SortedSet<TopicPartition> revokedPartitions = new TreeSet<>(COMPARATOR);
        if (generation == Generation.NO_GENERATION.generationId ||
            memberId.equals(Generation.NO_GENERATION.memberId)) {
            revokedPartitions.addAll(subscriptions.assignedPartitions());

            if (!revokedPartitions.isEmpty()) {
                log.info("Giving away all assigned partitions as lost since generation/memberID has been reset," +
                    "indicating that consumer is in old state or no longer part of the group");
                exception = rebalanceListenerInvoker.invokePartitionsLost(revokedPartitions);

                subscriptions.assignFromSubscribed(Collections.emptySet());
            }
        } else {
            switch (protocol) {
                case EAGER:
                    // revoke all partitions
                    revokedPartitions.addAll(subscriptions.assignedPartitions());
                    exception = rebalanceListenerInvoker.invokePartitionsRevoked(revokedPartitions);

                    subscriptions.assignFromSubscribed(Collections.emptySet());

                    break;

                case COOPERATIVE:
                    // only revoke those partitions that are not in the subscription anymore.
                    Set<TopicPartition> ownedPartitions = new HashSet<>(subscriptions.assignedPartitions());
                    revokedPartitions.addAll(ownedPartitions.stream()
                        .filter(tp -> !subscriptions.subscription().contains(tp.topic()))
                        .collect(Collectors.toSet()));

                    if (!revokedPartitions.isEmpty()) {
                        exception = rebalanceListenerInvoker.invokePartitionsRevoked(revokedPartitions);

                        ownedPartitions.removeAll(revokedPartitions);
                        subscriptions.assignFromSubscribed(ownedPartitions);
                    }

                    break;
            }
        }

        isLeader = false;
        subscriptions.resetGroupSubscription();
        joinPrepareTimer = null;
        autoCommitOffsetRequestFuture = null;
        timer.update();

        if (exception != null) {
            throw new KafkaException("User rebalance callback throws an error", exception);
        }
        return true;
    }

    private void maybeMarkPartitionsPendingRevocation() {
        if (protocol != RebalanceProtocol.EAGER) {
            return;
        }

        // When asynchronously committing offsets prior to the revocation of a set of partitions, there will be a
        // window of time between when the offset commit is sent and when it returns and revocation completes. It is
        // possible for pending fetches for these partitions to return during this time, which means the application's
        // position may get ahead of the committed position prior to revocation. This can cause duplicate consumption.
        // To prevent this, we mark the partitions as "pending revocation," which stops the Fetcher from sending new
        // fetches or returning data from previous fetches to the user.
        Set<TopicPartition> partitions = subscriptions.assignedPartitions();
        log.debug("Marking assigned partitions pending for revocation: {}", partitions);
        subscriptions.markPendingRevocation(partitions);
    }

    @Override
    public void onLeavePrepare() {
        // Save the current Generation, as the hb thread can change it at any time
        final Generation currentGeneration = generation();

        log.debug("Executing onLeavePrepare with generation {}", currentGeneration);

        // we should reset assignment and trigger the callback before leaving group
        SortedSet<TopicPartition> droppedPartitions = new TreeSet<>(COMPARATOR);
        droppedPartitions.addAll(subscriptions.assignedPartitions());

        if (subscriptions.hasAutoAssignedPartitions() && !droppedPartitions.isEmpty()) {
            final Exception e;
            if ((currentGeneration.generationId == Generation.NO_GENERATION.generationId ||
                currentGeneration.memberId.equals(Generation.NO_GENERATION.memberId)) ||
                rebalanceInProgress()) {
                e = rebalanceListenerInvoker.invokePartitionsLost(droppedPartitions);
            } else {
                e = rebalanceListenerInvoker.invokePartitionsRevoked(droppedPartitions);
            }

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
        if (!subscriptions.hasAutoAssignedPartitions())
            return false;

        // we need to rejoin if we performed the assignment and metadata has changed;
        // also for those owned-but-no-longer-existed partitions we should drop them as lost
        if (assignmentSnapshot != null && !assignmentSnapshot.matches(metadataSnapshot)) {
            final String fullReason = String.format("cached metadata has changed from %s at the beginning of the rebalance to %s",
                assignmentSnapshot, metadataSnapshot);
            requestRejoinIfNecessary("cached metadata has changed", fullReason);
            return true;
        }

        // we need to join if our subscription has changed since the last join
        if (joinedSubscription != null && !joinedSubscription.equals(subscriptions.subscription())) {
            final String fullReason = String.format("subscription has changed from %s at the beginning of the rebalance to %s",
                joinedSubscription, subscriptions.subscription());
            requestRejoinIfNecessary("subscription has changed", fullReason);
            return true;
        }

        return super.rejoinNeededOrPending();
    }

    /**
     * Refresh the committed offsets for partitions that require initialization.
     *
     * @param timer Timer bounding how long this method can block
     * @return true iff the operation completed within the timeout
     */
    public boolean initWithCommittedOffsetsIfNeeded(Timer timer) {
        final Set<TopicPartition> initializingPartitions = subscriptions.initializingPartitions();
        final Map<TopicPartition, OffsetAndMetadata> offsets = fetchCommittedOffsets(initializingPartitions, timer);

        // "offsets" will be null if the offset fetch requests did not receive responses within the given timeout
        if (offsets == null)
            return false;

        refreshCommittedOffsets(offsets, this.metadata, this.subscriptions);
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

        final Generation generationForOffsetRequest = generationIfStable();
        if (pendingCommittedOffsetRequest != null &&
            !pendingCommittedOffsetRequest.sameRequest(partitions, generationForOffsetRequest)) {
            // if we were waiting for a different request, then just clear it.
            pendingCommittedOffsetRequest = null;
        }

        long attempts = 0L;
        do {
            if (!ensureCoordinatorReady(timer)) return null;

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
                    timer.sleep(retryBackoff.backoff(attempts++));
                }
            } else {
                return null;
            }
        } while (timer.notExpired());
        return null;
    }

    /**
     * Return the consumer group metadata.
     *
     * @return the current consumer group metadata
     */
    public ConsumerGroupMetadata groupMetadata() {
        return groupMetadata;
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

    public RequestFuture<Void> commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        invokeCompletedOffsetCommitCallbacks();

        RequestFuture<Void> future = null;
        if (offsets.isEmpty()) {
            // No need to check coordinator if offsets is empty since commit of empty offsets is completed locally.
            future = doCommitOffsetsAsync(offsets, callback);
        } else if (!coordinatorUnknownAndUnreadyAsync()) {
            // we need to make sure coordinator is ready before committing, since
            // this is for async committing we do not try to block, but just try once to
            // clear the previous discover-coordinator future, resend, or get responses;
            // if the coordinator is not ready yet then we would just proceed and put that into the
            // pending requests, and future poll calls would still try to complete them.
            //
            // the key here though is that we have to try sending the discover-coordinator if
            // it's not known or ready, since this is the only place we can send such request
            // under manual assignment (there we would not have heartbeat thread trying to auto-rediscover
            // the coordinator).
            future = doCommitOffsetsAsync(offsets, callback);
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
        return future;
    }

    private RequestFuture<Void> doCommitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
        inFlightAsyncCommits.incrementAndGet();
        final OffsetCommitCallback cb = callback == null ? defaultOffsetCommitCallback : callback;
        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                inFlightAsyncCommits.decrementAndGet();

                if (interceptors != null)
                    interceptors.onCommit(offsets);
                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, null));
            }

            @Override
            public void onFailure(RuntimeException e) {
                inFlightAsyncCommits.decrementAndGet();

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
        return future;
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

        if (offsets.isEmpty()) {
            // We guarantee that the callbacks for all commitAsync() will be invoked when
            // commitSync() completes, even if the user tries to commit empty offsets.
            return invokePendingAsyncCommits(timer);
        }

        long attempts = 0L;
        do {
            if (coordinatorUnknownAndUnreadySync(timer)) {
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

            timer.sleep(retryBackoff.backoff(attempts++));
        } while (timer.notExpired());

        return false;
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

    public void maybeAutoCommitOffsetsAsync(long now) {
        if (autoCommitEnabled) {
            nextAutoCommitTimer.update(now);
            if (nextAutoCommitTimer.isExpired()) {
                nextAutoCommitTimer.reset(autoCommitIntervalMs);
                autoCommitOffsetsAsync();
            }
        }
    }

    private boolean invokePendingAsyncCommits(Timer timer) {
        if (inFlightAsyncCommits.get() == 0) {
            return true;
        }

        long attempts = 0L;
        do {
            ensureCoordinatorReady(timer);
            client.poll(timer);
            invokeCompletedOffsetCommitCallbacks();

            if (inFlightAsyncCommits.get() == 0) {
                return true;
            }

            timer.sleep(retryBackoff.backoff(attempts++));
        } while (timer.notExpired());

        return false;
    }

    private RequestFuture<Void> autoCommitOffsetsAsync() {
        Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
        log.debug("Sending asynchronous auto-commit of offsets {}", allConsumedOffsets);

        return commitOffsetsAsync(allConsumedOffsets, (offsets, exception) -> {
            if (exception != null) {
                if (exception instanceof RetriableCommitFailedException) {
                    log.debug("Asynchronous auto-commit of offsets {} failed due to retriable error.", offsets,
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

    private RequestFuture<Void> maybeAutoCommitOffsetsAsync() {
        if (autoCommitEnabled)
            return autoCommitOffsetsAsync();
        return null;
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
                        .setGenerationIdOrMemberEpoch(generation.generationId)
                        .setMemberId(generation.memberId)
                        .setGroupInstanceId(groupInstanceId)
                        .setTopics(new ArrayList<>(requestTopicDataMap.values()))
        );

        log.trace("Sending OffsetCommit request with {} to coordinator {}", offsets, coordinator);

        return client.send(coordinator, builder)
                .compose(new OffsetCommitResponseHandler(offsets, generation));
    }

    private class OffsetCommitResponseHandler extends CoordinatorResponseHandler<OffsetCommitResponse, Void> {
        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        private OffsetCommitResponseHandler(Map<TopicPartition, OffsetAndMetadata> offsets, Generation generation) {
            super(generation);
            this.offsets = offsets;
        }

        @Override
        public void handle(OffsetCommitResponse commitResponse, RequestFuture<Void> future) {
            coordinatorMetrics.commitSensor.record(response.requestLatencyMs());
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

                            // if the generation has changed or we are not in rebalancing, do not raise the fatal error but rebalance-in-progress
                            if (generationUnchanged()) {
                                future.raise(error);
                            } else {
                                KafkaException exception;
                                synchronized (ConsumerCoordinator.this) {
                                    if (ConsumerCoordinator.this.state == MemberState.PREPARING_REBALANCE) {
                                        exception = new RebalanceInProgressException("Offset commit cannot be completed since the " +
                                            "consumer member's old generation is fenced by its group instance id, it is possible that " +
                                            "this consumer has already participated another rebalance and got a new generation");
                                    } else {
                                        exception = new CommitFailedException();
                                    }
                                }
                                future.raise(exception);
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
                            requestRejoin("offset commit failed since group is already rebalancing");
                            future.raise(new RebalanceInProgressException("Offset commit cannot be completed since the " +
                                "consumer group is executing a rebalance at the moment. You can try completing the rebalance " +
                                "by calling poll() and then retry commit again"));
                            return;
                        } else if (error == Errors.UNKNOWN_MEMBER_ID
                                || error == Errors.ILLEGAL_GENERATION) {
                            log.info("OffsetCommit failed with {}: {}", sentGeneration, error.message());

                            // only need to reset generation and re-join group if generation has not changed or we are not in rebalancing;
                            // otherwise only raise rebalance-in-progress error
                            KafkaException exception;
                            synchronized (ConsumerCoordinator.this) {
                                if (!generationUnchanged() && ConsumerCoordinator.this.state == MemberState.PREPARING_REBALANCE) {
                                    exception = new RebalanceInProgressException("Offset commit cannot be completed since the " +
                                        "consumer member's generation is already stale, meaning it has already participated another rebalance and " +
                                        "got a new generation. You can try completing the rebalance by calling poll() and then retry commit again");
                                } else {
                                    // don't reset generation member ID when ILLEGAL_GENERATION, since the member might be still valid
                                    resetStateOnResponseError(ApiKeys.OFFSET_COMMIT, error, error != Errors.ILLEGAL_GENERATION);
                                    exception = new CommitFailedException();
                                }
                            }
                            future.raise(exception);
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
        OffsetFetchRequest.Builder requestBuilder =
            new OffsetFetchRequest.Builder(this.rebalanceConfig.groupId, true, new ArrayList<>(partitions), throwOnFetchStableOffsetsUnsupported);

        // send the request with a callback
        return client.send(coordinator, requestBuilder)
                .compose(new OffsetFetchResponseHandler());
    }

    private class OffsetFetchResponseHandler extends CoordinatorResponseHandler<OffsetFetchResponse, Map<TopicPartition, OffsetAndMetadata>> {
        private OffsetFetchResponseHandler() {
            super(Generation.NO_GENERATION);
        }

        @Override
        public void handle(OffsetFetchResponse response, RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
            Errors responseError = response.groupLevelError(rebalanceConfig.groupId);
            if (responseError != Errors.NONE) {
                log.debug("Offset fetch failed: {}", responseError.message());

                if (responseError == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                    // just retry
                    future.raise(responseError);
                } else if (responseError == Errors.NOT_COORDINATOR) {
                    // re-discover the coordinator and retry
                    markCoordinatorUnknown(responseError);
                    future.raise(responseError);
                } else if (responseError == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                } else {
                    future.raise(new KafkaException("Unexpected error in fetch offset response: " + responseError.message()));
                }
                return;
            }

            Set<String> unauthorizedTopics = null;
            Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData =
                response.partitionDataMap(rebalanceConfig.groupId);
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(responseData.size());
            Set<TopicPartition> unstableTxnOffsetTopicPartitions = new HashSet<>();
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : responseData.entrySet()) {
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

    private static class MetadataSnapshot {
        private final int version;
        private final Map<String, List<PartitionRackInfo>> partitionsPerTopic;

        private MetadataSnapshot(Optional<String> clientRack, SubscriptionState subscription, Cluster cluster, int version) {
            Map<String, List<PartitionRackInfo>> partitionsPerTopic = new HashMap<>();
            for (String topic : subscription.metadataTopics()) {
                List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
                if (partitions != null) {
                    List<PartitionRackInfo> partitionRacks = partitions.stream()
                            .map(p -> new PartitionRackInfo(clientRack, p))
                            .collect(Collectors.toList());
                    partitionsPerTopic.put(topic, partitionRacks);
                }
            }
            this.partitionsPerTopic = partitionsPerTopic;
            this.version = version;
        }

        boolean matches(MetadataSnapshot other) {
            return version == other.version || partitionsPerTopic.equals(other.partitionsPerTopic);
        }

        @Override
        public String toString() {
            return "(version" + version + ": " + partitionsPerTopic + ")";
        }
    }

    private class ConsumerCoordinatorMetrics {
        private final Sensor commitSensor;

        private ConsumerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            String metricGrpName = metricGrpPrefix + COORDINATOR_METRICS_SUFFIX;

            this.commitSensor = metrics.sensor("commit-latency");
            this.commitSensor.add(metrics.metricName("commit-latency-avg",
                metricGrpName,
                "The average time taken for a commit request"), new Avg());
            this.commitSensor.add(metrics.metricName("commit-latency-max",
                metricGrpName,
                "The max time taken for a commit request"), new Max());
            this.commitSensor.add(createMeter(metrics, metricGrpName, "commit", "commit calls"));

            Measurable numParts = (config, now) -> subscriptions.numAssignedPartitions();
            metrics.addMetric(metrics.metricName("assigned-partitions",
                metricGrpName,
                "The number of partitions currently assigned to this consumer"), numParts);
        }
    }

    private static class PartitionRackInfo {
        private final Set<String> racks;

        PartitionRackInfo(Optional<String> clientRack, PartitionInfo partition) {
            if (clientRack.isPresent() && partition.replicas() != null) {
                racks = Arrays.stream(partition.replicas()).map(Node::rack).collect(Collectors.toSet());
            } else {
                racks = Collections.emptySet();
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PartitionRackInfo)) {
                return false;
            }
            PartitionRackInfo rackInfo = (PartitionRackInfo) o;
            return Objects.equals(racks, rackInfo.racks);
        }

        @Override
        public int hashCode() {
            return Objects.hash(racks);
        }

        @Override
        public String toString() {
            return racks.isEmpty() ? "NO_RACKS" : "racks=" + racks;
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

    boolean poll(Timer timer) {
        return poll(timer, true);
    }
}
