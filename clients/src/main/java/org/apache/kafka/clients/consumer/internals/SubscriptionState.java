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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.PartitionStates;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.hasUsableOffsetForLeaderEpochVersion;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET;

/**
 * A class for tracking the topics, partitions, and offsets for the consumer. A partition
 * is "assigned" either directly with {@link #assignFromUser(Set)} (manual assignment)
 * or with {@link #assignFromSubscribed(Collection)} (automatic assignment from subscription).
 * <p>
 * Once assigned, the partition is not considered "fetchable" until its initial position has
 * been set with {@link #seekValidated(TopicPartition, FetchPosition)}. Fetchable partitions
 * track a position which is the last offset that has been returned to the user. You can
 * suspend fetching from a partition through {@link #pause(TopicPartition)} without affecting the consumed
 * position. The partition will remain unfetchable until the {@link #resume(TopicPartition)} is
 * used. You can also query the pause state independently with {@link #isPaused(TopicPartition)}.
 * <p>
 * Note that pause state as well as the consumed positions are not preserved when partition
 * assignment is changed whether directly by the user or through a group rebalance.
 * <p>
 * Thread Safety: this class is thread-safe.
 */
public class SubscriptionState {
    private static final String SUBSCRIPTION_EXCEPTION_MESSAGE =
            "Subscription to topics, partitions and pattern are mutually exclusive";

    private final Logger log;

    private enum SubscriptionType {
        NONE, AUTO_TOPICS, AUTO_PATTERN, USER_ASSIGNED
    }

    /* the type of subscription */
    private SubscriptionType subscriptionType;

    /* the pattern user has requested */
    private Pattern subscribedPattern;

    /* the list of topics the user has requested */
    private Set<String> subscription;

    /* The list of topics the group has subscribed to. This may include some topics which are not part
     * of `subscription` for the leader of a group since it is responsible for detecting metadata changes
     * which require a group rebalance. */
    private Set<String> groupSubscription;

    /* the partitions that are currently assigned, note that the order of partition matters (see FetchBuilder for more details) */
    private final PartitionStates<TopicPartitionState> assignment;

    /* Default offset reset strategy */
    private final OffsetResetStrategy defaultResetStrategy;

    /* User-provided listener to be invoked when assignment changes */
    private Optional<ConsumerRebalanceListener> rebalanceListener;

    private int assignmentId = 0;

    @Override
    public synchronized String toString() {
        return "SubscriptionState{" +
            "type=" + subscriptionType +
            ", subscribedPattern=" + subscribedPattern +
            ", subscription=" + String.join(",", subscription) +
            ", groupSubscription=" + String.join(",", groupSubscription) +
            ", defaultResetStrategy=" + defaultResetStrategy +
            ", assignment=" + assignment.partitionStateValues() + " (id=" + assignmentId + ")}";
    }

    public synchronized String prettyString() {
        switch (subscriptionType) {
            case NONE:
                return "None";
            case AUTO_TOPICS:
                return "Subscribe(" + String.join(",", subscription) + ")";
            case AUTO_PATTERN:
                return "Subscribe(" + subscribedPattern + ")";
            case USER_ASSIGNED:
                return "Assign(" + assignedPartitions() + " , id=" + assignmentId + ")";
            default:
                throw new IllegalStateException("Unrecognized subscription type: " + subscriptionType);
        }
    }

    public SubscriptionState(LogContext logContext, OffsetResetStrategy defaultResetStrategy) {
        this.log = logContext.logger(this.getClass());
        this.defaultResetStrategy = defaultResetStrategy;
        this.subscription = new TreeSet<>(); // use a sorted set for better logging
        this.assignment = new PartitionStates<>();
        this.groupSubscription = new HashSet<>();
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
    }

    /**
     * Monotonically increasing id which is incremented after every assignment change. This can
     * be used to check when an assignment has changed.
     *
     * @return The current assignment Id
     */
    synchronized int assignmentId() {
        return assignmentId;
    }

    /**
     * This method sets the subscription type if it is not already set (i.e. when it is NONE),
     * or verifies that the subscription type is equal to the give type when it is set (i.e.
     * when it is not NONE)
     * @param type The given subscription type
     */
    private void setSubscriptionType(SubscriptionType type) {
        if (this.subscriptionType == SubscriptionType.NONE)
            this.subscriptionType = type;
        else if (this.subscriptionType != type)
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
    }

    public synchronized boolean subscribe(Set<String> topics, Optional<ConsumerRebalanceListener> listener) {
        registerRebalanceListener(listener);
        setSubscriptionType(SubscriptionType.AUTO_TOPICS);
        return changeSubscription(topics);
    }

    public synchronized void subscribe(Pattern pattern, Optional<ConsumerRebalanceListener> listener) {
        registerRebalanceListener(listener);
        setSubscriptionType(SubscriptionType.AUTO_PATTERN);
        this.subscribedPattern = pattern;
    }

    public synchronized boolean subscribeFromPattern(Set<String> topics) {
        if (subscriptionType != SubscriptionType.AUTO_PATTERN)
            throw new IllegalArgumentException("Attempt to subscribe from pattern while subscription type set to " +
                    subscriptionType);

        return changeSubscription(topics);
    }

    private boolean changeSubscription(Set<String> topicsToSubscribe) {
        if (subscription.equals(topicsToSubscribe))
            return false;

        subscription = topicsToSubscribe;
        return true;
    }

    /**
     * Set the current group subscription. This is used by the group leader to ensure
     * that it receives metadata updates for all topics that the group is interested in.
     *
     * @param topics All topics from the group subscription
     * @return true if the group subscription contains topics which are not part of the local subscription
     */
    synchronized boolean groupSubscribe(Collection<String> topics) {
        if (!hasAutoAssignedPartitions())
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
        groupSubscription = new HashSet<>(topics);
        return !subscription.containsAll(groupSubscription);
    }

    /**
     * Reset the group's subscription to only contain topics subscribed by this consumer.
     */
    synchronized void resetGroupSubscription() {
        groupSubscription = Collections.emptySet();
    }

    /**
     * Change the assignment to the specified partitions provided by the user,
     * note this is different from {@link #assignFromSubscribed(Collection)}
     * whose input partitions are provided from the subscribed topics.
     */
    public synchronized boolean assignFromUser(Set<TopicPartition> partitions) {
        setSubscriptionType(SubscriptionType.USER_ASSIGNED);

        if (this.assignment.partitionSet().equals(partitions))
            return false;

        assignmentId++;

        // update the subscribed topics
        Set<String> manualSubscribedTopics = new HashSet<>();
        Map<TopicPartition, TopicPartitionState> partitionToState = new HashMap<>();
        for (TopicPartition partition : partitions) {
            TopicPartitionState state = assignment.stateValue(partition);
            if (state == null)
                state = new TopicPartitionState();
            partitionToState.put(partition, state);

            manualSubscribedTopics.add(partition.topic());
        }

        this.assignment.set(partitionToState);
        return changeSubscription(manualSubscribedTopics);
    }

    /**
     * @return true if assignments matches subscription, otherwise false
     */
    public synchronized boolean checkAssignmentMatchedSubscription(Collection<TopicPartition> assignments) {
        for (TopicPartition topicPartition : assignments) {
            if (this.subscribedPattern != null) {
                if (!this.subscribedPattern.matcher(topicPartition.topic()).matches()) {
                    log.info("Assigned partition {} for non-subscribed topic regex pattern; subscription pattern is {}",
                        topicPartition,
                        this.subscribedPattern);

                    return false;
                }
            } else {
                if (!this.subscription.contains(topicPartition.topic())) {
                    log.info("Assigned partition {} for non-subscribed topic; subscription is {}", topicPartition, this.subscription);

                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Change the assignment to the specified partitions returned from the coordinator, note this is
     * different from {@link #assignFromUser(Set)} which directly set the assignment from user inputs.
     */
    public synchronized void assignFromSubscribed(Collection<TopicPartition> assignments) {
        if (!this.hasAutoAssignedPartitions())
            throw new IllegalArgumentException("Attempt to dynamically assign partitions while manual assignment in use");

        Map<TopicPartition, TopicPartitionState> assignedPartitionStates = new HashMap<>(assignments.size());
        for (TopicPartition tp : assignments) {
            TopicPartitionState state = this.assignment.stateValue(tp);
            if (state == null)
                state = new TopicPartitionState();
            assignedPartitionStates.put(tp, state);
        }

        assignmentId++;
        this.assignment.set(assignedPartitionStates);
    }

    private void registerRebalanceListener(Optional<ConsumerRebalanceListener> listener) {
        this.rebalanceListener = Objects.requireNonNull(listener, "RebalanceListener cannot be null");
    }

    /**
     * Check whether pattern subscription is in use.
     *
     */
    synchronized boolean hasPatternSubscription() {
        return this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    public synchronized boolean hasNoSubscriptionOrUserAssignment() {
        return this.subscriptionType == SubscriptionType.NONE;
    }

    public synchronized void unsubscribe() {
        this.subscription = Collections.emptySet();
        this.groupSubscription = Collections.emptySet();
        this.assignment.clear();
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
        this.assignmentId++;
    }

    /**
     * Check whether a topic matches a subscribed pattern.
     *
     * @return true if pattern subscription is in use and the topic matches the subscribed pattern, false otherwise
     */
    synchronized boolean matchesSubscribedPattern(String topic) {
        Pattern pattern = this.subscribedPattern;
        if (hasPatternSubscription() && pattern != null)
            return pattern.matcher(topic).matches();
        return false;
    }

    public synchronized Set<String> subscription() {
        if (hasAutoAssignedPartitions())
            return this.subscription;
        return Collections.emptySet();
    }

    public synchronized Set<TopicPartition> pausedPartitions() {
        return collectPartitions(TopicPartitionState::isPaused);
    }

    /**
     * Get the subscription topics for which metadata is required. For the leader, this will include
     * the union of the subscriptions of all group members. For followers, it is just that member's
     * subscription. This is used when querying topic metadata to detect the metadata changes which would
     * require rebalancing. The leader fetches metadata for all topics in the group so that it
     * can do the partition assignment (which requires at least partition counts for all topics
     * to be assigned).
     *
     * @return The union of all subscribed topics in the group if this member is the leader
     *   of the current generation; otherwise it returns the same set as {@link #subscription()}
     */
    synchronized Set<String> metadataTopics() {
        if (groupSubscription.isEmpty())
            return subscription;
        else if (groupSubscription.containsAll(subscription))
            return groupSubscription;
        else {
            // When subscription changes `groupSubscription` may be outdated, ensure that
            // new subscription topics are returned.
            Set<String> topics = new HashSet<>(groupSubscription);
            topics.addAll(subscription);
            return topics;
        }
    }

    synchronized boolean needsMetadata(String topic) {
        return subscription.contains(topic) || groupSubscription.contains(topic);
    }

    private TopicPartitionState assignedState(TopicPartition tp) {
        TopicPartitionState state = this.assignment.stateValue(tp);
        if (state == null)
            throw new IllegalStateException("No current assignment for partition " + tp);
        return state;
    }

    private TopicPartitionState assignedStateOrNull(TopicPartition tp) {
        return this.assignment.stateValue(tp);
    }

    public synchronized void seekValidated(TopicPartition tp, FetchPosition position) {
        assignedState(tp).seekValidated(position);
    }

    public void seek(TopicPartition tp, long offset) {
        seekValidated(tp, new FetchPosition(offset));
    }

    public void seekUnvalidated(TopicPartition tp, FetchPosition position) {
        assignedState(tp).seekUnvalidated(position);
    }

    synchronized void maybeSeekUnvalidated(TopicPartition tp, FetchPosition position, OffsetResetStrategy requestedResetStrategy) {
        TopicPartitionState state = assignedStateOrNull(tp);
        if (state == null) {
            log.debug("Skipping reset of partition {} since it is no longer assigned", tp);
        } else if (!state.awaitingReset()) {
            log.debug("Skipping reset of partition {} since reset is no longer needed", tp);
        } else if (requestedResetStrategy != state.resetStrategy) {
            log.debug("Skipping reset of partition {} since an alternative reset has been requested", tp);
        } else {
            log.info("Resetting offset for partition {} to position {}.", tp, position);
            state.seekUnvalidated(position);
        }
    }

    /**
     * @return a modifiable copy of the currently assigned partitions
     */
    public synchronized Set<TopicPartition> assignedPartitions() {
        return new HashSet<>(this.assignment.partitionSet());
    }

    /**
     * @return a modifiable copy of the currently assigned partitions as a list
     */
    public synchronized List<TopicPartition> assignedPartitionsList() {
        return new ArrayList<>(this.assignment.partitionSet());
    }

    /**
     * Provides the number of assigned partitions in a thread safe manner.
     * @return the number of assigned partitions.
     */
    synchronized int numAssignedPartitions() {
        return this.assignment.size();
    }

    // Visible for testing
    public synchronized List<TopicPartition> fetchablePartitions(Predicate<TopicPartition> isAvailable) {
        // Since this is in the hot-path for fetching, we do this instead of using java.util.stream API
        List<TopicPartition> result = new ArrayList<>();
        assignment.forEach((topicPartition, topicPartitionState) -> {
            // Cheap check is first to avoid evaluating the predicate if possible
            if (topicPartitionState.isFetchable() && isAvailable.test(topicPartition)) {
                result.add(topicPartition);
            }
        });
        return result;
    }

    public synchronized boolean hasAutoAssignedPartitions() {
        return this.subscriptionType == SubscriptionType.AUTO_TOPICS || this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    public synchronized void position(TopicPartition tp, FetchPosition position) {
        assignedState(tp).position(position);
    }

    /**
     * Enter the offset validation state if the leader for this partition is known to support a usable version of the
     * OffsetsForLeaderEpoch API. If the leader node does not support the API, simply complete the offset validation.
     *
     * @param apiVersions supported API versions
     * @param tp topic partition to validate
     * @param leaderAndEpoch leader epoch of the topic partition
     * @return true if we enter the offset validation state
     */
    public synchronized boolean maybeValidatePositionForCurrentLeader(ApiVersions apiVersions,
                                                                      TopicPartition tp,
                                                                      Metadata.LeaderAndEpoch leaderAndEpoch) {
        TopicPartitionState state = assignedStateOrNull(tp);
        if (state == null) {
            log.debug("Skipping validating position for partition {} which is not currently assigned.", tp);
            return false;
        }
        if (leaderAndEpoch.leader.isPresent()) {
            NodeApiVersions nodeApiVersions = apiVersions.get(leaderAndEpoch.leader.get().idString());
            if (nodeApiVersions == null || hasUsableOffsetForLeaderEpochVersion(nodeApiVersions)) {
                return state.maybeValidatePosition(leaderAndEpoch);
            } else {
                // If the broker does not support a newer version of OffsetsForLeaderEpoch, we skip validation
                state.updatePositionLeaderNoValidation(leaderAndEpoch);
                return false;
            }
        } else {
            return state.maybeValidatePosition(leaderAndEpoch);
        }
    }

    /**
     * Attempt to complete validation with the end offset returned from the OffsetForLeaderEpoch request.
     * @return Log truncation details if detected and no reset policy is defined.
     */
    public synchronized Optional<LogTruncation> maybeCompleteValidation(TopicPartition tp,
                                                                        FetchPosition requestPosition,
                                                                        EpochEndOffset epochEndOffset) {
        TopicPartitionState state = assignedStateOrNull(tp);
        if (state == null) {
            log.debug("Skipping completed validation for partition {} which is not currently assigned.", tp);
        } else if (!state.awaitingValidation()) {
            log.debug("Skipping completed validation for partition {} which is no longer expecting validation.", tp);
        } else {
            SubscriptionState.FetchPosition currentPosition = state.position;
            if (!currentPosition.equals(requestPosition)) {
                log.debug("Skipping completed validation for partition {} since the current position {} " +
                          "no longer matches the position {} when the request was sent",
                          tp, currentPosition, requestPosition);
            } else if (epochEndOffset.endOffset() == UNDEFINED_EPOCH_OFFSET ||
                        epochEndOffset.leaderEpoch() == UNDEFINED_EPOCH) {
                if (hasDefaultOffsetResetPolicy()) {
                    log.info("Truncation detected for partition {} at offset {}, resetting offset",
                             tp, currentPosition);
                    requestOffsetReset(tp);
                } else {
                    log.warn("Truncation detected for partition {} at offset {}, but no reset policy is set",
                             tp, currentPosition);
                    return Optional.of(new LogTruncation(tp, requestPosition, Optional.empty()));
                }
            } else if (epochEndOffset.endOffset() < currentPosition.offset) {
                if (hasDefaultOffsetResetPolicy()) {
                    SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                            epochEndOffset.endOffset(), Optional.of(epochEndOffset.leaderEpoch()),
                            currentPosition.currentLeader);
                    log.info("Truncation detected for partition {} at offset {}, resetting offset to " +
                             "the first offset known to diverge {}", tp, currentPosition, newPosition);
                    state.seekValidated(newPosition);
                } else {
                    OffsetAndMetadata divergentOffset = new OffsetAndMetadata(epochEndOffset.endOffset(),
                        Optional.of(epochEndOffset.leaderEpoch()), null);
                    log.warn("Truncation detected for partition {} at offset {} (the end offset from the " +
                             "broker is {}), but no reset policy is set", tp, currentPosition, divergentOffset);
                    return Optional.of(new LogTruncation(tp, requestPosition, Optional.of(divergentOffset)));
                }
            } else {
                state.completeValidation();
            }
        }

        return Optional.empty();
    }

    public synchronized boolean awaitingValidation(TopicPartition tp) {
        return assignedState(tp).awaitingValidation();
    }

    public synchronized void completeValidation(TopicPartition tp) {
        assignedState(tp).completeValidation();
    }

    public synchronized FetchPosition validPosition(TopicPartition tp) {
        return assignedState(tp).validPosition();
    }

    public synchronized FetchPosition position(TopicPartition tp) {
        return assignedState(tp).position;
    }

    public synchronized FetchPosition positionOrNull(TopicPartition tp) {
        final TopicPartitionState state = assignedStateOrNull(tp);
        if (state == null) {
            return null;
        }
        return assignedState(tp).position;
    }

    public synchronized Long partitionLag(TopicPartition tp, IsolationLevel isolationLevel) {
        TopicPartitionState topicPartitionState = assignedState(tp);
        if (topicPartitionState.position == null) {
            return null;
        } else if (isolationLevel == IsolationLevel.READ_COMMITTED) {
            return topicPartitionState.lastStableOffset == null ? null : topicPartitionState.lastStableOffset - topicPartitionState.position.offset;
        } else {
            return topicPartitionState.highWatermark == null ? null : topicPartitionState.highWatermark - topicPartitionState.position.offset;
        }
    }

    public synchronized Long partitionEndOffset(TopicPartition tp, IsolationLevel isolationLevel) {
        TopicPartitionState topicPartitionState = assignedState(tp);
        if (isolationLevel == IsolationLevel.READ_COMMITTED) {
            return topicPartitionState.lastStableOffset;
        } else {
            return topicPartitionState.highWatermark;
        }
    }

    public synchronized void requestPartitionEndOffset(TopicPartition tp) {
        TopicPartitionState topicPartitionState = assignedState(tp);
        topicPartitionState.requestEndOffset();
    }

    public synchronized boolean partitionEndOffsetRequested(TopicPartition tp) {
        TopicPartitionState topicPartitionState = assignedState(tp);
        return topicPartitionState.endOffsetRequested();
    }

    synchronized Long partitionLead(TopicPartition tp) {
        TopicPartitionState topicPartitionState = assignedState(tp);
        return topicPartitionState.logStartOffset == null ? null : topicPartitionState.position.offset - topicPartitionState.logStartOffset;
    }

    synchronized void updateHighWatermark(TopicPartition tp, long highWatermark) {
        assignedState(tp).highWatermark(highWatermark);
    }

    synchronized boolean tryUpdatingHighWatermark(TopicPartition tp, long highWatermark) {
        final TopicPartitionState state = assignedStateOrNull(tp);
        if (state != null) {
            assignedState(tp).highWatermark(highWatermark);
            return true;
        }
        return false;
    }

    synchronized boolean tryUpdatingLogStartOffset(TopicPartition tp, long highWatermark) {
        final TopicPartitionState state = assignedStateOrNull(tp);
        if (state != null) {
            assignedState(tp).logStartOffset(highWatermark);
            return true;
        }
        return false;
    }

    synchronized void updateLastStableOffset(TopicPartition tp, long lastStableOffset) {
        assignedState(tp).lastStableOffset(lastStableOffset);
    }

    synchronized boolean tryUpdatingLastStableOffset(TopicPartition tp, long lastStableOffset) {
        final TopicPartitionState state = assignedStateOrNull(tp);
        if (state != null) {
            assignedState(tp).lastStableOffset(lastStableOffset);
            return true;
        }
        return false;
    }

    /**
     * Set the preferred read replica with a lease timeout. After this time, the replica will no longer be valid and
     * {@link #preferredReadReplica(TopicPartition, long)} will return an empty result.
     *
     * @param tp The topic partition
     * @param preferredReadReplicaId The preferred read replica
     * @param timeMs The time at which this preferred replica is no longer valid
     */
    public synchronized void updatePreferredReadReplica(TopicPartition tp, int preferredReadReplicaId, LongSupplier timeMs) {
        assignedState(tp).updatePreferredReadReplica(preferredReadReplicaId, timeMs);
    }

    /**
     * Tries to set the preferred read replica with a lease timeout. After this time, the replica will no longer be valid and
     * {@link #preferredReadReplica(TopicPartition, long)} will return an empty result. If the preferred replica of
     * the partition could not be updated (e.g. because the partition is not assigned) this method will return
     * {@code false}, otherwise it will return {@code true}.
     *
     * @param tp The topic partition
     * @param preferredReadReplicaId The preferred read replica
     * @param timeMs The time at which this preferred replica is no longer valid
     * @return {@code true} if the preferred read replica was updated, {@code false} otherwise.
     */
    public synchronized boolean tryUpdatingPreferredReadReplica(TopicPartition tp,
                                                             int preferredReadReplicaId,
                                                             LongSupplier timeMs) {
        final TopicPartitionState state = assignedStateOrNull(tp);
        if (state != null) {
            assignedState(tp).updatePreferredReadReplica(preferredReadReplicaId, timeMs);
            return true;
        }
        return false;
    }

    /**
     * Get the preferred read replica
     *
     * @param tp The topic partition
     * @param timeMs The current time
     * @return Returns the current preferred read replica, if it has been set and if it has not expired.
     */
    public synchronized Optional<Integer> preferredReadReplica(TopicPartition tp, long timeMs) {
        final TopicPartitionState topicPartitionState = assignedStateOrNull(tp);
        if (topicPartitionState == null) {
            return Optional.empty();
        } else {
            return topicPartitionState.preferredReadReplica(timeMs);
        }
    }

    /**
     * Unset the preferred read replica. This causes the fetcher to go back to the leader for fetches.
     *
     * @param tp The topic partition
     * @return the removed preferred read replica if set, Empty otherwise.
     */
    public synchronized Optional<Integer> clearPreferredReadReplica(TopicPartition tp) {
        final TopicPartitionState topicPartitionState = assignedStateOrNull(tp);
        if (topicPartitionState == null) {
            return Optional.empty();
        } else {
            return topicPartitionState.clearPreferredReadReplica();
        }
    }

    public synchronized Map<TopicPartition, OffsetAndMetadata> allConsumed() {
        Map<TopicPartition, OffsetAndMetadata> allConsumed = new HashMap<>();
        assignment.forEach((topicPartition, partitionState) -> {
            if (partitionState.hasValidPosition())
                allConsumed.put(topicPartition, new OffsetAndMetadata(partitionState.position.offset,
                        partitionState.position.offsetEpoch, ""));
        });
        return allConsumed;
    }

    public synchronized void requestOffsetReset(TopicPartition partition, OffsetResetStrategy offsetResetStrategy) {
        assignedState(partition).reset(offsetResetStrategy);
    }

    public synchronized void requestOffsetReset(Collection<TopicPartition> partitions, OffsetResetStrategy offsetResetStrategy) {
        partitions.forEach(tp -> {
            log.info("Seeking to {} offset of partition {}", offsetResetStrategy, tp);
            assignedState(tp).reset(offsetResetStrategy);
        });
    }

    public void requestOffsetReset(TopicPartition partition) {
        requestOffsetReset(partition, defaultResetStrategy);
    }

    public synchronized void requestOffsetResetIfPartitionAssigned(TopicPartition partition) {
        final TopicPartitionState state = assignedStateOrNull(partition);
        if (state != null) {
            state.reset(defaultResetStrategy);
        }
    }


    synchronized void setNextAllowedRetry(Set<TopicPartition> partitions, long nextAllowResetTimeMs) {
        for (TopicPartition partition : partitions) {
            assignedState(partition).setNextAllowedRetry(nextAllowResetTimeMs);
        }
    }

    boolean hasDefaultOffsetResetPolicy() {
        return defaultResetStrategy != OffsetResetStrategy.NONE;
    }

    public synchronized boolean isOffsetResetNeeded(TopicPartition partition) {
        return assignedState(partition).awaitingReset();
    }

    public synchronized OffsetResetStrategy resetStrategy(TopicPartition partition) {
        return assignedState(partition).resetStrategy();
    }

    public synchronized boolean hasAllFetchPositions() {
        // Since this is in the hot-path for fetching, we do this instead of using java.util.stream API
        Iterator<TopicPartitionState> it = assignment.stateIterator();
        while (it.hasNext()) {
            if (!it.next().hasValidPosition()) {
                return false;
            }
        }
        return true;
    }

    public synchronized Set<TopicPartition> initializingPartitions() {
        return collectPartitions(TopicPartitionState::shouldInitialize);
    }

    private Set<TopicPartition> collectPartitions(Predicate<TopicPartitionState> filter) {
        Set<TopicPartition> result = new HashSet<>();
        assignment.forEach((topicPartition, topicPartitionState) -> {
            if (filter.test(topicPartitionState)) {
                result.add(topicPartition);
            }
        });
        return result;
    }

    /**
     * Note: this will not attempt to reset partitions that are in the process of being assigned
     * and are pending the completion of any {@link ConsumerRebalanceListener#onPartitionsAssigned(Collection)}
     * callbacks.
     *
     * <p/>
     *
     * This method only appears to be invoked the by the {@link KafkaConsumer} during its
     * {@link KafkaConsumer#poll(Duration)} logic. <em>Direct</em> calls to methods like
     * {@link #requestOffsetReset(TopicPartition)}, {@link #requestOffsetResetIfPartitionAssigned(TopicPartition)},
     * etc. do <em>not</em> skip partitions pending assignment.
     */
    public synchronized void resetInitializingPositions() {
        final Set<TopicPartition> partitionsWithNoOffsets = new HashSet<>();
        assignment.forEach((tp, partitionState) -> {
            if (partitionState.shouldInitialize()) {
                if (defaultResetStrategy == OffsetResetStrategy.NONE)
                    partitionsWithNoOffsets.add(tp);
                else
                    requestOffsetReset(tp);
            }
        });

        if (!partitionsWithNoOffsets.isEmpty())
            throw new NoOffsetForPartitionException(partitionsWithNoOffsets);
    }

    public synchronized Set<TopicPartition> partitionsNeedingReset(long nowMs) {
        return collectPartitions(state -> state.awaitingReset() && !state.awaitingRetryBackoff(nowMs));
    }

    public synchronized Set<TopicPartition> partitionsNeedingValidation(long nowMs) {
        return collectPartitions(state -> state.awaitingValidation() && !state.awaitingRetryBackoff(nowMs));
    }

    public synchronized boolean isAssigned(TopicPartition tp) {
        return assignment.contains(tp);
    }

    public synchronized boolean isPaused(TopicPartition tp) {
        TopicPartitionState assignedOrNull = assignedStateOrNull(tp);
        return assignedOrNull != null && assignedOrNull.isPaused();
    }

    synchronized boolean isFetchable(TopicPartition tp) {
        TopicPartitionState assignedOrNull = assignedStateOrNull(tp);
        return assignedOrNull != null && assignedOrNull.isFetchable();
    }

    public synchronized boolean hasValidPosition(TopicPartition tp) {
        TopicPartitionState assignedOrNull = assignedStateOrNull(tp);
        return assignedOrNull != null && assignedOrNull.hasValidPosition();
    }

    public synchronized void pause(TopicPartition tp) {
        assignedState(tp).pause();
    }

    public synchronized void markPendingRevocation(Set<TopicPartition> tps) {
        tps.forEach(tp -> assignedState(tp).markPendingRevocation());
    }

    // Visible for testing
    synchronized void markPendingOnAssignedCallback(Collection<TopicPartition> tps,
                                                    boolean pendingOnAssignedCallback) {
        tps.forEach(tp -> assignedState(tp).markPendingOnAssignedCallback(pendingOnAssignedCallback));
    }

    /**
     * Change the assignment to the specified partitions returned from the coordinator and mark
     * them as awaiting onPartitionsAssigned callback. This will ensure that the partitions are
     * included in the assignment, but are not fetchable or initialize positions while the
     * callback runs. This is expected to be used by the async consumer.
     *
     * @param fullAssignment  Full collection of partitions assigned. Includes previously owned
     *                        and newly added partitions.
     * @param addedPartitions Subset of the fullAssignment containing the added partitions. These
     *                        are not fetchable until the onPartitionsAssigned callback completes.
     */
    public synchronized void assignFromSubscribedAwaitingCallback(Collection<TopicPartition> fullAssignment,
                                                                  Collection<TopicPartition> addedPartitions) {
        assignFromSubscribed(fullAssignment);
        markPendingOnAssignedCallback(addedPartitions, true);
    }

    /**
     * Enable fetching and updating positions for the given partitions that were added to the
     * assignment, but waiting for the onPartitionsAssigned callback to complete. This is
     * expected to be used by the async consumer.
     */
    public synchronized void enablePartitionsAwaitingCallback(Collection<TopicPartition> partitions) {
        markPendingOnAssignedCallback(partitions, false);
    }

    public synchronized void resume(TopicPartition tp) {
        assignedState(tp).resume();
    }

    synchronized void requestFailed(Set<TopicPartition> partitions, long nextRetryTimeMs) {
        for (TopicPartition partition : partitions) {
            // by the time the request failed, the assignment may no longer
            // contain this partition any more, in which case we would just ignore.
            final TopicPartitionState state = assignedStateOrNull(partition);
            if (state != null)
                state.requestFailed(nextRetryTimeMs);
        }
    }

    synchronized void movePartitionToEnd(TopicPartition tp) {
        assignment.moveToEnd(tp);
    }

    public synchronized Optional<ConsumerRebalanceListener> rebalanceListener() {
        return rebalanceListener;
    }

    private static class TopicPartitionState {

        private FetchState fetchState;
        private FetchPosition position; // last consumed position

        private Long highWatermark; // the high watermark from last fetch
        private Long logStartOffset; // the log start offset
        private Long lastStableOffset;
        private boolean paused;  // whether this partition has been paused by the user
        private boolean pendingRevocation;
        private boolean pendingOnAssignedCallback;
        private OffsetResetStrategy resetStrategy;  // the strategy to use if the offset needs resetting
        private Long nextRetryTimeMs;
        private Integer preferredReadReplica;
        private Long preferredReadReplicaExpireTimeMs;
        private boolean endOffsetRequested;
        
        TopicPartitionState() {
            this.paused = false;
            this.pendingRevocation = false;
            this.pendingOnAssignedCallback = false;
            this.endOffsetRequested = false;
            this.fetchState = FetchStates.INITIALIZING;
            this.position = null;
            this.highWatermark = null;
            this.logStartOffset = null;
            this.lastStableOffset = null;
            this.resetStrategy = null;
            this.nextRetryTimeMs = null;
            this.preferredReadReplica = null;
        }

        public boolean endOffsetRequested() {
            return endOffsetRequested;
        }

        public void requestEndOffset() {
            endOffsetRequested = true;
        }

        private void transitionState(FetchState newState, Runnable runIfTransitioned) {
            FetchState nextState = this.fetchState.transitionTo(newState);
            if (nextState.equals(newState)) {
                this.fetchState = nextState;
                runIfTransitioned.run();
                if (this.position == null && nextState.requiresPosition()) {
                    throw new IllegalStateException("Transitioned subscription state to " + nextState + ", but position is null");
                } else if (!nextState.requiresPosition()) {
                    this.position = null;
                }
            }
        }

        private Optional<Integer> preferredReadReplica(long timeMs) {
            if (preferredReadReplicaExpireTimeMs != null && timeMs > preferredReadReplicaExpireTimeMs) {
                preferredReadReplica = null;
                return Optional.empty();
            } else {
                return Optional.ofNullable(preferredReadReplica);
            }
        }

        private void updatePreferredReadReplica(int preferredReadReplica, LongSupplier timeMs) {
            if (this.preferredReadReplica == null || preferredReadReplica != this.preferredReadReplica) {
                this.preferredReadReplica = preferredReadReplica;
                this.preferredReadReplicaExpireTimeMs = timeMs.getAsLong();
            }
        }

        private Optional<Integer> clearPreferredReadReplica() {
            if (preferredReadReplica != null) {
                int removedReplicaId = this.preferredReadReplica;
                this.preferredReadReplica = null;
                this.preferredReadReplicaExpireTimeMs = null;
                return Optional.of(removedReplicaId);
            } else {
                return Optional.empty();
            }
        }

        private void reset(OffsetResetStrategy strategy) {
            transitionState(FetchStates.AWAIT_RESET, () -> {
                this.resetStrategy = strategy;
                this.nextRetryTimeMs = null;
            });
        }

        /**
         * Check if the position exists and needs to be validated. If so, enter the AWAIT_VALIDATION state. This method
         * also will update the position with the current leader and epoch.
         *
         * @param currentLeaderAndEpoch leader and epoch to compare the offset with
         * @return true if the position is now awaiting validation
         */
        private boolean maybeValidatePosition(Metadata.LeaderAndEpoch currentLeaderAndEpoch) {
            if (this.fetchState.equals(FetchStates.AWAIT_RESET)) {
                return false;
            }

            if (!currentLeaderAndEpoch.leader.isPresent()) {
                return false;
            }

            if (position != null && !position.currentLeader.equals(currentLeaderAndEpoch)) {
                FetchPosition newPosition = new FetchPosition(position.offset, position.offsetEpoch, currentLeaderAndEpoch);
                validatePosition(newPosition);
                preferredReadReplica = null;
            }
            return this.fetchState.equals(FetchStates.AWAIT_VALIDATION);
        }

        /**
         * For older versions of the API, we cannot perform offset validation so we simply transition directly to FETCHING
         */
        private void updatePositionLeaderNoValidation(Metadata.LeaderAndEpoch currentLeaderAndEpoch) {
            if (position != null) {
                transitionState(FetchStates.FETCHING, () -> {
                    this.position = new FetchPosition(position.offset, position.offsetEpoch, currentLeaderAndEpoch);
                    this.nextRetryTimeMs = null;
                });
            }
        }

        private void validatePosition(FetchPosition position) {
            if (position.offsetEpoch.isPresent() && position.currentLeader.epoch.isPresent()) {
                transitionState(FetchStates.AWAIT_VALIDATION, () -> {
                    this.position = position;
                    this.nextRetryTimeMs = null;
                });
            } else {
                // If we have no epoch information for the current position, then we can skip validation
                transitionState(FetchStates.FETCHING, () -> {
                    this.position = position;
                    this.nextRetryTimeMs = null;
                });
            }
        }

        /**
         * Clear the awaiting validation state and enter fetching.
         */
        private void completeValidation() {
            if (hasPosition()) {
                transitionState(FetchStates.FETCHING, () -> this.nextRetryTimeMs = null);
            }
        }

        private boolean awaitingValidation() {
            return fetchState.equals(FetchStates.AWAIT_VALIDATION);
        }

        private boolean awaitingRetryBackoff(long nowMs) {
            return nextRetryTimeMs != null && nowMs < nextRetryTimeMs;
        }

        private boolean awaitingReset() {
            return fetchState.equals(FetchStates.AWAIT_RESET);
        }

        private void setNextAllowedRetry(long nextAllowedRetryTimeMs) {
            this.nextRetryTimeMs = nextAllowedRetryTimeMs;
        }

        private void requestFailed(long nextAllowedRetryTimeMs) {
            this.nextRetryTimeMs = nextAllowedRetryTimeMs;
        }

        private boolean hasValidPosition() {
            return fetchState.hasValidPosition();
        }

        private boolean hasPosition() {
            return position != null;
        }

        private boolean isPaused() {
            return paused;
        }

        private void seekValidated(FetchPosition position) {
            transitionState(FetchStates.FETCHING, () -> {
                this.position = position;
                this.resetStrategy = null;
                this.nextRetryTimeMs = null;
            });
        }

        private void seekUnvalidated(FetchPosition fetchPosition) {
            seekValidated(fetchPosition);
            validatePosition(fetchPosition);
        }

        private void position(FetchPosition position) {
            if (!hasValidPosition())
                throw new IllegalStateException("Cannot set a new position without a valid current position");
            this.position = position;
        }

        private FetchPosition validPosition() {
            if (hasValidPosition()) {
                return position;
            } else {
                return null;
            }
        }

        private void pause() {
            this.paused = true;
        }

        private void markPendingRevocation() {
            this.pendingRevocation = true;
        }

        private void markPendingOnAssignedCallback(boolean pendingOnAssignedCallback) {
            this.pendingOnAssignedCallback = pendingOnAssignedCallback;
        }

        private void resume() {
            this.paused = false;
        }

        /**
         * True if the partition is in {@link FetchStates#INITIALIZING} state. While in this
         * state, a position for the partition can be retrieved (based on committed offsets or
         * partitions offsets).
         * Note that retrieving a position does not mean that we can start fetching from the
         * partition (see {@link #isFetchable()})
         */
        private boolean shouldInitialize() {
            return fetchState.equals(FetchStates.INITIALIZING);
        }

        private boolean isFetchable() {
            return !paused && !pendingRevocation && !pendingOnAssignedCallback && hasValidPosition();
        }

        private void highWatermark(Long highWatermark) {
            this.highWatermark = highWatermark;
            this.endOffsetRequested = false;
        }

        private void logStartOffset(Long logStartOffset) {
            this.logStartOffset = logStartOffset;
        }

        private void lastStableOffset(Long lastStableOffset) {
            this.lastStableOffset = lastStableOffset;
            this.endOffsetRequested = false;
        }

        private OffsetResetStrategy resetStrategy() {
            return resetStrategy;
        }
    }

    /**
     * The fetch state of a partition. This class is used to determine valid state transitions and expose the some of
     * the behavior of the current fetch state. Actual state variables are stored in the {@link TopicPartitionState}.
     */
    interface FetchState {
        default FetchState transitionTo(FetchState newState) {
            if (validTransitions().contains(newState)) {
                return newState;
            } else {
                return this;
            }
        }

        /**
         * Return the valid states which this state can transition to
         */
        Collection<FetchState> validTransitions();

        /**
         * Test if this state requires a position to be set
         */
        boolean requiresPosition();

        /**
         * Test if this state is considered to have a valid position which can be used for fetching
         */
        boolean hasValidPosition();
    }

    /**
     * An enumeration of all the possible fetch states. The state transitions are encoded in the values returned by
     * {@link FetchState#validTransitions}.
     */
    enum FetchStates implements FetchState {
        INITIALIZING() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET, FetchStates.AWAIT_VALIDATION);
            }

            @Override
            public boolean requiresPosition() {
                return false;
            }

            @Override
            public boolean hasValidPosition() {
                return false;
            }
        },

        FETCHING() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET, FetchStates.AWAIT_VALIDATION);
            }

            @Override
            public boolean requiresPosition() {
                return true;
            }

            @Override
            public boolean hasValidPosition() {
                return true;
            }
        },

        AWAIT_RESET() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET);
            }

            @Override
            public boolean requiresPosition() {
                return false;
            }

            @Override
            public boolean hasValidPosition() {
                return false;
            }
        },

        AWAIT_VALIDATION() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET, FetchStates.AWAIT_VALIDATION);
            }

            @Override
            public boolean requiresPosition() {
                return true;
            }

            @Override
            public boolean hasValidPosition() {
                return false;
            }
        }
    }

    /**
     * Represents the position of a partition subscription.
     *
     * This includes the offset and epoch from the last record in
     * the batch from a FetchResponse. It also includes the leader epoch at the time the batch was consumed.
     */
    public static class FetchPosition {
        public final long offset;
        final Optional<Integer> offsetEpoch;
        final Metadata.LeaderAndEpoch currentLeader;

        FetchPosition(long offset) {
            this(offset, Optional.empty(), Metadata.LeaderAndEpoch.noLeaderOrEpoch());
        }

        public FetchPosition(long offset, Optional<Integer> offsetEpoch, Metadata.LeaderAndEpoch currentLeader) {
            this.offset = offset;
            this.offsetEpoch = Objects.requireNonNull(offsetEpoch);
            this.currentLeader = Objects.requireNonNull(currentLeader);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FetchPosition that = (FetchPosition) o;
            return offset == that.offset &&
                    offsetEpoch.equals(that.offsetEpoch) &&
                    currentLeader.equals(that.currentLeader);
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, offsetEpoch, currentLeader);
        }

        @Override
        public String toString() {
            return "FetchPosition{" +
                    "offset=" + offset +
                    ", offsetEpoch=" + offsetEpoch +
                    ", currentLeader=" + currentLeader +
                    '}';
        }
    }

    public static class LogTruncation {
        public final TopicPartition topicPartition;
        public final FetchPosition fetchPosition;
        public final Optional<OffsetAndMetadata> divergentOffsetOpt;

        public LogTruncation(TopicPartition topicPartition,
                             FetchPosition fetchPosition,
                             Optional<OffsetAndMetadata> divergentOffsetOpt) {
            this.topicPartition = topicPartition;
            this.fetchPosition = fetchPosition;
            this.divergentOffsetOpt = divergentOffsetOpt;
        }

        @Override
        public String toString() {
            StringBuilder bldr = new StringBuilder()
                .append("(partition=")
                .append(topicPartition)
                .append(", fetchOffset=")
                .append(fetchPosition.offset)
                .append(", fetchEpoch=")
                .append(fetchPosition.offsetEpoch);

            if (divergentOffsetOpt.isPresent()) {
                OffsetAndMetadata divergentOffset = divergentOffsetOpt.get();
                bldr.append(", divergentOffset=")
                    .append(divergentOffset.offset())
                    .append(", divergentEpoch=")
                    .append(divergentOffset.leaderEpoch());
            } else {
                bldr.append(", divergentOffset=unknown")
                    .append(", divergentEpoch=unknown");
            }

            return bldr.append(")").toString();

        }
    }
}
