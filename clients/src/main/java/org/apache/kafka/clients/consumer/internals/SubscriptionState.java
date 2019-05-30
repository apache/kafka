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

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.PartitionStates;
import org.apache.kafka.common.requests.EpochEndOffset;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * A class for tracking the topics, partitions, and offsets for the consumer. A partition
 * is "assigned" either directly with {@link #assignFromUser(Set)} (manual assignment)
 * or with {@link #assignFromSubscribed(Collection)} (automatic assignment from subscription).
 *
 * Once assigned, the partition is not considered "fetchable" until its initial position has
 * been set with {@link #seekValidated(TopicPartition, FetchPosition)}. Fetchable partitions track a fetch
 * position which is used to set the offset of the next fetch, and a consumed position
 * which is the last offset that has been returned to the user. You can suspend fetching
 * from a partition through {@link #pause(TopicPartition)} without affecting the fetched/consumed
 * offsets. The partition will remain unfetchable until the {@link #resume(TopicPartition)} is
 * used. You can also query the pause state independently with {@link #isPaused(TopicPartition)}.
 *
 * Note that pause state as well as fetch/consumed positions are not preserved when partition
 * assignment is changed whether directly by the user or through a group rebalance.
 *
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
    private ConsumerRebalanceListener rebalanceListener;

    private int assignmentId = 0;

    public SubscriptionState(LogContext logContext, OffsetResetStrategy defaultResetStrategy) {
        this.log = logContext.logger(this.getClass());
        this.defaultResetStrategy = defaultResetStrategy;
        this.subscription = new HashSet<>();
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

    public synchronized boolean subscribe(Set<String> topics, ConsumerRebalanceListener listener) {
        registerRebalanceListener(listener);
        setSubscriptionType(SubscriptionType.AUTO_TOPICS);
        return changeSubscription(topics);
    }

    public synchronized void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
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
        groupSubscription = new HashSet<>(groupSubscription);
        groupSubscription.addAll(topicsToSubscribe);
        return true;
    }

    /**
     * Add topics to the current group subscription. This is used by the group leader to ensure
     * that it receives metadata updates for all topics that the group is interested in.
     * @param topics The topics to add to the group subscription
     */
    synchronized boolean groupSubscribe(Collection<String> topics) {
        if (!partitionsAutoAssigned())
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
        groupSubscription = new HashSet<>(groupSubscription);
        return groupSubscription.addAll(topics);
    }

    /**
     * Reset the group's subscription to only contain topics subscribed by this consumer.
     */
    synchronized void resetGroupSubscription() {
        groupSubscription = subscription;
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
     * Change the assignment to the specified partitions returned from the coordinator, note this is
     * different from {@link #assignFromUser(Set)} which directly set the assignment from user inputs.
     *
     * @return true if assignments matches subscription, otherwise false
     */
    public synchronized boolean assignFromSubscribed(Collection<TopicPartition> assignments) {
        if (!this.partitionsAutoAssigned())
            throw new IllegalArgumentException("Attempt to dynamically assign partitions while manual assignment in use");

        boolean assignmentMatchedSubscription = true;
        for (TopicPartition topicPartition : assignments) {
            if (this.subscribedPattern != null) {
                assignmentMatchedSubscription = this.subscribedPattern.matcher(topicPartition.topic()).matches();
                if (!assignmentMatchedSubscription) {
                    log.info("Assigned partition {} for non-subscribed topic regex pattern; subscription pattern is {}",
                            topicPartition,
                            this.subscribedPattern);
                    break;
                }
            } else {
                assignmentMatchedSubscription = this.subscription.contains(topicPartition.topic());
                if (!assignmentMatchedSubscription) {
                    log.info("Assigned partition {} for non-subscribed topic; subscription is {}", topicPartition, this.subscription);
                    break;
                }
            }
        }

        if (assignmentMatchedSubscription) {
            Map<TopicPartition, TopicPartitionState> assignedPartitionStates = partitionToStateMap(
                    assignments);
            assignmentId++;
            this.assignment.set(assignedPartitionStates);
        }

        return assignmentMatchedSubscription;
    }

    private void registerRebalanceListener(ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");
        this.rebalanceListener = listener;
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
        if (partitionsAutoAssigned())
            return this.subscription;
        return Collections.emptySet();
    }

    public synchronized Set<TopicPartition> pausedPartitions() {
        return collectPartitions(TopicPartitionState::isPaused, Collectors.toSet());
    }

    /**
     * Get the subscription for the group. For the leader, this will include the union of the
     * subscriptions of all group members. For followers, it is just that member's subscription.
     * This is used when querying topic metadata to detect the metadata changes which would
     * require rebalancing. The leader fetches metadata for all topics in the group so that it
     * can do the partition assignment (which requires at least partition counts for all topics
     * to be assigned).
     *
     * @return The union of all subscribed topics in the group if this member is the leader
     *   of the current generation; otherwise it returns the same set as {@link #subscription()}
     */
    synchronized Set<String> groupSubscription() {
        return this.groupSubscription;
    }

    synchronized boolean isGroupSubscribed(String topic) {
        return groupSubscription.contains(topic);
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

    synchronized void maybeSeekUnvalidated(TopicPartition tp, long offset, OffsetResetStrategy requestedResetStrategy) {
        TopicPartitionState state = assignedStateOrNull(tp);
        if (state == null) {
            log.debug("Skipping reset of partition {} since it is no longer assigned", tp);
        } else if (!state.awaitingReset()) {
            log.debug("Skipping reset of partition {} since reset is no longer needed", tp);
        } else if (requestedResetStrategy != state.resetStrategy) {
            log.debug("Skipping reset of partition {} since an alternative reset has been requested", tp);
        } else {
            log.info("Resetting offset for partition {} to offset {}.", tp, offset);
            state.seekUnvalidated(new FetchPosition(offset));
        }
    }

    /**
     * @return a modifiable copy of the currently assigned partitions
     */
    public synchronized Set<TopicPartition> assignedPartitions() {
        return new HashSet<>(this.assignment.partitionSet());
    }

    /**
     * Provides the number of assigned partitions in a thread safe manner.
     * @return the number of assigned partitions.
     */
    synchronized int numAssignedPartitions() {
        return this.assignment.size();
    }

    synchronized List<TopicPartition> fetchablePartitions(Predicate<TopicPartition> isAvailable) {
        return assignment.stream()
                .filter(tpState -> isAvailable.test(tpState.topicPartition()) && tpState.value().isFetchable())
                .map(PartitionStates.PartitionState::topicPartition)
                .collect(Collectors.toList());
    }

    synchronized boolean partitionsAutoAssigned() {
        return this.subscriptionType == SubscriptionType.AUTO_TOPICS || this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    public synchronized void position(TopicPartition tp, FetchPosition position) {
        assignedState(tp).position(position);
    }

    public synchronized boolean maybeValidatePositionForCurrentLeader(TopicPartition tp, Metadata.LeaderAndEpoch leaderAndEpoch) {
        return assignedState(tp).maybeValidatePosition(leaderAndEpoch);
    }

    /**
     * Attempt to complete validation with the end offset returned from the OffsetForLeaderEpoch request.
     * @return The diverging offset if truncation was detected and no reset policy is defined.
     */
    public synchronized Optional<OffsetAndMetadata> maybeCompleteValidation(TopicPartition tp,
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
            } else if (epochEndOffset.endOffset() < currentPosition.offset) {
                if (hasDefaultOffsetResetPolicy()) {
                    SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                            epochEndOffset.endOffset(), Optional.of(epochEndOffset.leaderEpoch()),
                            currentPosition.currentLeader);
                    log.info("Truncation detected for partition {} at offset {}, resetting offset to " +
                            "the first offset known to diverge {}", tp, currentPosition, newPosition);
                    state.seekValidated(newPosition);
                } else {
                    log.warn("Truncation detected for partition {} at offset {} (the end offset from the " +
                                    "broker is {}), but no reset policy is set",
                            tp, currentPosition, epochEndOffset);
                    return Optional.of(new OffsetAndMetadata(epochEndOffset.endOffset(),
                            Optional.of(epochEndOffset.leaderEpoch()), null));
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

    synchronized Long partitionLag(TopicPartition tp, IsolationLevel isolationLevel) {
        TopicPartitionState topicPartitionState = assignedState(tp);
        if (isolationLevel == IsolationLevel.READ_COMMITTED)
            return topicPartitionState.lastStableOffset == null ? null : topicPartitionState.lastStableOffset - topicPartitionState.position.offset;
        else
            return topicPartitionState.highWatermark == null ? null : topicPartitionState.highWatermark - topicPartitionState.position.offset;
    }

    synchronized Long partitionLead(TopicPartition tp) {
        TopicPartitionState topicPartitionState = assignedState(tp);
        return topicPartitionState.logStartOffset == null ? null : topicPartitionState.position.offset - topicPartitionState.logStartOffset;
    }

    synchronized void updateHighWatermark(TopicPartition tp, long highWatermark) {
        assignedState(tp).highWatermark(highWatermark);
    }

    synchronized void updateLogStartOffset(TopicPartition tp, long logStartOffset) {
        assignedState(tp).logStartOffset(logStartOffset);
    }

    synchronized void updateLastStableOffset(TopicPartition tp, long lastStableOffset) {
        assignedState(tp).lastStableOffset(lastStableOffset);
    }

    /**
     * Set the preferred read replica with a lease timeout. After this time, the replica will no longer be valid and
     * {@link #preferredReadReplica(TopicPartition, long)} will return an empty result.
     *
     * @param tp The topic partition
     * @param preferredReadReplicaId The preferred read replica
     * @param timeMs The time at which this preferred replica is no longer valid
     */
    public synchronized void updatePreferredReadReplica(TopicPartition tp, int preferredReadReplicaId, Supplier<Long> timeMs) {
        assignedState(tp).updatePreferredReadReplica(preferredReadReplicaId, timeMs);
    }

    /**
     * Get the preferred read replica
     *
     * @param tp The topic partition
     * @param timeMs The current time
     * @return Returns the current preferred read replica, if it has been set and if it has not expired.
     */
    public synchronized Optional<Integer> preferredReadReplica(TopicPartition tp, long timeMs) {
        return assignedState(tp).preferredReadReplica(timeMs);
    }

    /**
     * Unset the preferred read replica. This causes the fetcher to go back to the leader for fetches.
     *
     * @param tp The topic partition
     * @return true if the preferred read replica was set, false otherwise.
     */
    public synchronized Optional<Integer> clearPreferredReadReplica(TopicPartition tp) {
        return assignedState(tp).clearPreferredReadReplica();
    }

    public synchronized Map<TopicPartition, OffsetAndMetadata> allConsumed() {
        Map<TopicPartition, OffsetAndMetadata> allConsumed = new HashMap<>();
        assignment.stream().forEach(state -> {
            TopicPartitionState partitionState = state.value();
            if (partitionState.hasValidPosition())
                allConsumed.put(state.topicPartition(), new OffsetAndMetadata(partitionState.position.offset,
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
        return assignment.stream().allMatch(state -> state.value().hasValidPosition());
    }

    public synchronized Set<TopicPartition> missingFetchPositions() {
        return collectPartitions(state -> !state.hasPosition(), Collectors.toSet());
    }

    private <T extends Collection<TopicPartition>> T collectPartitions(Predicate<TopicPartitionState> filter, Collector<TopicPartition, ?, T> collector) {
        return assignment.stream()
                .filter(state -> filter.test(state.value()))
                .map(PartitionStates.PartitionState::topicPartition)
                .collect(collector);
    }


    public synchronized void resetMissingPositions() {
        final Set<TopicPartition> partitionsWithNoOffsets = new HashSet<>();
        assignment.stream().forEach(state -> {
            TopicPartition tp = state.topicPartition();
            TopicPartitionState partitionState = state.value();
            if (!partitionState.hasPosition()) {
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
        return collectPartitions(state -> state.awaitingReset() && !state.awaitingRetryBackoff(nowMs),
                Collectors.toSet());
    }

    public synchronized Set<TopicPartition> partitionsNeedingValidation(long nowMs) {
        return collectPartitions(state -> state.awaitingValidation() && !state.awaitingRetryBackoff(nowMs),
                Collectors.toSet());
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

    public synchronized void resume(TopicPartition tp) {
        assignedState(tp).resume();
    }

    synchronized void requestFailed(Set<TopicPartition> partitions, long nextRetryTimeMs) {
        for (TopicPartition partition : partitions)
            assignedState(partition).requestFailed(nextRetryTimeMs);
    }

    synchronized void movePartitionToEnd(TopicPartition tp) {
        assignment.moveToEnd(tp);
    }

    public synchronized ConsumerRebalanceListener rebalanceListener() {
        return rebalanceListener;
    }

    private static Map<TopicPartition, TopicPartitionState> partitionToStateMap(Collection<TopicPartition> assignments) {
        Map<TopicPartition, TopicPartitionState> map = new HashMap<>(assignments.size());
        for (TopicPartition tp : assignments)
            map.put(tp, new TopicPartitionState());
        return map;
    }

    private static class TopicPartitionState {

        private FetchState fetchState;
        private FetchPosition position; // last consumed position

        private Long highWatermark; // the high watermark from last fetch
        private Long logStartOffset; // the log start offset
        private Long lastStableOffset;
        private boolean paused;  // whether this partition has been paused by the user
        private OffsetResetStrategy resetStrategy;  // the strategy to use if the offset needs resetting
        private Long nextRetryTimeMs;
        private Integer preferredReadReplica;
        private Long preferredReadReplicaExpireTimeMs;

        TopicPartitionState() {
            this.paused = false;
            this.fetchState = FetchStates.INITIALIZING;
            this.position = null;
            this.highWatermark = null;
            this.logStartOffset = null;
            this.lastStableOffset = null;
            this.resetStrategy = null;
            this.nextRetryTimeMs = null;
            this.preferredReadReplica = null;
        }

        private void transitionState(FetchState newState, Runnable runIfTransitioned) {
            FetchState nextState = this.fetchState.transitionTo(newState);
            if (nextState.equals(newState)) {
                this.fetchState = nextState;
                runIfTransitioned.run();
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

        private void updatePreferredReadReplica(int preferredReadReplica, Supplier<Long> timeMs) {
            if (this.preferredReadReplica == null || preferredReadReplica != this.preferredReadReplica) {
                this.preferredReadReplica = preferredReadReplica;
                this.preferredReadReplicaExpireTimeMs = timeMs.get();
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

        private boolean maybeValidatePosition(Metadata.LeaderAndEpoch currentLeaderAndEpoch) {
            if (this.fetchState.equals(FetchStates.AWAIT_RESET)) {
                return false;
            }

            if (currentLeaderAndEpoch.equals(Metadata.LeaderAndEpoch.noLeaderOrEpoch())) {
                // Ignore empty LeaderAndEpochs
                return false;
            }

            if (position != null && !position.currentLeader.equals(currentLeaderAndEpoch)) {
                FetchPosition newPosition = new FetchPosition(position.offset, position.offsetEpoch, currentLeaderAndEpoch);
                validatePosition(newPosition);
                preferredReadReplica = null;
            }
            return this.fetchState.equals(FetchStates.AWAIT_VALIDATION);
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
                transitionState(FetchStates.FETCHING, () -> {
                    this.nextRetryTimeMs = null;
                });
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
            return fetchState.hasPosition();
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

        private void resume() {
            this.paused = false;
        }

        private boolean isFetchable() {
            return !paused && hasValidPosition();
        }

        private void highWatermark(Long highWatermark) {
            this.highWatermark = highWatermark;
        }

        private void logStartOffset(Long logStartOffset) {
            this.logStartOffset = logStartOffset;
        }

        private void lastStableOffset(Long lastStableOffset) {
            this.lastStableOffset = lastStableOffset;
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

        Collection<FetchState> validTransitions();

        boolean hasPosition();

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
            public boolean hasPosition() {
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
            public boolean hasPosition() {
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
            public boolean hasPosition() {
                return true;
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
            public boolean hasPosition() {
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
     *
     * The last fetch epoch is used to
     */
    public static class FetchPosition {
        public final long offset;
        final Optional<Integer> offsetEpoch;
        final Metadata.LeaderAndEpoch currentLeader;

        FetchPosition(long offset) {
            this(offset, Optional.empty(), new Metadata.LeaderAndEpoch(Node.noNode(), Optional.empty()));
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
}
