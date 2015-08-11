/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerSeekCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A class for tracking the topics, partitions, and offsets for the consumer. A partition
 * is "assigned" either directly with {@link #subscribe(TopicPartition)} (manual assignment)
 * or with {@link #changePartitionAssignment(List)} (automatic assignment).
 *
 * Once assigned, the partition is not considered "fetchable" until its initial position has
 * been set with {@link #seek(TopicPartition, long)}. Fetchable partitions track a fetch
 * position which is used to set the offset of the next fetch, and a consumed position
 * which is the last offset that has been returned to the user. You can suspend fetching
 * from a partition through {@link #pause(TopicPartition)} without affecting the fetched/consumed
 * offsets. The partition will remain unfetchable until the {@link #resume(TopicPartition)} is
 * used. You can also query the pause state independently with {@link #isPaused(TopicPartition)}.
 *
 * Note that pause state as well as fetch/consumed positions are not preserved when partition
 * assignment is changed either with {@link #unsubscribe(TopicPartition)} or
 * {@link #changePartitionAssignment(List)}.
 *
 * This class also maintains a cache of the latest commit position for each of the assigned
 * partitions. This is updated through {@link #committed(TopicPartition, long)} and can be used
 * to set the initial fetch position (e.g. {@link Fetcher#resetOffset(TopicPartition)}.
 */
public class SubscriptionState {

    /* the list of topics the user has requested */
    private final Set<String> subscribedTopics;

    /* the list of partitions the user has requested */
    private final Set<TopicPartition> subscribedPartitions;

    /* the list of partitions currently assigned */
    private final Map<TopicPartition, TopicPartitionState> assignedPartitions;

    /* do we need to request a partition assignment from the coordinator? */
    private boolean needsPartitionAssignment;

    /* do we need to request the latest committed offsets from the coordinator? */
    private boolean needsFetchCommittedOffsets;

    /* Default offset reset strategy */
    private final OffsetResetStrategy defaultResetStrategy;

    public SubscriptionState(OffsetResetStrategy defaultResetStrategy) {
        this.defaultResetStrategy = defaultResetStrategy;
        this.subscribedTopics = new HashSet<>();
        this.subscribedPartitions = new HashSet<>();
        this.assignedPartitions = new HashMap<>();
        this.needsPartitionAssignment = false;
        this.needsFetchCommittedOffsets = true; // initialize to true for the consumers to fetch offset upon starting up
    }

    public void subscribe(String topic) {
        if (!this.subscribedPartitions.isEmpty())
            throw new IllegalStateException("Subscription to topics and partitions are mutually exclusive");
        if (!this.subscribedTopics.contains(topic)) {
            this.subscribedTopics.add(topic);
            this.needsPartitionAssignment = true;
        }
    }

    public void unsubscribe(String topic) {
        if (!this.subscribedTopics.contains(topic))
            throw new IllegalStateException("Topic " + topic + " was never subscribed to.");
        this.subscribedTopics.remove(topic);
        this.needsPartitionAssignment = true;
        final List<TopicPartition> existingAssignedPartitions = new ArrayList<>(assignedPartitions());
        for (TopicPartition tp: existingAssignedPartitions)
            if (topic.equals(tp.topic()))
                clearPartition(tp);
    }

    public void needReassignment() {
        this.needsPartitionAssignment = true;
    }

    public void subscribe(TopicPartition tp) {
        if (!this.subscribedTopics.isEmpty())
            throw new IllegalStateException("Subscription to topics and partitions are mutually exclusive");
        this.subscribedPartitions.add(tp);
        addAssignedPartition(tp);
    }

    public void unsubscribe(TopicPartition partition) {
        if (!subscribedPartitions.contains(partition))
            throw new IllegalStateException("Partition " + partition + " was never subscribed to.");
        subscribedPartitions.remove(partition);
        clearPartition(partition);
    }
    
    private void clearPartition(TopicPartition tp) {
        invokeConsumerSeekCallback(tp, null);
        this.assignedPartitions.remove(tp);
    }

    public void clearAssignment() {
        for (TopicPartition tp: this.assignedPartitions.keySet())
            invokeConsumerSeekCallback(tp, null);
        this.assignedPartitions.clear();
        this.needsPartitionAssignment = !subscribedTopics().isEmpty();
    }

    public Set<String> subscribedTopics() {
        return this.subscribedTopics;
    }

    public Long fetched(TopicPartition tp) {
        return assignedState(tp).fetched;
    }

    public void fetched(TopicPartition tp, long offset) {
        assignedState(tp).fetched(offset);
    }

    private TopicPartitionState assignedState(TopicPartition tp) {
        TopicPartitionState state = this.assignedPartitions.get(tp);
        if (state == null)
            throw new IllegalStateException("No current assignment for partition " + tp);
        return state;
    }

    public void committed(TopicPartition tp, long offset) {
        assignedState(tp).committed(offset);
    }

    public Long committed(TopicPartition tp) {
        return assignedState(tp).committed;
    }

    public void needRefreshCommits() {
        this.needsFetchCommittedOffsets = true;
    }

    public boolean refreshCommitsNeeded() {
        return this.needsFetchCommittedOffsets;
    }

    public void commitsRefreshed() {
        this.needsFetchCommittedOffsets = false;
    }

    public void seek(TopicPartition tp, long offset) {
        seek(tp, offset, null);
    }

    public void seek(TopicPartition tp, long offset, ConsumerSeekCallback callback) {
        assignedState(tp).seek(offset, callback);
    }

    public void invokeConsumerSeekCallback(TopicPartition tp, Exception exception) {
        assignedState(tp).invokeConsumerSeekCallback(exception);
    }

    public Set<TopicPartition> assignedPartitions() {
        return this.assignedPartitions.keySet();
    }

    public Set<TopicPartition> fetchablePartitions() {
        Set<TopicPartition> fetchable = new HashSet<>();
        for (Map.Entry<TopicPartition, TopicPartitionState> entry : assignedPartitions.entrySet()) {
            if (entry.getValue().isFetchable())
                fetchable.add(entry.getKey());
        }
        return fetchable;
    }

    public boolean partitionsAutoAssigned() {
        return !this.subscribedTopics.isEmpty();
    }

    public void consumed(TopicPartition tp, long offset) {
        assignedState(tp).consumed(offset);
    }

    public Long consumed(TopicPartition tp) {
        return assignedState(tp).consumed;
    }

    public Map<TopicPartition, Long> allConsumed() {
        Map<TopicPartition, Long> allConsumed = new HashMap<>();
        for (Map.Entry<TopicPartition, TopicPartitionState> entry : assignedPartitions.entrySet()) {
            TopicPartitionState state = entry.getValue();
            if (state.hasValidPosition)
                allConsumed.put(entry.getKey(), state.consumed);
        }
        return allConsumed;
    }

    public void needOffsetReset(TopicPartition partition, OffsetResetStrategy offsetResetStrategy) {
        assignedState(partition).awaitReset(offsetResetStrategy);
    }

    public void needOffsetReset(TopicPartition partition) {
        needOffsetReset(partition, defaultResetStrategy);
    }

    public boolean isOffsetResetNeeded(TopicPartition partition) {
        return assignedState(partition).awaitingReset;
    }

    public OffsetResetStrategy resetStrategy(TopicPartition partition) {
        return assignedState(partition).resetStrategy;
    }

    public boolean hasAllFetchPositions() {
        for (TopicPartitionState state : assignedPartitions.values())
            if (!state.hasValidPosition)
                return false;
        return true;
    }

    public Set<TopicPartition> missingFetchPositions() {
        Set<TopicPartition> missing = new HashSet<>(this.assignedPartitions.keySet());
        for (Map.Entry<TopicPartition, TopicPartitionState> entry : assignedPartitions.entrySet())
            if (!entry.getValue().hasValidPosition)
                missing.add(entry.getKey());
        return missing;
    }

    public boolean partitionAssignmentNeeded() {
        return this.needsPartitionAssignment;
    }

    public void changePartitionAssignment(List<TopicPartition> assignments) {
        for (TopicPartition tp : assignments)
            if (!this.subscribedTopics.contains(tp.topic()))
                throw new IllegalArgumentException("Assigned partition " + tp + " for non-subscribed topic.");
        this.clearAssignment();
        for (TopicPartition tp: assignments)
            addAssignedPartition(tp);
        this.needsPartitionAssignment = false;
    }

    public boolean isAssigned(TopicPartition tp) {
        return assignedPartitions.containsKey(tp);
    }

    public boolean isPaused(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).paused;
    }

    public boolean isFetchable(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).isFetchable();
    }

    public void pause(TopicPartition tp) {
        assignedState(tp).pause();
    }

    public void resume(TopicPartition tp) {
        assignedState(tp).resume();
    }

    private void addAssignedPartition(TopicPartition tp) {
        this.assignedPartitions.put(tp, new TopicPartitionState());
    }

    private static class TopicPartitionState {
        private Long consumed;   // offset exposed to the user
        private Long fetched;    // current fetch position
        private Long committed;  // last committed position

        private boolean hasValidPosition; // whether we have valid consumed and fetched positions
        private boolean paused;  // whether this partition has been paused by the user
        private boolean awaitingReset; // whether we are awaiting reset
        private OffsetResetStrategy resetStrategy;  // the reset strategy if awaitingReset is set

        private ConsumerSeekCallback consumerSeekCallback; // callback to be executed when the fetch request after the seek finishes

        public TopicPartitionState() {
            this.paused = false;
            this.consumed = null;
            this.fetched = null;
            this.committed = null;
            this.awaitingReset = false;
            this.hasValidPosition = false;
            this.resetStrategy = null;
            this.consumerSeekCallback = null;
        }

        private void awaitReset(OffsetResetStrategy strategy) {
            this.awaitingReset = true;
            this.resetStrategy = strategy;
            this.consumed = null;
            this.fetched = null;
            this.hasValidPosition = false;
            this.consumerSeekCallback = null;
        }

        private void seek(long offset, ConsumerSeekCallback callback) {
            this.consumed = offset;
            this.fetched = offset;
            this.awaitingReset = false;
            this.resetStrategy = null;
            this.hasValidPosition = true;
            this.consumerSeekCallback = callback;
        }

        private void fetched(long offset) {
            if (!hasValidPosition)
                throw new IllegalStateException("Cannot update fetch position without valid consumed/fetched positions");
            this.fetched = offset;
        }

        private void consumed(long offset) {
            if (!hasValidPosition)
                throw new IllegalStateException("Cannot update consumed position without valid consumed/fetched positions");
            this.consumed = offset;
        }

        private void invokeConsumerSeekCallback(Exception exception) {
            if (consumerSeekCallback != null)
                consumerSeekCallback.onComplete(this.fetched, exception);
            consumerSeekCallback = null;
        }

        private void committed(Long offset) {
            this.committed = offset;
        }

        private void pause() {
            this.paused = true;
        }

        private void resume() {
            this.paused = false;
        }

        private boolean isFetchable() {
            return !paused && hasValidPosition;
        }

    }

}