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

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A class for tracking the topics, partitions, and offsets for the consumer
 */
public class SubscriptionState {

    /* the list of topics the user has requested */
    private final Set<String> subscribedTopics;

    /* the list of partitions the user has requested */
    private final Set<TopicPartition> subscribedPartitions;

    /* the list of partitions currently assigned */
    private final Set<TopicPartition> assignedPartitions;

    /* the offset exposed to the user */
    private final Map<TopicPartition, Long> consumed;

    /* the current point we have fetched up to */
    private final Map<TopicPartition, Long> fetched;

    /* the last committed offset for each partition */
    private final Map<TopicPartition, Long> committed;

    /* do we need to request a partition assignment from the coordinator? */
    private boolean needsPartitionAssignment;

    /* do we need to request the latest committed offsets from the coordinator? */
    private boolean needsFetchCommittedOffsets;

    /* Partitions that need to be reset before fetching */
    private Map<TopicPartition, OffsetResetStrategy> resetPartitions;

    /* Default offset reset strategy */
    private OffsetResetStrategy offsetResetStrategy;

    public SubscriptionState(OffsetResetStrategy offsetResetStrategy) {
        this.offsetResetStrategy = offsetResetStrategy;
        this.subscribedTopics = new HashSet<String>();
        this.subscribedPartitions = new HashSet<TopicPartition>();
        this.assignedPartitions = new HashSet<TopicPartition>();
        this.consumed = new HashMap<TopicPartition, Long>();
        this.fetched = new HashMap<TopicPartition, Long>();
        this.committed = new HashMap<TopicPartition, Long>();
        this.needsPartitionAssignment = false;
        this.needsFetchCommittedOffsets = true; // initialize to true for the consumers to fetch offset upon starting up
        this.resetPartitions = new HashMap<TopicPartition, OffsetResetStrategy>();
    }

    public void subscribe(String topic) {
        if (this.subscribedPartitions.size() > 0)
            throw new IllegalStateException("Subcription to topics and partitions are mutually exclusive");
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
        if (this.subscribedTopics.size() > 0)
            throw new IllegalStateException("Subcription to topics and partitions are mutually exclusive");
        this.subscribedPartitions.add(tp);
        this.assignedPartitions.add(tp);
    }

    public void unsubscribe(TopicPartition partition) {
        if (!subscribedPartitions.contains(partition))
            throw new IllegalStateException("Partition " + partition + " was never subscribed to.");
        subscribedPartitions.remove(partition);
        clearPartition(partition);
    }
    
    private void clearPartition(TopicPartition tp) {
        this.assignedPartitions.remove(tp);
        this.committed.remove(tp);
        this.fetched.remove(tp);
        this.consumed.remove(tp);
        this.resetPartitions.remove(tp);
    }

    public void clearAssignment() {
        this.assignedPartitions.clear();
        this.committed.clear();
        this.fetched.clear();
        this.consumed.clear();
        this.needsPartitionAssignment = !subscribedTopics().isEmpty();
    }

    public Set<String> subscribedTopics() {
        return this.subscribedTopics;
    }

    public Long fetched(TopicPartition tp) {
        return this.fetched.get(tp);
    }

    public void fetched(TopicPartition tp, long offset) {
        if (!this.assignedPartitions.contains(tp))
            throw new IllegalArgumentException("Can't change the fetch position for a partition you are not currently subscribed to.");
        this.fetched.put(tp, offset);
    }

    public void committed(TopicPartition tp, long offset) {
        this.committed.put(tp, offset);
    }

    public Long committed(TopicPartition tp) {
        return this.committed.get(tp);
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
        fetched(tp, offset);
        consumed(tp, offset);
        resetPartitions.remove(tp);
    }

    public Set<TopicPartition> assignedPartitions() {
        return this.assignedPartitions;
    }

    public boolean partitionsAutoAssigned() {
        return !this.subscribedTopics.isEmpty();
    }

    public void consumed(TopicPartition tp, long offset) {
        if (!this.assignedPartitions.contains(tp))
            throw new IllegalArgumentException("Can't change the consumed position for a partition you are not currently subscribed to.");
        this.consumed.put(tp, offset);
    }

    public Long consumed(TopicPartition partition) {
        return this.consumed.get(partition);
    }

    public Map<TopicPartition, Long> allConsumed() {
        return this.consumed;
    }

    public void needOffsetReset(TopicPartition partition, OffsetResetStrategy offsetResetStrategy) {
        this.resetPartitions.put(partition, offsetResetStrategy);
        this.fetched.remove(partition);
        this.consumed.remove(partition);
    }

    public void needOffsetReset(TopicPartition partition) {
        needOffsetReset(partition, offsetResetStrategy);
    }

    public boolean isOffsetResetNeeded(TopicPartition partition) {
        return resetPartitions.containsKey(partition);
    }

    public boolean isOffsetResetNeeded() {
        return !resetPartitions.isEmpty();
    }

    public OffsetResetStrategy resetStrategy(TopicPartition partition) {
        return resetPartitions.get(partition);
    }

    public boolean hasAllFetchPositions() {
        return this.fetched.size() >= this.assignedPartitions.size();
    }

    public Set<TopicPartition> missingFetchPositions() {
        Set<TopicPartition> copy = new HashSet<TopicPartition>(this.assignedPartitions);
        copy.removeAll(this.fetched.keySet());
        return copy;
    }

    public boolean partitionAssignmentNeeded() {
        return this.needsPartitionAssignment;
    }

    public void changePartitionAssignment(List<TopicPartition> assignments) {
        for (TopicPartition tp : assignments)
            if (!this.subscribedTopics.contains(tp.topic()))
                throw new IllegalArgumentException("Assigned partition " + tp + " for non-subscribed topic.");
        this.clearAssignment();
        this.assignedPartitions.addAll(assignments);
        this.needsPartitionAssignment = false;
    }


}