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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.PartitionStates;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A class for tracking the topics and partitions for a share consumer. Unlike
 * {@link ConsumerSubscriptionState}, which is used by the regular consumer, this class keeps only
 * what the share consumption path needs: the set of subscribed topic names, the set of assigned
 * partitions (in assignment order), and the assigned topic IDs.
 * <p>
 * A share consumer does not track positions, committed offsets, offset-reset strategy, pause state,
 * lag, high watermark, log-end-offset, preferred read replica, or a {@code ConsumerRebalanceListener}.
 * Assignments received from the coordinator are applied immediately on the background thread (there
 * is no rebalance callback to await), so every assigned partition is fetchable.
 * <p>
 * Thread Safety: this class is thread-safe.
 */
public class ShareSubscriptionState implements AbstractSubscriptionState {

    private final Logger log;

    /* the list of topics the user has requested */
    private Set<String> subscription;

    /* the topic IDs received in an assignment from the coordinator */
    private Set<Uuid> assignedTopicIds;

    /* the partitions that are currently assigned, note that the order of partition matters (see FetchBuilder for more details) */
    private final PartitionStates<Boolean> assignment;

    /* whether the consumer is currently subscribed to a share group */
    private boolean subscribedToShareGroup;

    public ShareSubscriptionState(LogContext logContext) {
        this.log = logContext.logger(this.getClass());
        this.subscription = Collections.emptySet();
        this.assignedTopicIds = Collections.emptySet();
        this.assignment = new PartitionStates<>();
        this.subscribedToShareGroup = false;
    }

    @Override
    public synchronized String toString() {
        return "ShareSubscriptionState{" +
            "subscribed=" + subscribedToShareGroup +
            ", subscription=" + String.join(",", subscription) +
            ", assignment=" + assignment.partitionSet() + "}";
    }

    /**
     * Subscribe to the given set of topics for the share group.
     *
     * @return {@code true} if the subscribed topics changed as a result of this call.
     */
    public synchronized boolean subscribeToShareGroup(Set<String> topics) {
        subscribedToShareGroup = true;
        if (subscription.equals(topics))
            return false;
        subscription = topics;
        return true;
    }

    public synchronized Set<String> subscription() {
        return subscription;
    }

    public synchronized boolean hasNoSubscriptionOrUserAssignment() {
        return !subscribedToShareGroup;
    }

    /**
     * Get the subscription topics for which metadata is required. For a share consumer this is
     * simply the set of subscribed topics (there is no group-leader subscription or broker-side
     * regex to account for).
     */
    synchronized Set<String> metadataTopics() {
        return subscription;
    }

    synchronized boolean needsMetadata(String topic) {
        return subscription.contains(topic);
    }

    /**
     * @return the currently assigned partitions, in assignment order, that are available to fetch.
     * For a share consumer every assigned partition is fetchable.
     */
    public synchronized List<TopicPartition> fetchablePartitions() {
        return new ArrayList<>(assignment.partitionSet());
    }

    synchronized void movePartitionToEnd(TopicPartition tp) {
        assignment.moveToEnd(tp);
    }

    @Override
    public synchronized void setAssignedTopicIds(Set<Uuid> assignedTopicIds) {
        this.assignedTopicIds = assignedTopicIds;
    }

    public synchronized Set<Uuid> assignedTopicIds() {
        return assignedTopicIds;
    }

    @Override
    public synchronized Set<TopicPartition> assignedPartitions() {
        return new HashSet<>(assignment.partitionSet());
    }

    @Override
    public synchronized boolean hasAutoAssignedPartitions() {
        return subscribedToShareGroup;
    }

    @Override
    public synchronized void assignFromSubscribed(Collection<TopicPartition> assignments) {
        Map<TopicPartition, Boolean> assignedPartitionStates = new LinkedHashMap<>(assignments.size());
        for (TopicPartition tp : assignments) {
            assignedPartitionStates.put(tp, Boolean.TRUE);
        }
        assignment.set(assignedPartitionStates);
        log.debug("Updated assignment to {}", assignment.partitionSet());
    }

    @Override
    public synchronized void unsubscribe() {
        subscription = Collections.emptySet();
        assignment.clear();
        assignedTopicIds = Collections.emptySet();
        subscribedToShareGroup = false;
    }
}
