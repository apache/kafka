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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.internals.StreamsRebalanceData;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Objects;
import java.util.SortedSet;

/**
 * Event sent from the background to the app thread, to notify that a new assignment has been reconciled.
 * The app thread is expected to apply the assignment change to the subscription state in the
 * next call to consumer.poll, and invoke the onTasksAssigned callback.
 */
public class StreamsTasksAssignedEvent extends CompletableBackgroundEvent<Void> {

    private final SortedSet<TopicPartition> assignedPartitions;
    private final SortedSet<TopicPartition> addedPartitions;
    private final StreamsRebalanceData.Assignment assignment;

    /**
     * Constructor for the streams partitions assigned event.
     *
     * @param assignedPartitions The full partition assignment to apply
     * @param addedPartitions The newly added partitions
     * @param assignment The task assignment for the callback
     */
    public StreamsTasksAssignedEvent(final SortedSet<TopicPartition> assignedPartitions,
                                     final SortedSet<TopicPartition> addedPartitions,
                                     final StreamsRebalanceData.Assignment assignment) {
        super(Type.STREAMS_TASKS_ASSIGNED, Long.MAX_VALUE);
        this.assignedPartitions = Collections.unmodifiableSortedSet(Objects.requireNonNull(assignedPartitions));
        this.addedPartitions = Collections.unmodifiableSortedSet(Objects.requireNonNull(addedPartitions));
        this.assignment = Objects.requireNonNull(assignment);
    }

    /**
     * @return The full partition assignment to apply.
     */
    public SortedSet<TopicPartition> assignedPartitions() {
        return assignedPartitions;
    }

    /**
     * @return The newly added partitions.
     */
    public SortedSet<TopicPartition> addedPartitions() {
        return addedPartitions;
    }

    /**
     * @return The task assignment for the onTasksAssigned callback.
     */
    public StreamsRebalanceData.Assignment assignment() {
        return assignment;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() +
                ", assignedPartitions=" + assignedPartitions +
                ", addedPartitions=" + addedPartitions +
                ", assignment=" + assignment;
    }
}
