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

import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;

/**
 * Event sent from the application thread to the background thread to
 * update the subscription state with a new group assignment after it has been reconciled.
 * Done via events to ensure that assignment updates happen on the background thread,
 * triggered by the application thread during poll, and completed before the call to poll returns to the user.
 */
public class ApplyAssignmentEvent extends CompletableApplicationEvent<Void> {

    /**
     * The full assignment to apply
     * This is used to update the subscription state.
     */
    private final Set<TopicPartition> assignedPartitions;

    /**
     * The newly added partitions.
     * This is used to mark them as awaiting callbacks if needed when updating the subscription state.
     */
    private final SortedSet<TopicPartition> addedPartitions;

    public ApplyAssignmentEvent(Set<TopicPartition> assignedPartitions,
                                SortedSet<TopicPartition> addedPartitions) {
        super(Type.APPLY_ASSIGNMENT, Long.MAX_VALUE);
        this.assignedPartitions = Collections.unmodifiableSet(Objects.requireNonNull(assignedPartitions));
        this.addedPartitions = Collections.unmodifiableSortedSet(Objects.requireNonNull(addedPartitions));
    }

    public Set<TopicPartition> assignedPartitions() {
        return assignedPartitions;
    }

    public SortedSet<TopicPartition> addedPartitions() {
        return addedPartitions;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() +
                ", assignedPartitions=" + assignedPartitions +
                ", addedPartitions=" + addedPartitions;
    }
}
