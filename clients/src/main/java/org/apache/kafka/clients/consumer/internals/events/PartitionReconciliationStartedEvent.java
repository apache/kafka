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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.SortedSet;

/**
 * Event that signifies that the background thread has started the partition assignment process. This event will
 * be processed by the application thread when the next {@link Consumer#poll(Duration)} call is performed by the
 * user. When processed, the application thread should invoke both the
 * {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection)} and
 * {@link ConsumerRebalanceListener#onPartitionsAssigned(Collection)} callbacks with the given partitions.
 */
public class PartitionReconciliationStartedEvent extends BackgroundEvent {

    private final SortedSet<TopicPartition> revokedPartitions;
    private final SortedSet<TopicPartition> assignedPartitions;

    public PartitionReconciliationStartedEvent(SortedSet<TopicPartition> revokedPartitions,
                                               SortedSet<TopicPartition> assignedPartitions) {
        super(Type.PARTITION_RECONCILIATION_STARTED);
        this.revokedPartitions = Collections.unmodifiableSortedSet(revokedPartitions);
        this.assignedPartitions = Collections.unmodifiableSortedSet(assignedPartitions);
    }

    public SortedSet<TopicPartition> revokedPartitions() {
        return revokedPartitions;
    }

    public SortedSet<TopicPartition> assignedPartitions() {
        return assignedPartitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        PartitionReconciliationStartedEvent that = (PartitionReconciliationStartedEvent) o;

        return revokedPartitions.equals(that.revokedPartitions) &&
                assignedPartitions.equals(that.assignedPartitions);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + revokedPartitions.hashCode();
        result = 31 * result + assignedPartitions.hashCode();
        return result;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() +
                ", revokedPartitions=" + revokedPartitions +
                ", assignedPartitions=" + assignedPartitions;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                toStringBase() +
                '}';
    }
}
