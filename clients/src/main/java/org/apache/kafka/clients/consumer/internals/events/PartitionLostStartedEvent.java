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
 * Event that signifies that the background thread has determined that the member should abandon its partition
 * assignment. This event will be processed by the application thread when the next {@link Consumer#poll(Duration)}
 * call is performed by the user. When processed, the application thread should invoke the
 * {@link ConsumerRebalanceListener#onPartitionsLost(Collection)} callback with the given partitions.
 */
public class PartitionLostStartedEvent extends BackgroundEvent {

    private final SortedSet<TopicPartition> lostPartitions;

    public PartitionLostStartedEvent(SortedSet<TopicPartition> lostPartitions) {
        super(Type.PARTITION_LOST_STARTED);
        this.lostPartitions = Collections.unmodifiableSortedSet(lostPartitions);
    }

    public SortedSet<TopicPartition> lostPartitions() {
        return lostPartitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        PartitionLostStartedEvent that = (PartitionLostStartedEvent) o;

        return lostPartitions.equals(that.lostPartitions);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + lostPartitions.hashCode();
        return result;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() + ", lostPartitions=" + lostPartitions;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                toStringBase() +
                '}';
    }
}
