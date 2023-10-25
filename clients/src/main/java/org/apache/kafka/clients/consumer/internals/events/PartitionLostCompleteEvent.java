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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.KafkaException;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.SortedSet;

/**
 * Event that signifies that the application thread has executed the
 * {@link ConsumerRebalanceListener#onPartitionsLost(Collection)} callback. If the callback execution threw an error,
 * it is included in the event should any event listener want to know.
 */
public class PartitionLostCompleteEvent extends ApplicationEvent {

    private final SortedSet<TopicPartition> lostPartitions;
    private final Optional<KafkaException> error;

    public PartitionLostCompleteEvent(SortedSet<TopicPartition> lostPartitions, Optional<KafkaException> error) {
        super(Type.PARTITION_LOST_COMPLETE);
        this.lostPartitions = Collections.unmodifiableSortedSet(lostPartitions);
        this.error = error;
    }

    public SortedSet<TopicPartition> lostPartitions() {
        return lostPartitions;
    }

    public Optional<KafkaException> error() {
        return error;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        PartitionLostCompleteEvent that = (PartitionLostCompleteEvent) o;

        return lostPartitions.equals(that.lostPartitions) && error.equals(that.error);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + lostPartitions.hashCode();
        result = 31 * result + error.hashCode();
        return result;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() + ", lostPartitions=" + lostPartitions + ", error=" + error;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                toStringBase() +
                '}';
    }
}
