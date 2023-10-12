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
import org.apache.kafka.common.KafkaException;

import java.util.Collections;
import java.util.Optional;
import java.util.SortedSet;

public class PartitionAssignmentLostCallbackInvokedEvent extends ApplicationEvent {

    private final SortedSet<TopicPartition> lostPartitions;
    private final Optional<KafkaException> error;

    public PartitionAssignmentLostCallbackInvokedEvent(SortedSet<TopicPartition> lostPartitions,
                                                       Optional<KafkaException> error) {
        super(Type.PARTITION_ASSIGNMENT_LOST_CALLBACK_INVOKED);
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

        PartitionAssignmentLostCallbackInvokedEvent that = (PartitionAssignmentLostCallbackInvokedEvent) o;

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
