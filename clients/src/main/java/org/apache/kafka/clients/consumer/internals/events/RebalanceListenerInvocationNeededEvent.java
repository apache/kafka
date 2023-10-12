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

import org.apache.kafka.clients.consumer.internals.RebalanceStep;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.SortedSet;

public class RebalanceListenerInvocationNeededEvent extends BackgroundEvent {

    private final RebalanceStep rebalanceStep;
    private final SortedSet<TopicPartition> partitions;

    public RebalanceListenerInvocationNeededEvent(RebalanceStep rebalanceStep, SortedSet<TopicPartition> partitions) {
        super(Type.REBALANCE_STARTED);
        this.rebalanceStep = rebalanceStep;
        this.partitions = Collections.unmodifiableSortedSet(partitions);
    }

    public RebalanceStep rebalanceStep() {
        return rebalanceStep;
    }

    public SortedSet<TopicPartition> partitions() {
        return partitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        RebalanceListenerInvocationNeededEvent that = (RebalanceListenerInvocationNeededEvent) o;

        return rebalanceStep.equals(that.rebalanceStep) && partitions.equals(that.partitions);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + rebalanceStep.hashCode();
        result = 31 * result + partitions.hashCode();
        return result;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() + ", rebalanceStep=" + rebalanceStep + ", partitions=" + partitions;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                toStringBase() +
                '}';
    }
}
