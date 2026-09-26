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

import org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Objects;
import java.util.SortedSet;

/**
 * Event sent from the background to the app thread
 * to notify that a new assignment has been reconciled in the background removing partitions.
 * The app thread is expected to invoke the onPartitionsRevoked/onPartitionsLost callback as needed.
 */
public class PartitionsRemovedEvent extends CompletableBackgroundEvent<Void> {

    private final ConsumerRebalanceListenerMethodName methodName;
    private final SortedSet<TopicPartition> partitions;

    public PartitionsRemovedEvent(final ConsumerRebalanceListenerMethodName methodName,
                                   final SortedSet<TopicPartition> partitions) {
        super(Type.PARTITIONS_REMOVED, Long.MAX_VALUE);
        this.methodName = Objects.requireNonNull(methodName);
        this.partitions = Collections.unmodifiableSortedSet(partitions);
    }

    public ConsumerRebalanceListenerMethodName methodName() {
        return methodName;
    }

    public SortedSet<TopicPartition> partitions() {
        return partitions;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() +
                ", methodName=" + methodName +
                ", partitions=" + partitions;
    }
}
