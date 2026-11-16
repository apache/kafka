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

import java.util.Set;
import java.util.SortedSet;

/**
 * Event sent from the application thread to the background thread to apply a resolved Streams
 * assignment to the consumer subscription. The application thread sends this event during poll
 * and waits for the assignment to be applied before invoking the assignment callback.
 */
public class StreamsApplyAssignmentEvent extends ApplyAssignmentEvent {

    private final StreamsRebalanceData.Assignment assignment;

    public StreamsApplyAssignmentEvent(Set<TopicPartition> assignedPartitions,
                                       SortedSet<TopicPartition> addedPartitions,
                                       StreamsRebalanceData.Assignment assignment
    ) {
        super(assignedPartitions, addedPartitions);
        this.assignment = assignment;
    }

    public StreamsRebalanceData.Assignment assignment() {
        return assignment;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() +
            ", assignment=" + assignment;
    }
}
