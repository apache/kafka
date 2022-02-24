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
package org.apache.kafka.clients.consumer;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Indicates that there is no stored offset for a partition and no defined offset
 * reset policy.
 */
public class NoOffsetForPartitionException extends InvalidOffsetException {

    private static final long serialVersionUID = 1L;

    private final Set<TopicPartition> partitions;
    private final Map<TopicPartition, SubscriptionState.FetchState> partitionFetchStates;

    public NoOffsetForPartitionException(TopicPartition partition) {
        super("Undefined offset with no reset policy for partition: " + partition);
        this.partitions = Collections.singleton(partition);
        this.partitionFetchStates = Collections.emptyMap();
    }

    public NoOffsetForPartitionException(Collection<TopicPartition> partitions) {
        super("Undefined offset with no reset policy for partitions: " + partitions);
        this.partitions = Collections.unmodifiableSet(new HashSet<>(partitions));
        this.partitionFetchStates = Collections.emptyMap();
    }

    public NoOffsetForPartitionException(Map<TopicPartition, SubscriptionState.FetchState> partitionFetchStates) {
        super("Undefined offset with no reset policy for partitionss with states: " + partitionFetchStates);
        this.partitionFetchStates = Collections.unmodifiableMap(new HashMap<>(partitionFetchStates));
        this.partitions = Collections.emptySet();
    }

    /**
     * returns all partitions for which no offests are defined.
     * @return all partitions without offsets
     */
    public Set<TopicPartition> partitions() {
        return partitions;
    }

    /**
     * returns a map of all partitions for which no offset are defined, and their respective fetch states
     * @return all partitions with no offset and their respective fetch states
     */
    public Map<TopicPartition, SubscriptionState.FetchState> partitionFetchStates() {
        return partitionFetchStates;
    }
}
