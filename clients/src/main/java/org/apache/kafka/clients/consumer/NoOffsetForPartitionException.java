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

    public NoOffsetForPartitionException(TopicPartition partition) {
        super("Undefined offset with no reset policy for partition: " + partition);
        this.partitions = Collections.singleton(partition);
    }

    public NoOffsetForPartitionException(Collection<TopicPartition> partitions) {
        super("Undefined offset with no reset policy for partitions: " + partitions);
        this.partitions = Collections.unmodifiableSet(new HashSet<>(partitions));
    }

    /**
     * returns all partitions for which no offests are defined.
     * @return all partitions without offsets
     */
    public Set<TopicPartition> partitions() {
        return partitions;
    }

}
