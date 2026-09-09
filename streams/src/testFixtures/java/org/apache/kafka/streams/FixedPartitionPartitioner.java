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
package org.apache.kafka.streams;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.StreamPartitioner;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

public class FixedPartitionPartitioner<K, V> implements StreamPartitioner<K, V> {

    private final int partition;

    public FixedPartitionPartitioner(final int partition) {
        this.partition = partition;
    }

    @SuppressWarnings("removal")
    @Override
    public Optional<Set<Integer>> partitions(final String topic, final K key, final V value, final int numPartitions) {
        throw new AssertionError("Deprecated 4-argument partitions method was called instead of 5-argument method containing headers.");
    }

    @Override
    public Optional<Set<Integer>> partitions(final String topic, final K key, final V value, final Headers headers, final int numPartitions) {
        return Optional.of(Collections.singleton(partition));
    }
}
