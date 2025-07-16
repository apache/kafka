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

package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Represents a set of topic partitions, where each entry contains topic ID, topic name and partition number.
 * Keeps in-memory references to provide easy access to this data in different forms.
 * (ex. retrieve topic IDs only, topic names, partitions with topic names, partitions with topic IDs)
 * Data is kept sorted by topic name and partition number, for improved logging.
 */
public class TopicIdPartitionSet {

    /**
     * TopicPartition comparator based on topic name and partition.
     */
    static final Utils.TopicPartitionComparator TOPIC_PARTITION_COMPARATOR = new Utils.TopicPartitionComparator();

    /**
     * TopicIdPartition comparator based on topic name and partition.
     * (Ignoring topic ID while sorting, as this is sorted mainly for logging purposes).
     */
    static final Utils.TopicIdPartitionComparator TOPIC_ID_PARTITION_COMPARATOR = new Utils.TopicIdPartitionComparator();

    private final SortedSet<TopicIdPartition> topicIdPartitions;
    private final SortedSet<TopicPartition> topicPartitions;
    private final Set<Uuid> topicIds;
    private final SortedSet<String> topicNames;

    public TopicIdPartitionSet() {
        this.topicIdPartitions = new TreeSet<>(TOPIC_ID_PARTITION_COMPARATOR);
        this.topicPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        this.topicIds = new HashSet<>();
        this.topicNames = new TreeSet<>();
    }

    /**
     * Add a single partition to the assignment, along with its topic ID and name.
     * This will keep it, and also save references to the topic ID, topic name and partition.
     * Visible for testing.
     */
    void add(TopicIdPartition topicIdPartition) {
        topicIdPartitions.add(topicIdPartition);
        topicPartitions.add(topicIdPartition.topicPartition());
        topicIds.add(topicIdPartition.topicId());
        topicNames.add(topicIdPartition.topicPartition().topic());
    }

    /**
     * Add a set of partitions to the assignment, along with the topic ID and name.
     */
    public void addAll(Uuid topicId, String topicName, Set<Integer> partitions) {
        partitions.forEach(tp -> add(new TopicIdPartition(topicId, tp, topicName)));
    }

    public boolean isEmpty() {
        return this.topicIdPartitions.isEmpty();
    }

    public SortedSet<TopicPartition> topicPartitions() {
        return Collections.unmodifiableSortedSet(topicPartitions);
    }

    public Set<Uuid> topicIds() {
        return Collections.unmodifiableSet(topicIds);
    }

    public SortedSet<String> topicNames() {
        return Collections.unmodifiableSortedSet(topicNames);
    }

    /**
     * @return Map of partition numbers per topic ID, sorted by topic names (for improved logging).
     */
    public Map<Uuid, SortedSet<Integer>> toTopicIdPartitionMap() {
        Map<Uuid, SortedSet<Integer>> partitions = new HashMap<>();
        topicIdPartitions.forEach(topicIdPartition -> {
            Uuid topicId = topicIdPartition.topicId();
            partitions.computeIfAbsent(topicId, k -> new TreeSet<>()).add(topicIdPartition.partition());
        });
        return partitions;
    }

    /**
     * @return Set of topic partitions (with topic name and partition number)
     */
    protected SortedSet<TopicPartition> toTopicNamePartitionSet() {
        SortedSet<TopicPartition> result = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        topicIdPartitions.forEach(topicIdPartition -> result.add(topicIdPartition.topicPartition()));
        return result;
    }

    @Override
    public String toString() {
        return this.topicIdPartitions.toString();
    }
}
