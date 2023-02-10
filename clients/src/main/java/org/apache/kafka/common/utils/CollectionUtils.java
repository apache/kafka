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
package org.apache.kafka.common.utils;

import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class CollectionUtils {

    private CollectionUtils() {}

    /**
     * Given two maps (A, B), returns all the key-value pairs in A whose keys are not contained in B
     */
    public static <K, V> Map<K, V> subtractMap(Map<? extends K, ? extends V> minuend, Map<? extends K, ? extends V> subtrahend) {
        return minuend.entrySet().stream()
                .filter(entry -> !subtrahend.containsKey(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * group data by topic
     *
     * @param data Data to be partitioned
     * @param <T> Partition data type
     * @return partitioned data
     */
    public static <T> Map<String, Map<Integer, T>> groupPartitionDataByTopic(Map<TopicPartition, ? extends T> data) {
        Map<String, Map<Integer, T>> dataByTopic = new HashMap<>();
        for (Map.Entry<TopicPartition, ? extends T> entry : data.entrySet()) {
            String topic = entry.getKey().topic();
            int partition = entry.getKey().partition();
            Map<Integer, T> topicData = dataByTopic.computeIfAbsent(topic, t -> new HashMap<>());
            topicData.put(partition, entry.getValue());
        }
        return dataByTopic;
    }

    /**
     * Group a list of partitions by the topic name.
     *
     * @param partitions The partitions to collect
     * @return partitions per topic
     */
    public static Map<String, List<Integer>> groupPartitionsByTopic(Collection<TopicPartition> partitions) {
        return groupPartitionsByTopic(
            partitions,
            topic -> new ArrayList<>(),
            List::add
        );
    }

    /**
     * Group a collection of partitions by topic
     *
     * @return The map used to group the partitions
     */
    public static <T> Map<String, T> groupPartitionsByTopic(
        Collection<TopicPartition> partitions,
        Function<String, T> buildGroup,
        BiConsumer<T, Integer> addToGroup
    ) {
        Map<String, T> dataByTopic = new HashMap<>();
        for (TopicPartition tp : partitions) {
            String topic = tp.topic();
            T topicData = dataByTopic.computeIfAbsent(topic, buildGroup);
            addToGroup.accept(topicData, tp.partition());
        }
        return dataByTopic;
    }
}
