/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.internals;

import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This class is a useful building block for doing fetch requests where topic partitions have to be rotated via
 * round-robin to ensure fairness and some level of determinism given the existence of a limit on the fetch response
 * size. Because the serialization of fetch requests is more efficient if all partitions for the same topic are grouped
 * together, we do such grouping in the method `set`.
 *
 * As partitions are moved to the end, the same topic may be repeated more than once. In the optimal case, a single
 * topic would "wrap around" and appear twice. However, as partitions are fetched in different orders and partition
 * leadership changes, we will deviate from the optimal. If this turns out to be an issue in practice, we can improve
 * it by tracking the partitions per node or calling `set` every so often.
 */
public class PartitionStates<S> {

    private final LinkedHashMap<TopicPartition, S> map = new LinkedHashMap<>();

    public PartitionStates() {}

    public void moveToEnd(TopicPartition topicPartition) {
        S state = map.remove(topicPartition);
        if (state != null)
            map.put(topicPartition, state);
    }

    public void updateAndMoveToEnd(TopicPartition topicPartition, S state) {
        map.remove(topicPartition);
        map.put(topicPartition, state);
    }

    public void remove(TopicPartition topicPartition) {
        map.remove(topicPartition);
    }

    /**
     * Returns the partitions in random order.
     */
    public Set<TopicPartition> partitionSet() {
        return new HashSet<>(map.keySet());
    }

    public void clear() {
        map.clear();
    }

    public boolean contains(TopicPartition topicPartition) {
        return map.containsKey(topicPartition);
    }

    /**
     * Returns the partition states in order.
     */
    public List<PartitionState<S>> partitionStates() {
        List<PartitionState<S>> result = new ArrayList<>();
        for (Map.Entry<TopicPartition, S> entry : map.entrySet()) {
            result.add(new PartitionState<>(entry.getKey(), entry.getValue()));
        }
        return result;
    }

    /**
     * Returns the partition state values in order.
     */
    public List<S> partitionStateValues() {
        return new ArrayList<>(map.values());
    }

    public S stateValue(TopicPartition topicPartition) {
        return map.get(topicPartition);
    }

    public int size() {
        return map.size();
    }

    /**
     * Update the builder to have the received map as its state (i.e. the previous state is cleared). The builder will
     * "batch by topic", so if we have a, b and c, each with two partitions, we may end up with something like the
     * following (the order of topics and partitions within topics is dependent on the iteration order of the received
     * map): a0, a1, b1, b0, c0, c1.
     */
    public void set(Map<TopicPartition, S> partitionToState) {
        map.clear();
        update(partitionToState);
    }

    private void update(Map<TopicPartition, S> partitionToState) {
        LinkedHashMap<String, List<TopicPartition>> topicToPartitions = new LinkedHashMap<>();
        for (TopicPartition tp : partitionToState.keySet()) {
            List<TopicPartition> partitions = topicToPartitions.get(tp.topic());
            if (partitions == null) {
                partitions = new ArrayList<>();
                topicToPartitions.put(tp.topic(), partitions);
            }
            partitions.add(tp);
        }
        for (Map.Entry<String, List<TopicPartition>> entry : topicToPartitions.entrySet()) {
            for (TopicPartition tp : entry.getValue()) {
                S state = partitionToState.get(tp);
                map.put(tp, state);
            }
        }
    }

    public static class PartitionState<S> {
        private final TopicPartition topicPartition;
        private final S value;

        public PartitionState(TopicPartition topicPartition, S state) {
            this.topicPartition = Objects.requireNonNull(topicPartition);
            this.value = Objects.requireNonNull(state);
        }

        public S value() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            PartitionState<?> that = (PartitionState<?>) o;

            return topicPartition.equals(that.topicPartition) && value.equals(that.value);
        }

        @Override
        public int hashCode() {
            int result = topicPartition.hashCode();
            result = 31 * result + value.hashCode();
            return result;
        }

        public TopicPartition topicPartition() {
            return topicPartition;
        }

        @Override
        public String toString() {
            return "PartitionState(" + topicPartition + "=" + value + ')';
        }
    }

}
