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

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstract assignor implementation which does some common grunt work (in particular collecting
 * partition counts which are always needed in assignors).
 */
public abstract class AbstractPartitionAssignor implements ConsumerPartitionAssignor {
    private static final Logger log = LoggerFactory.getLogger(AbstractPartitionAssignor.class);
    private static final Node[] NO_NODES = new Node[] {Node.noNode()};

    // Used only in unit tests to verify rack-aware assignment when all racks have all partitions.
    boolean preferRackAwareLogic;

    /**
     * Perform the group assignment given the partition counts and member subscriptions
     * @param partitionsPerTopic The number of partitions for each subscribed topic. Topics not in metadata will be excluded
     *                           from this map.
     * @param subscriptions Map from the member id to their respective topic subscription
     * @return Map from each member to the list of partitions assigned to them.
     */
    public abstract Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                             Map<String, Subscription> subscriptions);

    /**
     * Default implementation of assignPartitions() that does not include racks. This is only
     * included to avoid breaking any custom implementation that extends AbstractPartitionAssignor.
     * Note that this class is internal, but to be safe, we are maintaining compatibility.
     */
    public Map<String, List<TopicPartition>> assignPartitions(Map<String, List<PartitionInfo>> partitionsPerTopic,
            Map<String, Subscription> subscriptions) {
        Map<String, Integer> partitionCountPerTopic = partitionsPerTopic.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().size()));
        return assign(partitionCountPerTopic, subscriptions);
    }

    @Override
    public GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {
        Map<String, Subscription> subscriptions = groupSubscription.groupSubscription();
        Set<String> allSubscribedTopics = new HashSet<>();
        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet())
            allSubscribedTopics.addAll(subscriptionEntry.getValue().topics());

        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        for (String topic : allSubscribedTopics) {
            List<PartitionInfo> partitions = metadata.partitionsForTopic(topic);
            if (partitions != null && !partitions.isEmpty()) {
                partitions = new ArrayList<>(partitions);
                partitions.sort(Comparator.comparingInt(PartitionInfo::partition));
                partitionsPerTopic.put(topic, partitions);
            } else {
                log.debug("Skipping assignment for topic {} since no metadata is available", topic);
            }
        }

        Map<String, List<TopicPartition>> rawAssignments = assignPartitions(partitionsPerTopic, subscriptions);

        // this class maintains no user data, so just wrap the results
        Map<String, Assignment> assignments = new HashMap<>();
        for (Map.Entry<String, List<TopicPartition>> assignmentEntry : rawAssignments.entrySet())
            assignments.put(assignmentEntry.getKey(), new Assignment(assignmentEntry.getValue()));
        return new GroupAssignment(assignments);
    }

    protected static <K, V> void put(Map<K, List<V>> map, K key, V value) {
        List<V> list = map.computeIfAbsent(key, k -> new ArrayList<>());
        list.add(value);
    }

    protected static List<TopicPartition> partitions(String topic, int numPartitions) {
        List<TopicPartition> partitions = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++)
            partitions.add(new TopicPartition(topic, i));
        return partitions;
    }

    protected static Map<String, List<PartitionInfo>> partitionInfosWithoutRacks(Map<String, Integer> partitionsPerTopic) {
        return partitionsPerTopic.entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> {
            String topic = e.getKey();
            int numPartitions = e.getValue();
            List<PartitionInfo> partitionInfos = new ArrayList<>(numPartitions);
            for (int i = 0; i < numPartitions; i++)
                partitionInfos.add(new PartitionInfo(topic, i, Node.noNode(), NO_NODES, NO_NODES));
            return partitionInfos;
        }));
    }

    protected boolean useRackAwareAssignment(Set<String> consumerRacks, Set<String> partitionRacks, Map<TopicPartition, Set<String>> racksPerPartition) {
        if (consumerRacks.isEmpty() || Collections.disjoint(consumerRacks, partitionRacks))
            return false;
        else if (preferRackAwareLogic)
            return true;
        else {
            return !racksPerPartition.values().stream().allMatch(partitionRacks::equals);
        }
    }

    public static class MemberInfo implements Comparable<MemberInfo> {
        public final String memberId;
        public final Optional<String> groupInstanceId;
        public final Optional<String> rackId;

        public MemberInfo(String memberId, Optional<String> groupInstanceId, Optional<String> rackId) {
            this.memberId = memberId;
            this.groupInstanceId = groupInstanceId;
            this.rackId = rackId;
        }

        public MemberInfo(String memberId, Optional<String> groupInstanceId) {
            this(memberId, groupInstanceId, Optional.empty());
        }

        @Override
        public int compareTo(MemberInfo otherMemberInfo) {
            if (this.groupInstanceId.isPresent() &&
                    otherMemberInfo.groupInstanceId.isPresent()) {
                return this.groupInstanceId.get()
                        .compareTo(otherMemberInfo.groupInstanceId.get());
            } else if (this.groupInstanceId.isPresent()) {
                return -1;
            } else if (otherMemberInfo.groupInstanceId.isPresent()) {
                return 1;
            } else {
                return this.memberId.compareTo(otherMemberInfo.memberId);
            }
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof MemberInfo && this.memberId.equals(((MemberInfo) o).memberId);
        }

        /**
         * We could just use member.id to be the hashcode, since it's unique
         * across the group.
         */
        @Override
        public int hashCode() {
            return memberId.hashCode();
        }

        @Override
        public String toString() {
            return "MemberInfo [member.id: " + memberId
                    + ", group.instance.id: " + groupInstanceId.orElse("{}")
                    + "]";
        }
    }
}
