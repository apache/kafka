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
package org.apache.kafka.jmh.assignor;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.coordinator.group.assignor.SubscriptionType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor.DEFAULT_GENERATION;
import static org.apache.kafka.coordinator.group.assignor.SubscriptionType.HETEROGENEOUS;
import static org.apache.kafka.coordinator.group.assignor.SubscriptionType.HOMOGENEOUS;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ClientSideAssignorBenchmark {

    public enum AssignorType {
        RANGE(new RangeAssignor()),
        COOPERATIVE_STICKY(new CooperativeStickyAssignor());

        private final ConsumerPartitionAssignor assignor;

        AssignorType(ConsumerPartitionAssignor assignor) {
            this.assignor = assignor;
        }

        public ConsumerPartitionAssignor assignor() {
            return assignor;
        }
    }

    /**
     * The subscription pattern followed by the members of the group.
     *
     * A subscription model is considered homogenous if all the members of the group
     * are subscribed to the same set of topics, it is heterogeneous otherwise.
     */
    public enum SubscriptionModel {
        HOMOGENEOUS, HETEROGENEOUS
    }

    /**
     * The assignment type is decided based on whether all the members are assigned partitions
     * for the first time (full), or incrementally when a rebalance is triggered.
     */
    public enum AssignmentType {
        FULL, INCREMENTAL
    }

    @Param({"100", "500", "1000", "5000", "10000"})
    private int memberCount;

    @Param({"5", "10", "50"})
    private int partitionsToMemberRatio;

    @Param({"10", "100", "1000"})
    private int topicCount;

    @Param({"true", "false"})
    private boolean isRackAware;

    @Param({"HOMOGENEOUS", "HETEROGENEOUS"})
    private SubscriptionType subscriptionType;

    @Param({"RANGE", "COOPERATIVE_STICKY"})
    private AssignorType assignorType;

    @Param({"FULL", "INCREMENTAL"})
    private AssignmentType assignmentType;

    private Map<String, ConsumerPartitionAssignor.Subscription> subscriptions = new HashMap<>();

    private ConsumerPartitionAssignor.GroupSubscription groupSubscription;

    private static final int NUMBER_OF_RACKS = 3;

    private static final int MAX_BUCKET_COUNT = 5;

    private ConsumerPartitionAssignor assignor;

    private Cluster metadata;

    private final List<String> allTopicNames = new ArrayList<>();

    @Setup(Level.Trial)
    public void setup() {
        // Ensure there are enough racks and brokers for the replication factor = 3.
        if (NUMBER_OF_RACKS < 3) {
            throw new IllegalArgumentException("Number of broker racks must be at least equal to 3.");
        }

        populateTopicMetadata();

        addMemberSubscriptions();

        assignor = assignorType.assignor();

        if (assignmentType == AssignmentType.INCREMENTAL) {
            simulateIncrementalRebalance();
        }
    }

    private void populateTopicMetadata() {
        List<PartitionInfo> partitions = new ArrayList<>();
        int partitionsPerTopicCount = (memberCount * partitionsToMemberRatio) / topicCount;

        // Create nodes (brokers), one for each rack.
        List<Node> nodes = new ArrayList<>(NUMBER_OF_RACKS);
        for (int i = 0; i < NUMBER_OF_RACKS; i++) {
            nodes.add(new Node(i, "", i, "rack" + i));
        }

        for (int i = 0; i < topicCount; i++) {
            String topicName = "topic" + i;
            allTopicNames.add(topicName);
            partitions.addAll(partitionInfos(topicName, partitionsPerTopicCount, nodes));
        }

        metadata = new Cluster("test-cluster", nodes, partitions, Collections.emptySet(), Collections.emptySet());
    }

    private void addMemberSubscriptions() {
        // In the rebalance case, we will add the last member as a trigger.
        // This is done to keep the final members count consistent with the input.
        int numberOfMembers = assignmentType.equals(AssignmentType.INCREMENTAL) ? memberCount - 1 : memberCount;

        // When subscriptions are homogeneous, all members are assigned all topics.
        if (subscriptionType == HOMOGENEOUS) {
            for (int i = 0; i < numberOfMembers; i++) {
                String memberName = "member" + i;
                subscriptions.put(memberName, subscription(allTopicNames, i));
            }
        } else {
            // Adjust bucket count based on member count when member count < max bucket count.
            int bucketCount = Math.min(MAX_BUCKET_COUNT, numberOfMembers);

            // Check minimum topics requirement
            if (topicCount < bucketCount) {
                throw new IllegalArgumentException("At least " + bucketCount + " topics are recommended for effective bucketing.");
            }

            int bucketSizeTopics = (int) Math.ceil((double) topicCount / bucketCount);
            int bucketSizeMembers = (int) Math.ceil((double) numberOfMembers / bucketCount);

            // Define buckets for each member and assign topics from the same bucket
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                int memberStartIndex = bucket * bucketSizeMembers;
                int memberEndIndex = Math.min((bucket + 1) * bucketSizeMembers, numberOfMembers);

                int topicStartIndex = bucket * bucketSizeTopics;
                int topicEndIndex = Math.min((bucket + 1) * bucketSizeTopics, topicCount);

                List<String> bucketTopics = allTopicNames.subList(topicStartIndex, topicEndIndex);

                // Assign topics to each member in the current bucket
                for (int i = memberStartIndex; i < memberEndIndex; i++) {
                    String memberName = "member" + i;
                    subscriptions.put(memberName, subscription(bucketTopics, i));
                }
            }
        }

        groupSubscription = new ConsumerPartitionAssignor.GroupSubscription(subscriptions);
    }

    private List<PartitionInfo> partitionInfos(String topic, int numberOfPartitions, List<Node> nodes) {
        // Create PartitionInfo for each partition.
        // Replication factor is hardcoded to 2.
        List<PartitionInfo> partitionInfos = new ArrayList<>(numberOfPartitions);
        for (int i = 0; i < numberOfPartitions; i++) {
            Node[] replicas = new Node[3];
            for (int j = 0; j < 3; j++) {
                // Assign nodes based on partition number to mimic mkMapOfPartitionRacks logic.
                int nodeIndex = (i + j) % NUMBER_OF_RACKS;
                replicas[j] = nodes.get(nodeIndex);
            }
            partitionInfos.add(new PartitionInfo(topic, i, replicas[0], replicas, replicas));
        }

        return partitionInfos;
    }

    private ConsumerPartitionAssignor.Subscription subscription(List<String> topics, int consumerIndex) {
        Optional<String> rackId = rackId(consumerIndex);
        return new ConsumerPartitionAssignor.Subscription(
            topics,
            null,
            Collections.emptyList(),
            DEFAULT_GENERATION,
            rackId
        );
    }

    private Optional<String> rackId(int consumerIndex) {
        return isRackAware ? Optional.of("rack" + consumerIndex % NUMBER_OF_RACKS) : Optional.empty();
    }

    private ConsumerPartitionAssignor.Subscription subscriptionWithOwnedPartitions(
        List<TopicPartition> ownedPartitions,
        ConsumerPartitionAssignor.Subscription prevSubscription
    ) {
        return new ConsumerPartitionAssignor.Subscription(
            prevSubscription.topics(),
            null,
            ownedPartitions,
            DEFAULT_GENERATION,
            prevSubscription.rackId()
        );
    }

    private void simulateIncrementalRebalance() {
        ConsumerPartitionAssignor.GroupAssignment initialAssignment = assignor.assign(metadata, groupSubscription);
        Map<String, ConsumerPartitionAssignor.Subscription> newSubscriptions = new HashMap<>();
        subscriptions.forEach((member, subscription) ->
            newSubscriptions.put(
                member,
                subscriptionWithOwnedPartitions(
                    initialAssignment.groupAssignment().get(member).partitions(),
                    subscription
                )
            )
        );

        List<String> subscribedTopicsForNewMember;
        if (subscriptionType == HETEROGENEOUS) {
            subscribedTopicsForNewMember = subscriptions.get("member" + (memberCount - 2)).topics();
        } else {
            subscribedTopicsForNewMember = allTopicNames;
        }

        // Add new member to trigger a reassignment.
        newSubscriptions.put("newMember", subscription(
            subscribedTopicsForNewMember,
            memberCount - 1
        ));

        subscriptions = newSubscriptions;
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void doAssignment() {
        assignor.assign(metadata, groupSubscription);
    }
}
