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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.lang.Integer.max;
import static org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor.DEFAULT_GENERATION;

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

    @Param({"100", "500", "1000", "5000", "10000"})
    private int memberCount;

    @Param({"5", "10", "50"})
    private int partitionsToMemberRatio;

    @Param({"10", "100", "1000"})
    private int topicCount;

    @Param({"true", "false"})
    private boolean isRackAware;

    @Param({"HOMOGENEOUS", "HETEROGENEOUS"})
    private SubscriptionModel subscriptionModel;

    @Param({"RANGE", "COOPERATIVE_STICKY"})
    private AssignorType assignorType;

    @Param({"true", "false"})
    private boolean simulateRebalanceTrigger;

    private Map<String, ConsumerPartitionAssignor.Subscription> subscriptions = new HashMap<>();

    private ConsumerPartitionAssignor.GroupSubscription groupSubscription;

    private static final int NUMBER_OF_RACKS = 3;

    private ConsumerPartitionAssignor assignor;

    private Cluster metadata;

    private final List<String> allTopicNames = new ArrayList<>(topicCount);

    @Setup(Level.Trial)
    public void setup() {
        // Ensure there are enough racks and brokers for the replication factor.
        if (NUMBER_OF_RACKS < 2) {
            throw new IllegalArgumentException("Number of broker racks must be at least equal to 2.");
        }

        populateTopicMetadata();

        addMemberSubscriptions();

        assignor = assignorType.assignor();

        if (simulateRebalanceTrigger) simulateRebalance();
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
        subscriptions.clear();
        int topicCounter = 0;
        Map<Integer, List<String>> tempMemberIndexToSubscriptions = new HashMap<>(memberCount);

        // In the rebalance case, we will add the last member as a trigger.
        // This is done to keep the total members count consistent with the input.
        int numberOfMembers = simulateRebalanceTrigger ? memberCount -1 : memberCount;

        for (int i = 0; i < numberOfMembers; i++) {
            // When subscriptions are homogeneous, all members are assigned all topics.
            if (subscriptionModel == SubscriptionModel.HOMOGENEOUS) {
                String memberName = "member" + i;
                subscriptions.put(memberName, subscription(allTopicNames, i));
            } else {
                List<String> subscribedTopics = Arrays.asList(
                    allTopicNames.get(i % topicCount),
                    allTopicNames.get((i+1) % topicCount)
                );
                topicCounter = max (topicCounter, ((i+1) % topicCount));
                tempMemberIndexToSubscriptions.put(i, subscribedTopics);
            }
        }

        int lastAssignedTopicIndex = topicCounter;
        tempMemberIndexToSubscriptions.forEach((memberIndex, subscriptionList) -> {
            String memberName = "member" + memberIndex;
            if (lastAssignedTopicIndex < topicCount - 1) {
                subscriptionList.addAll(allTopicNames.subList(lastAssignedTopicIndex + 1, topicCount - 1));
            }
            subscriptions.put(memberName, subscription(allTopicNames, memberIndex));
        });

        groupSubscription = new ConsumerPartitionAssignor.GroupSubscription(subscriptions);
    }

    private List<PartitionInfo> partitionInfos(String topic, int numberOfPartitions, List<Node> nodes) {
        // Create PartitionInfo for each partition.
        // Replication factor is hardcoded to 2.
        List<PartitionInfo> partitionInfos = new ArrayList<>(numberOfPartitions);
        for (int i = 0; i < numberOfPartitions; i++) {
            Node[] replicas = new Node[2];
            for (int j = 0; j < 2; j++) {
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

    private Optional<String> rackId(int index) {
        return isRackAware ? Optional.of("rack" + index % NUMBER_OF_RACKS) : Optional.empty();
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

    private void simulateRebalance() {
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

            // Add new member to trigger a reassignment.
            newSubscriptions.put("newMember", subscription(
                allTopicNames,
                memberCount - 1
            ));

            this.subscriptions = newSubscriptions;
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void doAssignment() {
        assignor.assign(metadata, groupSubscription);
    }
}
