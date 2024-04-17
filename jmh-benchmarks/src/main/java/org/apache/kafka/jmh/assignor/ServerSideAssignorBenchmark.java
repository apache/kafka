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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.assignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.assignor.AssignmentSpec;
import org.apache.kafka.coordinator.group.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.assignor.RangeAssignor;
import org.apache.kafka.coordinator.group.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.assignor.UniformAssignor;
import org.apache.kafka.coordinator.group.consumer.SubscribedTopicMetadata;
import org.apache.kafka.coordinator.group.consumer.TopicMetadata;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.Integer.max;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 0)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ServerSideAssignorBenchmark {

    public enum AssignorType {
        RANGE(new RangeAssignor()),
        UNIFORM(new UniformAssignor());

        private final PartitionAssignor assignor;

        AssignorType(PartitionAssignor assignor) {
            this.assignor = assignor;
        }

        public PartitionAssignor assignor() {
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

    @Param({"RANGE", "UNIFORM"})
    private AssignorType assignorType;

    @Param({"true", "false"})
    private boolean simulateRebalanceTrigger;

    private PartitionAssignor partitionAssignor;

    private static final int NUMBER_OF_RACKS = 3;

    private AssignmentSpec assignmentSpec;

    private SubscribedTopicDescriber subscribedTopicDescriber;

    private final List<Uuid> allTopicIds = new ArrayList<>(topicCount);

    @Setup(Level.Trial)
    public void setup() {
        Map<Uuid, TopicMetadata> topicMetadata = createTopicMetadata();
        subscribedTopicDescriber = new SubscribedTopicMetadata(topicMetadata);

        createAssignmentSpec();

        partitionAssignor = assignorType.assignor();

        if (simulateRebalanceTrigger) {
            simulateIncrementalRebalance(topicMetadata);
        }
    }

    private Map<Uuid, TopicMetadata> createTopicMetadata() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        int partitionsPerTopicCount = (memberCount * partitionsToMemberRatio) / topicCount;

        Map<Integer, Set<String>> partitionRacks = isRackAware ?
            mkMapOfPartitionRacks(partitionsPerTopicCount) :
            Collections.emptyMap();

        for (int i = 0; i < topicCount; i++) {
            Uuid topicUuid = Uuid.randomUuid();
            String topicName = "topic" + i;
            allTopicIds.add(topicUuid);
            topicMetadata.put(topicUuid, new TopicMetadata(
                topicUuid,
                topicName,
                partitionsPerTopicCount,
                partitionRacks
            ));
        }

        return topicMetadata;
    }

    private void createAssignmentSpec() {
        Map<String, AssignmentMemberSpec> members = new HashMap<>();

        int topicCounter = 0;
        Map<Integer, Set<Uuid>> tempMemberIndexToSubscriptions = new HashMap<>(memberCount);

        // In the rebalance case, we will add the last member as a trigger.
        // This is done to keep the total members count consistent with the input.
        int numberOfMembers = simulateRebalanceTrigger ? memberCount -1 : memberCount;

        for (int i = 0; i < numberOfMembers; i++) {
            if (subscriptionModel == SubscriptionModel.HOMOGENEOUS) {
                addMemberSpec(members, i, new HashSet<>(allTopicIds));
            } else {
                Set<Uuid> subscribedTopics = new HashSet<>(Arrays.asList(
                    allTopicIds.get(i % topicCount),
                    allTopicIds.get((i+1) % topicCount)
                ));
                topicCounter = max (topicCounter, ((i+1) % topicCount));
                tempMemberIndexToSubscriptions.put(i, subscribedTopics);
            }
        }

        int lastAssignedTopicIndex = topicCounter;
        tempMemberIndexToSubscriptions.forEach((memberIndex, subscriptions) -> {
            if (lastAssignedTopicIndex < topicCount - 1) {
                subscriptions.addAll(allTopicIds.subList(lastAssignedTopicIndex + 1, topicCount - 1));
            }
            addMemberSpec(members, memberIndex, subscriptions);
        });

        this.assignmentSpec = new AssignmentSpec(members);
    }

    private Optional<String> rackId(int index) {
        return isRackAware ? Optional.of("rack" + index % NUMBER_OF_RACKS) : Optional.empty();
    }

    private void addMemberSpec(
        Map<String, AssignmentMemberSpec> members,
        int memberIndex,
        Set<Uuid> subscribedTopicIds
    ) {
        String memberName = "member" + memberIndex;
        Optional<String> rackId = rackId(memberIndex);

        members.put(memberName, new AssignmentMemberSpec(
            Optional.empty(),
            rackId,
            subscribedTopicIds,
            Collections.emptyMap()
        ));
    }

    private static Map<Integer, Set<String>> mkMapOfPartitionRacks(int numPartitions) {
        Map<Integer, Set<String>> partitionRacks = new HashMap<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            partitionRacks.put(i, new HashSet<>(Arrays.asList("rack" + i % NUMBER_OF_RACKS, "rack" + (i + 1) % NUMBER_OF_RACKS)));
        }
        return partitionRacks;
    }

    private void simulateIncrementalRebalance(Map<Uuid, TopicMetadata> topicMetadata) {
        GroupAssignment initialAssignment = partitionAssignor.assign(assignmentSpec, subscribedTopicDescriber);
        Map<String, MemberAssignment> members = initialAssignment.members();

        Map<String, AssignmentMemberSpec> updatedMembers = new HashMap<>();
        members.forEach((memberId, memberAssignment) -> {
            AssignmentMemberSpec memberSpec = assignmentSpec.members().get(memberId);
            updatedMembers.put(memberId, new AssignmentMemberSpec(
                memberSpec.instanceId(),
                memberSpec.rackId(),
                memberSpec.subscribedTopicIds(),
                memberAssignment.targetPartitions()
            ));
        });

        Optional<String> rackId = rackId(memberCount-1);
        updatedMembers.put("newMember", new AssignmentMemberSpec(
            Optional.empty(),
            rackId,
            topicMetadata.keySet(),
            Collections.emptyMap()
        ));

        this.assignmentSpec = new AssignmentSpec(updatedMembers);
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void doAssignment() {
        partitionAssignor.assign(assignmentSpec, subscribedTopicDescriber);
    }
}
