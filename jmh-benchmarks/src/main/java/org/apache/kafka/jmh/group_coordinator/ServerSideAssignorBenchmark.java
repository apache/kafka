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
package org.apache.kafka.jmh.group_coordinator;

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
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static java.lang.Integer.max;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
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

    @Param({"1000", "10000"})
    private int memberCount;

    @Param({"10", "50"})
    private int partitionsPerTopicCount;

    @Param({"100", "1000"})
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

    private static final int numberOfRacks = 3;

    private AssignmentSpec assignmentSpec;

    private SubscribedTopicDescriber subscribedTopicDescriber;

    @Setup(Level.Trial)
    public void setup() {
        Map<Uuid, TopicMetadata> topicMetadata = createTopicMetadata();
        subscribedTopicDescriber = new SubscribedTopicMetadata(topicMetadata);

        createAssignmentSpec(topicMetadata);

        partitionAssignor = assignorType.assignor();

        if (simulateRebalanceTrigger) {
            simulateIncrementalRebalance(topicMetadata);
        }
    }

    private Map<Uuid, TopicMetadata> createTopicMetadata() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        Map<Integer, Set<String>> partitionRacks = isRackAware ?
            mkMapOfPartitionRacks(partitionsPerTopicCount) :
            Collections.emptyMap();

        for (int i = 1; i <= topicCount; i++) {
            Uuid topicUuid = Uuid.randomUuid();
            String topicName = "topic" + i;
            topicMetadata.put(topicUuid, new TopicMetadata(
                topicUuid,
                topicName,
                partitionsPerTopicCount,
                partitionRacks
            ));
        }

        return topicMetadata;
    }

    private void createAssignmentSpec(Map<Uuid, TopicMetadata> topicMetadata) {
        Map<String, AssignmentMemberSpec> members = new TreeMap<>();
        List<Uuid> allTopicIds = new ArrayList<>(topicMetadata.keySet());
        int topicCounter = 0;

        for (int i = 0; i < memberCount; i++) {
            String memberName = "member" + i;
            Optional<String> rackId = rackId(i);

            // When subscriptions are homogenous, all members are assigned all topics.
            List<Uuid> subscribedTopicIds;

            if (subscriptionModel == SubscriptionModel.HOMOGENEOUS) {
                subscribedTopicIds = allTopicIds;
            } else {
                subscribedTopicIds = Arrays.asList(
                    allTopicIds.get(i % topicCount),
                    allTopicIds.get((i+1) % topicCount)
                );
                topicCounter = max (topicCounter, ((i+1) % topicCount));

                if (i == memberCount - 1 && topicCounter < topicCount - 1) {
                    subscribedTopicIds.addAll(allTopicIds.subList(topicCounter + 1, topicCount - 1));
                }
            }

            members.put(memberName, new AssignmentMemberSpec(
                Optional.empty(),
                rackId,
                subscribedTopicIds,
                Collections.emptyMap()
            ));
        }

        this.assignmentSpec = new AssignmentSpec(members);
    }

    private Optional<String> rackId(int index) {
        return isRackAware ? Optional.of("rack" + index % numberOfRacks) : Optional.empty();
    }

    private static Map<Integer, Set<String>> mkMapOfPartitionRacks(int numPartitions) {
        Map<Integer, Set<String>> partitionRacks = new HashMap<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            partitionRacks.put(i, new HashSet<>(Arrays.asList("rack" + i % numberOfRacks, "rack" + (i + 1) % numberOfRacks)));
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

        Optional<String> rackId = isRackAware ? Optional.of("rack" + (memberCount + 1) % numberOfRacks) : Optional.empty();
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
