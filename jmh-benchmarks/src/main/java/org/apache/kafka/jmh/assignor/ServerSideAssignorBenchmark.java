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
import org.apache.kafka.coordinator.group.assignor.SubscriptionType;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.coordinator.group.assignor.SubscriptionType.HETEROGENEOUS;
import static org.apache.kafka.coordinator.group.assignor.SubscriptionType.HOMOGENEOUS;

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

    @Param({"RANGE", "UNIFORM"})
    private AssignorType assignorType;

    @Param({"FULL", "INCREMENTAL"})
    private AssignmentType assignmentType;

    private PartitionAssignor partitionAssignor;

    private static final int NUMBER_OF_RACKS = 3;

    private static final int MAX_BUCKET_COUNT = 5;

    private AssignmentSpec assignmentSpec;

    private SubscribedTopicDescriber subscribedTopicDescriber;

    private final List<Uuid> allTopicIds = new ArrayList<>();

    @Setup(Level.Trial)
    public void setup() {
        Map<Uuid, TopicMetadata> topicMetadata = createTopicMetadata();
        subscribedTopicDescriber = new SubscribedTopicMetadata(topicMetadata);

        createAssignmentSpec();

        partitionAssignor = assignorType.assignor();

        if (assignmentType == AssignmentType.INCREMENTAL) {
            simulateIncrementalRebalance();
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

        // In the rebalance case, we will add the last member as a trigger.
        // This is done to keep the total members count consistent with the input.
        int numberOfMembers = assignmentType.equals(AssignmentType.INCREMENTAL) ? memberCount - 1 : memberCount;

        if (subscriptionType == HOMOGENEOUS) {
            for (int i = 0; i < numberOfMembers; i++) {
                addMemberSpec(members, i, new HashSet<>(allTopicIds));
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

                Set<Uuid> bucketTopics = new HashSet<>(allTopicIds.subList(topicStartIndex, topicEndIndex));

                // Assign topics to each member in the current bucket
                for (int i = memberStartIndex; i < memberEndIndex; i++) {
                    addMemberSpec(members, i, bucketTopics);
                }
            }
        }

        this.assignmentSpec = new AssignmentSpec(members, subscriptionType);
    }

    private Optional<String> rackId(int memberIndex) {
        return isRackAware ? Optional.of("rack" + memberIndex % NUMBER_OF_RACKS) : Optional.empty();
    }

    private void addMemberSpec(
        Map<String, AssignmentMemberSpec> members,
        int memberIndex,
        Set<Uuid> subscribedTopicIds
    ) {
        String memberId = "member" + memberIndex;
        Optional<String> rackId = rackId(memberIndex);

        members.put(memberId, new AssignmentMemberSpec(
            Optional.empty(),
            rackId,
            subscribedTopicIds,
            Collections.emptyMap()
        ));
    }

    private static Map<Integer, Set<String>> mkMapOfPartitionRacks(int numPartitions) {
        Map<Integer, Set<String>> partitionRacks = new HashMap<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            partitionRacks.put(i, new HashSet<>(Arrays.asList(
                "rack" + i % NUMBER_OF_RACKS,
                "rack" + (i + 1) % NUMBER_OF_RACKS,
                "rack" + (i + 2) % NUMBER_OF_RACKS
            )));
        }
        return partitionRacks;
    }

    private void simulateIncrementalRebalance() {
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

        Collection<Uuid> subscribedTopicIdsForNewMember;
        if (subscriptionType == HETEROGENEOUS) {
            subscribedTopicIdsForNewMember = updatedMembers.get("member" + (memberCount - 2)).subscribedTopicIds();
        } else {
            subscribedTopicIdsForNewMember = allTopicIds;
        }

        Optional<String> rackId = rackId(memberCount - 1);
        updatedMembers.put("newMember", new AssignmentMemberSpec(
            Optional.empty(),
            rackId,
            subscribedTopicIdsForNewMember,
            Collections.emptyMap()
        ));

        assignmentSpec = new AssignmentSpec(updatedMembers, subscriptionType);
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void doAssignment() {
        partitionAssignor.assign(assignmentSpec, subscribedTopicDescriber);
    }
}
