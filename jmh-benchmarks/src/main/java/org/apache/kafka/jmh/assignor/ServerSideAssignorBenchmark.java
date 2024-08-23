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
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.assignor.RangeAssignor;
import org.apache.kafka.coordinator.group.assignor.UniformAssignor;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.GroupSpecImpl;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;
import org.apache.kafka.coordinator.group.modern.SubscribedTopicDescriberImpl;
import org.apache.kafka.coordinator.group.modern.TopicIds;
import org.apache.kafka.coordinator.group.modern.TopicMetadata;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.TopicsImage;

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

import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HETEROGENEOUS;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;

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

    private GroupSpecImpl groupSpec;

    private SubscribedTopicDescriber subscribedTopicDescriber;

    private final List<String> allTopicNames = new ArrayList<>();

    private TopicsImage topicsImage = TopicsImage.EMPTY;

    @Setup(Level.Trial)
    public void setup() {
        Map<Uuid, TopicMetadata> topicMetadata = createTopicMetadata();
        subscribedTopicDescriber = new SubscribedTopicDescriberImpl(topicMetadata);

        createGroupSpec();

        partitionAssignor = assignorType.assignor();

        if (assignmentType == AssignmentType.INCREMENTAL) {
            simulateIncrementalRebalance();
        }
    }

    private Map<Uuid, TopicMetadata> createTopicMetadata() {
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        int partitionsPerTopicCount = (memberCount * partitionsToMemberRatio) / topicCount;

        Map<Integer, Set<String>> partitionRacks = isRackAware ?
            mkMapOfPartitionRacks(partitionsPerTopicCount) :
            Collections.emptyMap();

        for (int i = 0; i < topicCount; i++) {
            Uuid topicUuid = Uuid.randomUuid();
            String topicName = "topic" + i;
            allTopicNames.add(topicName);
            topicMetadata.put(topicUuid, new TopicMetadata(
                topicUuid,
                topicName,
                partitionsPerTopicCount,
                partitionRacks
            ));

            AssignorBenchmarkUtils.addTopic(
                delta,
                topicUuid,
                topicName,
                partitionsPerTopicCount
            );
        }

        topicsImage = delta.apply(MetadataProvenance.EMPTY).topics();
        return topicMetadata;
    }

    private void createGroupSpec() {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new HashMap<>();

        // In the rebalance case, we will add the last member as a trigger.
        // This is done to keep the total members count consistent with the input.
        int numberOfMembers = assignmentType.equals(AssignmentType.INCREMENTAL) ? memberCount - 1 : memberCount;

        if (subscriptionType == HOMOGENEOUS) {
            for (int i = 0; i < numberOfMembers; i++) {
                addMemberSpec(members, i, new TopicIds(new HashSet<>(allTopicNames), topicsImage));
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

                TopicIds bucketTopics = new TopicIds(new HashSet<>(allTopicNames.subList(topicStartIndex, topicEndIndex)), topicsImage);

                // Assign topics to each member in the current bucket
                for (int i = memberStartIndex; i < memberEndIndex; i++) {
                    addMemberSpec(members, i, bucketTopics);
                }
            }
        }

        this.groupSpec = new GroupSpecImpl(
            members,
            subscriptionType,
            Collections.emptyMap()
        );
    }

    private Optional<String> rackId(int memberIndex) {
        return isRackAware ? Optional.of("rack" + memberIndex % NUMBER_OF_RACKS) : Optional.empty();
    }

    private void addMemberSpec(
        Map<String, MemberSubscriptionAndAssignmentImpl> members,
        int memberIndex,
        Set<Uuid> subscribedTopicIds
    ) {
        String memberId = "member" + memberIndex;
        Optional<String> rackId = rackId(memberIndex);

        members.put(memberId, new MemberSubscriptionAndAssignmentImpl(
            rackId,
            Optional.empty(),
            subscribedTopicIds,
            Assignment.EMPTY
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
        GroupAssignment initialAssignment = partitionAssignor.assign(groupSpec, subscribedTopicDescriber);
        Map<String, MemberAssignment> members = initialAssignment.members();

        Map<Uuid, Map<Integer, String>> invertedTargetAssignment = AssignorBenchmarkUtils.computeInvertedTargetAssignment(initialAssignment);

        Map<String, MemberSubscriptionAndAssignmentImpl> updatedMemberSpec = new HashMap<>();

        for (String memberId : groupSpec.memberIds()) {
            MemberAssignment memberAssignment = members.getOrDefault(
                memberId,
                new MemberAssignmentImpl(Collections.emptyMap())
            );

            updatedMemberSpec.put(memberId, new MemberSubscriptionAndAssignmentImpl(
                groupSpec.memberSubscription(memberId).rackId(),
                Optional.empty(),
                groupSpec.memberSubscription(memberId).subscribedTopicIds(),
                new Assignment(Collections.unmodifiableMap(memberAssignment.partitions()))
            ));
        }

        Set<Uuid> subscribedTopicIdsForNewMember;
        if (subscriptionType == HETEROGENEOUS) {
            subscribedTopicIdsForNewMember = updatedMemberSpec.get("member" + (memberCount - 2)).subscribedTopicIds();
        } else {
            subscribedTopicIdsForNewMember = new TopicIds(new HashSet<>(allTopicNames), topicsImage);
        }

        Optional<String> rackId = rackId(memberCount - 1);
        updatedMemberSpec.put("newMember", new MemberSubscriptionAndAssignmentImpl(
            rackId,
            Optional.empty(),
            subscribedTopicIdsForNewMember,
            Assignment.EMPTY
        ));

        groupSpec = new GroupSpecImpl(
            updatedMemberSpec,
            subscriptionType,
            invertedTargetAssignment
        );
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void doAssignment() {
        partitionAssignor.assign(groupSpec, subscribedTopicDescriber);
    }
}
