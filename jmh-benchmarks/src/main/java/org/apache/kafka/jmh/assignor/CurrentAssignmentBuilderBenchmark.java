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
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.modern.consumer.CurrentAssignmentBuilder;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class CurrentAssignmentBuilderBenchmark {

    @Param({"5", "50"})
    private int partitionsPerTopic;

    @Param({"10", "100", "1000"})
    private int topicCount;

    private List<String> topicNames;

    private List<Uuid> topicIds;

    private CoordinatorMetadataImage metadataImage;

    private ConsumerGroupMember member;

    private ConsumerGroupMember memberWithUnsubscribedTopics;

    private Assignment targetAssignment;

    @Setup(Level.Trial)
    public void setup() {
        setupTopics();
        setupMember();
        setupTargetAssignment();
    }

    private void setupTopics() {
        topicNames = AssignorBenchmarkUtils.createTopicNames(topicCount);
        topicIds = new ArrayList<>(topicCount);
        metadataImage = AssignorBenchmarkUtils.createMetadataImage(topicNames, partitionsPerTopic);

        for (String topicName : topicNames) {
            Uuid topicId = metadataImage.topicMetadata(topicName).get().id();
            topicIds.add(topicId);
        }
    }

    private void setupMember() {
        Map<Uuid, Set<Integer>> assignedPartitions = new HashMap<>();
        for (Uuid topicId : topicIds) {
            Set<Integer> partitions = IntStream.range(0, partitionsPerTopic)
                .boxed()
                .collect(Collectors.toSet());
            assignedPartitions.put(topicId, partitions);
        }

        ConsumerGroupMember.Builder memberBuilder = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setSubscribedTopicNames(topicNames)
            .setAssignedPartitions(assignedPartitions);

        member = memberBuilder.build();
        memberWithUnsubscribedTopics = memberBuilder
            .setSubscribedTopicNames(topicNames.subList(0, topicNames.size() - 1))
            .build();
    }

    private void setupTargetAssignment() {
        Map<Uuid, Set<Integer>> assignedPartitions = new HashMap<>();
        for (Uuid topicId : topicIds) {
            Set<Integer> partitions = IntStream.range(0, partitionsPerTopic)
                .boxed()
                .collect(Collectors.toSet());
            assignedPartitions.put(topicId, partitions);
        }
        targetAssignment = new Assignment(assignedPartitions);
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public ConsumerGroupMember stableToStableWithNoChange() {
        return new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
            .withTargetAssignment(member.memberEpoch(), targetAssignment)
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .build();
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public ConsumerGroupMember stableToStableWithNewTargetAssignment() {
        return new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
            .withTargetAssignment(member.memberEpoch() + 1, targetAssignment)
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .build();
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public ConsumerGroupMember stableToStableWithSubscriptionChange() {
        return new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
            .withTargetAssignment(member.memberEpoch(), targetAssignment)
            .withHasSubscriptionChanged(true)
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .build();
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public ConsumerGroupMember stableToUnrevokedPartitionsWithSubscriptionChange() {
        return new CurrentAssignmentBuilder(memberWithUnsubscribedTopics)
            .withMetadataImage(metadataImage)
            .withTargetAssignment(memberWithUnsubscribedTopics.memberEpoch(), targetAssignment)
            .withHasSubscriptionChanged(true)
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .build();
    }
}
