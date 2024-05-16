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
import org.apache.kafka.coordinator.group.assignor.UniformAssignor;
import org.apache.kafka.coordinator.group.consumer.Assignment;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.consumer.SubscribedTopicMetadata;
import org.apache.kafka.coordinator.group.consumer.TargetAssignmentBuilder;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.coordinator.group.assignor.SubscriptionType.HOMOGENEOUS;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class TargetAssignmentBuilderBenchmark {

    @Param({"100", "500", "1000", "5000", "10000"})
    private int memberCount;

    @Param({"5", "10", "50"})
    private int partitionsToMemberRatio;

    @Param({"10", "100", "1000"})
    private int topicCount;

    private static final String GROUP_ID = "benchmark-group";

    private static final int GROUP_EPOCH = 0;

    private PartitionAssignor partitionAssignor;

    private Map<String, TopicMetadata> subscriptionMetadata = Collections.emptyMap();

    private TargetAssignmentBuilder targetAssignmentBuilder;

    private AssignmentSpec assignmentSpec;

    private final List<String> allTopicNames = new ArrayList<>();

    private final List<Uuid> allTopicIds = new ArrayList<>();

    @Setup(Level.Trial)
    public void setup() {
        // For this benchmark we will use the Uniform Assignor
        // and a group that has a homogeneous subscription model.
        partitionAssignor = new UniformAssignor();

        subscriptionMetadata = generateMockSubscriptionMetadata();
        Map<String, ConsumerGroupMember> members = generateMockMembers();
        Map<String, Assignment> existingTargetAssignment = generateMockInitialTargetAssignment();

        ConsumerGroupMember newMember = new ConsumerGroupMember.Builder("newMember")
            .setSubscribedTopicNames(allTopicNames)
            .build();

        targetAssignmentBuilder = new TargetAssignmentBuilder(GROUP_ID, GROUP_EPOCH, partitionAssignor)
            .withMembers(members)
            .withSubscriptionMetadata(subscriptionMetadata)
            .withTargetAssignment(existingTargetAssignment)
            .withSubscriptionType(HOMOGENEOUS)
            .addOrUpdateMember(newMember.memberId(), newMember);
    }

    private Map<String, ConsumerGroupMember> generateMockMembers() {
        Map<String, ConsumerGroupMember> members = new HashMap<>();

        for (int i = 0; i < memberCount - 1; i++) {
            ConsumerGroupMember member = new ConsumerGroupMember.Builder("member" + i)
                .setSubscribedTopicNames(allTopicNames)
                .build();
            members.put("member" + i, member);
        }
        return members;
    }

    private Map<String, TopicMetadata> generateMockSubscriptionMetadata() {
        Map<String, TopicMetadata> subscriptionMetadata = new HashMap<>();
        int partitionsPerTopicCount = (memberCount * partitionsToMemberRatio) / topicCount;

        for (int i = 0; i < topicCount; i++) {
            String topicName = "topic-" + i;
            Uuid topicId = Uuid.randomUuid();
            allTopicNames.add(topicName);
            allTopicIds.add(topicId);

            TopicMetadata metadata = new TopicMetadata(
                topicId,
                topicName,
                partitionsPerTopicCount,
                Collections.emptyMap()
            );
            subscriptionMetadata.put(topicName, metadata);
        }

        return subscriptionMetadata;
    }

    private Map<String, Assignment> generateMockInitialTargetAssignment() {
        Map<Uuid, TopicMetadata> topicMetadataMap = new HashMap<>(topicCount);
        subscriptionMetadata.forEach((topicName, topicMetadata) ->
            topicMetadataMap.put(
                topicMetadata.id(),
                topicMetadata
            )
        );

        createAssignmentSpec();

        GroupAssignment groupAssignment = partitionAssignor.assign(
            assignmentSpec,
            new SubscribedTopicMetadata(topicMetadataMap)
        );

        Map<String, Assignment> initialTargetAssignment = new HashMap<>(memberCount);

        for (Map.Entry<String, MemberAssignment> entry : groupAssignment.members().entrySet()) {
            String memberId = entry.getKey();
            Map<Uuid, Set<Integer>> topicPartitions = entry.getValue().targetPartitions();

            Assignment assignment = new Assignment(topicPartitions);

            initialTargetAssignment.put(memberId, assignment);
        }

        return initialTargetAssignment;
    }

    private void createAssignmentSpec() {
        Map<String, AssignmentMemberSpec> members = new HashMap<>();

        for (int i = 0; i < memberCount - 1; i++) {
            String memberId = "member" + i;

            members.put(memberId, new AssignmentMemberSpec(
                Optional.empty(),
                Optional.empty(),
                allTopicIds,
                Collections.emptyMap()
            ));
        }
        assignmentSpec = new AssignmentSpec(members, HOMOGENEOUS);
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void build() {
        targetAssignmentBuilder.build();
    }
}
