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
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.assignor.UniformAssignor;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.SubscribedTopicDescriberImpl;
import org.apache.kafka.coordinator.group.modern.TargetAssignmentBuilder;
import org.apache.kafka.coordinator.group.modern.TopicIds;
import org.apache.kafka.coordinator.group.modern.TopicMetadata;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;

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

    @Param({"HOMOGENEOUS", "HETEROGENEOUS"})
    private SubscriptionType subscriptionType;

    private static final String GROUP_ID = "benchmark-group";

    private static final int GROUP_EPOCH = 0;

    private PartitionAssignor partitionAssignor;

    private TargetAssignmentBuilder.ConsumerTargetAssignmentBuilder targetAssignmentBuilder;

    /** The number of homogeneous subgroups to create for the heterogeneous subscription case. */
    private static final int MAX_BUCKET_COUNT = 5;

    private GroupSpec groupSpec;

    private Map<Uuid, Map<Integer, String>> invertedTargetAssignment;

    private List<String> allTopicNames = Collections.emptyList();

    private Map<String, TopicMetadata> subscriptionMetadata = Collections.emptyMap();

    private TopicsImage topicsImage;

    private TopicIds.TopicResolver topicResolver;

    private SubscribedTopicDescriber subscribedTopicDescriber;

    @Setup(Level.Trial)
    public void setup() {
        // For this benchmark we will use the Uniform Assignor
        // and a group that has a homogeneous subscription model.
        partitionAssignor = new UniformAssignor();

        setupTopics();

        Map<String, ConsumerGroupMember> members = createMembers();
        Map<String, Assignment> existingTargetAssignment = generateMockInitialTargetAssignmentAndUpdateInvertedTargetAssignment(members);

        ConsumerGroupMember newMember = new ConsumerGroupMember.Builder("newMember")
            .setSubscribedTopicNames(allTopicNames)
            .build();

        targetAssignmentBuilder = new TargetAssignmentBuilder.ConsumerTargetAssignmentBuilder(GROUP_ID, GROUP_EPOCH, partitionAssignor)
            .withMembers(members)
            .withSubscriptionMetadata(subscriptionMetadata)
            .withSubscriptionType(subscriptionType)
            .withTargetAssignment(existingTargetAssignment)
            .withInvertedTargetAssignment(invertedTargetAssignment)
            .withTopicsImage(topicsImage)
            .addOrUpdateMember(newMember.memberId(), newMember);
    }

    private void setupTopics() {
        allTopicNames = AssignorBenchmarkUtils.createTopicNames(topicCount);

        int partitionsPerTopic = (memberCount * partitionsToMemberRatio) / topicCount;
        subscriptionMetadata = AssignorBenchmarkUtils.createSubscriptionMetadata(
            allTopicNames,
            partitionsPerTopic
        );

        topicsImage = AssignorBenchmarkUtils.createTopicsImage(subscriptionMetadata);
        topicResolver = new TopicIds.CachedTopicResolver(topicsImage);

        Map<Uuid, TopicMetadata> topicMetadata = AssignorBenchmarkUtils.createTopicMetadata(subscriptionMetadata);
        subscribedTopicDescriber = new SubscribedTopicDescriberImpl(topicMetadata);
    }

    private Map<String, Assignment> generateMockInitialTargetAssignmentAndUpdateInvertedTargetAssignment(
        Map<String, ConsumerGroupMember> members
    ) {
        this.groupSpec = AssignorBenchmarkUtils.createGroupSpec(
            members,
            subscriptionType,
            topicResolver
        );

        GroupAssignment groupAssignment = partitionAssignor.assign(
            groupSpec,
            subscribedTopicDescriber
        );
        invertedTargetAssignment = AssignorBenchmarkUtils.computeInvertedTargetAssignment(groupAssignment);

        Map<String, Assignment> initialTargetAssignment = new HashMap<>(memberCount);

        for (Map.Entry<String, MemberAssignment> entry : groupAssignment.members().entrySet()) {
            String memberId = entry.getKey();
            Map<Uuid, Set<Integer>> topicPartitions = entry.getValue().partitions();
            initialTargetAssignment.put(memberId, new Assignment(topicPartitions));
        }

        return initialTargetAssignment;
    }

    private Map<String, ConsumerGroupMember> createMembers() {
        if (subscriptionType == HOMOGENEOUS) {
            return AssignorBenchmarkUtils.createHomogeneousMembers(
                memberCount - 1,
                this::memberId,
                this::rackId,
                allTopicNames
            );
        } else {
            return AssignorBenchmarkUtils.createHeterogeneousBucketedMembers(
                memberCount - 1,
                MAX_BUCKET_COUNT,
                this::memberId,
                this::rackId,
                allTopicNames
            );
        }
    }

    private String memberId(int memberIndex) {
        return "member" + memberIndex;
    }

    private Optional<String> rackId(int memberIndex) {
        return Optional.empty();
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void build() {
        targetAssignmentBuilder.build();
    }
}
