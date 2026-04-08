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

import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.TopologyMetadata;
import org.apache.kafka.coordinator.group.streams.assignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpecImpl;
import org.apache.kafka.coordinator.group.streams.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.streams.assignor.StickyTaskAssignor;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignor;
import org.apache.kafka.coordinator.group.streams.assignor.TopologyDescriber;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;

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
import org.openjdk.jmh.infra.Blackhole;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class StreamsStickyAssignorBenchmark {

    /**
     * The assignment type is decided based on whether all the members are assigned partitions
     * for the first time (full), or incrementally when a rebalance is triggered.
     */
    public enum AssignmentType {
        FULL, INCREMENTAL
    }

    @Param({"100", "1000"})
    private int memberCount;

    @Param({"10", "100"})
    private int partitionCount;

    @Param({"10", "100"})
    private int subtopologyCount;

    @Param({"0", "1"})
    private int standbyReplicas;

    @Param({"1", "50"})
    private int membersPerProcess;

    @Param({"FULL", "INCREMENTAL"})
    private AssignmentType assignmentType;

    private TaskAssignor taskAssignor;

    private GroupSpec groupSpec;

    private TopologyDescriber topologyDescriber;

    private Map<String, String> assignmentConfigs;

    @Setup(Level.Trial)
    public void setup() {
        List<String> allTopicNames = AssignorBenchmarkUtils.createTopicNames(subtopologyCount);

        SortedMap<String, ConfiguredSubtopology> subtopologyMap = StreamsAssignorBenchmarkUtils.createSubtopologyMap(partitionCount, allTopicNames);

        CoordinatorMetadataImage metadataImage = AssignorBenchmarkUtils.createMetadataImage(allTopicNames, partitionCount);

        topologyDescriber = new TopologyMetadata(metadataImage, subtopologyMap);

        taskAssignor = new StickyTaskAssignor();

        Map<String, StreamsGroupMember> members = createMembers();
        this.assignmentConfigs = Map.of(
            "num.standby.replicas",
            Integer.toString(standbyReplicas)
        );
        this.groupSpec = StreamsAssignorBenchmarkUtils.createGroupSpec(members, assignmentConfigs);

        if (assignmentType == AssignmentType.INCREMENTAL) {
            simulateIncrementalRebalance();
        }
    }

    private Map<String, StreamsGroupMember> createMembers() {
        // In the rebalance case, we will add the last member as a trigger.
        // This is done to keep the total members count consistent with the input.
        int numberOfMembers = assignmentType.equals(AssignmentType.INCREMENTAL) ? memberCount - 1 : memberCount;

        return StreamsAssignorBenchmarkUtils.createStreamsMembers(
            numberOfMembers,
            membersPerProcess
        );
    }

    private void simulateIncrementalRebalance() {
        GroupAssignment initialAssignment = new StickyTaskAssignor().assign(groupSpec, topologyDescriber);
        Map<String, MemberAssignment> members = initialAssignment.members();

        Map<String, AssignmentMemberSpec> updatedMemberSpec = new HashMap<>();

        for (Map.Entry<String, AssignmentMemberSpec> member : groupSpec.members().entrySet()) {
            MemberAssignment memberAssignment = members.getOrDefault(
                member.getKey(),
                new MemberAssignment(Map.of(), Map.of(), Map.of())
            );

            updatedMemberSpec.put(member.getKey(), new AssignmentMemberSpec(
                Optional.empty(),
                Optional.empty(),
                memberAssignment.activeTasks(),
                memberAssignment.standbyTasks(),
                memberAssignment.warmupTasks(),
                member.getValue().processId(),
                Map.of(),
                Map.of(),
                Map.of()
            ));
        }

        updatedMemberSpec.put("newMember", new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Map.of(),
            Map.of(),
            Map.of(),
            "process-newMember",
            Map.of(),
            Map.of(),
            Map.of()
        ));

        groupSpec = new GroupSpecImpl(
            updatedMemberSpec,
            assignmentConfigs
        );
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void doAssignment(Blackhole blackhole) {
        blackhole.consume(taskAssignor.assign(groupSpec, topologyDescriber));
    }
}
