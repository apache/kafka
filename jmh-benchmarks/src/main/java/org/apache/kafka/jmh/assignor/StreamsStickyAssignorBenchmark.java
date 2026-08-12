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
import org.apache.kafka.coordinator.group.api.streams.assignor.AssignmentConfigs;
import org.apache.kafka.coordinator.group.api.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.streams.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.TaskAssignor;
import org.apache.kafka.coordinator.group.api.streams.assignor.TopologyDescriber;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.TopologyMetadata;
import org.apache.kafka.coordinator.group.streams.assignor.AssignmentConfigsImpl;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpecImpl;
import org.apache.kafka.coordinator.group.streams.assignor.MemberMetadataAndStateImpl;
import org.apache.kafka.coordinator.group.streams.assignor.StickyTaskAssignor;
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

import java.util.ArrayList;
import java.util.Collections;
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

    /**
     * Whether the members report offset sums for the state they hold on local disk. NONE is the behaviour of a
     * group whose clients do not report offsets, OWNED_AND_DORMANT also reports state left behind by earlier
     * assignments, so several members compete as candidates for the same task.
     */
    public enum ReportedOffsets {
        NONE, OWNED_AND_DORMANT
    }

    /**
     * The number of members reporting a task on top of the one owning it, under OWNED_AND_DORMANT.
     */
    private static final int DORMANT_REPLICAS = 1;

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

    @Param({"NONE", "OWNED_AND_DORMANT"})
    private ReportedOffsets reportedOffsets;

    private TaskAssignor taskAssignor;

    private GroupSpec groupSpec;

    private TopologyDescriber topologyDescriber;

    private AssignmentConfigs assignmentConfigs;

    private Map<String, Map<String, Map<Integer, Long>>> taskOffsets;

    @Setup(Level.Trial)
    public void setup() {
        List<String> allTopicNames = AssignorBenchmarkUtils.createTopicNames(subtopologyCount);

        SortedMap<String, ConfiguredSubtopology> subtopologyMap = StreamsAssignorBenchmarkUtils.createSubtopologyMap(partitionCount, allTopicNames);

        CoordinatorMetadataImage metadataImage = AssignorBenchmarkUtils.createMetadataImage(allTopicNames, partitionCount);

        topologyDescriber = new TopologyMetadata(metadataImage, subtopologyMap);

        taskAssignor = new StickyTaskAssignor();

        Map<String, StreamsGroupMember> members = createMembers();
        this.assignmentConfigs = AssignmentConfigsImpl.DEFAULT.withNumStandbyReplicas(standbyReplicas);

        List<String> memberIds = new ArrayList<>(members.keySet());
        Collections.sort(memberIds);
        this.taskOffsets = reportedOffsets == ReportedOffsets.NONE
            ? Map.of()
            : StreamsAssignorBenchmarkUtils.createTaskOffsets(memberIds, subtopologyMap, DORMANT_REPLICAS);

        if (assignmentType == AssignmentType.INCREMENTAL) {
            // The setup assignment is only fixture for the measured one, so it is left offset-free. The offsets
            // go into the member spec it produces, which is what the measured assignment sees.
            this.groupSpec = StreamsAssignorBenchmarkUtils.createGroupSpec(members, assignmentConfigs, Map.of());
            simulateIncrementalRebalance();
        } else {
            this.groupSpec = StreamsAssignorBenchmarkUtils.createGroupSpec(members, assignmentConfigs, taskOffsets);
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

        Map<String, MemberMetadataAndStateImpl> updatedMemberSpec = new HashMap<>();

        for (String memberId : groupSpec.memberIds()) {
            MemberAssignment memberAssignment = members.getOrDefault(
                memberId,
                new MemberAssignment(Map.of(), Map.of())
            );

            updatedMemberSpec.put(memberId, new MemberMetadataAndStateImpl(
                Optional.empty(),
                Optional.empty(),
                groupSpec.memberMetadata(memberId).processId(),
                Map.of(),
                memberAssignment.activeTasks(),
                memberAssignment.standbyTasks(),
                // Warm-up tasks are not assigned by the assignor; they are decided during reconciliation.
                Map.of(),
                taskOffsets.getOrDefault(memberId, Map.of()),
                Map.of()
            ));
        }

        updatedMemberSpec.put("newMember", new MemberMetadataAndStateImpl(
            Optional.empty(),
            Optional.empty(),
            "process-newMember",
            Map.of(),
            Map.of(),
            Map.of(),
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
