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
import org.apache.kafka.coordinator.group.api.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.streams.AssignmentRefiner;
import org.apache.kafka.coordinator.group.streams.AssignmentRefinerImpl;
import org.apache.kafka.coordinator.group.streams.MemberTaskOffsets;
import org.apache.kafka.coordinator.group.streams.NoOpAssignmentRefiner;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.TasksTuple;
import org.apache.kafka.coordinator.group.streams.TopologyMetadata;
import org.apache.kafka.coordinator.group.streams.assignor.AssignmentConfigsImpl;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * Measures one derivation of the streams group's intermediate assignment: what the group coordinator runs when it
 * refines the target assignment into the warm-up steps the members converge through, together with the check it
 * applies to the result before using it.
 * <p>
 * The group is set up as a refinement step finds it: the target assignment comes from the sticky assignor, a fraction
 * of the stateful active tasks still runs on a member of another process, the warm-up budget is spent on migrations
 * already under way, and the members report their restore progress as the lag picture says.
 * <p>
 * The full grid is large; narrow it with -p, for example -p memberCount=1000 -p lagPicture=MIXED.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class AssignmentRefinerBenchmark {

    /**
     * The default of acceptable.recovery.lag.
     */
    private static final long ACCEPTABLE_RECOVERY_LAG = 10_000L;

    @Param({"100", "1000"})
    private int memberCount;

    @Param({"1", "50"})
    private int membersPerProcess;

    @Param({"10", "100"})
    private int subtopologyCount;

    @Param({"100"})
    private int partitionCount;

    @Param({"0", "1"})
    private int standbyReplicas;

    @Param({"2", "100"})
    private int numWarmupReplicas;

    @Param({"0.0", "0.01", "0.5"})
    private double migratingTaskFraction;

    @Param({"ALL_CAUGHT_UP", "ALL_LAGGING", "NOT_REPORTED", "MIXED", "RESTORING_DESTINATIONS"})
    private StreamsAssignorBenchmarkUtils.LagPicture lagPicture;

    private final AssignmentRefiner noOpRefiner = new NoOpAssignmentRefiner();

    private final AssignmentRefiner refinerImpl = new AssignmentRefinerImpl();

    private Map<String, StreamsGroupMember> members;

    private Map<String, TasksTuple> targetAssignment;

    private Map<String, MemberTaskOffsets> taskOffsets;

    private SortedMap<String, ConfiguredSubtopology> subtopologyMap;

    @Setup(Level.Trial)
    public void setup() {
        List<String> allTopicNames = AssignorBenchmarkUtils.createTopicNames(subtopologyCount);
        subtopologyMap = StreamsAssignorBenchmarkUtils.createSubtopologyMap(partitionCount, allTopicNames);
        CoordinatorMetadataImage metadataImage = AssignorBenchmarkUtils.createMetadataImage(allTopicNames, partitionCount);

        Map<String, StreamsGroupMember> newMembers = StreamsAssignorBenchmarkUtils.createStreamsMembers(memberCount, membersPerProcess);
        GroupAssignment groupAssignment = new StickyTaskAssignor().assign(
            StreamsAssignorBenchmarkUtils.createGroupSpec(
                newMembers,
                AssignmentConfigsImpl.DEFAULT.withNumStandbyReplicas(standbyReplicas),
                Map.of()
            ),
            new TopologyMetadata(metadataImage, subtopologyMap)
        );

        targetAssignment = new HashMap<>();
        groupAssignment.members().forEach((memberId, memberAssignment) -> targetAssignment.put(
            memberId,
            new TasksTuple(memberAssignment.activeTasks(), memberAssignment.standbyTasks(), Map.of())
        ));

        members = StreamsAssignorBenchmarkUtils.divergeAssignment(
            newMembers,
            targetAssignment,
            subtopologyMap,
            migratingTaskFraction,
            numWarmupReplicas
        );
        taskOffsets = StreamsAssignorBenchmarkUtils.createMemberTaskOffsets(
            members,
            targetAssignment,
            subtopologyMap,
            lagPicture,
            ACCEPTABLE_RECOVERY_LAG
        );
    }

    @Benchmark
    @Threads(1)
    public void noOpRefiner(Blackhole blackhole) {
        refine(noOpRefiner, blackhole);
    }

    @Benchmark
    @Threads(1)
    public void refinerImpl(Blackhole blackhole) {
        refine(refinerImpl, blackhole);
    }

    private void refine(AssignmentRefiner refiner, Blackhole blackhole) {
        Map<String, TasksTuple> refinedAssignment = refiner.refine(
            members,
            targetAssignment,
            taskOffsets,
            subtopologyMap,
            numWarmupReplicas,
            ACCEPTABLE_RECOVERY_LAG
        );
        blackhole.consume(AssignmentRefiner.preservesActiveTaskCount(targetAssignment, refinedAssignment));
        blackhole.consume(refinedAssignment);
    }
}
