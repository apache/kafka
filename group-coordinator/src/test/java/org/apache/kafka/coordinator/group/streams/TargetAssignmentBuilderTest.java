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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.api.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.TaskAssignor;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.streams.assignor.AssignmentConfigsImpl;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpecImpl;
import org.apache.kafka.coordinator.group.streams.assignor.MemberMetadataAndStateImpl;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredTopology;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.coordinator.group.Assertions.assertUnorderedRecordsEquals;
import static org.apache.kafka.coordinator.group.streams.StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord;
import static org.apache.kafka.coordinator.group.streams.StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord;
import static org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.createMemberMetadataAndState;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasks;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksTuple;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TargetAssignmentBuilderTest {

    @Test
    public void testBuildEmptyAssignmentWhenTopologyNotReady() {
        String groupId = "test-group";
        int groupEpoch = 1;
        TaskAssignor assignor = mock(TaskAssignor.class);
        ConfiguredTopology topology = mock(ConfiguredTopology.class);
        Map<String, String> assignmentConfigs = new HashMap<>();

        when(topology.isReady()).thenReturn(false);

        TargetAssignmentBuilder builder = new TargetAssignmentBuilder(groupId, groupEpoch, assignor, assignmentConfigs)
            .withTime(new MockTime(0, 12345L, 12345L))
            .withTopology(topology);

        TargetAssignmentBuilder.TargetAssignmentResult result = builder.build();

        List<CoordinatorRecord> expectedRecords = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, groupEpoch, 12345L)
        );

        assertEquals(expectedRecords, result.records());
        assertEquals(Map.of(), result.targetAssignment());
    }

    @ParameterizedTest
    // Active, standby and warm-up tasks all come from the member's current assignment, so this
    // test varies which role the member's current tasks are in.
    @EnumSource(value = TaskRole.class, names = {"ACTIVE", "STANDBY"})
    public void testCreateMemberMetadataAndState(TaskRole taskRole) {
        String fooSubtopologyId = Uuid.randomUuid().toString();
        String barSubtopologyId = Uuid.randomUuid().toString();

        final Map<String, String> clientTags = mkMap(mkEntry("tag1", "value1"), mkEntry("tag2", "value2"));

        Map<String, Set<Integer>> activeTasks = taskRole == TaskRole.ACTIVE
            ? Map.of(fooSubtopologyId, Set.of(1, 2, 3), barSubtopologyId, Set.of(1, 2, 3)) : Map.of();
        Map<String, Set<Integer>> standbyTasks = taskRole == TaskRole.STANDBY
            ? Map.of(fooSubtopologyId, Set.of(1, 2, 3), barSubtopologyId, Set.of(1, 2, 3)) : Map.of();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member-id")
            .setRackId("rackId")
            .setInstanceId("instanceId")
            .setProcessId("processId")
            .setClientTags(clientTags)
            .setAssignedTasks(new TasksTupleWithEpochs(
                taskRole == TaskRole.ACTIVE
                    ? Map.of(fooSubtopologyId, Map.of(1, 0, 2, 0, 3, 0), barSubtopologyId, Map.of(1, 0, 2, 0, 3, 0))
                    : Map.of(),
                standbyTasks,
                Map.of()))
            .build();

        MemberMetadataAndStateImpl memberMetadata = createMemberMetadataAndState(
            member,
            MemberTaskOffsets.EMPTY
        );

        assertEquals(new MemberMetadataAndStateImpl(
            Optional.of("instanceId"),
            Optional.of("rackId"),
            "processId",
            clientTags,
            activeTasks,
            standbyTasks,
            Map.of(),
            Map.of(),
            Map.of()
        ), memberMetadata);
    }

    @Test
    public void testCreateMemberMetadataAndStatePopulatesTaskOffsets() {
        String fooSubtopologyId = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member-id")
            .setRackId("rackId")
            .setInstanceId("instanceId")
            .setProcessId("processId")
            .setClientTags(Map.of())
            .setAssignedTasks(TasksTupleWithEpochs.EMPTY)
            .build();

        Map<String, Map<Integer, Long>> taskOffsets = Map.of(fooSubtopologyId, Map.of(0, 10L));
        Map<String, Map<Integer, Long>> taskEndOffsets = Map.of(fooSubtopologyId, Map.of(0, 20L));

        MemberMetadataAndStateImpl memberMetadata = createMemberMetadataAndState(
            member,
            new MemberTaskOffsets(taskOffsets, taskEndOffsets)
        );

        assertEquals(taskOffsets, memberMetadata.taskOffsets());
        assertEquals(taskEndOffsets, memberMetadata.taskEndOffsets());
    }

    @Test
    public void testCreateMemberMetadataAndStateSourcesWarmupTasksFromCurrentAssignment() {
        String fooSubtopologyId = Uuid.randomUuid().toString();

        // Warm-up tasks are decided during reconciliation and stored in the member's current
        // assignment; they must be sourced from there, not from the target assignment.
        StreamsGroupMember member = new StreamsGroupMember.Builder("member-id")
            .setInstanceId("instanceId")
            .setRackId("rackId")
            .setProcessId("processId")
            .setClientTags(Map.of())
            .setAssignedTasks(new TasksTupleWithEpochs(Map.of(), Map.of(), Map.of(fooSubtopologyId, Set.of(1, 2, 3))))
            .build();

        MemberMetadataAndStateImpl memberMetadata = createMemberMetadataAndState(
            member,
            MemberTaskOffsets.EMPTY
        );

        assertEquals(Map.of(fooSubtopologyId, Set.of(1, 2, 3)), memberMetadata.warmupTasks());
    }

    @Test
    public void testEmpty() {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20,
            12345L
        );

        org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.TargetAssignmentResult result = context.build();
        assertEquals(List.of(newStreamsGroupTargetAssignmentMetadataRecord(
            "my-group",
            20,
            12345L
        )), result.records());
        assertEquals(Map.of(), result.targetAssignment());
    }

    
    @ParameterizedTest
    // Warm-up tasks are not produced by the assignor (only active and standby), so they cannot appear
    // in the resulting target assignment. See MemberAssignment.
    @EnumSource(value = TaskRole.class, names = {"ACTIVE", "STANDBY"})
    public void testAssignmentHasNotChanged(TaskRole taskRole) {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20,
            12345L
        );

        String fooSubtopologyId = context.addSubtopologyWithSingleSourceTopic("foo", 6);
        String barSubtopologyId = context.addSubtopologyWithSingleSourceTopic("bar", 6);

        context.addGroupMember("member-1", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 1, 2, 3),
            mkTasks(barSubtopologyId, 1, 2, 3)
        ));

        context.addGroupMember("member-2", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 4, 5, 6),
            mkTasks(barSubtopologyId, 4, 5, 6)
        ));

        context.prepareMemberAssignment("member-1", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 1, 2, 3),
            mkTasks(barSubtopologyId, 1, 2, 3)
        ));

        context.prepareMemberAssignment("member-2", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 4, 5, 6),
            mkTasks(barSubtopologyId, 4, 5, 6)
        ));

        org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(List.of(newStreamsGroupTargetAssignmentMetadataRecord(
            "my-group",
            20,
            12345L
        )), result.records());

        Map<String, TasksTuple> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", mkTasksTuple(taskRole, 
            mkTasks(fooSubtopologyId, 1, 2, 3),
            mkTasks(barSubtopologyId, 1, 2, 3)
        ));
        expectedAssignment.put("member-2", mkTasksTuple(taskRole, 
            mkTasks(fooSubtopologyId, 4, 5, 6),
            mkTasks(barSubtopologyId, 4, 5, 6)
        ));

        assertEquals(expectedAssignment, result.targetAssignment());
    }

    
    @ParameterizedTest
    // Warm-up tasks are not produced by the assignor (only active and standby), so they cannot appear
    // in the resulting target assignment. See MemberAssignment.
    @EnumSource(value = TaskRole.class, names = {"ACTIVE", "STANDBY"})
    public void testAssignmentSwapped(TaskRole taskRole) {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20,
            12345L
        );

        String fooSubtopologyId = context.addSubtopologyWithSingleSourceTopic("foo", 6);
        String barSubtopologyId = context.addSubtopologyWithSingleSourceTopic("bar", 6);

        context.addGroupMember("member-1", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 1, 2, 3),
            mkTasks(barSubtopologyId, 1, 2, 3)
        ));

        context.addGroupMember("member-2", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 4, 5, 6),
            mkTasks(barSubtopologyId, 4, 5, 6)
        ));

        context.prepareMemberAssignment("member-2", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 1, 2, 3),
            mkTasks(barSubtopologyId, 1, 2, 3)
        ));

        context.prepareMemberAssignment("member-1", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 4, 5, 6),
            mkTasks(barSubtopologyId, 4, 5, 6)
        ));

        org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(3, result.records().size());

        assertUnorderedRecordsEquals(List.of(List.of(
            newStreamsGroupTargetAssignmentRecord("my-group", "member-1", mkTasksTuple(taskRole,
                mkTasks(fooSubtopologyId, 4, 5, 6),
                mkTasks(barSubtopologyId, 4, 5, 6)
            )),
            newStreamsGroupTargetAssignmentRecord("my-group", "member-2", mkTasksTuple(taskRole,
                mkTasks(fooSubtopologyId, 1, 2, 3),
                mkTasks(barSubtopologyId, 1, 2, 3)
            ))
        )), result.records().subList(0, 2));

        assertEquals(newStreamsGroupTargetAssignmentMetadataRecord(
            "my-group",
            20,
            12345L
        ), result.records().get(2));

        Map<String, TasksTuple> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-2", mkTasksTuple(taskRole, 
            mkTasks(fooSubtopologyId, 1, 2, 3),
            mkTasks(barSubtopologyId, 1, 2, 3)
        ));
        expectedAssignment.put("member-1", mkTasksTuple(taskRole, 
            mkTasks(fooSubtopologyId, 4, 5, 6),
            mkTasks(barSubtopologyId, 4, 5, 6)
        ));

        assertEquals(expectedAssignment, result.targetAssignment());
    }

    
    @ParameterizedTest
    // Warm-up tasks are not produced by the assignor (only active and standby), so they cannot appear
    // in the resulting target assignment. See MemberAssignment.
    @EnumSource(value = TaskRole.class, names = {"ACTIVE", "STANDBY"})
    public void testPartialAssignmentUpdate(TaskRole taskRole) {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20,
            12345L
        );

        String fooSubtopologyId = context.addSubtopologyWithSingleSourceTopic("foo", 6);
        String barSubtopologyId = context.addSubtopologyWithSingleSourceTopic("bar", 6);

        context.addGroupMember("member-1", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 1, 2),
            mkTasks(barSubtopologyId, 1, 2)
        ));

        context.addGroupMember("member-2", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 3, 4),
            mkTasks(barSubtopologyId, 3, 4)
        ));

        context.addGroupMember("member-3", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 5, 6),
            mkTasks(barSubtopologyId, 5, 6)
        ));

        context.prepareMemberAssignment("member-1", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 1, 2),
            mkTasks(barSubtopologyId, 1, 2)
        ));

        context.prepareMemberAssignment("member-2", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 3, 4, 5),
            mkTasks(barSubtopologyId, 3, 4, 5)
        ));

        context.prepareMemberAssignment("member-3", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 6),
            mkTasks(barSubtopologyId, 6)
        ));

        org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(3, result.records().size());

        // Member 1 has no record because its assignment did not change.
        assertUnorderedRecordsEquals(List.of(List.of(
            newStreamsGroupTargetAssignmentRecord("my-group", "member-2", mkTasksTuple(taskRole,
                mkTasks(fooSubtopologyId, 3, 4, 5),
                mkTasks(barSubtopologyId, 3, 4, 5)
            )),
            newStreamsGroupTargetAssignmentRecord("my-group", "member-3", mkTasksTuple(taskRole,
                mkTasks(fooSubtopologyId, 6),
                mkTasks(barSubtopologyId, 6)
            ))
        )), result.records().subList(0, 2));

        assertEquals(newStreamsGroupTargetAssignmentMetadataRecord(
            "my-group",
            20,
            12345L
        ), result.records().get(2));

        Map<String, TasksTuple> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", mkTasksTuple(taskRole, 
            mkTasks(fooSubtopologyId, 1, 2),
            mkTasks(barSubtopologyId, 1, 2)
        ));
        expectedAssignment.put("member-2", mkTasksTuple(taskRole, 
            mkTasks(fooSubtopologyId, 3, 4, 5),
            mkTasks(barSubtopologyId, 3, 4, 5)
        ));
        expectedAssignment.put("member-3", mkTasksTuple(taskRole, 
            mkTasks(fooSubtopologyId, 6),
            mkTasks(barSubtopologyId, 6)
        ));

        assertEquals(expectedAssignment, result.targetAssignment());
    }

    
    public static class TargetAssignmentBuilderTestContext {

        private final String groupId;
        private final int groupEpoch;
        private final long assignmentTimestamp;
        private final TaskAssignor assignor = mock(TaskAssignor.class);
        private final SortedMap<String, ConfiguredSubtopology> subtopologies = new TreeMap<>();
        private final ConfiguredTopology topology = new ConfiguredTopology(0, 0, Optional.of(subtopologies), new HashMap<>(),
            Optional.empty());
        private final Map<String, StreamsGroupMember> members = new HashMap<>();
        private final Map<String, org.apache.kafka.coordinator.group.streams.TopicMetadata> subscriptionMetadata = new HashMap<>();
        private final Map<String, TasksTuple> targetAssignment = new HashMap<>();
        private final Map<String, MemberAssignment> memberAssignments = new HashMap<>();
        private MetadataImageBuilder topicsImageBuilder = new MetadataImageBuilder();

        public TargetAssignmentBuilderTestContext(
            String groupId,
            int groupEpoch,
            long assignmentTimestamp
        ) {
            this.groupId = groupId;
            this.groupEpoch = groupEpoch;
            this.assignmentTimestamp = assignmentTimestamp;
        }

        public void addGroupMember(
            String memberId,
            TasksTuple targetTasks
        ) {
            StreamsGroupMember.Builder memberBuilder = new StreamsGroupMember.Builder(memberId);
            memberBuilder.setProcessId("processId");
            memberBuilder.setClientTags(Map.of());
            memberBuilder.setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090));
            memberBuilder.setInstanceId(null);
            memberBuilder.setRackId(null);
            memberBuilder.setAssignedTasks(TasksTupleWithEpochs.EMPTY);
            members.put(memberId, memberBuilder.build());
            targetAssignment.put(memberId, targetTasks);
        }

        public String addSubtopologyWithSingleSourceTopic(
            String topicName,
            int numTasks
        ) {
            String subtopologyId = Uuid.randomUuid().toString();
            Uuid topicId = Uuid.randomUuid();
            topicsImageBuilder = topicsImageBuilder.addTopic(topicId, topicName, numTasks);
            subtopologies.put(subtopologyId, new ConfiguredSubtopology(numTasks, Set.of(topicId.toString()), Map.of(), Set.of(), Map.of()));

            return subtopologyId;
        }

        public void prepareMemberAssignment(
            String memberId,
            TasksTuple assignment
        ) {
            memberAssignments.put(memberId, new MemberAssignment(assignment.activeTasks(), assignment.standbyTasks()));
        }

        public org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.TargetAssignmentResult build() {
            // Prepare expected member specs.
            Map<String, MemberMetadataAndStateImpl> memberMetadataMap = new HashMap<>();
            members.forEach((memberId, member) ->
                memberMetadataMap.put(memberId, createMemberMetadataAndState(
                        member,
                        MemberTaskOffsets.EMPTY
                    )
                ));

            CoordinatorMetadataImage metadataImage = new KRaftCoordinatorMetadataImage(topicsImageBuilder.build());

            // Prepare the expected topology metadata.
            TopologyMetadata topologyMetadata = new TopologyMetadata(metadataImage, subtopologies);

            // Prepare the expected assignment spec.
            GroupSpecImpl groupSpec = new GroupSpecImpl(memberMetadataMap, AssignmentConfigsImpl.DEFAULT);

            // We use `any` here to always return an assignment but use `verify` later on
            // to ensure that the input was correct.
            when(assignor.assign(any(), any()))
                .thenReturn(new GroupAssignment(memberAssignments));

            // Create and populate the assignment builder.
            org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder builder = new org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder(
                groupId, groupEpoch, assignor, Map.of())
                .withTime(new MockTime(0, assignmentTimestamp, assignmentTimestamp))
                .withMembers(members)
                .withTopology(topology)
                .withMetadataImage(metadataImage)
                .withTargetAssignment(targetAssignment);

            // Execute the builder.
            org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.TargetAssignmentResult result = builder.build();

            // Verify that the assignor was called once with the expected
            // assignment spec.
            verify(assignor, times(1))
                .assign(groupSpec, topologyMetadata);

            return result;
        }
    }
}
