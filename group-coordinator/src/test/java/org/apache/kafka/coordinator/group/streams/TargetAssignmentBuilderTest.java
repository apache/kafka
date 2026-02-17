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
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.TaskRole;
import org.apache.kafka.coordinator.group.streams.assignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpecImpl;
import org.apache.kafka.coordinator.group.streams.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignor;
import org.apache.kafka.coordinator.group.streams.topics.TopologyValidationResult;
import org.apache.kafka.coordinator.group.streams.topics.TopicConfigurationException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.coordinator.group.Assertions.assertUnorderedRecordsEquals;
import static org.apache.kafka.coordinator.group.streams.StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentEpochRecord;
import static org.apache.kafka.coordinator.group.streams.StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord;
import static org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.createAssignmentMemberSpec;
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
        TopologyValidationResult topologyValidationResult = new TopologyValidationResult(
            0,
            0L,
            Map.of(),
            Optional.of(TopicConfigurationException.missingSourceTopics("missing")),
            Optional.empty(),
            Map.of()
        );
        Map<String, String> assignmentConfigs = new HashMap<>();

        TargetAssignmentBuilder builder = new TargetAssignmentBuilder(groupId, groupEpoch, assignor, assignmentConfigs)
            .withTopologyValidationResult(topologyValidationResult);

        TargetAssignmentBuilder.TargetAssignmentResult result = builder.build();

        List<CoordinatorRecord> expectedRecords = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentEpochRecord(groupId, groupEpoch)
        );

        assertEquals(expectedRecords, result.records());
        assertEquals(Map.of(), result.targetAssignment());
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testCreateAssignmentMemberSpec(TaskRole taskRole) {
        String fooSubtopologyId = Uuid.randomUuid().toString();
        String barSubtopologyId = Uuid.randomUuid().toString();

        final Map<String, String> clientTags = mkMap(mkEntry("tag1", "value1"), mkEntry("tag2", "value2"));
        StreamsGroupMember member = new StreamsGroupMember.Builder("member-id")
            .setRackId("rackId")
            .setInstanceId("instanceId")
            .setProcessId("processId")
            .setClientTags(clientTags)
            .build();

        TasksTuple assignment = mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 1, 2, 3),
            mkTasks(barSubtopologyId, 1, 2, 3)
        );

        AssignmentMemberSpec assignmentMemberSpec = createAssignmentMemberSpec(
            member,
            assignment
        );

        assertEquals(new AssignmentMemberSpec(
            Optional.of("instanceId"),
            Optional.of("rackId"),
            assignment.activeTasks(),
            assignment.standbyTasks(),
            assignment.warmupTasks(),
            "processId",
            clientTags,
            Map.of(),
            Map.of()
        ), assignmentMemberSpec);
    }

    @Test
    public void testEmpty() {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
        );

        org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.TargetAssignmentResult result = context.build();
        assertEquals(List.of(newStreamsGroupTargetAssignmentEpochRecord(
            "my-group",
            20
        )), result.records());
        assertEquals(Map.of(), result.targetAssignment());
    }


    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testAssignmentHasNotChanged(TaskRole taskRole) {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
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

        assertEquals(List.of(newStreamsGroupTargetAssignmentEpochRecord(
            "my-group",
            20
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
    @EnumSource(TaskRole.class)
    public void testAssignmentSwapped(TaskRole taskRole) {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
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

        assertEquals(newStreamsGroupTargetAssignmentEpochRecord(
            "my-group",
            20
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
    @EnumSource(TaskRole.class)
    public void testNewMember(TaskRole taskRole) {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
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

        context.updateMemberMetadata("member-3");

        context.prepareMemberAssignment("member-1", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 1, 2),
            mkTasks(barSubtopologyId, 1, 2)
        ));

        context.prepareMemberAssignment("member-2", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 3, 4),
            mkTasks(barSubtopologyId, 3, 4)
        ));

        context.prepareMemberAssignment("member-3", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 5, 6),
            mkTasks(barSubtopologyId, 5, 6)
        ));

        org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(4, result.records().size());

        assertUnorderedRecordsEquals(List.of(List.of(
            newStreamsGroupTargetAssignmentRecord("my-group", "member-1", mkTasksTuple(taskRole,
                mkTasks(fooSubtopologyId, 1, 2),
                mkTasks(barSubtopologyId, 1, 2)
            )),
            newStreamsGroupTargetAssignmentRecord("my-group", "member-2", mkTasksTuple(taskRole,
                mkTasks(fooSubtopologyId, 3, 4),
                mkTasks(barSubtopologyId, 3, 4)
            )),
            newStreamsGroupTargetAssignmentRecord("my-group", "member-3", mkTasksTuple(taskRole,
                mkTasks(fooSubtopologyId, 5, 6),
                mkTasks(barSubtopologyId, 5, 6)
            ))
        )), result.records().subList(0, 3));

        assertEquals(newStreamsGroupTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(3));

        Map<String, TasksTuple> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 1, 2),
            mkTasks(barSubtopologyId, 1, 2)
        ));
        expectedAssignment.put("member-2", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 3, 4),
            mkTasks(barSubtopologyId, 3, 4)
        ));
        expectedAssignment.put("member-3", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 5, 6),
            mkTasks(barSubtopologyId, 5, 6)
        ));

        assertEquals(expectedAssignment, result.targetAssignment());
    }


    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUpdateMember(TaskRole taskRole) {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
        );

        String fooSubtopologyId = context.addSubtopologyWithSingleSourceTopic("foo", 6);
        String barSubtopologyId = context.addSubtopologyWithSingleSourceTopic("bar", 6);

        context.addGroupMember("member-1", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 1, 2, 3),
            mkTasks(barSubtopologyId, 1, 2)
        ));

        context.addGroupMember("member-2", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 4, 5, 6),
            mkTasks(barSubtopologyId, 3, 4)
        ));

        context.addGroupMember("member-3", mkTasksTuple(taskRole,
            mkTasks(barSubtopologyId, 5, 6)
        ));

        context.updateMemberMetadata(
            "member-3",
            Optional.of("instance-id-3"),
            Optional.of("rack-0")
        );

        context.prepareMemberAssignment("member-1", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 1, 2),
            mkTasks(barSubtopologyId, 1, 2)
        ));

        context.prepareMemberAssignment("member-2", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 3, 4),
            mkTasks(barSubtopologyId, 3, 4)
        ));

        context.prepareMemberAssignment("member-3", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 5, 6),
            mkTasks(barSubtopologyId, 5, 6)
        ));

        org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(4, result.records().size());

        assertUnorderedRecordsEquals(List.of(List.of(
            newStreamsGroupTargetAssignmentRecord("my-group", "member-1", mkTasksTuple(taskRole,
                mkTasks(fooSubtopologyId, 1, 2),
                mkTasks(barSubtopologyId, 1, 2)
            )),
            newStreamsGroupTargetAssignmentRecord("my-group", "member-2", mkTasksTuple(taskRole,
                mkTasks(fooSubtopologyId, 3, 4),
                mkTasks(barSubtopologyId, 3, 4)
            )),
            newStreamsGroupTargetAssignmentRecord("my-group", "member-3", mkTasksTuple(taskRole,
                mkTasks(fooSubtopologyId, 5, 6),
                mkTasks(barSubtopologyId, 5, 6)
            ))
        )), result.records().subList(0, 3));

        assertEquals(newStreamsGroupTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(3));

        Map<String, TasksTuple> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 1, 2),
            mkTasks(barSubtopologyId, 1, 2)
        ));
        expectedAssignment.put("member-2", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 3, 4),
            mkTasks(barSubtopologyId, 3, 4)
        ));
        expectedAssignment.put("member-3", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 5, 6),
            mkTasks(barSubtopologyId, 5, 6)
        ));

        assertEquals(expectedAssignment, result.targetAssignment());
    }


    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testPartialAssignmentUpdate(TaskRole taskRole) {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
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

        assertEquals(newStreamsGroupTargetAssignmentEpochRecord(
            "my-group",
            20
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


    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testDeleteMember(TaskRole taskRole) {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
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

        context.removeMember("member-3");

        context.prepareMemberAssignment("member-1", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 1, 2, 3),
            mkTasks(barSubtopologyId, 1, 2, 3)
        ));

        context.prepareMemberAssignment("member-2", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 4, 5, 6),
            mkTasks(barSubtopologyId, 4, 5, 6)
        ));

        org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(3, result.records().size());

        assertUnorderedRecordsEquals(List.of(List.of(
            newStreamsGroupTargetAssignmentRecord("my-group", "member-1", mkTasksTuple(taskRole,
                mkTasks(fooSubtopologyId, 1, 2, 3),
                mkTasks(barSubtopologyId, 1, 2, 3)
            )),
            newStreamsGroupTargetAssignmentRecord("my-group", "member-2", mkTasksTuple(taskRole,
                mkTasks(fooSubtopologyId, 4, 5, 6),
                mkTasks(barSubtopologyId, 4, 5, 6)
            ))
        )), result.records().subList(0, 2));

        assertEquals(newStreamsGroupTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(2));

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
    @EnumSource(TaskRole.class)
    public void testReplaceStaticMember(TaskRole taskRole) {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
        );

        String fooSubtopologyId = context.addSubtopologyWithSingleSourceTopic("foo", 6);
        String barSubtopologyId = context.addSubtopologyWithSingleSourceTopic("bar", 6);

        context.addGroupMember("member-1", "instance-member-1", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 1, 2),
            mkTasks(barSubtopologyId, 1, 2)
        ));

        context.addGroupMember("member-2", "instance-member-2", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 3, 4),
            mkTasks(barSubtopologyId, 3, 4)
        ));

        context.addGroupMember("member-3", "instance-member-3", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 5, 6),
            mkTasks(barSubtopologyId, 5, 6)
        ));

        // Static member 3 leaves
        context.removeMember("member-3");

        // Another static member joins with the same instance id as the departed one
        context.updateMemberMetadata("member-3-a", Optional.of("instance-member-3"),
            Optional.empty());

        context.prepareMemberAssignment("member-1", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 1, 2),
            mkTasks(barSubtopologyId, 1, 2)
        ));

        context.prepareMemberAssignment("member-2", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 3, 4),
            mkTasks(barSubtopologyId, 3, 4)
        ));

        context.prepareMemberAssignment("member-3-a", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 5, 6),
            mkTasks(barSubtopologyId, 5, 6)
        ));

        TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(2, result.records().size());

        assertUnorderedRecordsEquals(List.of(List.of(
            newStreamsGroupTargetAssignmentRecord("my-group", "member-3-a", mkTasksTuple(taskRole,
                mkTasks(fooSubtopologyId, 5, 6),
                mkTasks(barSubtopologyId, 5, 6)
            ))
        )), result.records().subList(0, 1));

        assertEquals(newStreamsGroupTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(1));

        Map<String, TasksTuple> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 1, 2),
            mkTasks(barSubtopologyId, 1, 2)
        ));
        expectedAssignment.put("member-2", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 3, 4),
            mkTasks(barSubtopologyId, 3, 4)
        ));

        expectedAssignment.put("member-3-a", mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 5, 6),
            mkTasks(barSubtopologyId, 5, 6)
        ));

        assertEquals(expectedAssignment, result.targetAssignment());
    }

    public static class TargetAssignmentBuilderTestContext {

        private final String groupId;
        private final int groupEpoch;
        private final TaskAssignor assignor = mock(TaskAssignor.class);
        private final Map<String, StreamsGroupTopologyValue.Subtopology> subtopologies = new HashMap<>();
        private final Map<String, StreamsGroupMember> members = new HashMap<>();
        private final Map<String, StreamsGroupMember> updatedMembers = new HashMap<>();
        private final Map<String, TasksTuple> targetAssignment = new HashMap<>();
        private final Map<String, MemberAssignment> memberAssignments = new HashMap<>();
        private final Map<String, String> staticMembers = new HashMap<>();
        private MetadataImageBuilder topicsImageBuilder = new MetadataImageBuilder();

        public TargetAssignmentBuilderTestContext(
            String groupId,
            int groupEpoch
        ) {
            this.groupId = groupId;
            this.groupEpoch = groupEpoch;
        }

        public void addGroupMember(
            String memberId,
            TasksTuple targetTasks
        ) {
            addGroupMember(memberId, null, targetTasks);
        }

        private void addGroupMember(
            String memberId,
            String instanceId,
            TasksTuple targetTasks
        ) {
            StreamsGroupMember.Builder memberBuilder = new StreamsGroupMember.Builder(memberId);
            memberBuilder.setProcessId("processId");
            memberBuilder.setClientTags(Map.of());
            memberBuilder.setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090));

            if (instanceId != null) {
                memberBuilder.setInstanceId(instanceId);
                staticMembers.put(instanceId, memberId);
            } else {
                memberBuilder.setInstanceId(null);
            }
            memberBuilder.setRackId(null);
            members.put(memberId, memberBuilder.build());
            targetAssignment.put(memberId, targetTasks);
        }

        public String addSubtopologyWithSingleSourceTopic(
            String topicName,
            int numPartitions
        ) {
            String subtopologyId = Uuid.randomUuid().toString();
            Uuid topicId = Uuid.randomUuid();
            topicsImageBuilder = topicsImageBuilder.addTopic(topicId, topicName, numPartitions);

            StreamsGroupTopologyValue.Subtopology subtopology = new StreamsGroupTopologyValue.Subtopology()
                .setSubtopologyId(subtopologyId)
                .setSourceTopics(List.of(topicName));
            subtopologies.put(subtopologyId, subtopology);

            return subtopologyId;
        }

        public void updateMemberMetadata(
            String memberId
        ) {
            updateMemberMetadata(
                memberId,
                Optional.empty(),
                Optional.empty()
            );
        }

        public void updateMemberMetadata(
            String memberId,
            Optional<String> instanceId,
            Optional<String> rackId
        ) {
            StreamsGroupMember existingMember = members.get(memberId);
            StreamsGroupMember.Builder builder;
            if (existingMember != null) {
                builder = new StreamsGroupMember.Builder(existingMember);
            } else {
                builder = new StreamsGroupMember.Builder(memberId);
                builder.setProcessId("processId");
                builder.setRackId(null);
                builder.setInstanceId(null);
                builder.setClientTags(Map.of());
                builder.setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090));
            }
            updatedMembers.put(memberId, builder
                .maybeUpdateInstanceId(instanceId)
                .maybeUpdateRackId(rackId)
                .build());
        }

        public void removeMember(
            String memberId
        ) {
            this.updatedMembers.put(memberId, null);
        }

        public void prepareMemberAssignment(
            String memberId,
            TasksTuple assignment
        ) {
            memberAssignments.put(memberId, new MemberAssignment(assignment.activeTasks(), assignment.standbyTasks(), assignment.warmupTasks()));
        }

        public org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.TargetAssignmentResult build() {
            // Prepare expected member specs.
            Map<String, AssignmentMemberSpec> memberSpecs = new HashMap<>();

            // All the existing members are prepared.
            members.forEach((memberId, member) ->
                memberSpecs.put(memberId, createAssignmentMemberSpec(
                        member,
                        targetAssignment.getOrDefault(memberId, TasksTuple.EMPTY)
                    )
                ));

            // All the updated are added and all the deleted
            // members are removed.
            updatedMembers.forEach((memberId, updatedMemberOrNull) -> {
                if (updatedMemberOrNull == null) {
                    memberSpecs.remove(memberId);
                } else {
                    TasksTuple assignment = targetAssignment.getOrDefault(memberId,
                        TasksTuple.EMPTY);

                    // A new static member joins and needs to replace an existing departed one.
                    if (updatedMemberOrNull.instanceId().isPresent()) {
                        String previousMemberId = staticMembers.get(updatedMemberOrNull.instanceId().get());
                        if (previousMemberId != null && !previousMemberId.equals(memberId)) {
                            assignment = targetAssignment.getOrDefault(previousMemberId,
                                TasksTuple.EMPTY);
                        }
                    }

                    memberSpecs.put(memberId, createAssignmentMemberSpec(
                        updatedMemberOrNull,
                        assignment
                    ));
                }
            });

            CoordinatorMetadataImage metadataImage = new KRaftCoordinatorMetadataImage(topicsImageBuilder.build());

            // Create the StreamsTopology
            StreamsTopology topology = new StreamsTopology(1, subtopologies);

            // Compute max partitions per subtopology (simulating InternalTopicManager)
            Map<String, Integer> numTasksBySubtopology = new HashMap<>();
            for (Map.Entry<String, StreamsGroupTopologyValue.Subtopology> entry : subtopologies.entrySet()) {
                String subtopologyId = entry.getKey();
                StreamsGroupTopologyValue.Subtopology subtopology = entry.getValue();
                int maxPartitions = subtopology.sourceTopics().stream()
                    .mapToInt(topic -> metadataImage.topicMetadata(topic)
                        .orElseThrow(() -> new IllegalStateException("Topic " + topic + " not found"))
                        .partitionCount())
                    .max()
                    .orElse(0);
                numTasksBySubtopology.put(subtopologyId, maxPartitions);
            }

            // Prepare the expected topology metadata.
            TopologyMetadata topologyMetadata = new TopologyMetadata(topology, numTasksBySubtopology);

            // Create the TopologyValidationResult
            TopologyValidationResult topologyValidationResult = new TopologyValidationResult(
                1,
                0L,
                Map.of(),
                Optional.empty(),
                Optional.of(numTasksBySubtopology),
                Map.of()
            );

            // Prepare the expected assignment spec.
            GroupSpecImpl groupSpec = new GroupSpecImpl(memberSpecs, new HashMap<>());

            // We use `any` here to always return an assignment but use `verify` later on
            // to ensure that the input was correct.
            when(assignor.assign(any(), any()))
                .thenReturn(new GroupAssignment(memberAssignments));

            // Create and populate the assignment builder.
            org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder builder = new org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder(
                groupId, groupEpoch, assignor, Map.of())
                .withMembers(members)
                .withTopology(topology)
                .withTopologyValidationResult(topologyValidationResult)
                .withStaticMembers(staticMembers)
                .withTargetAssignment(targetAssignment);

            // Add the updated members or delete the deleted members.
            updatedMembers.forEach((memberId, updatedMemberOrNull) -> {
                if (updatedMemberOrNull != null) {
                    builder.addOrUpdateMember(memberId, updatedMemberOrNull);
                } else {
                    builder.removeMember(memberId);
                }
            });

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
