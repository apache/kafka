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
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.streams.assignor.AssignmentConfigsImpl;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpecImpl;
import org.apache.kafka.coordinator.group.streams.assignor.MemberMetadataAndStateImpl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.coordinator.group.streams.GroupSpecBuilder.createMemberMetadataAndState;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasks;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksTupleWithCommonEpoch;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class GroupSpecBuilderTest {

    @ParameterizedTest
    // Active, standby and warm-up tasks all come from the member's current assignment, so this
    // test varies which role the member's current tasks are in.
    @EnumSource(value = TaskRole.class, names = {"ACTIVE", "STANDBY"})
    public void testCreateAssignmentMemberSpec(TaskRole taskRole) {
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

        Map<String, Map<Integer, Long>> taskOffsets = Map.of(fooSubtopologyId, Map.of(0, 10L));
        Map<String, Map<Integer, Long>> taskEndOffsets = Map.of(fooSubtopologyId, Map.of(0, 20L));

        MemberMetadataAndStateImpl memberMetadata = createMemberMetadataAndState(
            member,
            new MemberTaskOffsets(taskOffsets, taskEndOffsets)
        );

        assertEquals(new MemberMetadataAndStateImpl(
            Optional.of("instanceId"),
            Optional.of("rackId"),
            "processId",
            clientTags,
            activeTasks,
            standbyTasks,
            Map.of(),
            taskOffsets,
            taskEndOffsets
        ), memberMetadata);
    }

    @Test
    public void testEmpty() {
        GroupSpecBuilder builder = new GroupSpecBuilder(Map.of())
            .withMembers(Map.of());

        assertEquals(
            new GroupSpecImpl(Map.of(), AssignmentConfigsImpl.DEFAULT),
            builder.build()
        );
    }

    @ParameterizedTest
    @EnumSource(value = TaskRole.class, names = {"ACTIVE", "STANDBY"})
    public void testGroupSpec(TaskRole taskRole) {
        String fooSubtopologyId = Uuid.randomUuid().toString();
        String barSubtopologyId = Uuid.randomUuid().toString();

        TasksTupleWithEpochs assignment1 = mkTasksTupleWithCommonEpoch(taskRole, 0,
            mkTasks(fooSubtopologyId, 1, 2),
            mkTasks(barSubtopologyId, 1, 2)
        );
        TasksTupleWithEpochs assignment2 = mkTasksTupleWithCommonEpoch(taskRole, 0,
            mkTasks(fooSubtopologyId, 3, 4),
            mkTasks(barSubtopologyId, 3, 4)
        );
        TasksTupleWithEpochs assignment3 = mkTasksTupleWithCommonEpoch(taskRole, 0,
            mkTasks(fooSubtopologyId, 5, 6),
            mkTasks(barSubtopologyId, 5, 6)
        );

        MemberTaskOffsets memberTaskOffsets1 = new MemberTaskOffsets(
            Map.of(fooSubtopologyId, Map.of(0, 10L)),
            Map.of(fooSubtopologyId, Map.of(0, 20L))
        );
        MemberTaskOffsets memberTaskOffsets2 = new MemberTaskOffsets(
            Map.of(fooSubtopologyId, Map.of(0, 30L)),
            Map.of(fooSubtopologyId, Map.of(0, 40L))
        );
        MemberTaskOffsets memberTaskOffsets3 = new MemberTaskOffsets(
            Map.of(fooSubtopologyId, Map.of(0, 50L)),
            Map.of(fooSubtopologyId, Map.of(0, 60L))
        );

        GroupSpecBuilder builder = new GroupSpecBuilder(Map.of(AssignmentConfigsImpl.NUM_STANDBY_REPLICAS_CONFIG, "1"))
            .withMembers(Map.of(
                "member-1", new StreamsGroupMember.Builder("member-1")
                    .setProcessId("processId")
                    .setClientTags(Map.of("tag1", "value1"))
                    .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090))
                    .setInstanceId(null)
                    .setRackId(null)
                    .setAssignedTasks(assignment1)
                    .build(),
                "member-2", new StreamsGroupMember.Builder("member-2")
                    .setProcessId("processId")
                    .setClientTags(Map.of())
                    .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090))
                    .setInstanceId(null)
                    .setRackId("rack-2")
                    .setAssignedTasks(assignment2)
                    .build(),
                "member-3", new StreamsGroupMember.Builder("member-3")
                    .setProcessId("processId")
                    .setClientTags(Map.of())
                    .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090))
                    .setInstanceId("instance-3")
                    .setRackId(null)
                    .setAssignedTasks(assignment3)
                    .build()
            ))
            .withTaskOffsets(Map.of(
                "member-1", memberTaskOffsets1,
                "member-2", memberTaskOffsets2,
                "member-3", memberTaskOffsets3
            ));

        assertEquals(
            new GroupSpecImpl(
                Map.of(
                    "member-1", new MemberMetadataAndStateImpl(
                        Optional.empty(),
                        Optional.empty(),
                        "processId",
                        Map.of("tag1", "value1"),
                        assignment1.activeTasks(),
                        assignment1.standbyTasks(),
                        assignment1.warmupTasks(),
                        memberTaskOffsets1.taskOffsets(),
                        memberTaskOffsets1.taskEndOffsets()
                    ),
                    "member-2", new MemberMetadataAndStateImpl(
                        Optional.empty(),
                        Optional.of("rack-2"),
                        "processId",
                        Map.of(),
                        assignment2.activeTasks(),
                        assignment2.standbyTasks(),
                        assignment2.warmupTasks(),
                        memberTaskOffsets2.taskOffsets(),
                        memberTaskOffsets2.taskEndOffsets()
                    ),
                    "member-3", new MemberMetadataAndStateImpl(
                        Optional.of("instance-3"),
                        Optional.empty(),
                        "processId",
                        Map.of(),
                        assignment3.activeTasks(),
                        assignment3.standbyTasks(),
                        assignment3.warmupTasks(),
                        memberTaskOffsets3.taskOffsets(),
                        memberTaskOffsets3.taskEndOffsets()
                    )
                ),
                AssignmentConfigsImpl.DEFAULT
                    .withNumStandbyReplicas(1)
            ),
            builder.build()
        );
    }
}
