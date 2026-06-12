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
import org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.TaskRole;
import org.apache.kafka.coordinator.group.streams.assignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpecImpl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.coordinator.group.streams.GroupSpecBuilder.createAssignmentMemberSpec;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasks;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksTuple;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class GroupSpecBuilderTest {

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
        GroupSpecBuilder builder = new GroupSpecBuilder(Map.of())
            .withMembers(Map.of())
            .withTargetAssignment(Map.of());

        assertEquals(
            new GroupSpecImpl(Map.of(), Map.of()),
            builder.build()
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testGroupSpec(TaskRole taskRole) {
        String fooSubtopologyId = Uuid.randomUuid().toString();
        String barSubtopologyId = Uuid.randomUuid().toString();

        TasksTuple assignment1 = mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 1, 2),
            mkTasks(barSubtopologyId, 1, 2)
        );
        TasksTuple assignment2 = mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 3, 4),
            mkTasks(barSubtopologyId, 3, 4)
        );
        TasksTuple assignment3 = mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 5, 6),
            mkTasks(barSubtopologyId, 5, 6)
        );

        GroupSpecBuilder builder = new GroupSpecBuilder(Map.of())
            .withMembers(Map.of(
                "member-1", new StreamsGroupMember.Builder("member-1")
                    .setProcessId("processId")
                    .setClientTags(Map.of("tag1", "value1"))
                    .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090))
                    .setInstanceId(null)
                    .setRackId(null)
                    .build(),
                "member-2", new StreamsGroupMember.Builder("member-2")
                    .setProcessId("processId")
                    .setClientTags(Map.of())
                    .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090))
                    .setInstanceId(null)
                    .setRackId("rack-2")
                    .build(),
                "member-3", new StreamsGroupMember.Builder("member-3")
                    .setProcessId("processId")
                    .setClientTags(Map.of())
                    .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090))
                    .setInstanceId("instance-3")
                    .setRackId(null)
                    .build()
            ))
            .withTargetAssignment(Map.of(
                "member-1", assignment1,
                "member-2", assignment2,
                "member-3", assignment3
            ));

        assertEquals(
            new GroupSpecImpl(
                Map.of(
                    "member-1", new AssignmentMemberSpec(
                        Optional.empty(),
                        Optional.empty(),
                        assignment1.activeTasks(),
                        assignment1.standbyTasks(),
                        assignment1.warmupTasks(),
                        "processId",
                        Map.of("tag1", "value1"),
                        Map.of(),
                        Map.of()
                    ),
                    "member-2", new AssignmentMemberSpec(
                        Optional.empty(),
                        Optional.of("rack-2"),
                        assignment2.activeTasks(),
                        assignment2.standbyTasks(),
                        assignment2.warmupTasks(),
                        "processId",
                        Map.of(),
                        Map.of(),
                        Map.of()
                    ),
                    "member-3", new AssignmentMemberSpec(
                        Optional.of("instance-3"),
                        Optional.empty(),
                        assignment3.activeTasks(),
                        assignment3.standbyTasks(),
                        assignment3.warmupTasks(),
                        "processId",
                        Map.of(),
                        Map.of(),
                        Map.of()
                    )
                ),
                Map.of()
            ),
            builder.build()
        );
    }

    @Test
    public void testAssignorOffload() {
        String fooSubtopologyId = Uuid.randomUuid().toString();

        Map<String, String> assignmentConfigs = new HashMap<>(Map.of(
            "num.standby.replicas", "1"
        ));

        StreamsGroupMember member = new StreamsGroupMember.Builder("member-1")
            .setProcessId("processId")
            .setClientTags(Map.of())
            .setInstanceId(null)
            .setRackId(null)
            .build();
        Map<String, StreamsGroupMember> members = new HashMap<>(Map.of(
            "member-1", member
        ));

        Map<String, TasksTuple> targetAssignment = new HashMap<>(Map.of(
            "member-1", mkTasksTuple(TaskRole.ACTIVE,
                mkTasks(fooSubtopologyId, 1, 2)
            )
        ));

        GroupSpec groupSpec = new GroupSpecBuilder(assignmentConfigs)
            .withMembers(members)
            .withTargetAssignment(targetAssignment)
            .withAssignorOffload(true)
            .build();

        // Modifications after the GroupSpec has been built should not be visible in the GroupSpec.
        assignmentConfigs.clear();
        members.clear();
        targetAssignment.clear();

        assertEquals(
            new GroupSpecImpl(
                // members and targetAssignment
                Map.of("member-1", createAssignmentMemberSpec(member, mkTasksTuple(TaskRole.ACTIVE,
                    mkTasks(fooSubtopologyId, 1, 2)
                ))),
                // assignmentConfigs
                Map.of("num.standby.replicas", "1")
            ),
            groupSpec
        );
    }
}
