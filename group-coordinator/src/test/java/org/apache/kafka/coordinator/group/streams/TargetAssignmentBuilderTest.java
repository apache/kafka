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
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.TargetAssignmentMetadata;
import org.apache.kafka.coordinator.group.api.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.streams.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.TaskAssignor;
import org.apache.kafka.coordinator.group.streams.assignor.AssignmentConfigsImpl;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpecImpl;
import org.apache.kafka.coordinator.group.streams.assignor.MemberMetadataAndStateImpl;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredTopology;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

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
        TaskAssignor assignor = mock(TaskAssignor.class);
        ConfiguredTopology topology = mock(ConfiguredTopology.class);

        when(topology.isReady()).thenReturn(false);

        TargetAssignmentBuilder builder = new TargetAssignmentBuilder(1, assignor)
            .withTime(new MockTime(0, 12345L, 12345L))
            .withTopology(topology)
            .withGroupSpec(new GroupSpecImpl(Map.of(), AssignmentConfigsImpl.DEFAULT));

        TargetAssignmentBuilder.TargetAssignmentResult result = builder.build();

        assertEquals(Map.of(), result.targetAssignment());
        assertEquals(new TargetAssignmentMetadata(1, 12345L), result.targetAssignmentMetadata());
    }

    @ParameterizedTest
    // Warm-up tasks are not produced by the assignor (only active and standby), so they cannot appear
    // in the resulting target assignment. See MemberAssignment.
    @EnumSource(value = TaskRole.class, names = {"ACTIVE", "STANDBY"})
    public void testAssignment(TaskRole taskRole) {
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, "foo", 6)
            .addTopic(barTopicId, "bar", 6)
            .buildCoordinatorMetadataImage();

        String fooSubtopologyId = Uuid.randomUuid().toString();
        String barSubtopologyId = Uuid.randomUuid().toString();

        SortedMap<String, ConfiguredSubtopology> subtopologies = new TreeMap<>(Map.of(
            fooSubtopologyId, new ConfiguredSubtopology(6, Set.of(fooTopicId.toString()), Map.of(), Set.of(), Map.of()),
            barSubtopologyId, new ConfiguredSubtopology(6, Set.of(barTopicId.toString()), Map.of(), Set.of(), Map.of())
        ));
        ConfiguredTopology topology = new ConfiguredTopology(0, 0, Optional.of(subtopologies), new HashMap<>(),
            Optional.empty());

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

        GroupSpec groupSpec = new GroupSpecImpl(
            Map.of(
                "member-1", new MemberMetadataAndStateImpl(
                    Optional.empty(),
                    Optional.empty(),
                    "processId",
                    Map.of("tag1", "value1"),
                    assignment1.activeTasks(),
                    assignment1.standbyTasks(),
                    assignment1.warmupTasks(),
                    Map.of(),
                    Map.of()
                ),
                "member-2", new MemberMetadataAndStateImpl(
                    Optional.empty(),
                    Optional.of("rack-2"),
                    "processId",
                    Map.of(),
                    assignment2.activeTasks(),
                    assignment2.standbyTasks(),
                    assignment2.warmupTasks(),
                    Map.of(),
                    Map.of()
                ),
                "member-3", new MemberMetadataAndStateImpl(
                    Optional.of("instance-3"),
                    Optional.empty(),
                    "processId",
                    Map.of(),
                    assignment3.activeTasks(),
                    assignment3.standbyTasks(),
                    assignment3.warmupTasks(),
                    Map.of(),
                    Map.of()
                )
            ),
            AssignmentConfigsImpl.DEFAULT
        );

        TasksTuple newAssignment1 = mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 1, 2),
            mkTasks(barSubtopologyId, 1, 2)
        );
        TasksTuple newAssignment2 = mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 3, 4, 5),
            mkTasks(barSubtopologyId, 3, 4, 5)
        );
        TasksTuple newAssignment3 = mkTasksTuple(taskRole,
            mkTasks(fooSubtopologyId, 6),
            mkTasks(barSubtopologyId, 6)
        );

        GroupAssignment groupAssignment = new GroupAssignment(Map.of(
            "member-1", new MemberAssignment(newAssignment1.activeTasks(), newAssignment1.standbyTasks()),
            "member-2", new MemberAssignment(newAssignment2.activeTasks(), newAssignment2.standbyTasks()),
            "member-3", new MemberAssignment(newAssignment3.activeTasks(), newAssignment3.standbyTasks())
        ));

        // Prepare the expected topology metadata.
        TopologyMetadata topologyMetadata = new TopologyMetadata(metadataImage, subtopologies);

        // We use `any` here to always return an assignment but use `verify` later on
        // to ensure that the input was correct.
        TaskAssignor assignor = mock(TaskAssignor.class);
        when(assignor.assign(any(), any()))
            .thenReturn(groupAssignment);

        // Create and populate the assignment builder.
        TargetAssignmentBuilder builder = new TargetAssignmentBuilder(20, assignor)
            .withTime(new MockTime(0, 12345L, 12345L))
            .withTopology(topology)
            .withMetadataImage(metadataImage)
            .withGroupSpec(groupSpec);

        // Execute the builder.
        TargetAssignmentBuilder.TargetAssignmentResult result = builder.build();

        // Verify that the assignor was called once with the expected
        // assignment spec.
        verify(assignor, times(1))
            .assign(groupSpec, topologyMetadata);

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
        assertEquals(new TargetAssignmentMetadata(20, 12345L), result.targetAssignmentMetadata());
    }
}
