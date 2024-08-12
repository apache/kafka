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
import org.apache.kafka.coordinator.group.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.taskassignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.taskassignor.GroupAssignment;
import org.apache.kafka.coordinator.group.taskassignor.GroupSpecImpl;
import org.apache.kafka.coordinator.group.taskassignor.MemberAssignment;
import org.apache.kafka.coordinator.group.taskassignor.TaskAssignor;
import org.apache.kafka.image.TopicsImage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.coordinator.group.Assertions.assertUnorderedListEquals;
import static org.apache.kafka.coordinator.group.CoordinatorRecordHelpersTest.mkMapOfPartitionRacks;
import static org.apache.kafka.coordinator.group.streams.CoordinatorStreamsRecordHelpers.newStreamsTargetAssignmentEpochRecord;
import static org.apache.kafka.coordinator.group.streams.CoordinatorStreamsRecordHelpers.newStreamsTargetAssignmentRecord;
import static org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.createAssignmentMemberSpec;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTaskAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TargetAssignmentBuilderTest {

    public static class TargetAssignmentBuilderTestContext {

        private final String groupId;
        private final int groupEpoch;
        private final TaskAssignor assignor = mock(TaskAssignor.class);
        private final StreamsTopology topology = new StreamsTopology("", new HashMap<>());
        private final Map<String, StreamsGroupMember> members = new HashMap<>();
        private final Map<String, org.apache.kafka.coordinator.group.streams.TopicMetadata> subscriptionMetadata = new HashMap<>();
        private final Map<String, StreamsGroupMember> updatedMembers = new HashMap<>();
        private final Map<String, org.apache.kafka.coordinator.group.streams.Assignment> targetAssignment = new HashMap<>();
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
            Map<String, Set<Integer>> targetTasks
        ) {
            addGroupMember(memberId, null, targetTasks);
        }

        private void addGroupMember(
            String memberId,
            String instanceId,
            Map<String, Set<Integer>> targetTasks
        ) {
            StreamsGroupMember.Builder memberBuilder = new StreamsGroupMember.Builder(memberId);
            memberBuilder.setProcessId("processId");
            memberBuilder.setClientTags(Collections.emptyMap());
            memberBuilder.setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090));

            if (instanceId != null) {
                memberBuilder.setInstanceId(instanceId);
                staticMembers.put(instanceId, memberId);
            }
            members.put(memberId, memberBuilder.build());
            targetAssignment.put(memberId, new org.apache.kafka.coordinator.group.streams.Assignment(targetTasks));
        }

        public String addSubtopologyWithSingleSourceTopic(
            String topicName,
            int numTasks,
            Map<Integer, Set<String>> partitionRacks
        ) {
            String subtopologyId = Uuid.randomUuid().toString();
            Uuid topicId = Uuid.randomUuid();
            subscriptionMetadata.put(topicName, new org.apache.kafka.coordinator.group.streams.TopicMetadata(
                topicId,
                topicName,
                numTasks,
                partitionRacks
            ));
            topicsImageBuilder = topicsImageBuilder.addTopic(topicId, topicName, numTasks);
            topology.subtopologies().put(subtopologyId, new StreamsGroupTopologyValue.Subtopology()
                .setSubtopology(subtopologyId)
                .setSourceTopics(Collections.singletonList(topicId.toString())));

            return subtopologyId;
        }

        public void updateMemberSubscription(
            String memberId
        ) {
            updateMemberSubscription(
                memberId,
                Optional.empty(),
                Optional.empty()
            );
        }

        public void updateMemberSubscription(
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
                builder.setClientTags(Collections.emptyMap());
                builder.setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090));
            }
            updatedMembers.put(memberId, builder
                .maybeUpdateInstanceId(instanceId)
                .maybeUpdateRackId(rackId)
                .build());
        }

        public void removeMemberSubscription(
            String memberId
        ) {
            this.updatedMembers.put(memberId, null);
        }

        public void prepareMemberAssignment(
            String memberId,
            Map<String, Set<Integer>> activeTasks
        ) {
            memberAssignments.put(memberId, new MemberAssignment(activeTasks, Collections.emptyMap(), Collections.emptyMap()));
        }

        public org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.TargetAssignmentResult build() {
            TopicsImage topicsImage = topicsImageBuilder.build().topics();
            // Prepare expected member specs.
            Map<String, AssignmentMemberSpec> memberSpecs = new HashMap<>();

            // All the existing members are prepared.
            members.forEach((memberId, member) ->
                memberSpecs.put(memberId, createAssignmentMemberSpec(
                        member,
                        targetAssignment.getOrDefault(memberId, org.apache.kafka.coordinator.group.streams.Assignment.EMPTY)
                    )
                ));

            // All the updated are added and all the deleted
            // members are removed.
            updatedMembers.forEach((memberId, updatedMemberOrNull) -> {
                if (updatedMemberOrNull == null) {
                    memberSpecs.remove(memberId);
                } else {
                    org.apache.kafka.coordinator.group.streams.Assignment assignment = targetAssignment.getOrDefault(memberId,
                        org.apache.kafka.coordinator.group.streams.Assignment.EMPTY);

                    // A new static member joins and needs to replace an existing departed one.
                    if (updatedMemberOrNull.instanceId() != null) {
                        String previousMemberId = staticMembers.get(updatedMemberOrNull.instanceId());
                        if (previousMemberId != null && !previousMemberId.equals(memberId)) {
                            assignment = targetAssignment.getOrDefault(previousMemberId,
                                org.apache.kafka.coordinator.group.streams.Assignment.EMPTY);
                        }
                    }

                    memberSpecs.put(memberId, createAssignmentMemberSpec(
                        updatedMemberOrNull,
                        assignment
                    ));
                }
            });

            // Prepare the expected subscription topic metadata.
            TopologyMetadata topologyMetadata = new TopologyMetadata(subscriptionMetadata, topology);

            // Prepare the member assignments per topic partition.
            Map<String, Map<Integer, String>> invertedTargetAssignment = TaskAssignmentTestUtil.invertedTargetAssignment(memberSpecs);

            // Prepare the expected assignment spec.
            GroupSpecImpl groupSpec = new GroupSpecImpl(memberSpecs, new ArrayList<>(topology.subtopologies().keySet()));

            // We use `any` here to always return an assignment but use `verify` later on
            // to ensure that the input was correct.
            when(assignor.assign(any(), any()))
                .thenReturn(new GroupAssignment(memberAssignments));

            // Create and populate the assignment builder.
            org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder builder = new org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder(
                groupId, groupEpoch, assignor)
                .withMembers(members)
                .withTopology(topology)
                .withStaticMembers(staticMembers)
                .withSubscriptionMetadata(subscriptionMetadata)
                .withTargetAssignment(targetAssignment)
                .withTopicsImage(topicsImage);

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

    @Test
    public void testCreateAssignmentMemberSpec() {
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();
        String fooSubtopologyId = Uuid.randomUuid().toString();
        String barSubtopologyId = Uuid.randomUuid().toString();
        TopicsImage topicsImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, "foo", 5)
            .addTopic(barTopicId, "bar", 5)
            .build()
            .topics();

        final Map<String, String> clientTags = mkMap(mkEntry("tag1", "value1"), mkEntry("tag2", "value2"));
        final Map<String, String> assignmentConfigs = mkMap(mkEntry("conf1", "value1"), mkEntry("conf2", "value2"));
        StreamsGroupMember member = new StreamsGroupMember.Builder("member-id")
            .setRackId("rackId")
            .setInstanceId("instanceId")
            .setProcessId("processId")
            .setAssignor("assignor")
            .setClientTags(clientTags)
            .build();

        org.apache.kafka.coordinator.group.streams.Assignment assignment = new org.apache.kafka.coordinator.group.streams.Assignment(
            mkAssignment(
                mkTaskAssignment(fooSubtopologyId, 1, 2, 3),
                mkTaskAssignment(barSubtopologyId, 1, 2, 3)
            ));

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
            Collections.emptyMap()
        ), assignmentMemberSpec);
    }

    @Test
    public void testEmpty() {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
        );

        org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.TargetAssignmentResult result = context.build();
        assertEquals(Collections.singletonList(newStreamsTargetAssignmentEpochRecord(
            "my-group",
            20
        )), result.records());
        assertEquals(Collections.emptyMap(), result.targetAssignment());
    }

    @Test
    public void testAssignmentHasNotChanged() {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
        );

        String fooSubtopologyId = context.addSubtopologyWithSingleSourceTopic("foo", 6, Collections.emptyMap());
        String barSubtopologyId = context.addSubtopologyWithSingleSourceTopic("bar", 6, Collections.emptyMap());

        context.addGroupMember("member-1", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 1, 2, 3),
            mkTaskAssignment(barSubtopologyId, 1, 2, 3)
        ));

        context.addGroupMember("member-2", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 4, 5, 6),
            mkTaskAssignment(barSubtopologyId, 4, 5, 6)
        ));

        context.prepareMemberAssignment("member-1", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 1, 2, 3),
            mkTaskAssignment(barSubtopologyId, 1, 2, 3)
        ));

        context.prepareMemberAssignment("member-2", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 4, 5, 6),
            mkTaskAssignment(barSubtopologyId, 4, 5, 6)
        ));

        org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(Collections.singletonList(newStreamsTargetAssignmentEpochRecord(
            "my-group",
            20
        )), result.records());

        Map<String, org.apache.kafka.coordinator.group.streams.Assignment> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 1, 2, 3),
            mkTaskAssignment(barSubtopologyId, 1, 2, 3)
        )));
        expectedAssignment.put("member-2", new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 4, 5, 6),
            mkTaskAssignment(barSubtopologyId, 4, 5, 6)
        )));

        assertEquals(expectedAssignment, result.targetAssignment());
    }

    @Test
    public void testAssignmentSwapped() {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
        );

        String fooSubtopologyId = context.addSubtopologyWithSingleSourceTopic("foo", 6, Collections.emptyMap());
        String barSubtopologyId = context.addSubtopologyWithSingleSourceTopic("bar", 6, Collections.emptyMap());

        context.addGroupMember("member-1", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 1, 2, 3),
            mkTaskAssignment(barSubtopologyId, 1, 2, 3)
        ));

        context.addGroupMember("member-2", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 4, 5, 6),
            mkTaskAssignment(barSubtopologyId, 4, 5, 6)
        ));

        context.prepareMemberAssignment("member-2", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 1, 2, 3),
            mkTaskAssignment(barSubtopologyId, 1, 2, 3)
        ));

        context.prepareMemberAssignment("member-1", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 4, 5, 6),
            mkTaskAssignment(barSubtopologyId, 4, 5, 6)
        ));

        org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(3, result.records().size());

        assertUnorderedListEquals(Arrays.asList(
            newStreamsTargetAssignmentRecord("my-group", "member-1", mkAssignment(
                mkTaskAssignment(fooSubtopologyId, 4, 5, 6),
                mkTaskAssignment(barSubtopologyId, 4, 5, 6)
            ), Collections.emptyMap(), Collections.emptyMap()),
            newStreamsTargetAssignmentRecord("my-group", "member-2", mkAssignment(
                mkTaskAssignment(fooSubtopologyId, 1, 2, 3),
                mkTaskAssignment(barSubtopologyId, 1, 2, 3)
            ), Collections.emptyMap(), Collections.emptyMap())
        ), result.records().subList(0, 2));

        assertEquals(newStreamsTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(2));

        Map<String, org.apache.kafka.coordinator.group.streams.Assignment> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-2", new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 1, 2, 3),
            mkTaskAssignment(barSubtopologyId, 1, 2, 3)
        )));
        expectedAssignment.put("member-1", new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 4, 5, 6),
            mkTaskAssignment(barSubtopologyId, 4, 5, 6)
        )));

        assertEquals(expectedAssignment, result.targetAssignment());
    }

    @Test
    public void testNewMember() {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
        );

        String fooSubtopologyId = context.addSubtopologyWithSingleSourceTopic("foo", 6, Collections.emptyMap());
        String barSubtopologyId = context.addSubtopologyWithSingleSourceTopic("bar", 6, Collections.emptyMap());

        context.addGroupMember("member-1", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 1, 2, 3),
            mkTaskAssignment(barSubtopologyId, 1, 2, 3)
        ));

        context.addGroupMember("member-2", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 4, 5, 6),
            mkTaskAssignment(barSubtopologyId, 4, 5, 6)
        ));

        context.updateMemberSubscription("member-3");

        context.prepareMemberAssignment("member-1", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 1, 2),
            mkTaskAssignment(barSubtopologyId, 1, 2)
        ));

        context.prepareMemberAssignment("member-2", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 3, 4),
            mkTaskAssignment(barSubtopologyId, 3, 4)
        ));

        context.prepareMemberAssignment("member-3", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 5, 6),
            mkTaskAssignment(barSubtopologyId, 5, 6)
        ));

        org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(4, result.records().size());

        assertUnorderedListEquals(Arrays.asList(
            newStreamsTargetAssignmentRecord("my-group", "member-1", mkAssignment(
                mkTaskAssignment(fooSubtopologyId, 1, 2),
                mkTaskAssignment(barSubtopologyId, 1, 2)
            ), Collections.emptyMap(), Collections.emptyMap()),
            newStreamsTargetAssignmentRecord("my-group", "member-2", mkAssignment(
                mkTaskAssignment(fooSubtopologyId, 3, 4),
                mkTaskAssignment(barSubtopologyId, 3, 4)
            ), Collections.emptyMap(), Collections.emptyMap()),
            newStreamsTargetAssignmentRecord("my-group", "member-3", mkAssignment(
                mkTaskAssignment(fooSubtopologyId, 5, 6),
                mkTaskAssignment(barSubtopologyId, 5, 6)
            ), Collections.emptyMap(), Collections.emptyMap())
        ), result.records().subList(0, 3));

        assertEquals(newStreamsTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(3));

        Map<String, org.apache.kafka.coordinator.group.streams.Assignment> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 1, 2),
            mkTaskAssignment(barSubtopologyId, 1, 2)
        )));
        expectedAssignment.put("member-2", new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 3, 4),
            mkTaskAssignment(barSubtopologyId, 3, 4)
        )));
        expectedAssignment.put("member-3", new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 5, 6),
            mkTaskAssignment(barSubtopologyId, 5, 6)
        )));

        assertEquals(expectedAssignment, result.targetAssignment());
    }

    @Test
    public void testUpdateMember() {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
        );

        String fooSubtopologyId = context.addSubtopologyWithSingleSourceTopic("foo", 6, Collections.emptyMap());
        String barSubtopologyId = context.addSubtopologyWithSingleSourceTopic("bar", 6, Collections.emptyMap());

        context.addGroupMember("member-1", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 1, 2, 3),
            mkTaskAssignment(barSubtopologyId, 1, 2)
        ));

        context.addGroupMember("member-2", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 4, 5, 6),
            mkTaskAssignment(barSubtopologyId, 3, 4)
        ));

        context.addGroupMember("member-3", mkAssignment(
            mkTaskAssignment(barSubtopologyId, 5, 6)
        ));

        context.updateMemberSubscription(
            "member-3",
            Optional.of("instance-id-3"),
            Optional.of("rack-0")
        );

        context.prepareMemberAssignment("member-1", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 1, 2),
            mkTaskAssignment(barSubtopologyId, 1, 2)
        ));

        context.prepareMemberAssignment("member-2", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 3, 4),
            mkTaskAssignment(barSubtopologyId, 3, 4)
        ));

        context.prepareMemberAssignment("member-3", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 5, 6),
            mkTaskAssignment(barSubtopologyId, 5, 6)
        ));

        org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(4, result.records().size());

        assertUnorderedListEquals(Arrays.asList(
            newStreamsTargetAssignmentRecord("my-group", "member-1", mkAssignment(
                mkTaskAssignment(fooSubtopologyId, 1, 2),
                mkTaskAssignment(barSubtopologyId, 1, 2)
            ), Collections.emptyMap(), Collections.emptyMap()),
            newStreamsTargetAssignmentRecord("my-group", "member-2", mkAssignment(
                mkTaskAssignment(fooSubtopologyId, 3, 4),
                mkTaskAssignment(barSubtopologyId, 3, 4)
            ), Collections.emptyMap(), Collections.emptyMap()),
            newStreamsTargetAssignmentRecord("my-group", "member-3", mkAssignment(
                mkTaskAssignment(fooSubtopologyId, 5, 6),
                mkTaskAssignment(barSubtopologyId, 5, 6)
            ), Collections.emptyMap(), Collections.emptyMap())
        ), result.records().subList(0, 3));

        assertEquals(newStreamsTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(3));

        Map<String, org.apache.kafka.coordinator.group.streams.Assignment> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 1, 2),
            mkTaskAssignment(barSubtopologyId, 1, 2)
        )));
        expectedAssignment.put("member-2", new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 3, 4),
            mkTaskAssignment(barSubtopologyId, 3, 4)
        )));
        expectedAssignment.put("member-3", new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 5, 6),
            mkTaskAssignment(barSubtopologyId, 5, 6)
        )));

        assertEquals(expectedAssignment, result.targetAssignment());
    }

    @Test
    public void testPartialAssignmentUpdate() {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
        );

        String fooSubtopologyId = context.addSubtopologyWithSingleSourceTopic("foo", 6, mkMapOfPartitionRacks(6));
        String barSubtopologyId = context.addSubtopologyWithSingleSourceTopic("bar", 6, mkMapOfPartitionRacks(6));

        context.addGroupMember("member-1", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 1, 2),
            mkTaskAssignment(barSubtopologyId, 1, 2)
        ));

        context.addGroupMember("member-2", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 3, 4),
            mkTaskAssignment(barSubtopologyId, 3, 4)
        ));

        context.addGroupMember("member-3", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 5, 6),
            mkTaskAssignment(barSubtopologyId, 5, 6)
        ));

        context.prepareMemberAssignment("member-1", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 1, 2),
            mkTaskAssignment(barSubtopologyId, 1, 2)
        ));

        context.prepareMemberAssignment("member-2", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 3, 4, 5),
            mkTaskAssignment(barSubtopologyId, 3, 4, 5)
        ));

        context.prepareMemberAssignment("member-3", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 6),
            mkTaskAssignment(barSubtopologyId, 6)
        ));

        org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(3, result.records().size());

        // Member 1 has no record because its assignment did not change.
        assertUnorderedListEquals(Arrays.asList(
            newStreamsTargetAssignmentRecord("my-group", "member-2", mkAssignment(
                mkTaskAssignment(fooSubtopologyId, 3, 4, 5),
                mkTaskAssignment(barSubtopologyId, 3, 4, 5)
            ), Collections.emptyMap(), Collections.emptyMap()),
            newStreamsTargetAssignmentRecord("my-group", "member-3", mkAssignment(
                mkTaskAssignment(fooSubtopologyId, 6),
                mkTaskAssignment(barSubtopologyId, 6)
            ), Collections.emptyMap(), Collections.emptyMap())
        ), result.records().subList(0, 2));

        assertEquals(newStreamsTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(2));

        Map<String, org.apache.kafka.coordinator.group.streams.Assignment> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 1, 2),
            mkTaskAssignment(barSubtopologyId, 1, 2)
        )));
        expectedAssignment.put("member-2", new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 3, 4, 5),
            mkTaskAssignment(barSubtopologyId, 3, 4, 5)
        )));
        expectedAssignment.put("member-3", new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 6),
            mkTaskAssignment(barSubtopologyId, 6)
        )));

        assertEquals(expectedAssignment, result.targetAssignment());
    }

    @Test
    public void testDeleteMember() {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
        );

        String fooSubtopologyId = context.addSubtopologyWithSingleSourceTopic("foo", 6, Collections.emptyMap());
        String barSubtopologyId = context.addSubtopologyWithSingleSourceTopic("bar", 6, Collections.emptyMap());

        context.addGroupMember("member-1", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 1, 2),
            mkTaskAssignment(barSubtopologyId, 1, 2)
        ));

        context.addGroupMember("member-2", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 3, 4),
            mkTaskAssignment(barSubtopologyId, 3, 4)
        ));

        context.addGroupMember("member-3", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 5, 6),
            mkTaskAssignment(barSubtopologyId, 5, 6)
        ));

        context.removeMemberSubscription("member-3");

        context.prepareMemberAssignment("member-1", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 1, 2, 3),
            mkTaskAssignment(barSubtopologyId, 1, 2, 3)
        ));

        context.prepareMemberAssignment("member-2", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 4, 5, 6),
            mkTaskAssignment(barSubtopologyId, 4, 5, 6)
        ));

        org.apache.kafka.coordinator.group.streams.TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(3, result.records().size());

        assertUnorderedListEquals(Arrays.asList(
            newStreamsTargetAssignmentRecord("my-group", "member-1", mkAssignment(
                mkTaskAssignment(fooSubtopologyId, 1, 2, 3),
                mkTaskAssignment(barSubtopologyId, 1, 2, 3)
            ), Collections.emptyMap(), Collections.emptyMap()),
            newStreamsTargetAssignmentRecord("my-group", "member-2", mkAssignment(
                mkTaskAssignment(fooSubtopologyId, 4, 5, 6),
                mkTaskAssignment(barSubtopologyId, 4, 5, 6)
            ), Collections.emptyMap(), Collections.emptyMap())
        ), result.records().subList(0, 2));

        assertEquals(newStreamsTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(2));

        Map<String, org.apache.kafka.coordinator.group.streams.Assignment> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 1, 2, 3),
            mkTaskAssignment(barSubtopologyId, 1, 2, 3)
        )));
        expectedAssignment.put("member-2", new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 4, 5, 6),
            mkTaskAssignment(barSubtopologyId, 4, 5, 6)
        )));

        assertEquals(expectedAssignment, result.targetAssignment());
    }

    @Test
    public void testReplaceStaticMember() {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
        );

        String fooSubtopologyId = context.addSubtopologyWithSingleSourceTopic("foo", 6, Collections.emptyMap());
        String barSubtopologyId = context.addSubtopologyWithSingleSourceTopic("bar", 6, Collections.emptyMap());

        context.addGroupMember("member-1", "instance-member-1", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 1, 2),
            mkTaskAssignment(barSubtopologyId, 1, 2)
        ));

        context.addGroupMember("member-2", "instance-member-2", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 3, 4),
            mkTaskAssignment(barSubtopologyId, 3, 4)
        ));

        context.addGroupMember("member-3", "instance-member-3", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 5, 6),
            mkTaskAssignment(barSubtopologyId, 5, 6)
        ));

        // Static member 3 leaves
        context.removeMemberSubscription("member-3");

        // Another static member joins with the same instance id as the departed one
        context.updateMemberSubscription("member-3-a", Optional.of("instance-member-3"),
            Optional.empty());

        context.prepareMemberAssignment("member-1", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 1, 2),
            mkTaskAssignment(barSubtopologyId, 1, 2)
        ));

        context.prepareMemberAssignment("member-2", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 3, 4),
            mkTaskAssignment(barSubtopologyId, 3, 4)
        ));

        context.prepareMemberAssignment("member-3-a", mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 5, 6),
            mkTaskAssignment(barSubtopologyId, 5, 6)
        ));

        TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(2, result.records().size());

        assertUnorderedListEquals(Collections.singletonList(
            newStreamsTargetAssignmentRecord("my-group", "member-3-a", mkAssignment(
                mkTaskAssignment(fooSubtopologyId, 5, 6),
                mkTaskAssignment(barSubtopologyId, 5, 6)
            ), Collections.emptyMap(), Collections.emptyMap())
        ), result.records().subList(0, 1));

        assertEquals(newStreamsTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(1));

        Map<String, org.apache.kafka.coordinator.group.streams.Assignment> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 1, 2),
            mkTaskAssignment(barSubtopologyId, 1, 2)
        )));
        expectedAssignment.put("member-2", new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 3, 4),
            mkTaskAssignment(barSubtopologyId, 3, 4)
        )));

        expectedAssignment.put("member-3-a", new Assignment(mkAssignment(
            mkTaskAssignment(fooSubtopologyId, 5, 6),
            mkTaskAssignment(barSubtopologyId, 5, 6)
        )));

        assertEquals(expectedAssignment, result.targetAssignment());
    }
}
