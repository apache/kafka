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
package org.apache.kafka.coordinator.group.consumer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.assignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.assignor.AssignmentSpec;
import org.apache.kafka.coordinator.group.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.RecordHelpers.newTargetAssignmentEpochRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newTargetAssignmentRecord;
import static org.apache.kafka.coordinator.group.RecordHelpersTest.mkMapOfPartitionRacks;
import static org.apache.kafka.coordinator.group.consumer.TargetAssignmentBuilder.createAssignmentMemberSpec;
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
        private final PartitionAssignor assignor = mock(PartitionAssignor.class);
        private final Map<String, ConsumerGroupMember> members = new HashMap<>();
        private final Map<String, TopicMetadata> subscriptionMetadata = new HashMap<>();
        private final Map<String, ConsumerGroupMember> updatedMembers = new HashMap<>();
        private final Map<String, Assignment> targetAssignment = new HashMap<>();
        private final Map<String, MemberAssignment> memberAssignments = new HashMap<>();

        public TargetAssignmentBuilderTestContext(
            String groupId,
            int groupEpoch
        ) {
            this.groupId = groupId;
            this.groupEpoch = groupEpoch;
        }

        public void addGroupMember(
            String memberId,
            List<String> subscriptions,
            Map<Uuid, Set<Integer>> targetPartitions
        ) {
            members.put(memberId, new ConsumerGroupMember.Builder(memberId)
                .setSubscribedTopicNames(subscriptions)
                .build());

            targetAssignment.put(memberId, new Assignment(
                (byte) 0,
                targetPartitions,
                VersionedMetadata.EMPTY
            ));
        }

        public Uuid addTopicMetadata(
            String topicName,
            int numPartitions,
            Map<Integer, Set<String>> partitionRacks
        ) {
            Uuid topicId = Uuid.randomUuid();
            subscriptionMetadata.put(topicName, new TopicMetadata(
                topicId,
                topicName,
                numPartitions,
                partitionRacks
            ));
            return topicId;
        }

        public void updateMemberSubscription(
            String memberId,
            List<String> subscriptions
        ) {
            updateMemberSubscription(
                memberId,
                subscriptions,
                Optional.empty(),
                Optional.empty()
            );
        }

        public void updateMemberSubscription(
            String memberId,
            List<String> subscriptions,
            Optional<String> instanceId,
            Optional<String> rackId
        ) {
            ConsumerGroupMember existingMember = members.get(memberId);
            ConsumerGroupMember.Builder builder;
            if (existingMember != null) {
                builder = new ConsumerGroupMember.Builder(existingMember);
            } else {
                builder = new ConsumerGroupMember.Builder(memberId);
            }
            updatedMembers.put(memberId, builder
                .setSubscribedTopicNames(subscriptions)
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
            Map<Uuid, Set<Integer>> assignment
        ) {
            memberAssignments.put(memberId, new MemberAssignment(assignment));
        }

        public TargetAssignmentBuilder.TargetAssignmentResult build() {
            // Prepare expected member specs.
            Map<String, AssignmentMemberSpec> memberSpecs = new HashMap<>();

            // All the existing members are prepared.
            members.forEach((memberId, member) ->
                memberSpecs.put(memberId, createAssignmentMemberSpec(
                    member,
                    targetAssignment.getOrDefault(memberId, Assignment.EMPTY),
                    subscriptionMetadata
                )
            ));

            // All the updated are added and all the deleted
            // members are removed.
            updatedMembers.forEach((memberId, updatedMemberOrNull) -> {
                if (updatedMemberOrNull == null) {
                    memberSpecs.remove(memberId);
                } else {
                    memberSpecs.put(memberId, createAssignmentMemberSpec(
                        updatedMemberOrNull,
                        targetAssignment.getOrDefault(memberId, Assignment.EMPTY),
                        subscriptionMetadata
                    ));
                }
            });

            // Prepare the expected topic metadata.
            Map<Uuid, TopicMetadata> topicMetadataMap = new HashMap<>();
            subscriptionMetadata.forEach((topicName, topicMetadata) ->
                topicMetadataMap.put(topicMetadata.id(), topicMetadata));

            // Prepare the expected subscription topic metadata.
            SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadataMap);

            // Prepare the expected assignment spec.
            AssignmentSpec assignmentSpec = new AssignmentSpec(memberSpecs);

            // We use `any` here to always return an assignment but use `verify` later on
            // to ensure that the input was correct.
            when(assignor.assign(any(), any()))
                .thenReturn(new GroupAssignment(memberAssignments));


            // Create and populate the assignment builder.
            TargetAssignmentBuilder builder = new TargetAssignmentBuilder(groupId, groupEpoch, assignor)
                .withMembers(members)
                .withSubscriptionMetadata(subscriptionMetadata)
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
            TargetAssignmentBuilder.TargetAssignmentResult result = builder.build();

            // Verify that the assignor was called once with the expected
            // assignment spec.
            verify(assignor, times(1))
                .assign(assignmentSpec, subscribedTopicMetadata);

            return result;
        }
    }

    @Test
    public void testCreateAssignmentMemberSpec() {
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member-id")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar", "zar"))
            .setRackId("rackId")
            .setInstanceId("instanceId")
            .build();

        Map<String, TopicMetadata> subscriptionMetadata = new HashMap<String, TopicMetadata>() {
            {
                put("foo", new TopicMetadata(fooTopicId, "foo", 5, Collections.emptyMap()));
                put("bar", new TopicMetadata(barTopicId, "bar", 5, Collections.emptyMap()));
            }
        };

        Assignment assignment = new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3),
            mkTopicAssignment(barTopicId, 1, 2, 3)
        ));

        AssignmentMemberSpec assignmentMemberSpec = createAssignmentMemberSpec(
            member,
            assignment,
            subscriptionMetadata
        );

        assertEquals(new AssignmentMemberSpec(
            Optional.of("instanceId"),
            Optional.of("rackId"),
            new HashSet<>(Arrays.asList(fooTopicId, barTopicId)),
            assignment.partitions()
        ), assignmentMemberSpec);
    }

    @Test
    public void testEmpty() {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
        );

        TargetAssignmentBuilder.TargetAssignmentResult result = context.build();
        assertEquals(Collections.singletonList(newTargetAssignmentEpochRecord(
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

        Uuid fooTopicId = context.addTopicMetadata("foo", 6, Collections.emptyMap());
        Uuid barTopicId = context.addTopicMetadata("bar", 6, Collections.emptyMap());

        context.addGroupMember("member-1", Arrays.asList("foo", "bar", "zar"), mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3),
            mkTopicAssignment(barTopicId, 1, 2, 3)
        ));

        context.addGroupMember("member-2", Arrays.asList("foo", "bar", "zar"), mkAssignment(
            mkTopicAssignment(fooTopicId, 4, 5, 6),
            mkTopicAssignment(barTopicId, 4, 5, 6)
        ));

        context.prepareMemberAssignment("member-1", mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3),
            mkTopicAssignment(barTopicId, 1, 2, 3)
        ));

        context.prepareMemberAssignment("member-2", mkAssignment(
            mkTopicAssignment(fooTopicId, 4, 5, 6),
            mkTopicAssignment(barTopicId, 4, 5, 6)
        ));

        TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(Collections.singletonList(newTargetAssignmentEpochRecord(
            "my-group",
            20
        )), result.records());

        Map<String, Assignment> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3),
            mkTopicAssignment(barTopicId, 1, 2, 3)
        )));
        expectedAssignment.put("member-2", new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 4, 5, 6),
            mkTopicAssignment(barTopicId, 4, 5, 6)
        )));

        assertEquals(expectedAssignment, result.targetAssignment());
    }

    @Test
    public void testAssignmentSwapped() {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
        );

        Uuid fooTopicId = context.addTopicMetadata("foo", 6, Collections.emptyMap());
        Uuid barTopicId = context.addTopicMetadata("bar", 6, Collections.emptyMap());

        context.addGroupMember("member-1", Arrays.asList("foo", "bar", "zar"), mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3),
            mkTopicAssignment(barTopicId, 1, 2, 3)
        ));

        context.addGroupMember("member-2", Arrays.asList("foo", "bar", "zar"), mkAssignment(
            mkTopicAssignment(fooTopicId, 4, 5, 6),
            mkTopicAssignment(barTopicId, 4, 5, 6)
        ));

        context.prepareMemberAssignment("member-2", mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3),
            mkTopicAssignment(barTopicId, 1, 2, 3)
        ));

        context.prepareMemberAssignment("member-1", mkAssignment(
            mkTopicAssignment(fooTopicId, 4, 5, 6),
            mkTopicAssignment(barTopicId, 4, 5, 6)
        ));

        TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(3, result.records().size());

        assertUnorderedList(Arrays.asList(
            newTargetAssignmentRecord("my-group", "member-1", mkAssignment(
                mkTopicAssignment(fooTopicId, 4, 5, 6),
                mkTopicAssignment(barTopicId, 4, 5, 6)
            )),
            newTargetAssignmentRecord("my-group", "member-2", mkAssignment(
                mkTopicAssignment(fooTopicId, 1, 2, 3),
                mkTopicAssignment(barTopicId, 1, 2, 3)
            ))
        ), result.records().subList(0, 2));

        assertEquals(newTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(2));

        Map<String, Assignment> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-2", new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3),
            mkTopicAssignment(barTopicId, 1, 2, 3)
        )));
        expectedAssignment.put("member-1", new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 4, 5, 6),
            mkTopicAssignment(barTopicId, 4, 5, 6)
        )));

        assertEquals(expectedAssignment, result.targetAssignment());
    }

    @Test
    public void testNewMember() {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
        );

        Uuid fooTopicId = context.addTopicMetadata("foo", 6, Collections.emptyMap());
        Uuid barTopicId = context.addTopicMetadata("bar", 6, Collections.emptyMap());

        context.addGroupMember("member-1", Arrays.asList("foo", "bar", "zar"), mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3),
            mkTopicAssignment(barTopicId, 1, 2, 3)
        ));

        context.addGroupMember("member-2", Arrays.asList("foo", "bar", "zar"), mkAssignment(
            mkTopicAssignment(fooTopicId, 4, 5, 6),
            mkTopicAssignment(barTopicId, 4, 5, 6)
        ));

        context.updateMemberSubscription("member-3", Arrays.asList("foo", "bar", "zar"));

        context.prepareMemberAssignment("member-1", mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2),
            mkTopicAssignment(barTopicId, 1, 2)
        ));

        context.prepareMemberAssignment("member-2", mkAssignment(
            mkTopicAssignment(fooTopicId, 3, 4),
            mkTopicAssignment(barTopicId, 3, 4)
        ));

        context.prepareMemberAssignment("member-3", mkAssignment(
            mkTopicAssignment(fooTopicId, 5, 6),
            mkTopicAssignment(barTopicId, 5, 6)
        ));

        TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(4, result.records().size());

        assertUnorderedList(Arrays.asList(
            newTargetAssignmentRecord("my-group", "member-1", mkAssignment(
                mkTopicAssignment(fooTopicId, 1, 2),
                mkTopicAssignment(barTopicId, 1, 2)
            )),
            newTargetAssignmentRecord("my-group", "member-2", mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4),
                mkTopicAssignment(barTopicId, 3, 4)
            )),
            newTargetAssignmentRecord("my-group", "member-3", mkAssignment(
                mkTopicAssignment(fooTopicId, 5, 6),
                mkTopicAssignment(barTopicId, 5, 6)
            ))
        ), result.records().subList(0, 3));

        assertEquals(newTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(3));

        Map<String, Assignment> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2),
            mkTopicAssignment(barTopicId, 1, 2)
        )));
        expectedAssignment.put("member-2", new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 3, 4),
            mkTopicAssignment(barTopicId, 3, 4)
        )));
        expectedAssignment.put("member-3", new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 5, 6),
            mkTopicAssignment(barTopicId, 5, 6)
        )));

        assertEquals(expectedAssignment, result.targetAssignment());
    }

    @Test
    public void testUpdateMember() {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
        );

        Uuid fooTopicId = context.addTopicMetadata("foo", 6, Collections.emptyMap());
        Uuid barTopicId = context.addTopicMetadata("bar", 6, Collections.emptyMap());

        context.addGroupMember("member-1", Arrays.asList("foo", "bar", "zar"), mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3),
            mkTopicAssignment(barTopicId, 1, 2)
        ));

        context.addGroupMember("member-2", Arrays.asList("foo", "bar", "zar"), mkAssignment(
            mkTopicAssignment(fooTopicId, 4, 5, 6),
            mkTopicAssignment(barTopicId, 3, 4)
        ));

        context.addGroupMember("member-3", Arrays.asList("bar", "zar"), mkAssignment(
            mkTopicAssignment(barTopicId, 5, 6)
        ));

        context.updateMemberSubscription(
            "member-3",
            Arrays.asList("foo", "bar", "zar"),
            Optional.of("instance-id-3"),
            Optional.of("rack-0")
        );

        context.prepareMemberAssignment("member-1", mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2),
            mkTopicAssignment(barTopicId, 1, 2)
        ));

        context.prepareMemberAssignment("member-2", mkAssignment(
            mkTopicAssignment(fooTopicId, 3, 4),
            mkTopicAssignment(barTopicId, 3, 4)
        ));

        context.prepareMemberAssignment("member-3", mkAssignment(
            mkTopicAssignment(fooTopicId, 5, 6),
            mkTopicAssignment(barTopicId, 5, 6)
        ));

        TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(4, result.records().size());

        assertUnorderedList(Arrays.asList(
            newTargetAssignmentRecord("my-group", "member-1", mkAssignment(
                mkTopicAssignment(fooTopicId, 1, 2),
                mkTopicAssignment(barTopicId, 1, 2)
            )),
            newTargetAssignmentRecord("my-group", "member-2", mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4),
                mkTopicAssignment(barTopicId, 3, 4)
            )),
            newTargetAssignmentRecord("my-group", "member-3", mkAssignment(
                mkTopicAssignment(fooTopicId, 5, 6),
                mkTopicAssignment(barTopicId, 5, 6)
            ))
        ), result.records().subList(0, 3));

        assertEquals(newTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(3));

        Map<String, Assignment> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2),
            mkTopicAssignment(barTopicId, 1, 2)
        )));
        expectedAssignment.put("member-2", new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 3, 4),
            mkTopicAssignment(barTopicId, 3, 4)
        )));
        expectedAssignment.put("member-3", new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 5, 6),
            mkTopicAssignment(barTopicId, 5, 6)
        )));

        assertEquals(expectedAssignment, result.targetAssignment());
    }

    @Test
    public void testPartialAssignmentUpdate() {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
        );

        Uuid fooTopicId = context.addTopicMetadata("foo", 6, mkMapOfPartitionRacks(6));
        Uuid barTopicId = context.addTopicMetadata("bar", 6, mkMapOfPartitionRacks(6));

        context.addGroupMember("member-1", Arrays.asList("foo", "bar", "zar"), mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2),
            mkTopicAssignment(barTopicId, 1, 2)
        ));

        context.addGroupMember("member-2", Arrays.asList("foo", "bar", "zar"), mkAssignment(
            mkTopicAssignment(fooTopicId, 3, 4),
            mkTopicAssignment(barTopicId, 3, 4)
        ));

        context.addGroupMember("member-3", Arrays.asList("bar", "zar"), mkAssignment(
            mkTopicAssignment(fooTopicId, 5, 6),
            mkTopicAssignment(barTopicId, 5, 6)
        ));

        context.prepareMemberAssignment("member-1", mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2),
            mkTopicAssignment(barTopicId, 1, 2)
        ));

        context.prepareMemberAssignment("member-2", mkAssignment(
            mkTopicAssignment(fooTopicId, 3, 4, 5),
            mkTopicAssignment(barTopicId, 3, 4, 5)
        ));

        context.prepareMemberAssignment("member-3", mkAssignment(
            mkTopicAssignment(fooTopicId, 6),
            mkTopicAssignment(barTopicId, 6)
        ));

        TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(3, result.records().size());

        // Member 1 has no record because its assignment did not change.
        assertUnorderedList(Arrays.asList(
            newTargetAssignmentRecord("my-group", "member-2", mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 3, 4, 5)
            )),
            newTargetAssignmentRecord("my-group", "member-3", mkAssignment(
                mkTopicAssignment(fooTopicId, 6),
                mkTopicAssignment(barTopicId, 6)
            ))
        ), result.records().subList(0, 2));

        assertEquals(newTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(2));

        Map<String, Assignment> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2),
            mkTopicAssignment(barTopicId, 1, 2)
        )));
        expectedAssignment.put("member-2", new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 3, 4, 5),
            mkTopicAssignment(barTopicId, 3, 4, 5)
        )));
        expectedAssignment.put("member-3", new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 6),
            mkTopicAssignment(barTopicId, 6)
        )));

        assertEquals(expectedAssignment, result.targetAssignment());
    }

    @Test
    public void testDeleteMember() {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
        );

        Uuid fooTopicId = context.addTopicMetadata("foo", 6, Collections.emptyMap());
        Uuid barTopicId = context.addTopicMetadata("bar", 6, Collections.emptyMap());

        context.addGroupMember("member-1", Arrays.asList("foo", "bar", "zar"), mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2),
            mkTopicAssignment(barTopicId, 1, 2)
        ));

        context.addGroupMember("member-2", Arrays.asList("foo", "bar", "zar"), mkAssignment(
            mkTopicAssignment(fooTopicId, 3, 4),
            mkTopicAssignment(barTopicId, 3, 4)
        ));

        context.addGroupMember("member-3", Arrays.asList("foo", "bar", "zar"), mkAssignment(
            mkTopicAssignment(fooTopicId, 5, 6),
            mkTopicAssignment(barTopicId, 5, 6)
        ));

        context.removeMemberSubscription("member-3");

        context.prepareMemberAssignment("member-1", mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3),
            mkTopicAssignment(barTopicId, 1, 2, 3)
        ));

        context.prepareMemberAssignment("member-2", mkAssignment(
            mkTopicAssignment(fooTopicId, 4, 5, 6),
            mkTopicAssignment(barTopicId, 4, 5, 6)
        ));

        TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(3, result.records().size());

        assertUnorderedList(Arrays.asList(
            newTargetAssignmentRecord("my-group", "member-1", mkAssignment(
                mkTopicAssignment(fooTopicId, 1, 2, 3),
                mkTopicAssignment(barTopicId, 1, 2, 3)
            )),
            newTargetAssignmentRecord("my-group", "member-2", mkAssignment(
                mkTopicAssignment(fooTopicId, 4, 5, 6),
                mkTopicAssignment(barTopicId, 4, 5, 6)
            ))
        ), result.records().subList(0, 2));

        assertEquals(newTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(2));

        Map<String, Assignment> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3),
            mkTopicAssignment(barTopicId, 1, 2, 3)
        )));
        expectedAssignment.put("member-2", new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 4, 5, 6),
            mkTopicAssignment(barTopicId, 4, 5, 6)
        )));

        assertEquals(expectedAssignment, result.targetAssignment());
    }

    private static <T> void assertUnorderedList(
        List<T> expected,
        List<T> actual
    ) {
        assertEquals(expected.size(), actual.size());
        assertEquals(new HashSet<>(expected), new HashSet<>(actual));
    }
}
