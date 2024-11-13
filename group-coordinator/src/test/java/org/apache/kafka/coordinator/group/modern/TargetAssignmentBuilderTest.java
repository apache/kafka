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
package org.apache.kafka.coordinator.group.modern;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.AssignmentTestUtil;
import org.apache.kafka.coordinator.group.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.modern.consumer.ResolvedRegularExpression;
import org.apache.kafka.image.TopicsImage;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.coordinator.group.Assertions.assertUnorderedListEquals;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;
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
        private final Map<String, String> staticMembers = new HashMap<>();
        private final Map<String, ResolvedRegularExpression> resolvedRegularExpressions = new HashMap<>();
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
            List<String> subscriptions,
            Map<Uuid, Set<Integer>> targetPartitions
        ) {
            addGroupMember(memberId, null, subscriptions, "", targetPartitions);
        }

        public void addGroupMember(
            String memberId,
            List<String> subscriptions,
            String subscribedRegex,
            Map<Uuid, Set<Integer>> targetPartitions
        ) {
            addGroupMember(memberId, null, subscriptions, subscribedRegex, targetPartitions);
        }

        public void addGroupMember(
            String memberId,
            String instanceId,
            List<String> subscriptions,
            Map<Uuid, Set<Integer>> targetPartitions
        ) {
            addGroupMember(memberId, instanceId, subscriptions, "", targetPartitions);
        }

        public void addGroupMember(
            String memberId,
            String instanceId,
            List<String> subscriptions,
            String subscribedRegex,
            Map<Uuid, Set<Integer>> targetPartitions
        ) {
            ConsumerGroupMember.Builder memberBuilder = new ConsumerGroupMember.Builder(memberId)
                .setSubscribedTopicNames(subscriptions)
                .setSubscribedTopicRegex(subscribedRegex);

            if (instanceId != null) {
                memberBuilder.setInstanceId(instanceId);
                staticMembers.put(instanceId, memberId);
            }
            members.put(memberId, memberBuilder.build());
            targetAssignment.put(memberId, new Assignment(targetPartitions));
        }

        public Uuid addTopicMetadata(
            String topicName,
            int numPartitions
        ) {
            Uuid topicId = Uuid.randomUuid();
            subscriptionMetadata.put(topicName, new TopicMetadata(
                topicId,
                topicName,
                numPartitions
            ));
            topicsImageBuilder = topicsImageBuilder.addTopic(topicId, topicName, numPartitions);

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
            memberAssignments.put(memberId, new MemberAssignmentImpl(assignment));
        }

        public void addResolvedRegularExpression(
            String regex,
            ResolvedRegularExpression resolvedRegularExpression
        ) {
            resolvedRegularExpressions.put(regex, resolvedRegularExpression);
        }

        private MemberSubscriptionAndAssignmentImpl newMemberSubscriptionAndAssignment(
            ConsumerGroupMember member,
            Assignment memberAssignment,
            TopicIds.TopicResolver topicResolver
        ) {
            Set<String> subscriptions = member.subscribedTopicNames();

            // Check whether the member is also subscribed to a regular expression. If it is,
            // create the union of the two subscriptions.
            String subscribedTopicRegex = member.subscribedTopicRegex();
            if (subscribedTopicRegex != null && !subscribedTopicRegex.isEmpty()) {
                ResolvedRegularExpression resolvedRegularExpression = resolvedRegularExpressions.get(subscribedTopicRegex);
                if (resolvedRegularExpression != null) {
                    if (subscriptions.isEmpty()) {
                        subscriptions = resolvedRegularExpression.topics;
                    } else if (!resolvedRegularExpression.topics.isEmpty()) {
                        // We only use a UnionSet when the member uses both type of subscriptions. The
                        // protocol allows it. However, the Apache Kafka Consumer does not support it.
                        // Other clients such as librdkafka may support it.
                        subscriptions = new UnionSet<>(subscriptions, resolvedRegularExpression.topics);
                    }
                }
            }

            return new MemberSubscriptionAndAssignmentImpl(
                Optional.ofNullable(member.rackId()),
                Optional.ofNullable(member.instanceId()),
                new TopicIds(subscriptions, topicResolver),
                memberAssignment
            );
        }

        public TargetAssignmentBuilder.TargetAssignmentResult build() {
            TopicsImage topicsImage = topicsImageBuilder.build().topics();
            TopicIds.TopicResolver topicResolver = new TopicIds.CachedTopicResolver(topicsImage);
            // Prepare expected member specs.
            Map<String, MemberSubscriptionAndAssignmentImpl> memberSubscriptions = new HashMap<>();

            // All the existing members are prepared.
            members.forEach((memberId, member) ->
                memberSubscriptions.put(memberId, newMemberSubscriptionAndAssignment(
                    member,
                    targetAssignment.getOrDefault(memberId, Assignment.EMPTY),
                    topicResolver
                ))
            );

            // All the updated are added and all the deleted
            // members are removed.
            updatedMembers.forEach((memberId, updatedMemberOrNull) -> {
                if (updatedMemberOrNull == null) {
                    memberSubscriptions.remove(memberId);
                } else {
                    Assignment assignment = targetAssignment.getOrDefault(memberId, Assignment.EMPTY);

                    // A new static member joins and needs to replace an existing departed one.
                    if (updatedMemberOrNull.instanceId() != null) {
                        String previousMemberId = staticMembers.get(updatedMemberOrNull.instanceId());
                        if (previousMemberId != null && !previousMemberId.equals(memberId)) {
                            assignment = targetAssignment.getOrDefault(previousMemberId, Assignment.EMPTY);
                        }
                    }

                    memberSubscriptions.put(memberId, newMemberSubscriptionAndAssignment(
                        updatedMemberOrNull,
                        assignment,
                        topicResolver
                    ));
                }
            });

            // Prepare the expected topic metadata.
            Map<Uuid, TopicMetadata> topicMetadataMap = new HashMap<>();
            subscriptionMetadata.forEach((topicName, topicMetadata) ->
                topicMetadataMap.put(topicMetadata.id(), topicMetadata));

            // Prepare the expected subscription topic metadata.
            SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadataMap);
            SubscriptionType subscriptionType = HOMOGENEOUS;

            // Prepare the member assignments per topic partition.
            Map<Uuid, Map<Integer, String>> invertedTargetAssignment = AssignmentTestUtil
                .invertedTargetAssignment(memberSubscriptions);

            // Prepare the expected assignment spec.
            GroupSpecImpl groupSpec = new GroupSpecImpl(
                memberSubscriptions,
                subscriptionType,
                invertedTargetAssignment
            );

            // We use `any` here to always return an assignment but use `verify` later on
            // to ensure that the input was correct.
            when(assignor.assign(any(), any()))
                .thenReturn(new GroupAssignment(memberAssignments));

            // Create and populate the assignment builder.
            TargetAssignmentBuilder.ConsumerTargetAssignmentBuilder builder =
                new TargetAssignmentBuilder.ConsumerTargetAssignmentBuilder(groupId, groupEpoch, assignor)
                    .withMembers(members)
                    .withStaticMembers(staticMembers)
                    .withSubscriptionMetadata(subscriptionMetadata)
                    .withSubscriptionType(subscriptionType)
                    .withTargetAssignment(targetAssignment)
                    .withInvertedTargetAssignment(invertedTargetAssignment)
                    .withTopicsImage(topicsImage)
                    .withResolvedRegularExpressions(resolvedRegularExpressions);

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
                .assign(groupSpec, subscribedTopicMetadata);

            return result;
        }
    }

    @Test
    public void testEmpty() {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
        );

        TargetAssignmentBuilder.TargetAssignmentResult result = context.build();
        assertEquals(Collections.singletonList(newConsumerGroupTargetAssignmentEpochRecord(
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

        Uuid fooTopicId = context.addTopicMetadata("foo", 6);
        Uuid barTopicId = context.addTopicMetadata("bar", 6);

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

        assertEquals(Collections.singletonList(newConsumerGroupTargetAssignmentEpochRecord(
            "my-group",
            20
        )), result.records());

        Map<String, MemberAssignment> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", new MemberAssignmentImpl(mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3),
            mkTopicAssignment(barTopicId, 1, 2, 3)
        )));
        expectedAssignment.put("member-2", new MemberAssignmentImpl(mkAssignment(
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

        Uuid fooTopicId = context.addTopicMetadata("foo", 6);
        Uuid barTopicId = context.addTopicMetadata("bar", 6);

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

        assertUnorderedListEquals(Arrays.asList(
            newConsumerGroupTargetAssignmentRecord("my-group", "member-1", mkAssignment(
                mkTopicAssignment(fooTopicId, 4, 5, 6),
                mkTopicAssignment(barTopicId, 4, 5, 6)
            )),
            newConsumerGroupTargetAssignmentRecord("my-group", "member-2", mkAssignment(
                mkTopicAssignment(fooTopicId, 1, 2, 3),
                mkTopicAssignment(barTopicId, 1, 2, 3)
            ))
        ), result.records().subList(0, 2));

        assertEquals(newConsumerGroupTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(2));

        Map<String, MemberAssignment> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-2", new MemberAssignmentImpl(mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3),
            mkTopicAssignment(barTopicId, 1, 2, 3)
        )));
        expectedAssignment.put("member-1", new MemberAssignmentImpl(mkAssignment(
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

        Uuid fooTopicId = context.addTopicMetadata("foo", 6);
        Uuid barTopicId = context.addTopicMetadata("bar", 6);

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

        assertUnorderedListEquals(Arrays.asList(
            newConsumerGroupTargetAssignmentRecord("my-group", "member-1", mkAssignment(
                mkTopicAssignment(fooTopicId, 1, 2),
                mkTopicAssignment(barTopicId, 1, 2)
            )),
            newConsumerGroupTargetAssignmentRecord("my-group", "member-2", mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4),
                mkTopicAssignment(barTopicId, 3, 4)
            )),
            newConsumerGroupTargetAssignmentRecord("my-group", "member-3", mkAssignment(
                mkTopicAssignment(fooTopicId, 5, 6),
                mkTopicAssignment(barTopicId, 5, 6)
            ))
        ), result.records().subList(0, 3));

        assertEquals(newConsumerGroupTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(3));

        Map<String, MemberAssignment> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", new MemberAssignmentImpl(mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2),
            mkTopicAssignment(barTopicId, 1, 2)
        )));
        expectedAssignment.put("member-2", new MemberAssignmentImpl(mkAssignment(
            mkTopicAssignment(fooTopicId, 3, 4),
            mkTopicAssignment(barTopicId, 3, 4)
        )));
        expectedAssignment.put("member-3", new MemberAssignmentImpl(mkAssignment(
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

        Uuid fooTopicId = context.addTopicMetadata("foo", 6);
        Uuid barTopicId = context.addTopicMetadata("bar", 6);

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

        assertUnorderedListEquals(Arrays.asList(
            newConsumerGroupTargetAssignmentRecord("my-group", "member-1", mkAssignment(
                mkTopicAssignment(fooTopicId, 1, 2),
                mkTopicAssignment(barTopicId, 1, 2)
            )),
            newConsumerGroupTargetAssignmentRecord("my-group", "member-2", mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4),
                mkTopicAssignment(barTopicId, 3, 4)
            )),
            newConsumerGroupTargetAssignmentRecord("my-group", "member-3", mkAssignment(
                mkTopicAssignment(fooTopicId, 5, 6),
                mkTopicAssignment(barTopicId, 5, 6)
            ))
        ), result.records().subList(0, 3));

        assertEquals(newConsumerGroupTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(3));

        Map<String, MemberAssignment> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", new MemberAssignmentImpl(mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2),
            mkTopicAssignment(barTopicId, 1, 2)
        )));
        expectedAssignment.put("member-2", new MemberAssignmentImpl(mkAssignment(
            mkTopicAssignment(fooTopicId, 3, 4),
            mkTopicAssignment(barTopicId, 3, 4)
        )));
        expectedAssignment.put("member-3", new MemberAssignmentImpl(mkAssignment(
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

        Uuid fooTopicId = context.addTopicMetadata("foo", 6);
        Uuid barTopicId = context.addTopicMetadata("bar", 6);

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
        assertUnorderedListEquals(Arrays.asList(
            newConsumerGroupTargetAssignmentRecord("my-group", "member-2", mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 3, 4, 5)
            )),
            newConsumerGroupTargetAssignmentRecord("my-group", "member-3", mkAssignment(
                mkTopicAssignment(fooTopicId, 6),
                mkTopicAssignment(barTopicId, 6)
            ))
        ), result.records().subList(0, 2));

        assertEquals(newConsumerGroupTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(2));

        Map<String, MemberAssignment> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", new MemberAssignmentImpl(mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2),
            mkTopicAssignment(barTopicId, 1, 2)
        )));
        expectedAssignment.put("member-2", new MemberAssignmentImpl(mkAssignment(
            mkTopicAssignment(fooTopicId, 3, 4, 5),
            mkTopicAssignment(barTopicId, 3, 4, 5)
        )));
        expectedAssignment.put("member-3", new MemberAssignmentImpl(mkAssignment(
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

        Uuid fooTopicId = context.addTopicMetadata("foo", 6);
        Uuid barTopicId = context.addTopicMetadata("bar", 6);

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

        assertUnorderedListEquals(Arrays.asList(
            newConsumerGroupTargetAssignmentRecord("my-group", "member-1", mkAssignment(
                mkTopicAssignment(fooTopicId, 1, 2, 3),
                mkTopicAssignment(barTopicId, 1, 2, 3)
            )),
            newConsumerGroupTargetAssignmentRecord("my-group", "member-2", mkAssignment(
                mkTopicAssignment(fooTopicId, 4, 5, 6),
                mkTopicAssignment(barTopicId, 4, 5, 6)
            ))
        ), result.records().subList(0, 2));

        assertEquals(newConsumerGroupTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(2));

        Map<String, MemberAssignment> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", new MemberAssignmentImpl(mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3),
            mkTopicAssignment(barTopicId, 1, 2, 3)
        )));
        expectedAssignment.put("member-2", new MemberAssignmentImpl(mkAssignment(
            mkTopicAssignment(fooTopicId, 4, 5, 6),
            mkTopicAssignment(barTopicId, 4, 5, 6)
        )));

        assertEquals(expectedAssignment, result.targetAssignment());
    }

    @Test
    public void testReplaceStaticMember() {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
        );

        Uuid fooTopicId = context.addTopicMetadata("foo", 6);
        Uuid barTopicId = context.addTopicMetadata("bar", 6);

        context.addGroupMember("member-1", "instance-member-1", Arrays.asList("foo", "bar", "zar"), mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2),
            mkTopicAssignment(barTopicId, 1, 2)
        ));

        context.addGroupMember("member-2", "instance-member-2", Arrays.asList("foo", "bar", "zar"), mkAssignment(
            mkTopicAssignment(fooTopicId, 3, 4),
            mkTopicAssignment(barTopicId, 3, 4)
        ));

        context.addGroupMember("member-3", "instance-member-3", Arrays.asList("foo", "bar", "zar"), mkAssignment(
            mkTopicAssignment(fooTopicId, 5, 6),
            mkTopicAssignment(barTopicId, 5, 6)
        ));

        // Static member 3 leaves
        context.removeMemberSubscription("member-3");

        // Another static member joins with the same instance id as the departed one
        context.updateMemberSubscription("member-3-a", Arrays.asList("foo", "bar", "zar"), Optional.of("instance-member-3"), Optional.empty());

        context.prepareMemberAssignment("member-1", mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2),
            mkTopicAssignment(barTopicId, 1, 2)
        ));

        context.prepareMemberAssignment("member-2", mkAssignment(
            mkTopicAssignment(fooTopicId, 3, 4),
            mkTopicAssignment(barTopicId, 3, 4)
        ));

        context.prepareMemberAssignment("member-3-a", mkAssignment(
            mkTopicAssignment(fooTopicId, 5, 6),
            mkTopicAssignment(barTopicId, 5, 6)
        ));

        TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(2, result.records().size());

        assertUnorderedListEquals(Collections.singletonList(
            newConsumerGroupTargetAssignmentRecord("my-group", "member-3-a", mkAssignment(
                mkTopicAssignment(fooTopicId, 5, 6),
                mkTopicAssignment(barTopicId, 5, 6)
            ))
        ), result.records().subList(0, 1));

        assertEquals(newConsumerGroupTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(1));

        Map<String, MemberAssignment> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", new MemberAssignmentImpl(mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2),
            mkTopicAssignment(barTopicId, 1, 2)
        )));
        expectedAssignment.put("member-2", new MemberAssignmentImpl(mkAssignment(
            mkTopicAssignment(fooTopicId, 3, 4),
            mkTopicAssignment(barTopicId, 3, 4)
        )));

        expectedAssignment.put("member-3-a", new MemberAssignmentImpl(mkAssignment(
            mkTopicAssignment(fooTopicId, 5, 6),
            mkTopicAssignment(barTopicId, 5, 6)
        )));

        assertEquals(expectedAssignment, result.targetAssignment());
    }

    @Test
    public void testRegularExpressions() {
        TargetAssignmentBuilderTestContext context = new TargetAssignmentBuilderTestContext(
            "my-group",
            20
        );

        Uuid fooTopicId = context.addTopicMetadata("foo", 6);
        Uuid barTopicId = context.addTopicMetadata("bar", 6);

        context.addGroupMember("member-1", Arrays.asList("bar", "zar"), "foo*", mkAssignment());

        context.addGroupMember("member-2", Arrays.asList("foo", "bar", "zar"), mkAssignment());

        context.addGroupMember("member-3", Collections.emptyList(), "foo*", mkAssignment());

        context.addResolvedRegularExpression("foo*", new ResolvedRegularExpression(
            Collections.singleton("foo"),
            10L,
            12345L
        ));

        context.prepareMemberAssignment("member-1", mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2),
            mkTopicAssignment(barTopicId, 1, 2, 3)
        ));

        context.prepareMemberAssignment("member-2", mkAssignment(
            mkTopicAssignment(fooTopicId, 3, 4),
            mkTopicAssignment(barTopicId, 4, 5, 6)
        ));

        context.prepareMemberAssignment("member-3", mkAssignment(
            mkTopicAssignment(fooTopicId, 5, 6)
        ));

        TargetAssignmentBuilder.TargetAssignmentResult result = context.build();

        assertEquals(4, result.records().size());

        assertUnorderedListEquals(Arrays.asList(
            newConsumerGroupTargetAssignmentRecord("my-group", "member-1", mkAssignment(
                mkTopicAssignment(fooTopicId, 1, 2),
                mkTopicAssignment(barTopicId, 1, 2, 3)
            )),
            newConsumerGroupTargetAssignmentRecord("my-group", "member-2", mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4),
                mkTopicAssignment(barTopicId, 4, 5, 6)
            )),
            newConsumerGroupTargetAssignmentRecord("my-group", "member-3", mkAssignment(
                mkTopicAssignment(fooTopicId, 5, 6)
            ))
        ), result.records().subList(0, 3));

        assertEquals(newConsumerGroupTargetAssignmentEpochRecord(
            "my-group",
            20
        ), result.records().get(3));

        Map<String, MemberAssignment> expectedAssignment = new HashMap<>();
        expectedAssignment.put("member-1", new MemberAssignmentImpl(mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2),
            mkTopicAssignment(barTopicId, 1, 2, 3)
        )));
        expectedAssignment.put("member-2", new MemberAssignmentImpl(mkAssignment(
            mkTopicAssignment(fooTopicId, 3, 4),
            mkTopicAssignment(barTopicId, 4, 5, 6)
        )));

        expectedAssignment.put("member-3", new MemberAssignmentImpl(mkAssignment(
            mkTopicAssignment(fooTopicId, 5, 6)
        )));

        assertEquals(expectedAssignment, result.targetAssignment());
    }
}
