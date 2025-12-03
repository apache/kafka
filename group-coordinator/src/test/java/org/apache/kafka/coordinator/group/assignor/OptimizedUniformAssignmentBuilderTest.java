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
package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.GroupSpecImpl;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;
import org.apache.kafka.coordinator.group.modern.SubscribedTopicDescriberImpl;
import org.apache.kafka.image.MetadataImage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.assertAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.invertedTargetAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkOrderedAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OptimizedUniformAssignmentBuilderTest {
    private final UniformAssignor assignor = new UniformAssignor();
    private final Uuid topic1Uuid = Uuid.fromString("T1-A4s3VTwiI5CTbEp6POw");
    private final Uuid topic2Uuid = Uuid.fromString("T2-B4s3VTwiI5YHbPp6YUe");
    private final Uuid topic3Uuid = Uuid.fromString("T3-CU8fVTLCz5YMkLoDQsa");
    private final String topic1Name = "topic1";
    private final String topic2Name = "topic2";
    private final String topic3Name = "topic3";
    private final String memberA = "A";
    private final String memberB = "B";
    private final String memberC = "C";

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testAssignmentReuse(boolean rackAware) {
        CommonAssignorTests.testAssignmentReuse(assignor, HOMOGENEOUS, rackAware);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testReassignmentStickiness(boolean rackAware) {
        CommonAssignorTests.testReassignmentStickiness(assignor, HOMOGENEOUS, rackAware);
    }

    @Test
    public void testOneMemberNoTopicSubscription() {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 3)
            .build();
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = Map.of(
            memberA,
            new MemberSubscriptionAndAssignmentImpl(
                Optional.empty(),
                Optional.empty(),
                Set.of(),
                Assignment.EMPTY
            )
        );

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            Map.of()
        );

        GroupAssignment groupAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        assertEquals(Map.of(), groupAssignment.members());
    }

    @Test
    public void testOneMemberSubscribedToNonexistentTopic() {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 3)
            .build();
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = Map.of(
            memberA,
            new MemberSubscriptionAndAssignmentImpl(
                Optional.empty(),
                Optional.empty(),
                Set.of(topic2Uuid),
                Assignment.EMPTY
            )
        );

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            Map.of()
        );

        assertThrows(PartitionAssignorException.class,
            () -> assignor.assign(groupSpec, subscribedTopicMetadata));
    }

    @Test
    public void testFirstAssignmentTwoMembersTwoTopicsNoMemberRacks() {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 3)
            .addTopic(topic3Uuid, topic3Name, 2)
            .build();

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic3Uuid),
            Assignment.EMPTY
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic3Uuid),
            Assignment.EMPTY
        ));

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic3Uuid, 0, 1)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1, 2)
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            Map.of()
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testFirstAssignmentNumMembersGreaterThanTotalNumPartitions() {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic3Uuid, topic3Name, 2)
            .build();

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic3Uuid),
            Assignment.EMPTY
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic3Uuid),
            Assignment.EMPTY
        ));

        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic3Uuid),
            Assignment.EMPTY
        ));

        // Topic 3 has 2 partitions but three members subscribed to it - one of them should not get an assignment.
        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA,
            Map.of()
        );
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic3Uuid, 0)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic3Uuid, 1)
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            Map.of()
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testValidityAndBalanceForLargeSampleSet() {
        MetadataImageBuilder metadataImageBuilder = new MetadataImageBuilder();
        Set<Uuid> topicIds = new HashSet<>();
        for (int i = 1; i < 101; i++) {
            Uuid topicId = Uuid.randomUuid();
            metadataImageBuilder.addTopic(topicId, "topic-" + i, 3);
            topicIds.add(topicId);
        }
        MetadataImage metadataImage = metadataImageBuilder.build();

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        for (int i = 1; i < 50; i++) {
            members.put("member" + i, new MemberSubscriptionAndAssignmentImpl(
                Optional.empty(),
                Optional.empty(),
                topicIds,
                Assignment.EMPTY
            ));
        }

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            Map.of()
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testReassignmentForTwoMembersTwoTopicsGivenUnbalancedPrevAssignment() {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 3)
            .addTopic(topic2Uuid, topic2Name, 3)
            .build();

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            new Assignment(mkOrderedAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1),
                mkTopicAssignment(topic2Uuid, 0, 1)
            ))
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            new Assignment(mkOrderedAssignment(
                mkTopicAssignment(topic1Uuid, 2),
                mkTopicAssignment(topic2Uuid, 2)
            ))
        ));

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1),
            mkTopicAssignment(topic2Uuid, 0)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2),
            mkTopicAssignment(topic2Uuid, 1, 2)
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testReassignmentWhenPartitionsAreAddedForTwoMembersTwoTopics() {
        // Simulating adding partition to T1 and T2 - originally T1 -> 3 Partitions and T2 -> 3 Partitions
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 6)
            .addTopic(topic2Uuid, topic2Name, 5)
            .build();

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            new Assignment(mkOrderedAssignment(
                mkTopicAssignment(topic1Uuid, 0, 2),
                mkTopicAssignment(topic2Uuid, 0)
            ))
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            new Assignment(mkOrderedAssignment(
                mkTopicAssignment(topic1Uuid, 1),
                mkTopicAssignment(topic2Uuid, 1, 2)
            ))
        ));

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 2),
            mkTopicAssignment(topic2Uuid, 0, 3, 4)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1, 3, 4, 5),
            mkTopicAssignment(topic2Uuid, 1, 2)
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneMemberAddedAfterInitialAssignmentWithTwoMembersTwoTopics() {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 3)
            .addTopic(topic2Uuid, topic2Name, 3)
            .build();

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            new Assignment(mkOrderedAssignment(
                mkTopicAssignment(topic1Uuid, 0, 2),
                mkTopicAssignment(topic2Uuid, 0)
            ))
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            new Assignment(mkOrderedAssignment(
                mkTopicAssignment(topic1Uuid, 1),
                mkTopicAssignment(topic2Uuid, 1, 2)
            ))
        ));

        // Add a new member to trigger a re-assignment.
        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            Assignment.EMPTY
        ));

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 2)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic2Uuid, 1)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic2Uuid, 0, 2)
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneMemberRemovedAfterInitialAssignmentWithThreeMembersTwoTopics() {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 3)
            .addTopic(topic2Uuid, topic2Name, 3)
            .build();

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 0),
                mkTopicAssignment(topic2Uuid, 0)
            ))
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 1),
                mkTopicAssignment(topic2Uuid, 1)
            ))
        ));

        // Member C was removed

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0),
            mkTopicAssignment(topic2Uuid, 0, 2)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1, 2),
            mkTopicAssignment(topic2Uuid, 1)
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneSubscriptionRemovedAfterInitialAssignmentWithTwoMembersTwoTopics() {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topic1Uuid, topic1Name, 2)
            .addTopic(topic2Uuid, topic2Name, 2)
            .build();

        // Initial subscriptions were [T1, T2]
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 0),
                mkTopicAssignment(topic2Uuid, 0)
            ))
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 1),
                mkTopicAssignment(topic2Uuid, 1)
            ))
        ));

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic2Uuid, 0)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic2Uuid, 1)
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    /**
     * Verifies that the given assignment is valid with respect to the given subscriptions.
     * Validity requirements:
     *      - each member is subscribed to topics of all partitions assigned to it, and
     *      - each partition is assigned to no more than one member.
     * Balance requirements:
     *      - the assignment is fully balanced (the numbers of topic partitions assigned to memberSubscriptionSpec differ by at most one), or
     *      - there is no topic partition that can be moved from one member to another with 2+ fewer topic partitions.
     *
     * @param memberSubscriptionSpec        Members subscription metadata structure from the group Spec.
     * @param computedGroupAssignment       Assignment computed by the uniform assignor.
     */
    private void checkValidityAndBalance(
        Map<String, MemberSubscriptionAndAssignmentImpl> memberSubscriptionSpec,
        GroupAssignment computedGroupAssignment
    ) {
        List<String> membersList = new ArrayList<>(computedGroupAssignment.members().keySet());
        int numMembers = membersList.size();
        List<Integer> totalAssignmentSizesOfAllMembers = new ArrayList<>(membersList.size());
        membersList.forEach(member -> {
            Map<Uuid, Set<Integer>> computedAssignmentForMember = computedGroupAssignment
                .members().get(member).partitions();
            int sum = computedAssignmentForMember.values().stream().mapToInt(Set::size).sum();
            totalAssignmentSizesOfAllMembers.add(sum);
        });

        for (int i = 0; i < numMembers; i++) {
            String memberId = membersList.get(i);
            Map<Uuid, Set<Integer>> computedAssignmentForMember =
                computedGroupAssignment.members().get(memberId).partitions();
            // Each member is subscribed to topics of all the partitions assigned to it.
            computedAssignmentForMember.keySet().forEach(topicId -> {
                // Check if the topic exists in the subscription.
                assertTrue(memberSubscriptionSpec.get(memberId).subscribedTopicIds().contains(topicId),
                    "Error: Partitions for topic " + topicId + " are assigned to member " + memberId +
                        " but it is not part of the members subscription ");
            });

            for (int j = i + 1; j < numMembers; j++) {
                String otherMemberId = membersList.get(j);
                Map<Uuid, Set<Integer>> computedAssignmentForOtherMember = computedGroupAssignment
                    .members().get(otherMemberId).partitions();
                // Each partition should be assigned to at most one member
                computedAssignmentForMember.keySet().forEach(topicId -> {
                    Set<Integer> intersection = new HashSet<>();
                    if (computedAssignmentForOtherMember.containsKey(topicId)) {
                        intersection = new HashSet<>(computedAssignmentForMember.get(topicId));
                        intersection.retainAll(computedAssignmentForOtherMember.get(topicId));
                    }
                    assertTrue(
                        intersection.isEmpty(),
                        "Error : Member 1 " + memberId + " and Member 2 " + otherMemberId +
                            "have common partitions assigned to them " + computedAssignmentForOtherMember.get(topicId)
                    );
                });

                // Difference in the sizes of any two partitions should be 1 at max
                int size1 = totalAssignmentSizesOfAllMembers.get(i);
                int size2 = totalAssignmentSizesOfAllMembers.get(j);
                assertTrue(
                    Math.abs(size1 - size2) <= 1,
                    "Size of one assignment is greater than the other assignment by more than one partition "
                        + size1 + " " + size2 + "abs = " + Math.abs(size1 - size2)
                );
            }
        }
    }
}
