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
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.GroupSpecImpl;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;
import org.apache.kafka.coordinator.group.modern.SubscribedTopicDescriberImpl;
import org.apache.kafka.image.MetadataImage;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.assertAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.invertedTargetAssignment;
import static org.junit.jupiter.api.Assertions.assertSame;

public class CommonAssignorTests {
    private static final Uuid TOPIC_1_UUID = Uuid.randomUuid();
    private static final String TOPIC_1_NAME = "topic1";
    private static final Uuid TOPIC_2_UUID = Uuid.randomUuid();
    private static final String TOPIC_2_NAME = "topic2";
    private static final Uuid TOPIC_3_UUID = Uuid.randomUuid();
    private static final String TOPIC_3_NAME = "topic3";
    private static final String MEMBER_A = "A";
    private static final String MEMBER_B = "B";
    private static final String MEMBER_C = "C";

    /**
     * Tests that an assignor reuses the same assignment maps when the assignment is unchanged.
     * @param assignor         The assignor.
     * @param subscriptionType The subscription type.
     * @param rackAware        Whether to test with rack awareness.
     */
    public static void testAssignmentReuse(PartitionAssignor assignor, SubscriptionType subscriptionType, boolean rackAware) {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(TOPIC_1_UUID, TOPIC_1_NAME, 2)
            .addTopic(TOPIC_2_UUID, TOPIC_2_NAME, 5)
            .addTopic(TOPIC_3_UUID, TOPIC_3_NAME, 7)
            .addRacks()
            .build();

        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new HashMap<>();
        members.put(MEMBER_A, new MemberSubscriptionAndAssignmentImpl(
            rackAware ? Optional.of("rack1") : Optional.empty(),
            Optional.empty(),
            Set.of(TOPIC_1_UUID, TOPIC_2_UUID, TOPIC_3_UUID),
            Assignment.EMPTY
        ));
        members.put(MEMBER_B, new MemberSubscriptionAndAssignmentImpl(
            rackAware ? Optional.of("rack2") : Optional.empty(),
            Optional.empty(),
            Set.of(TOPIC_1_UUID, TOPIC_2_UUID, TOPIC_3_UUID),
            Assignment.EMPTY
        ));
        members.put(MEMBER_C, new MemberSubscriptionAndAssignmentImpl(
            rackAware ? Optional.of("rack3") : Optional.empty(),
            Optional.empty(),
            Set.of(TOPIC_1_UUID, TOPIC_2_UUID, TOPIC_3_UUID),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            subscriptionType,
            Map.of()
        );

        GroupAssignment firstAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> membersWithAssignment = new LinkedHashMap<>();
        for (Map.Entry<String, MemberSubscriptionAndAssignmentImpl> entry : members.entrySet()) {
            String memberId = entry.getKey();
            MemberSubscriptionAndAssignmentImpl memberSubscriptionAndAssignment = entry.getValue();
            membersWithAssignment.put(memberId, new MemberSubscriptionAndAssignmentImpl(
                memberSubscriptionAndAssignment.rackId(),
                memberSubscriptionAndAssignment.instanceId(),
                memberSubscriptionAndAssignment.subscribedTopicIds(),
                new Assignment(firstAssignment.members().get(memberId).partitions())
            ));
        }
        GroupSpec groupSpecWithAssignment = new GroupSpecImpl(
            membersWithAssignment,
            subscriptionType,
            invertedTargetAssignment(membersWithAssignment)
        );

        GroupAssignment secondAssignment = assignor.assign(
            groupSpecWithAssignment,
            subscribedTopicMetadata
        );

        for (String memberId : members.keySet()) {
            // The assignment map from the assignor must be the same as the immutable assignment map
            // that went in.
            assertSame(membersWithAssignment.get(memberId).partitions(), secondAssignment.members().get(memberId).partitions());
        }
    }

    /**
     * Tests that an assignor produces the same assignment when the members are iterated in
     * different orders.
     * @param assignor         The assignor.
     * @param subscriptionType The subscription type.
     * @param rackAware        Whether to test with rack awareness.
     */
    public static void testReassignmentStickiness(PartitionAssignor assignor, SubscriptionType subscriptionType, boolean rackAware) {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(TOPIC_1_UUID, TOPIC_1_NAME, 2)
            .addTopic(TOPIC_2_UUID, TOPIC_2_NAME, 5)
            .addTopic(TOPIC_3_UUID, TOPIC_3_NAME, 7)
            .addRacks()
            .build();

        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new HashMap<>();
        members.put(MEMBER_A, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(TOPIC_1_UUID, TOPIC_2_UUID, TOPIC_3_UUID),
            Assignment.EMPTY
        ));
        members.put(MEMBER_B, new MemberSubscriptionAndAssignmentImpl(
            // We want there to be multiple valid assignments, otherwise we aren't really
            // testing stickiness. Only give a single member a rack, so that the other members
            // are interchangeable.
            rackAware ? Optional.of("rack1") : Optional.empty(),
            Optional.empty(),
            Set.of(TOPIC_1_UUID, TOPIC_2_UUID, TOPIC_3_UUID),
            Assignment.EMPTY
        ));
        members.put(MEMBER_C, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(TOPIC_1_UUID, TOPIC_2_UUID, TOPIC_3_UUID),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            subscriptionType,
            Map.of()
        );

        GroupAssignment firstAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        firstAssignment.members().forEach((memberId, memberAssignment) ->
            expectedAssignment.put(memberId, memberAssignment.partitions())
        );

        // Try running the assignor with the members in different orders. The assignment should be
        // the same every time.
        List<List<String>> memberIdOrders = List.of(
            List.of(MEMBER_A, MEMBER_B, MEMBER_C),
            List.of(MEMBER_A, MEMBER_C, MEMBER_B),
            List.of(MEMBER_B, MEMBER_A, MEMBER_C),
            List.of(MEMBER_B, MEMBER_C, MEMBER_A),
            List.of(MEMBER_C, MEMBER_A, MEMBER_B),
            List.of(MEMBER_C, MEMBER_B, MEMBER_A)
        );
        for (List<String> memberIdOrder : memberIdOrders) {
            Map<String, MemberSubscriptionAndAssignmentImpl> membersWithAssignment = new LinkedHashMap<>();
            for (String memberId : memberIdOrder) {
                MemberSubscriptionAndAssignmentImpl memberSubscriptionAndAssignment = members.get(memberId);
                membersWithAssignment.put(memberId, new MemberSubscriptionAndAssignmentImpl(
                    memberSubscriptionAndAssignment.rackId(),
                    memberSubscriptionAndAssignment.instanceId(),
                    memberSubscriptionAndAssignment.subscribedTopicIds(),
                    new Assignment(firstAssignment.members().get(memberId).partitions())
                ));
            }
            GroupSpec groupSpecWithAssignment = new GroupSpecImpl(
                membersWithAssignment,
                subscriptionType,
                invertedTargetAssignment(membersWithAssignment)
            );

            GroupAssignment secondAssignment = assignor.assign(
                groupSpecWithAssignment,
                subscribedTopicMetadata
            );

            // The second assignment should be the same as the first
            assertAssignment(expectedAssignment, secondAssignment);
        }
    }
}
