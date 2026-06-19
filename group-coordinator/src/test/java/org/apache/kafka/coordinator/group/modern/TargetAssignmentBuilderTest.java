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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.TargetAssignmentMetadata;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignor;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HETEROGENEOUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TargetAssignmentBuilderTest {

    @Test
    public void testAssignment() {
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, "foo", 6)
            .addTopic(barTopicId, "bar", 6)
            .buildCoordinatorMetadataImage();

        GroupSpec groupSpec = new GroupSpecImpl(
            Map.of(
                "member-1", new MemberSubscriptionAndAssignmentImpl(
                    Optional.empty(),
                    Optional.empty(),
                    new TopicIds(Set.of("foo", "bar", "zar"), metadataImage),
                    Assignment.EMPTY
                ),
                "member-2", new MemberSubscriptionAndAssignmentImpl(
                    Optional.of("rack-2"),
                    Optional.empty(),
                    new TopicIds(Set.of("foo", "bar", "zar"), metadataImage),
                    Assignment.EMPTY
                ),
                "member-3", new MemberSubscriptionAndAssignmentImpl(
                    Optional.empty(),
                    Optional.of("instance-3"),
                    new TopicIds(Set.of("bar", "zar"), metadataImage),
                    Assignment.EMPTY
                )
            ),
            HETEROGENEOUS,
            Map.of(
                fooTopicId, Map.of(
                    1, "member-1",
                    2, "member-1",
                    3, "member-2",
                    4, "member-2",
                    5, "member-3",
                    6, "member-3"
                ),
                barTopicId, Map.of(
                    1, "member-1",
                    2, "member-1",
                    3, "member-2",
                    4, "member-2",
                    5, "member-3",
                    6, "member-3"
                )
            )
        );

        GroupAssignment groupAssignment = new GroupAssignment(Map.of(
            "member-1", new MemberAssignmentImpl(Map.of(
                fooTopicId, Set.of(1, 2),
                barTopicId, Set.of(1, 2)
            )),
            "member-2", new MemberAssignmentImpl(Map.of(
                fooTopicId, Set.of(3, 4, 5),
                barTopicId, Set.of(3, 4, 5)
            )),
            "member-3", new MemberAssignmentImpl(Map.of(
                fooTopicId, Set.of(6),
                barTopicId, Set.of(6)
            ))
        ));

        // We use `any` here to always return an assignment but use `verify` later on
        // to ensure that the input was correct.
        PartitionAssignor assignor = mock(PartitionAssignor.class);
        when(assignor.assign(any(), any()))
            .thenReturn(groupAssignment);

        // Create and populate the assignment builder.
        TargetAssignmentBuilder builder = new TargetAssignmentBuilder(20, assignor)
            .withTime(new MockTime(0, 12345L, 12345L))
            .withMetadataImage(metadataImage)
            .withGroupSpec(groupSpec);

        // Execute the builder.
        TargetAssignmentBuilder.TargetAssignmentResult result = builder.build();

        // Verify that the assignor was called once with the expected
        // assignment spec.
        verify(assignor, times(1))
            .assign(groupSpec, new SubscribedTopicDescriberImpl(metadataImage));

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
        assertEquals(new TargetAssignmentMetadata(20, 12345L), result.targetAssignmentMetadata());
    }
}
