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
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.modern.consumer.ResolvedRegularExpression;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HETEROGENEOUS;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class GroupSpecBuilderTest {

    @Test
    public void testEmpty() {
        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .buildCoordinatorMetadataImage();

        GroupSpecBuilder.ConsumerGroupSpecBuilder builder =
            new GroupSpecBuilder.ConsumerGroupSpecBuilder()
                .withMembers(Map.of())
                .withSubscriptionType(HOMOGENEOUS)
                .withTargetAssignment(Map.of())
                .withInvertedTargetAssignment(Map.of())
                .withMetadataImage(metadataImage)
                .withResolvedRegularExpressions(Map.of());

        assertEquals(
            new GroupSpecImpl(
                Map.of(),
                HOMOGENEOUS,
                Map.of()
            ),
            builder.build()
        );
    }

    @Test
    public void testGroupSpec() {
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, "foo", 6)
            .addTopic(barTopicId, "bar", 6)
            .buildCoordinatorMetadataImage();

        TopicIds.TopicResolver topicResolver = new TopicIds.CachedTopicResolver(metadataImage);

        Assignment assignment1 = new Assignment(Map.of(
            fooTopicId, Set.of(1, 2),
            barTopicId, Set.of(1, 2)
        ));
        Assignment assignment2 = new Assignment(Map.of(
            fooTopicId, Set.of(3, 4),
            barTopicId, Set.of(3, 4)
        ));
        Assignment assignment3 = new Assignment(Map.of(
            fooTopicId, Set.of(5, 6),
            barTopicId, Set.of(5, 6)
        ));

        GroupSpecBuilder.ConsumerGroupSpecBuilder builder =
            new GroupSpecBuilder.ConsumerGroupSpecBuilder()
                .withMembers(Map.of(
                    "member-1", new ConsumerGroupMember.Builder("member-1")
                        .setSubscribedTopicNames(Arrays.asList("foo", "bar", "zar"))
                        .build(),
                    "member-2", new ConsumerGroupMember.Builder("member-2")
                        .setRackId("rack-2")
                        .setSubscribedTopicNames(Arrays.asList("foo", "bar", "zar"))
                        .build(),
                    "member-3", new ConsumerGroupMember.Builder("member-3")
                        .setInstanceId("instance-3")
                        .setSubscribedTopicNames(Arrays.asList("bar", "zar"))
                        .build()
                ))
                .withSubscriptionType(HETEROGENEOUS)
                .withTargetAssignment(Map.of(
                    "member-1", assignment1,
                    "member-2", assignment2,
                    "member-3", assignment3
                ))
                .withInvertedTargetAssignment(Map.of(
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
                ))
                .withMetadataImage(metadataImage)
                .withResolvedRegularExpressions(Map.of());

        assertEquals(
            new GroupSpecImpl(
                Map.of(
                    "member-1", new MemberSubscriptionAndAssignmentImpl(
                        Optional.empty(),
                        Optional.empty(),
                        new TopicIds(Set.of("foo", "bar", "zar"), topicResolver),
                        assignment1
                    ),
                    "member-2", new MemberSubscriptionAndAssignmentImpl(
                        Optional.of("rack-2"),
                        Optional.empty(),
                        new TopicIds(Set.of("foo", "bar", "zar"), topicResolver),
                        assignment2
                    ),
                    "member-3", new MemberSubscriptionAndAssignmentImpl(
                        Optional.empty(),
                        Optional.of("instance-3"),
                        new TopicIds(Set.of("bar", "zar"), topicResolver),
                        assignment3
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
            ),
            builder.build()
        );
    }

    @Test
    public void testRegularExpressions() {
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, "foo", 6)
            .addTopic(barTopicId, "bar", 6)
            .buildCoordinatorMetadataImage();

        TopicIds.TopicResolver topicResolver = new TopicIds.CachedTopicResolver(metadataImage);

        GroupSpecBuilder.ConsumerGroupSpecBuilder builder =
            new GroupSpecBuilder.ConsumerGroupSpecBuilder()
                .withMembers(Map.of(
                    "member-1", new ConsumerGroupMember.Builder("member-1")
                        .setSubscribedTopicNames(Arrays.asList("bar", "zar"))
                        .build(),
                    "member-2", new ConsumerGroupMember.Builder("member-2")
                        .setSubscribedTopicNames(Arrays.asList("foo", "bar", "zar"))
                        .build(),
                    "member-3", new ConsumerGroupMember.Builder("member-3")
                        .setSubscribedTopicRegex("foo*")
                        .build()
                ))
                .withSubscriptionType(HETEROGENEOUS)
                .withTargetAssignment(Map.of(
                    "member-1", Assignment.EMPTY,
                    "member-2", Assignment.EMPTY,
                    "member-3", Assignment.EMPTY
                ))
                .withInvertedTargetAssignment(Map.of())
                .withMetadataImage(metadataImage)
                .withResolvedRegularExpressions(Map.of(
                    "foo*", new ResolvedRegularExpression(
                        Set.of("foo"),
                        10L,
                        12345L
                    )
                ));

        assertEquals(
            new GroupSpecImpl(
                Map.of(
                    "member-1", new MemberSubscriptionAndAssignmentImpl(
                        Optional.empty(),
                        Optional.empty(),
                        new TopicIds(Set.of("bar", "zar"), topicResolver),
                        Assignment.EMPTY
                    ),
                    "member-2", new MemberSubscriptionAndAssignmentImpl(
                        Optional.empty(),
                        Optional.empty(),
                        new TopicIds(Set.of("foo", "bar", "zar"), topicResolver),
                        Assignment.EMPTY
                    ),
                    "member-3", new MemberSubscriptionAndAssignmentImpl(
                        Optional.empty(),
                        Optional.empty(),
                        new TopicIds(Set.of("foo"), topicResolver),
                        Assignment.EMPTY
                    )
                ),
                HETEROGENEOUS,
                Map.of()
            ),
            builder.build()
        );
    }
}
