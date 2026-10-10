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
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.assignor.uniform2.GroupSpecFixture;
import org.apache.kafka.coordinator.group.assignor.uniform2.TopicsFixture;
import org.apache.kafka.coordinator.group.modern.SubscribedTopicDescriberImpl;
import org.apache.kafka.coordinator.group.modern.TopicIds;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.assertAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.assertStable;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.assertValidAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * End to end tests of the uniform2 assignor, with expectations traced by hand.
 *
 * <p>The topic ids sort as T1 &lt; T2 &lt; T3, which is the order in which the assignor hands
 * out their extra partitions when they have the same number of subscribers.
 */
public class Uniform2AssignorTest {
    private static final Uuid T1 = new Uuid(1L, 1L);
    private static final Uuid T2 = new Uuid(2L, 1L);
    private static final Uuid T3 = new Uuid(3L, 1L);
    private static final Uuid MISSING = new Uuid(9L, 1L);

    private final Uniform2Assignor assignor = new Uniform2Assignor();

    @Test
    public void testName() {
        assertEquals("uniform2", assignor.name());
    }

    @Test
    public void testEmptyGroup() {
        var result = assignor.assign(new GroupSpecFixture().build(), new TopicsFixture().withTopic(T1, 3).build());
        assertEquals(Map.of(), result.members());
    }

    @Test
    public void testMissingAndUnsubscribedTopicsAreIgnored() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(MISSING))
            .withMember("B", Set.of(MISSING))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of("A", Map.of(), "B", Map.of()), result);
    }

    /**
     * T1 has 2 partitions, so no base partition and 2 extra partitions, T2 has 5, so 1 base
     * partition and 2 extra ones, and T3 has 7, so 2 base partitions and 1 extra one. The 5 extra
     * partitions go round robin: T1 to A and B, T2 to C and A, T3 to B. The members below their
     * share then take the partitions of every topic in partition order.
     */
    @Test
    public void testHomogeneousSpreadsEveryTopic() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3))
            .withMember("B", Set.of(T1, T2, T3))
            .withMember("C", Set.of(T1, T2, T3))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 2)
            .withTopic(T2, 5)
            .withTopic(T3, 7)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", mkAssignment(mkTopicAssignment(T1, 0), mkTopicAssignment(T2, 0, 1), mkTopicAssignment(T3, 0, 1)),
            "B", mkAssignment(mkTopicAssignment(T1, 1), mkTopicAssignment(T2, 2), mkTopicAssignment(T3, 2, 3, 4)),
            "C", mkAssignment(mkTopicAssignment(T2, 3, 4), mkTopicAssignment(T3, 5, 6))
        ), result);
        assertValidAssignment(spec, describer, result);
        assertStable(spec, describer, result, assignor);
    }

    /**
     * C leaves the assignment of the previous test. T1 now gives 1 base partition to A and B,
     * which they hold. T2 has 2 base partitions and 1 extra one, T3 has 3 base partitions and 1
     * extra one, and neither A nor B owns more than the base partitions of them, so their extra
     * partitions are handed out: T2's to A, T3's to B. A and B then take the partitions of C in
     * partition order, and nothing moves between them.
     */
    @Test
    public void testLeaveOnlyMovesThePartitionsOfTheLeaver() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3), mkAssignment(
                mkTopicAssignment(T1, 0), mkTopicAssignment(T2, 0, 1), mkTopicAssignment(T3, 0, 1)))
            .withMember("B", Set.of(T1, T2, T3), mkAssignment(
                mkTopicAssignment(T1, 1), mkTopicAssignment(T2, 2), mkTopicAssignment(T3, 2, 3, 4)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 2)
            .withTopic(T2, 5)
            .withTopic(T3, 7)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", mkAssignment(mkTopicAssignment(T1, 0), mkTopicAssignment(T2, 0, 1, 3), mkTopicAssignment(T3, 0, 1, 5)),
            "B", mkAssignment(mkTopicAssignment(T1, 1), mkTopicAssignment(T2, 2, 4), mkTopicAssignment(T3, 2, 3, 4, 6))
        ), result);
        assertValidAssignment(spec, describer, result);
    }

    /**
     * A subscribes to T1 and T2, B to T2 only. T1, 4 partitions, has a single subscriber, so A
     * gets all of them as base partitions. T2, 3 partitions, gives one base partition to each,
     * and its extra partition goes to B, whose assignment is smaller.
     */
    @Test
    public void testHeterogeneousBalancesAcrossTopics() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2))
            .withMember("B", Set.of(T2))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 3)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", mkAssignment(mkTopicAssignment(T1, 0, 1, 2, 3), mkTopicAssignment(T2, 0)),
            "B", mkAssignment(mkTopicAssignment(T2, 1, 2))
        ), result);
        assertValidAssignment(spec, describer, result);
        assertStable(spec, describer, result, assignor);
    }

    /**
     * A subscribes to T1 and T2, B to T1 and C to T2, of 3 partitions each: 1 base partition per
     * subscriber and 1 extra partition per topic. A owns 2 partitions of both, B and C one each, so
     * A keeps both extra partitions, and has 4 partitions against 1. The balance step then moves
     * the extra partition of T1 to B and the one of T2 to C: A gives up one partition of each, the
     * fewest revocations for sizes within one.
     */
    @Test
    public void testTheBalanceMovesExtraPartitionsTheirOwnerKept() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T1, 0, 1), mkTopicAssignment(T2, 0, 1)))
            .withMember("B", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 2)))
            .withMember("C", Set.of(T2), mkAssignment(mkTopicAssignment(T2, 2)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .withTopic(T2, 3)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", mkAssignment(mkTopicAssignment(T1, 0), mkTopicAssignment(T2, 0)),
            "B", mkAssignment(mkTopicAssignment(T1, 1, 2)),
            "C", mkAssignment(mkTopicAssignment(T2, 1, 2))
        ), result);
        assertValidAssignment(spec, describer, result);
        assertStable(spec, describer, result, assignor);
    }

    /**
     * An assignment that already has the properties is kept as it is, even though the assignor
     * would not have computed it from scratch. T1 gives 2 base partitions to each member. T2 gives
     * 1 base partition to each and has 1 extra partition, which A keeps since it owns 2 partitions
     * of T2. The sizes are 4 and 3. From scratch, A would get partitions 0 and 1 of T1 rather than
     * 2 and 3.
     */
    @Test
    public void testAdoptsAnAssignmentWithTheProperties() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T1, 2, 3), mkTopicAssignment(T2, 1, 2)))
            .withMember("B", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T1, 0, 1), mkTopicAssignment(T2, 0)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 3)
            .build();

        var result = assignor.assign(spec, describer);

        for (String id : spec.memberIds()) {
            assertSame(spec.memberAssignment(id).partitions(), result.members().get(id).partitions());
        }
    }

    /**
     * A holds a partition of a topic it no longer subscribes to, a partition of a topic which
     * no longer exists, and a partition beyond the partition count. They are dropped. T1 gives one
     * partition to each of A and B: A keeps partition 0 and gives 1 to B, which also gets the
     * single partition of T2.
     */
    @Test
    public void testStalePartitionsAreDropped() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), mkAssignment(
                mkTopicAssignment(T1, 0, 1, 7), mkTopicAssignment(T2, 0), mkTopicAssignment(MISSING, 0)))
            .withMember("B", Set.of(T1, T2))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 2)
            .withTopic(T2, 1)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", mkAssignment(mkTopicAssignment(T1, 0)),
            "B", mkAssignment(mkTopicAssignment(T1, 1), mkTopicAssignment(T2, 0))
        ), result);
        assertValidAssignment(spec, describer, result);
    }

    /**
     * The subscriptions are given as the coordinator gives them: topic names resolved lazily
     * against the metadata image, one of them unknown. They are the same for every member, so the
     * group is homogeneous, and the assignment is the one of the same topics given by id.
     */
    @Test
    public void testSubscriptionsAsTopicIds() {
        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(T1, "t1", 2)
            .addTopic(T2, "t2", 5)
            .addTopic(T3, "t3", 7)
            .buildCoordinatorMetadataImage();
        var resolver = new TopicIds.CachedTopicResolver(image);
        var spec = new GroupSpecFixture()
            .withMember("A", new TopicIds(Set.of("t1", "t2", "t3", "unknown"), resolver))
            .withMember("B", new TopicIds(Set.of("t1", "t2", "t3", "unknown"), resolver))
            .withMember("C", new TopicIds(Set.of("t1", "t2", "t3", "unknown"), resolver))
            .build();
        var describer = new SubscribedTopicDescriberImpl(image);

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", mkAssignment(mkTopicAssignment(T1, 0), mkTopicAssignment(T2, 0, 1), mkTopicAssignment(T3, 0, 1)),
            "B", mkAssignment(mkTopicAssignment(T1, 1), mkTopicAssignment(T2, 2), mkTopicAssignment(T3, 2, 3, 4)),
            "C", mkAssignment(mkTopicAssignment(T2, 3, 4), mkTopicAssignment(T3, 5, 6))
        ), result);
    }

    @ParameterizedTest
    @EnumSource(SubscriptionType.class)
    public void testAssignmentReuse(SubscriptionType subscriptionType) {
        CommonAssignorTests.testAssignmentReuse(assignor, subscriptionType, false);
    }

    @ParameterizedTest
    @EnumSource(SubscriptionType.class)
    public void testReassignmentStickiness(SubscriptionType subscriptionType) {
        CommonAssignorTests.testReassignmentStickiness(assignor, subscriptionType, false);
    }

    /**
     * The assignment does not depend on the order of the members, of the topics of their
     * subscriptions, nor of the entries of their current assignment.
     */
    @Test
    public void testOrderIndependence() {
        var describer = new TopicsFixture()
            .withTopic(T1, 2)
            .withTopic(T2, 5)
            .withTopic(T3, 7)
            .build();
        var forward = new GroupSpecFixture()
            .withMember("A", new LinkedHashSet<>(List.of(T1, T2, T3)), mkAssignment(mkTopicAssignment(T3, 0, 1, 2, 3)))
            .withMember("B", new LinkedHashSet<>(List.of(T1, T2)))
            .withMember("C", new LinkedHashSet<>(List.of(T2, T3)))
            .build();
        var backward = new GroupSpecFixture()
            .withMember("C", new LinkedHashSet<>(List.of(T3, T2)))
            .withMember("B", new LinkedHashSet<>(List.of(T2, T1)))
            .withMember("A", new LinkedHashSet<>(List.of(T3, T2, T1)), mkAssignment(mkTopicAssignment(T3, 3, 2, 1, 0)))
            .build();

        var first = assignor.assign(forward, describer);
        var second = assignor.assign(backward, describer);

        assertEquals(first, second);
        assertValidAssignment(forward, describer, first);
    }

    /**
     * A single member joining a stable homogeneous group only takes its share: with 3 topics of 6
     * partitions and 2 members, A has partitions 0 to 2 of each topic and B 3 to 5; with a third
     * member, each has 2, so A and B keep their 2 lowest partitions of each topic and give the
     * third one to the joiner, and nothing else moves.
     */
    @Test
    public void testJoinOnlyMovesTheIntakeOfTheJoiner() {
        var describer = new TopicsFixture()
            .withTopic(T1, 6)
            .withTopic(T2, 6)
            .withTopic(T3, 6)
            .build();
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3))
            .withMember("B", Set.of(T1, T2, T3))
            .build();
        var before = assignor.assign(spec, describer);
        var joined = GroupSpecFixture.after(spec, before)
            .withMember("C", Set.of(T1, T2, T3))
            .build();

        var after = assignor.assign(joined, describer);

        assertAssignment(Map.of(
            "A", mkAssignment(mkTopicAssignment(T1, 0, 1), mkTopicAssignment(T2, 0, 1), mkTopicAssignment(T3, 0, 1)),
            "B", mkAssignment(mkTopicAssignment(T1, 3, 4), mkTopicAssignment(T2, 3, 4), mkTopicAssignment(T3, 3, 4)),
            "C", mkAssignment(mkTopicAssignment(T1, 2, 5), mkTopicAssignment(T2, 2, 5), mkTopicAssignment(T3, 2, 5))
        ), after);
        assertValidAssignment(joined, describer, after);
    }
}
