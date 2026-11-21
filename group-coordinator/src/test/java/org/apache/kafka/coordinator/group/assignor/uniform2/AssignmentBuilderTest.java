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
package org.apache.kafka.coordinator.group.assignor.uniform2;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.assignor.RangeSet;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.assertAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests of how {@link AssignmentBuilder} builds the result: which maps and sets of the current
 * assignment it reuses, and which new sets it creates. A topic with {@code P} partitions and
 * {@code N} subscribers has a minimum quota of {@code P / N} and {@code P % N} extra partitions.
 */
public class AssignmentBuilderTest {
    private static final Uuid T1 = new Uuid(1L, 1L);
    private static final Uuid T2 = new Uuid(2L, 1L);
    private static final Uuid T3 = new Uuid(3L, 1L);
    private static final Uuid MISSING = new Uuid(9L, 1L);

    /**
     * T1 has 6 partitions and 3 subscribers, so a minimum quota of 2 and no extra partition. A owns
     * partitions 0 and 1, exactly its quota, and keeps its map. B owns partitions 2 to 5 and keeps
     * 2 and 3, while C gets 4 and 5. Assigned again, the result keeps every map.
     */
    @Test
    public void testUnchangedMemberKeepsItsMap() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), Map.of(T1, Set.of(0, 1)))
            .withMember("B", Set.of(T1), Map.of(T1, Set.of(2, 3, 4, 5)))
            .withMember("C", Set.of(T1))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 6)
            .build();

        var result = new AssignmentBuilder(spec, describer).build();

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0, 1)),
            "B", Map.of(T1, Set.of(2, 3)),
            "C", Map.of(T1, Set.of(4, 5))
        ), result);
        assertSame(spec.memberAssignment("A").partitions(), result.members().get("A").partitions());

        var fedBack = GroupSpecFixture.after(spec, result).build();
        var again = new AssignmentBuilder(fedBack, describer).build();
        for (String id : fedBack.memberIds()) {
            assertSame(fedBack.memberAssignment(id).partitions(), again.members().get(id).partitions(), id);
        }
    }

    /**
     * A member that changes on one topic gets a new map, which keeps its sets of the topics it does
     * not change on, whether partitions of these topics move or not. A subscribes to T1, T2 and T3,
     * and B and C to T1 and T2. T1, of 6 partitions, has a minimum quota of 2: A keeps partitions 0
     * and 1 of the 4 it owns, and C gets 2 and 3. T2, of 3 partitions, has a minimum quota of 1: A
     * keeps partition 0, B keeps 1, and C gets 2, which B gives up. Nothing moves on T3, which only
     * A subscribes to. A changes on T1 only, so its new map has the same sets of T2 and T3 that A
     * holds, and B changes on T2 only, so its new map has the same set of T1 that B holds.
     */
    @Test
    public void testChangedMemberSharesTheSetsOfItsUnchangedTopics() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3), Map.of(T1, Set.of(0, 1, 2, 3), T2, Set.of(0), T3, Set.of(0, 1)))
            .withMember("B", Set.of(T1, T2), Map.of(T1, Set.of(4, 5), T2, Set.of(1, 2)))
            .withMember("C", Set.of(T1, T2))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 6)
            .withTopic(T2, 3)
            .withTopic(T3, 2)
            .build();

        var result = new AssignmentBuilder(spec, describer).build();

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0, 1), T2, Set.of(0), T3, Set.of(0, 1)),
            "B", Map.of(T1, Set.of(4, 5), T2, Set.of(1)),
            "C", Map.of(T1, Set.of(2, 3), T2, Set.of(2))
        ), result);
        assertSame(spec.memberAssignment("A").partitions().get(T2), result.members().get("A").partitions().get(T2));
        assertSame(spec.memberAssignment("A").partitions().get(T3), result.members().get("A").partitions().get(T3));
        assertSame(spec.memberAssignment("B").partitions().get(T1), result.members().get("B").partitions().get(T1));
    }

    /**
     * T1 has 6 partitions and 3 subscribers, so a minimum quota of 2. A owns partitions 0 to 2 and
     * keeps 0 and 1, B owns 3 to 5 and keeps 3 and 4, and C gets partitions 2 and 5. The new sets
     * of A and B are consecutive, and that of C is not.
     */
    @Test
    public void testNewSetsAreRangeSetsWhenConsecutive() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), Map.of(T1, Set.of(0, 1, 2)))
            .withMember("B", Set.of(T1), Map.of(T1, Set.of(3, 4, 5)))
            .withMember("C", Set.of(T1))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 6)
            .build();

        var result = new AssignmentBuilder(spec, describer).build();

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0, 1)),
            "B", Map.of(T1, Set.of(3, 4)),
            "C", Map.of(T1, Set.of(2, 5))
        ), result);
        assertEquals(Map.of(
            "A", Map.of(T1, RangeSet.class),
            "B", Map.of(T1, RangeSet.class),
            "C", Map.of(T1, HashSet.class)
        ), setClasses(result));
    }

    /**
     * T1 has 6 partitions and 3 subscribers, so a minimum quota of 2. A owns partitions 0, 2, 3 and
     * 5, and keeps 0 and 2, which are not consecutive. B gets 1 and 3, which are not consecutive
     * either, and C gets 4 and 5.
     */
    @Test
    public void testNewSetsAreHashSetsWhenNotConsecutive() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), Map.of(T1, Set.of(0, 2, 3, 5)))
            .withMember("B", Set.of(T1))
            .withMember("C", Set.of(T1))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 6)
            .build();

        var result = new AssignmentBuilder(spec, describer).build();

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0, 2)),
            "B", Map.of(T1, Set.of(1, 3)),
            "C", Map.of(T1, Set.of(4, 5))
        ), result);
        assertEquals(Map.of(
            "A", Map.of(T1, HashSet.class),
            "B", Map.of(T1, HashSet.class),
            "C", Map.of(T1, RangeSet.class)
        ), setClasses(result));
    }

    /**
     * T1 and T2 have 6 partitions each over A, B and C, so a minimum quota of 2 and no extra
     * partition. A owns partitions 0, 2, 3 and 5 of both and keeps 0 and 2, B gets 1 and 3 of both,
     * and C gets 4 and 5 of both. The new sets are the same for both topics, as nothing of T1
     * carries over to T2.
     */
    @Test
    public void testNothingCarriesOverFromOneTopicToTheNext() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2), Map.of(T1, Set.of(0, 2, 3, 5), T2, Set.of(0, 2, 3, 5)))
            .withMember("B", Set.of(T1, T2))
            .withMember("C", Set.of(T1, T2))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 6)
            .withTopic(T2, 6)
            .build();

        var result = new AssignmentBuilder(spec, describer).build();

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0, 2), T2, Set.of(0, 2)),
            "B", Map.of(T1, Set.of(1, 3), T2, Set.of(1, 3)),
            "C", Map.of(T1, Set.of(4, 5), T2, Set.of(4, 5))
        ), result);
        assertEquals(Map.of(
            "A", Map.of(T1, HashSet.class, T2, HashSet.class),
            "B", Map.of(T1, HashSet.class, T2, HashSet.class),
            "C", Map.of(T1, RangeSet.class, T2, RangeSet.class)
        ), setClasses(result));
    }

    /**
     * A subscribes to T1, and B to T1 and T2. T1 has 4 partitions over its 2 subscribers, so a
     * minimum quota of 2, which they own, and T1 does not change. A also holds partition 0 of T2,
     * which it does not subscribe to, and a partition of the missing topic, so it gets a new map
     * with only its set of T1. B gets partition 0 of T2, which nobody owns, in a new map sharing
     * its set of T1.
     */
    @Test
    public void testStalePartitionsAreDroppedFromAMemberWhoseTopicsDoNotChange() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), Map.of(T1, Set.of(0, 1), T2, Set.of(0), MISSING, Set.of(0)))
            .withMember("B", Set.of(T1, T2), Map.of(T1, Set.of(2, 3)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 1)
            .build();

        var result = new AssignmentBuilder(spec, describer).build();

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0, 1)),
            "B", Map.of(T1, Set.of(2, 3), T2, Set.of(0))
        ), result);
        assertNotSame(spec.memberAssignment("A").partitions(), result.members().get("A").partitions());
        assertSame(spec.memberAssignment("A").partitions().get(T1), result.members().get("A").partitions().get(T1));
        assertSame(spec.memberAssignment("B").partitions().get(T1), result.members().get("B").partitions().get(T1));
    }

    /**
     * A and B subscribe to T1, which has a single partition, so a minimum quota of 0 and 1 extra
     * partition, which B keeps as it owns the partition. A holds only stale partitions, a partition
     * of T2, which exists but nobody subscribes to, and a partition of the missing topic, so it
     * gets a new, empty map.
     */
    @Test
    public void testStalePartitionsAreDroppedFromAMemberHoldingOnlyThem() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), Map.of(T2, Set.of(0), MISSING, Set.of(0)))
            .withMember("B", Set.of(T1), Map.of(T1, Set.of(0)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 1)
            .withTopic(T2, 1)
            .build();

        var result = new AssignmentBuilder(spec, describer).build();

        assertAssignment(Map.of(
            "A", Map.of(),
            "B", Map.of(T1, Set.of(0))
        ), result);
        assertNotSame(spec.memberAssignment("A").partitions(), result.members().get("A").partitions());
        assertSame(spec.memberAssignment("B").partitions(), result.members().get("B").partitions());
    }

    /**
     * A subscribes to T1 and T2, and B to T1. T1 has a single partition, so a minimum quota of 0
     * and 1 extra partition, which A keeps as it owns the partition. T2 has 2 partitions and the
     * single subscriber A, which owns both. The assignment size of A is then 3 and that of B is 0,
     * so the balance step moves the extra partition of T1 to B. A loses all the partitions of T1,
     * so its new map has no entry for T1, but it has the same set of T2 that A holds.
     */
    @Test
    public void testMemberLosingAllThePartitionsOfATopicHasNoEntryForIt() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2), Map.of(T1, Set.of(0), T2, Set.of(0, 1)))
            .withMember("B", Set.of(T1))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 1)
            .withTopic(T2, 2)
            .build();

        var result = new AssignmentBuilder(spec, describer).build();

        assertAssignment(Map.of(
            "A", Map.of(T2, Set.of(0, 1)),
            "B", Map.of(T1, Set.of(0))
        ), result);
        assertSame(spec.memberAssignment("A").partitions().get(T2), result.members().get("A").partitions().get(T2));
    }

    /**
     * @return Per member, the class of its set of partitions of every topic.
     */
    private static Map<String, Map<Uuid, Class<?>>> setClasses(GroupAssignment result) {
        var classes = new HashMap<String, Map<Uuid, Class<?>>>();
        result.members().forEach((memberId, assignment) -> {
            var topicClasses = new HashMap<Uuid, Class<?>>();
            assignment.partitions().forEach((topicId, partitions) -> topicClasses.put(topicId, partitions.getClass()));
            classes.put(memberId, topicClasses);
        });
        return classes;
    }
}
