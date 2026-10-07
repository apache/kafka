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
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests of how {@link AssignmentBuilder} builds the result, without rack awareness: which maps
 * and sets it reuses from the current assignment, and which new sets it creates.
 *
 * <p>The member ids sort as A &lt; B &lt; C and the topic ids as T1 &lt; T2. The missing topic
 * does not exist. A topic with {@code P} partitions and {@code N} subscribers gives {@code P / N}
 * base partitions to each of them and {@code P % N} extra partitions.
 */
public class AssignmentBuilderTest {
    private static final Uuid T1 = new Uuid(1L, 1L);
    private static final Uuid T2 = new Uuid(2L, 1L);
    private static final Uuid MISSING = new Uuid(9L, 1L);

    /**
     * T1 has 6 partitions and 3 subscribers: 2 base partitions each, without extra partitions. A
     * owns partitions 0 and 1, exactly its share, and keeps its very map. B owns partitions 2 to 5
     * and keeps 2 and 3, while C gets 4 and 5. Fed back, the result keeps every map.
     */
    @Test
    public void testUnchangedMemberKeepsItsMap() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 0, 1)))
            .withMember("B", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 2, 3, 4, 5)))
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
        assertSame(spec.memberAssignment("A").partitions(), partitions(result, "A"));

        var fedBack = GroupSpecFixture.after(spec, result).build();
        var again = new AssignmentBuilder(fedBack, describer).build();
        for (String id : fedBack.memberIds()) {
            assertSame(fedBack.memberAssignment(id).partitions(), partitions(again, id), id);
        }
    }

    /**
     * A and C subscribe to T1, and B to T1 and T2. T1 has 6 partitions: 2 base partitions for
     * each of its 3 subscribers. T2 has 2 partitions and the single subscriber B, which owns both.
     * B gives up partitions 4 and 5 of T1 to C, so it gets a new map, but T2 does not change: the
     * new map has the very set of T2 that B holds.
     */
    @Test
    public void testChangedMemberSharesTheSetsOfItsUnchangedTopics() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 0, 1)))
            .withMember("B", Set.of(T1, T2), mkAssignment(
                mkTopicAssignment(T1, 2, 3, 4, 5), mkTopicAssignment(T2, 0, 1)))
            .withMember("C", Set.of(T1))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 6)
            .withTopic(T2, 2)
            .build();

        var result = new AssignmentBuilder(spec, describer).build();

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0, 1)),
            "B", Map.of(T1, Set.of(2, 3), T2, Set.of(0, 1)),
            "C", Map.of(T1, Set.of(4, 5))
        ), result);
        assertSame(spec.memberAssignment("A").partitions(), partitions(result, "A"));
        assertNotSame(spec.memberAssignment("B").partitions(), partitions(result, "B"));
        assertSame(spec.memberAssignment("B").partitions().get(T2), partitions(result, "B").get(T2));
    }

    /**
     * T1 has 6 partitions and 3 subscribers: 2 base partitions each. A owns partitions 0 to 2 and
     * keeps 0 and 1, B owns 3 to 5 and keeps 3 and 4, and C gets partitions 2 and 5. The new sets
     * of A and B are consecutive, and that of C is not.
     */
    @Test
    public void testNewSetsAreRangeSetsWhenConsecutive() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 0, 1, 2)))
            .withMember("B", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 3, 4, 5)))
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
        assertEquals(Map.of("A", RangeSet.class, "B", RangeSet.class, "C", HashSet.class), setClasses(result, T1, "A", "B", "C"));
    }

    /**
     * T1 has 6 partitions and 3 subscribers: 2 base partitions each. A owns partitions 0, 2, 3
     * and 5, and keeps 0 and 2, which are not consecutive. B owns 1 and 4, its share, and keeps
     * its map. C gets 3 and 5.
     */
    @Test
    public void testNewSetsAreHashSetsWhenNotConsecutive() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 0, 2, 3, 5)))
            .withMember("B", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 1, 4)))
            .withMember("C", Set.of(T1))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 6)
            .build();

        var result = new AssignmentBuilder(spec, describer).build();

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0, 2)),
            "B", Map.of(T1, Set.of(1, 4)),
            "C", Map.of(T1, Set.of(3, 5))
        ), result);
        assertSame(spec.memberAssignment("B").partitions(), partitions(result, "B"));
        assertEquals(Map.of("A", HashSet.class, "C", HashSet.class), setClasses(result, T1, "A", "C"));
    }

    /**
     * A subscribes to T1, and B to T1 and T2. T1 has 4 partitions: 2 base partitions for each of
     * its 2 subscribers, which they own, so T1 does not change. A also holds partition 0 of T2,
     * which it does not subscribe to, and a partition of the missing topic: it gets a new map with
     * only its set of T1. B gets partition 0 of T2, which nobody owns, in a new map sharing its
     * set of T1.
     */
    @Test
    public void testStalePartitionsAreDroppedFromAMemberWhoseTopicsDoNotChange() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), mkAssignment(
                mkTopicAssignment(T1, 0, 1), mkTopicAssignment(T2, 0), mkTopicAssignment(MISSING, 0)))
            .withMember("B", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T1, 2, 3)))
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
        assertNotSame(spec.memberAssignment("A").partitions(), partitions(result, "A"));
        assertSame(spec.memberAssignment("A").partitions().get(T1), partitions(result, "A").get(T1));
        assertSame(spec.memberAssignment("B").partitions().get(T1), partitions(result, "B").get(T1));
    }

    /**
     * A and B subscribe to T1, which has a single partition: no base partition and 1 extra
     * partition, which B keeps as it owns the partition. A holds only stale partitions: an empty
     * set of T1, a partition of T2, which exists but nobody subscribes to, and a partition of the
     * missing topic. It gets a new, empty map.
     */
    @Test
    public void testMemberHoldingOnlyStalePartitionsGetsANewMapWithoutThem() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), mkAssignment(
                mkTopicAssignment(T1), mkTopicAssignment(T2, 0), mkTopicAssignment(MISSING, 0)))
            .withMember("B", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 0)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 1)
            .withTopic(T2, 1)
            .build();

        var result = new AssignmentBuilder(spec, describer).build();

        assertAssignment(Map.of("A", Map.of(), "B", Map.of(T1, Set.of(0))), result);
        assertNotSame(spec.memberAssignment("A").partitions(), partitions(result, "A"));
        assertSame(spec.memberAssignment("B").partitions(), partitions(result, "B"));
    }

    /**
     * A and B subscribe to T1, which has 2 partitions: 1 base partition for each. A holds
     * partitions 0 and 7, and owns only partition 0, which is its share; B owns partition 1. No
     * partition moves, but partition 7 is stale: A gets a new map with partition 0 only.
     */
    @Test
    public void testPartitionsBeyondThePartitionCountAreDropped() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 0, 7)))
            .withMember("B", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 1)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 2)
            .build();

        var result = new AssignmentBuilder(spec, describer).build();

        assertAssignment(Map.of("A", Map.of(T1, Set.of(0)), "B", Map.of(T1, Set.of(1))), result);
        assertSame(spec.memberAssignment("B").partitions(), partitions(result, "B"));
    }

    /**
     * A subscribes to T1 and T2, and B to T1. T1 has a single partition: no base partition and 1
     * extra partition, which A keeps as it owns the partition. T2 has 2 partitions and the single
     * subscriber A, which owns both. The size of A is then 3, and that of B is 0: the balance
     * moves the extra partition of T1 to B. A loses all the partitions of T1, and its new map has
     * no entry for T1, but the very set of T2 that it holds.
     */
    @Test
    public void testMemberLosingAllThePartitionsOfATopicHasNoEntryForIt() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T1, 0), mkTopicAssignment(T2, 0, 1)))
            .withMember("B", Set.of(T1))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 1)
            .withTopic(T2, 2)
            .build();

        var result = new AssignmentBuilder(spec, describer).build();

        assertAssignment(Map.of("A", Map.of(T2, Set.of(0, 1)), "B", Map.of(T1, Set.of(0))), result);
        assertSame(spec.memberAssignment("A").partitions().get(T2), partitions(result, "A").get(T2));
    }

    private static Map<Uuid, Set<Integer>> partitions(GroupAssignment result, String memberId) {
        return result.members().get(memberId).partitions();
    }

    /**
     * @return The class of the set of partitions of the topic of every one of the members.
     */
    private static Map<String, Class<?>> setClasses(GroupAssignment result, Uuid topicId, String... memberIds) {
        var classes = new HashMap<String, Class<?>>();
        for (String memberId : memberIds) {
            classes.put(memberId, partitions(result, memberId).get(topicId).getClass());
        }
        return classes;
    }
}
