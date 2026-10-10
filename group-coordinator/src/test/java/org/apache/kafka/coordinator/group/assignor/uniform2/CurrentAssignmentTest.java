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
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.assignor.uniform2.util.IntList;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Map.entry;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests of {@link CurrentAssignment}: the owners of the partitions, the members holding stale
 * partitions, and the sets of partitions beyond the partition count.
 *
 * <p>The member ids sort as A &lt; B &lt; C, so that A is member 0, B is member 1 and C is member
 * 2, and the topic ids as T1 &lt; T2, so that T1 is topic 0 and T2 is topic 1. T1 has 4
 * partitions and T2 has 3. The missing topic does not exist.
 */
public class CurrentAssignmentTest {
    private static final Uuid T1 = new Uuid(1L, 1L);
    private static final Uuid T2 = new Uuid(2L, 1L);
    private static final Uuid MISSING = new Uuid(9L, 1L);

    private static final int A = 0;
    private static final int B = 1;
    private static final int C = 2;

    private static final SubscribedTopicDescriber DESCRIBER = new TopicsFixture()
        .withTopic(T1, 4)
        .withTopic(T2, 3)
        .build();

    /**
     * A holds partitions 0 and 3 of T1 and 1 of T2, B holds partition 1 of T1, and C holds
     * partitions 0 and 2 of T2. Partition 2 of T1 has no owner. The assignment of every member
     * is its very map of partitions.
     */
    @Test
    public void testOwners() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T1, 0, 3), mkTopicAssignment(T2, 1)))
            .withMember("B", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T1, 1)))
            .withMember("C", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T2, 0, 2)))
            .build();
        var group = new GroupModel(spec, DESCRIBER);

        var current = new CurrentAssignment(spec, group);

        assertEquals(List.of(List.of(A, B, -1, A), List.of(C, A, C)), owners(group, current));
        assertEquals(List.of(List.of(entry(A, 2), entry(B, 1)), List.of(entry(C, 2), entry(A, 1))), ownedCounts(group, current));
        assertEquals(List.of(), membersHoldingStalePartitions(group, current));
        assertEquals(Map.of(), setsBeyondPartitionCount(group, current));
        for (int member = 0; member < 3; member++) {
            assertSame(spec.memberAssignment(group.memberId(member)).partitions(), current.assignment(member));
        }
    }

    /**
     * Nobody holds a partition of T2, which has no owners.
     */
    @Test
    public void testTopicNobodyOwnsHasNoOwners() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T1, 0, 1)))
            .withMember("B", Set.of(T1, T2))
            .build();
        var group = new GroupModel(spec, DESCRIBER);

        var current = new CurrentAssignment(spec, group);

        assertEquals(List.of(List.of(A, A, -1, -1), List.of()), owners(group, current));
        assertEquals(List.of(List.of(entry(A, 2)), List.of()), ownedCounts(group, current));
        assertEquals(List.of(), membersHoldingStalePartitions(group, current));
        assertEquals(Map.of(), setsBeyondPartitionCount(group, current));
    }

    /**
     * A subscribes to T1 only, and holds partition 0 of T2, to which B subscribes: A does not own
     * it, so T2 has no owner, and A holds a stale partition. A still owns its partition of T1.
     */
    @Test
    public void testPartitionsOfATopicTheMemberDoesNotSubscribeToAreStale() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 0), mkTopicAssignment(T2, 0)))
            .withMember("B", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T1, 1)))
            .build();
        var group = new GroupModel(spec, DESCRIBER);

        var current = new CurrentAssignment(spec, group);

        assertEquals(List.of(List.of(A, B, -1, -1), List.of()), owners(group, current));
        assertEquals(List.of(List.of(entry(A, 1), entry(B, 1)), List.of()), ownedCounts(group, current));
        assertEquals(List.of(A), membersHoldingStalePartitions(group, current));
        assertEquals(Map.of(), setsBeyondPartitionCount(group, current));
    }

    /**
     * A holds partition 0 of the missing topic, which is stale. A still owns its partition of T1.
     */
    @Test
    public void testPartitionsOfATopicThatNoLongerExistsAreStale() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 0), mkTopicAssignment(MISSING, 0)))
            .withMember("B", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 1)))
            .build();
        var group = new GroupModel(spec, DESCRIBER);

        var current = new CurrentAssignment(spec, group);

        assertEquals(List.of(List.of(A, B, -1, -1)), owners(group, current));
        assertEquals(List.of(List.of(entry(A, 1), entry(B, 1))), ownedCounts(group, current));
        assertEquals(List.of(A), membersHoldingStalePartitions(group, current));
        assertEquals(Map.of(), setsBeyondPartitionCount(group, current));
    }

    /**
     * A holds partitions 0 and 7 of T1, which has 4 partitions, and B partitions 1 and -1: they
     * own partitions 0 and 1 only, and both hold a set of T1 beyond its partition count.
     */
    @Test
    public void testPartitionsBeyondThePartitionCountAreStale() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 0, 7)))
            .withMember("B", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 1, -1)))
            .build();
        var group = new GroupModel(spec, DESCRIBER);

        var current = new CurrentAssignment(spec, group);

        assertEquals(List.of(List.of(A, B, -1, -1)), owners(group, current));
        assertEquals(List.of(List.of(entry(A, 1), entry(B, 1))), ownedCounts(group, current));
        assertEquals(List.of(A, B), membersHoldingStalePartitions(group, current));
        assertEquals(Map.of(A, List.of(0), B, List.of(0)), setsBeyondPartitionCount(group, current));
    }

    /**
     * A holds an empty set of partitions of T1: it owns nothing, and must get a new assignment
     * without the empty set.
     */
    @Test
    public void testEmptyPartitionSetIsStale() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), mkAssignment(mkTopicAssignment(T1)))
            .withMember("B", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 0, 1)))
            .build();
        var group = new GroupModel(spec, DESCRIBER);

        var current = new CurrentAssignment(spec, group);

        assertEquals(List.of(List.of(B, B, -1, -1)), owners(group, current));
        assertEquals(List.of(List.of(entry(B, 2))), ownedCounts(group, current));
        assertEquals(List.of(A), membersHoldingStalePartitions(group, current));
        assertEquals(Map.of(), setsBeyondPartitionCount(group, current));
    }

    /**
     * A holds partition 7 of T1, beyond its partition count, and B an empty set of partitions of
     * T1: both are stale, so no partition of T1 has an owner, and T1 has no owners, as for a
     * topic nobody holds.
     */
    @Test
    public void testTopicWithOnlyStalePartitionsHasNoOwners() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 7)))
            .withMember("B", Set.of(T1), mkAssignment(mkTopicAssignment(T1)))
            .build();
        var group = new GroupModel(spec, DESCRIBER);

        var current = new CurrentAssignment(spec, group);

        assertEquals(List.of(List.of()), owners(group, current));
        assertEquals(List.of(List.of()), ownedCounts(group, current));
        assertEquals(List.of(A, B), membersHoldingStalePartitions(group, current));
        assertEquals(Map.of(A, List.of(0)), setsBeyondPartitionCount(group, current));
    }

    /**
     * @return Per topic, the owner of every partition, -1 for none, or an empty list when the
     *         topic has no owners.
     */
    private static List<List<Integer>> owners(GroupModel group, CurrentAssignment current) {
        var result = new ArrayList<List<Integer>>();
        for (int topic = 0; topic < group.topicCount(); topic++) {
            var owners = current.owners(topic);
            result.add(owners == null ? List.of() : Arrays.stream(owners).boxed().toList());
        }
        return result;
    }

    /**
     * @return Per topic, its owners in the order {@link CurrentAssignment#countOwned} gives them,
     *         each with its number of partitions. The counts and the list of owners are reused
     *         from one topic to the next, as the assignor does: the counts are reset after every
     *         topic, and the list must be cleared by {@code countOwned}.
     */
    private static List<List<Map.Entry<Integer, Integer>>> ownedCounts(GroupModel group, CurrentAssignment current) {
        var result = new ArrayList<List<Map.Entry<Integer, Integer>>>();
        var counts = new int[group.memberCount()];
        var owners = new IntList(group.memberCount());
        for (int topic = 0; topic < group.topicCount(); topic++) {
            current.countOwned(topic, counts, owners);
            var ownedCounts = new ArrayList<Map.Entry<Integer, Integer>>();
            for (int i = 0; i < owners.size(); i++) {
                ownedCounts.add(entry(owners.get(i), counts[owners.get(i)]));
                counts[owners.get(i)] = 0;
            }
            result.add(ownedCounts);
        }
        return result;
    }

    /**
     * @return Per member holding sets of partitions beyond the partition count of their topic,
     *         these topics.
     */
    private static Map<Integer, List<Integer>> setsBeyondPartitionCount(GroupModel group, CurrentAssignment current) {
        var result = new HashMap<Integer, List<Integer>>();
        for (int member = 0; member < group.memberCount(); member++) {
            for (int topic = 0; topic < group.topicCount(); topic++) {
                if (current.holdsPartitionsBeyondCount(member, topic)) {
                    result.computeIfAbsent(member, m -> new ArrayList<>()).add(topic);
                }
            }
        }
        return result;
    }

    /**
     * @return The members holding stale partitions, in member order.
     */
    private static List<Integer> membersHoldingStalePartitions(GroupModel group, CurrentAssignment current) {
        var members = new ArrayList<Integer>();
        for (int member = 0; member < group.memberCount(); member++) {
            if (current.holdsStalePartitions(member)) {
                members.add(member);
            }
        }
        return members;
    }
}
