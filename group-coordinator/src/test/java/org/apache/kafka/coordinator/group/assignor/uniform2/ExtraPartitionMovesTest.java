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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the moves of extra partitions between members. The topic ids sort as T1 &lt; T2 &lt; T3,
 * and the members are numbered in member id order, A being 0.
 */
public class ExtraPartitionMovesTest {
    private static final Uuid T1 = new Uuid(1L, 1L);
    private static final Uuid T2 = new Uuid(2L, 1L);
    private static final Uuid T3 = new Uuid(3L, 1L);
    private static final Uuid T4 = new Uuid(4L, 1L);
    private static final int A = 0;
    private static final int B = 1;
    private static final int C = 2;
    private static final int D = 3;

    /**
     * A, B and C subscribe to T1, T2 and T3, of 4 partitions each: 1 base partition and 1 extra
     * partition each, all three extra partitions being A's. A owns 2 partitions of T1 and B 2 of
     * T3, more than the base partition. Moving the extra partition of T3 to B saves a move, since
     * B keeps a partition it owns; the one of T2 costs nothing; the one of T1 costs a move, since
     * A gives up a partition it owns. They move in that order, whatever the order in which A got
     * them.
     */
    @Test
    public void testTransferMovesTheCheapestExtraPartitionFirst() {
        for (var topicIds : List.of(new Uuid[] {T1, T2, T3}, new Uuid[] {T3, T2, T1})) {
            var fixture = threeTopicGroup();
            fixture.addExtras("A", topicIds);
            var moves = new ExtraPartitionMoves(fixture.group, fixture.current, fixture.shares);

            moves.transfer(A, B, 1);
            assertEquals(Map.of(T1, List.of("A"), T2, List.of("A"), T3, List.of("B")), fixture.membersWithExtra());

            moves.transfer(A, B, 1);
            assertEquals(Map.of(T1, List.of("A"), T2, List.of("B"), T3, List.of("B")), fixture.membersWithExtra());

            moves.transfer(A, B, 1);
            assertEquals(Map.of(T1, List.of("B"), T2, List.of("B"), T3, List.of("B")), fixture.membersWithExtra());
            assertEquals(List.of(3, 6, 3), fixture.sizes());
        }
    }

    /**
     * In the same group, A gets the extra partitions of T1 and T2, and B the one of T3. A owns 2
     * partitions of T1 and none of T2, so the extra partition of T2 is free: giving it costs no
     * move. B owns 2 partitions of T3, so its only extra partition is not free. C has none. Once A
     * gives its extra partition of T2 to B, the free one, A has only the one of T1.
     */
    @Test
    public void testHasFreeExtra() {
        var fixture = threeTopicGroup();
        fixture.addExtras("A", T1, T2);
        fixture.addExtras("B", T3);
        var moves = new ExtraPartitionMoves(fixture.group, fixture.current, fixture.shares);

        assertEquals(List.of("A"), fixture.members(moves::hasFreeExtra));

        moves.transfer(A, B, 1);

        assertEquals(Map.of(T1, List.of("A"), T2, List.of("B"), T3, List.of("B")), fixture.membersWithExtra());
        assertEquals(List.of("B"), fixture.members(moves::hasFreeExtra));
    }

    /**
     * A, B and C subscribe to T1, of 5 partitions, and T2, of 4: 1 base partition each, and 2
     * extra partitions of T1 and 1 of T2. A gets an extra partition of both, B one of T1, which
     * it owns 2 partitions of. B and C can take from A, and only C from B. Moving the extra
     * partition of T1 to B would be the cheapest, but B already has one: the one of T2 moves.
     */
    @Test
    public void testTransferSkipsTopicsTheReceiverHasAnExtraPartitionOf() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2))
            .withMember("B", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T1, 0, 1)))
            .withMember("C", Set.of(T1, T2))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 5)
            .withTopic(T2, 4)
            .build();
        var fixture = new SharesFixture(spec, describer);
        fixture.addExtras("A", T1, T2);
        fixture.addExtras("B", T1);
        var moves = new ExtraPartitionMoves(fixture.group, fixture.current, fixture.shares);

        assertEquals(List.of("B", "C"), receivers(fixture, moves, A));
        assertEquals(List.of("C"), receivers(fixture, moves, B));

        moves.transfer(A, B, 1);

        assertEquals(Map.of(T1, List.of("A", "B"), T2, List.of("B")), fixture.membersWithExtra());
    }

    /**
     * A subscribes to T1 and T2, B to T2 only and C to T1 only, so that they are in three
     * cohorts. T1 and T2 have 3 partitions over 2 subscribers each, so 1 extra partition each,
     * both A's. A owns 2 partitions of T2: moving the extra partition of T1 would cost nothing,
     * but B does not subscribe to T1, so the one of T2 moves, although it costs a move. Then only
     * C can take from A, and only A from B.
     */
    @Test
    public void testTransferToAnotherCohortOnlyMovesTopicsTheReceiverSubscribesTo() {
        var fixture = threeCohortGroup();
        fixture.addExtras("A", T1, T2);
        var moves = new ExtraPartitionMoves(fixture.group, fixture.current, fixture.shares);

        assertEquals(List.of("B", "C"), receivers(fixture, moves, A));

        moves.transfer(A, B, 1);

        assertEquals(Map.of(T1, List.of("A"), T2, List.of("B")), fixture.membersWithExtra());
        assertEquals(List.of("C"), receivers(fixture, moves, A));
        assertEquals(List.of("A"), receivers(fixture, moves, B));
    }

    /**
     * In the same group, B can only take the extra partition of T2 from A, not two extra
     * partitions.
     */
    @Test
    public void testTransferThrowsWhenTheGiverHasTooFewExtraPartitionsToGive() {
        var fixture = threeCohortGroup();
        fixture.addExtras("A", T1, T2);
        var moves = new ExtraPartitionMoves(fixture.group, fixture.current, fixture.shares);

        assertThrows(IllegalStateException.class, () -> moves.transfer(A, B, 2));
    }

    /**
     * The counts per cohort of every member fit when the members times the cohorts are at most
     * the topics of all the subscriptions. A subscribing to T1 and T2, B to T2 and C to T1 make 3
     * members and 3 cohorts for 4 topics: they do not fit. A and B subscribing to T1, T2 and T3,
     * and C to T1, make 3 members and 2 cohorts for 7 topics: they fit.
     */
    @Test
    public void testCountsFit() {
        assertFalse(ExtraPartitionMoves.countsFit(threeCohortGroup().group));
        assertTrue(ExtraPartitionMoves.countsFit(twoCohortGroup().group));
    }

    /**
     * The bit sets of the subscriptions fit when the cohorts times the longs of a bit set, one per
     * 64 topics, are at most the topics of all the subscriptions. A subscribing to T1 and T2, B to
     * T2 and C to T1 make 3 cohorts of 1 long for 4 topics: they fit. 65 members subscribing to a
     * topic each make 65 cohorts of 2 longs for 65 topics: they do not.
     */
    @Test
    public void testBitsFit() {
        assertTrue(ExtraPartitionMoves.bitsFit(threeCohortGroup().group));

        var spec = new GroupSpecFixture();
        var describer = new TopicsFixture();
        for (int i = 0; i < 65; i++) {
            var topicId = new Uuid(100L + i, 1L);
            spec.withMember(String.format("M%02d", i), Set.of(topicId));
            describer.withTopic(topicId, 1);
        }
        assertFalse(ExtraPartitionMoves.bitsFit(new GroupModel(spec.build(), describer.build())));
    }

    /**
     * In the three topic group, every topic has an extra partition. A owns 2 partitions of T1 and
     * B 2 of T3, more than the base partition: receiving an extra partition of these topics saves
     * them a move. C owns nothing.
     */
    @Test
    public void testOwnsAboveBase() {
        var fixture = threeTopicGroup();
        var moves = new ExtraPartitionMoves(fixture.group, fixture.current, fixture.shares);

        assertEquals(List.of("A", "B"), fixture.members(moves::ownsAboveBase));
    }

    /**
     * A and B subscribe to T1, T2 and T3, of 3 partitions each, and C to T1 only, in cohorts 0
     * and 1, so that the counts are kept per member. T1, 3 partitions over 3 subscribers, has no
     * extra partition; T2 and T3, 3 partitions over A and B, have 1 each, both A's. A counts 2
     * for its cohort and none for that of C, and reaches its own cohort only; once the extra
     * partition of T3 moves to B, A and B count 1 each for cohort 0, which both reach.
     */
    @Test
    public void testKeptCountsPerCohortFollowTheTransfers() {
        var fixture = twoCohortGroup();
        fixture.addExtras("A", T2, T3);
        var moves = new ExtraPartitionMoves(fixture.group, fixture.current, fixture.shares);

        assertEquals(List.of(List.of(2, 0), List.of(0, 0), List.of(0, 0)), extrasFor(fixture, moves));
        assertEquals(List.of(Set.of(0), Set.of(), Set.of()), cohortsReached(fixture, moves));

        moves.transfer(A, B, 1);

        assertEquals(List.of(List.of(1, 0), List.of(1, 0), List.of(0, 0)), extrasFor(fixture, moves));
        assertEquals(List.of(Set.of(0), Set.of(0), Set.of()), cohortsReached(fixture, moves));
    }

    /**
     * A subscribes to T1 and T2, B to T2 only and C to T1 only, in cohorts 0, 1 and 2, so that
     * the counts are held for one member at a time. A gets the extra partitions of T1 and T2: its
     * cohort subscribes to both, the cohort of B to T2 only and that of C to T1 only. Once the
     * extra partition of T2 moves to B, A counts none for the cohort of B, and B counts one for
     * its own cohort and that of A.
     */
    @Test
    public void testCountsPerCohortFollowTheTransfers() {
        var fixture = threeCohortGroup();
        fixture.addExtras("A", T1, T2);
        var moves = new ExtraPartitionMoves(fixture.group, fixture.current, fixture.shares);

        assertEquals(List.of(List.of(2, 1, 1), List.of(0, 0, 0), List.of(0, 0, 0)), extrasFor(fixture, moves));
        assertEquals(List.of(Set.of(0, 1, 2), Set.of(), Set.of()), cohortsReached(fixture, moves));

        moves.transfer(A, B, 1);

        assertEquals(List.of(List.of(1, 0, 1), List.of(1, 1, 0), List.of(0, 0, 0)), extrasFor(fixture, moves));
        assertEquals(List.of(Set.of(0, 2), Set.of(0, 1), Set.of()), cohortsReached(fixture, moves));
    }

    /**
     * In the same group, A has an extra partition of both of its topics, so it is full. B and C
     * have none of their only topic; each is full once it gets the extra partition of its topic.
     */
    @Test
    public void testIsFull() {
        var fixture = threeCohortGroup();
        fixture.addExtras("A", T1, T2);
        var moves = new ExtraPartitionMoves(fixture.group, fixture.current, fixture.shares);

        assertEquals(List.of("A"), fixture.members(moves::isFull));

        moves.transfer(A, B, 1);
        moves.transfer(A, C, 1);

        assertEquals(List.of("B", "C"), fixture.members(moves::isFull));
    }

    /**
     * A and D subscribe to T1 and T2, in cohort 0, and B and C to T1, in cohort 1, with T3 too
     * when the counts are kept per member. T1, 7 partitions over the four, has 3 extra
     * partitions, A's, B's and C's; T2, 3 partitions over A and D, has 1, D's. Every member of
     * cohort 1 has an extra partition of T1, the only topic of A's extra partitions: none of them
     * can take one, while D, in cohort 0, can. Once B gives its extra partition of T1 to D, cohort
     * 1 no longer holds them all, and cohort 0 does; A can still take the extra partition of T2
     * from D.
     */
    @Test
    public void testAllHeldBy() {
        for (boolean keepCounts : new boolean[] {true, false}) {
            var bAndC = keepCounts ? Set.of(T1, T3) : Set.of(T1);
            var spec = new GroupSpecFixture()
                .withMember("A", Set.of(T1, T2))
                .withMember("B", bAndC)
                .withMember("C", bAndC)
                .withMember("D", Set.of(T1, T2))
                .build();
            var describer = new TopicsFixture()
                .withTopic(T1, 7)
                .withTopic(T2, 3)
                .withTopic(T3, 2)
                .build();
            var fixture = new SharesFixture(spec, describer);
            fixture.addExtras("A", T1);
            fixture.addExtras("B", T1);
            fixture.addExtras("C", T1);
            fixture.addExtras("D", T2);
            var moves = new ExtraPartitionMoves(fixture.group, fixture.current, fixture.shares);
            assertEquals(keepCounts, ExtraPartitionMoves.countsFit(fixture.group));

            assertEquals(List.of(1), cohortsHolding(fixture, moves, A));

            moves.transfer(B, D, 1);

            assertEquals(Map.of(T1, List.of("A", "C", "D"), T2, List.of("D")), fixture.membersWithExtra());
            assertEquals(List.of(0), cohortsHolding(fixture, moves, A));
            assertEquals(List.of(), cohortsHolding(fixture, moves, D));
        }
    }

    /**
     * A subscribes to T1, and B and C to T1, T3 and T4. T1, 5 partitions over the three, has 2
     * extra partitions, A's and B's; T3 and T4, 3 partitions over B and C, have 1 each, both B's,
     * which owns 2 partitions of each. B, with 3 extra partitions, has the extra partition of T1,
     * the only one A has: it cannot take from A, which the count of A for the cohort of B, 1, does
     * not show, and only C can. Once B gives the extra partition of T1, its free one, to C, only B
     * can take from A.
     */
    @Test
    public void testAReceiverUnableToTakeCanTakeAgainOnceItGivesATopicOfTheGiver() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1))
            .withMember("B", Set.of(T1, T3, T4), mkAssignment(mkTopicAssignment(T3, 0, 1), mkTopicAssignment(T4, 0, 1)))
            .withMember("C", Set.of(T1, T3, T4))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 5)
            .withTopic(T3, 3)
            .withTopic(T4, 3)
            .build();
        var fixture = new SharesFixture(spec, describer);
        fixture.addExtras("A", T1);
        fixture.addExtras("B", T1, T3, T4);
        var moves = new ExtraPartitionMoves(fixture.group, fixture.current, fixture.shares);

        assertEquals(List.of("C"), receivers(fixture, moves, A));

        moves.transfer(B, C, 1);

        assertEquals(Map.of(T1, List.of("A", "C"), T3, List.of("B"), T4, List.of("B")), fixture.membersWithExtra());
        assertEquals(List.of("B"), receivers(fixture, moves, A));
    }

    /**
     * A, B and C subscribe to T1, T2 and T3. T1 and T3, of 4 partitions, have 1 extra partition
     * each, A's and C's; T2, of 5 partitions, has 2, A's and B's. A gives B the extra partition of
     * T1, passing over the one of T2, which B has. A then receives the extra partition of T3 from
     * C, at the end of its list. The next transfer from A to B does not resume where the first one
     * stopped, since A received since, and finds it.
     */
    @Test
    public void testATransferFindsTheExtraPartitionsTheGiverReceivedSinceItsLastTransfer() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3))
            .withMember("B", Set.of(T1, T2, T3))
            .withMember("C", Set.of(T1, T2, T3))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 5)
            .withTopic(T3, 4)
            .build();
        var fixture = new SharesFixture(spec, describer);
        fixture.addExtras("A", T1, T2);
        fixture.addExtras("B", T2);
        fixture.addExtras("C", T3);
        var moves = new ExtraPartitionMoves(fixture.group, fixture.current, fixture.shares);

        moves.transfer(A, B, 1);
        assertEquals(Map.of(T1, List.of("B"), T2, List.of("A", "B"), T3, List.of("C")), fixture.membersWithExtra());

        moves.transfer(C, A, 1);
        assertEquals(Map.of(T1, List.of("B"), T2, List.of("A", "B"), T3, List.of("A")), fixture.membersWithExtra());

        moves.transfer(A, B, 1);
        assertEquals(Map.of(T1, List.of("B"), T2, List.of("A", "B"), T3, List.of("B")), fixture.membersWithExtra());
    }

    /**
     * @return The members that can take an extra partition from the giver, in member id order.
     */
    private static List<String> receivers(SharesFixture fixture, ExtraPartitionMoves moves, int giver) {
        return fixture.members(receiver -> receiver != giver && moves.canTake(giver, receiver));
    }

    /**
     * @return Per member, in member id order, its number of extra partitions of the topics of
     *         every cohort.
     */
    private static List<List<Integer>> extrasFor(SharesFixture fixture, ExtraPartitionMoves moves) {
        var result = new ArrayList<List<Integer>>();
        for (int member = 0; member < fixture.group.memberCount(); member++) {
            var counts = new ArrayList<Integer>();
            for (int cohort = 0; cohort < fixture.group.cohortCount(); cohort++) {
                counts.add(moves.extrasFor(member, cohort));
            }
            result.add(counts);
        }
        return result;
    }

    /**
     * @return Per member, in member id order, the cohorts it reaches, each listed once.
     */
    private static List<Set<Integer>> cohortsReached(SharesFixture fixture, ExtraPartitionMoves moves) {
        var result = new ArrayList<Set<Integer>>();
        for (int member = 0; member < fixture.group.memberCount(); member++) {
            var reached = moves.cohortsReached(member);
            var cohorts = new HashSet<Integer>();
            for (int i = 0; i < reached.size(); i++) {
                assertTrue(cohorts.add(reached.get(i)));
            }
            result.add(cohorts);
        }
        return result;
    }

    /**
     * @return The cohorts none of whose members can take an extra partition of the member.
     */
    private static List<Integer> cohortsHolding(SharesFixture fixture, ExtraPartitionMoves moves, int member) {
        var cohorts = new ArrayList<Integer>();
        for (int cohort = 0; cohort < fixture.group.cohortCount(); cohort++) {
            if (moves.allHeldBy(member, cohort)) {
                cohorts.add(cohort);
            }
        }
        return cohorts;
    }

    private static SharesFixture threeTopicGroup() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3), mkAssignment(mkTopicAssignment(T1, 0, 1)))
            .withMember("B", Set.of(T1, T2, T3), mkAssignment(mkTopicAssignment(T3, 0, 1)))
            .withMember("C", Set.of(T1, T2, T3))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 4)
            .withTopic(T3, 4)
            .build();
        return new SharesFixture(spec, describer);
    }

    private static SharesFixture twoCohortGroup() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3))
            .withMember("B", Set.of(T1, T2, T3))
            .withMember("C", Set.of(T1))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .withTopic(T2, 3)
            .withTopic(T3, 3)
            .build();
        return new SharesFixture(spec, describer);
    }

    private static SharesFixture threeCohortGroup() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T2, 0, 1)))
            .withMember("B", Set.of(T2))
            .withMember("C", Set.of(T1))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .withTopic(T2, 3)
            .build();
        return new SharesFixture(spec, describer);
    }
}
