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
package org.apache.kafka.coordinator.group.assignor.uniform2.steps;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.assignor.uniform2.GroupSpecFixture;
import org.apache.kafka.coordinator.group.assignor.uniform2.QuotasFixture;
import org.apache.kafka.coordinator.group.assignor.uniform2.TopicsFixture;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of {@link BalanceExtraPartitions}.
 */
public class BalanceExtraPartitionsTest {
    private static final Uuid T1 = new Uuid(1L, 1L);
    private static final Uuid T2 = new Uuid(2L, 1L);
    private static final Uuid T3 = new Uuid(3L, 1L);
    private static final Uuid T4 = new Uuid(4L, 1L);
    private static final Uuid T5 = new Uuid(5L, 1L);
    private static final Uuid T6 = new Uuid(6L, 1L);

    /**
     * Nothing may move in an empty group, whose single cohort has no members.
     */
    @Test
    public void testNothingMayMoveInAnEmptyGroup() {
        var fixture = new QuotasFixture(new GroupSpecFixture().build(), new TopicsFixture().withTopic(T1, 3).build());
        assertFalse(BalanceExtraPartitions.mayMove(fixture.group, fixture.quotas));
    }

    /**
     * A and B subscribe to T1 and T2, of 3 partitions each, so each topic has 1 extra partition. A
     * has the one of T1 and B the one of T2. The assignment sizes are 3 and 3, so nothing may move.
     */
    @Test
    public void testNothingMayMoveWhenAssignmentSizesAreWithinOne() {
        var fixture = homogeneousGroup();
        fixture.addExtras("A", T1);
        fixture.addExtras("B", T2);
        assertEquals(List.of(3, 3), fixture.assignmentSizes());

        assertFalse(BalanceExtraPartitions.mayMove(fixture.group, fixture.quotas));
    }

    /**
     * A subscribes to T1 and T2, B to T1 only, in two cohorts. T1, 3 partitions, has 1 extra
     * partition, A's, and T2 gives A 1 more partition. The assignment sizes are 3 and 1, two apart,
     * so something may move.
     */
    @Test
    public void testMayMoveWhenAssignmentSizesAreTwoApart() {
        var fixture = twoCohortGroup();
        fixture.addExtras("A", T1);
        assertEquals(List.of(3, 1), fixture.assignmentSizes());

        assertTrue(BalanceExtraPartitions.mayMove(fixture.group, fixture.quotas));
    }

    /**
     * A subscribes to T1 and T2, and B to T3 and T4, in two cohorts. The counts per cohort take 2
     * members times 2 cohorts, 4 entries, as many as the 4 topics of the subscriptions, so they
     * fit. When B subscribes to T3 only, the subscriptions have 3 topics, and the counts do not.
     */
    @Test
    public void testCountsFitWhenTheyTakeNoMoreEntriesThanTheSubscriptions() {
        var describer = new TopicsFixture()
            .withTopic(T1, 1)
            .withTopic(T2, 1)
            .withTopic(T3, 1)
            .withTopic(T4, 1)
            .build();
        var fitting = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2))
            .withMember("B", Set.of(T3, T4))
            .build();
        var notFitting = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2))
            .withMember("B", Set.of(T3))
            .build();

        assertTrue(BalanceExtraPartitions.countsFit(new QuotasFixture(fitting, describer).group));
        assertFalse(BalanceExtraPartitions.countsFit(new QuotasFixture(notFitting, describer).group));
    }

    /**
     * A subscribes to T1, T2 and T3, B to T1 and T2, C to T2 and T3, and D to T1 and T3, in four
     * cohorts, three of which subscribe to each topic. T1, T2 and T3, of 5 partitions each, have a
     * minimum quota of 1 and 2 extra partitions each. A has one of every topic, B one of T1 and one
     * of T2, and C the second one of T3. The assignment sizes are 6, 4, 3 and 2. A gives the extra
     * partition of T3 to D, the smallest, then the one of T2 to C. The assignment sizes end at 4,
     * 4, 4 and 3, and no extra partition can move to a subscriber of its topic at least two
     * partitions smaller.
     */
    @Test
    public void testBalanceMovesUntilNoExtraPartitionCanMove() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3))
            .withMember("B", Set.of(T1, T2))
            .withMember("C", Set.of(T2, T3))
            .withMember("D", Set.of(T1, T3))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 5)
            .withTopic(T2, 5)
            .withTopic(T3, 5)
            .build();
        var fixture = new QuotasFixture(spec, describer);
        fixture.addExtras("A", T1, T2, T3);
        fixture.addExtras("B", T1, T2);
        fixture.addExtras("C", T3);
        assertEquals(List.of(6, 4, 3, 2), fixture.assignmentSizes());

        balanceAndAssertBalanced(fixture);

        assertEquals(Map.of(T1, List.of("A", "B"), T2, List.of("B", "C"), T3, List.of("C", "D")), fixture.membersWithExtraPartition());
        assertEquals(List.of(4, 4, 4, 3), fixture.assignmentSizes());
    }

    /**
     * A subscribes to T1 and T2, B to T1 only, and C to T2 and T3. T1 has no extra partition, T2
     * has 1, A's. The assignment sizes are 3, 1 and 2. A and B are two apart, so something may
     * move, but B cannot take the extra partition of T2, and C is only one partition smaller, so
     * nothing moves.
     */
    @Test
    public void testNothingMovesWhenNoExtraPartitionCanMove() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2))
            .withMember("B", Set.of(T1))
            .withMember("C", Set.of(T2, T3))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 2)
            .withTopic(T2, 3)
            .withTopic(T3, 1)
            .build();
        var fixture = new QuotasFixture(spec, describer);
        fixture.addExtras("A", T2);
        assertTrue(BalanceExtraPartitions.mayMove(fixture.group, fixture.quotas));

        balanceAndAssertBalanced(fixture);

        assertEquals(Map.of(T2, List.of("A")), fixture.membersWithExtraPartition());
        assertEquals(List.of(3, 1, 2), fixture.assignmentSizes());
    }

    /**
     * A and B subscribe to T1, T2 and T3, and C to T1 and T4, in cohorts 0 and 1. T1, 5 partitions,
     * has a minimum quota of 1 and 2 extra partitions, A's and B's. T2 and T3, 3 partitions over A
     * and B, have 1 extra partition each, both A's. T4 gives C 2 more partitions. The assignment
     * sizes are 6, 4 and 3. A gives the extra partition of T1 to C, the smallest, and the
     * assignment sizes end at 5, 4 and 4. That takes one move, where balancing the cohort of A and
     * B first would move an extra partition from A to B, and then one of T1 to C.
     */
    @Test
    public void testTheLargestMemberGivesToTheSmallestWhateverItsCohort() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3))
            .withMember("B", Set.of(T1, T2, T3))
            .withMember("C", Set.of(T1, T4))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 5)
            .withTopic(T2, 3)
            .withTopic(T3, 3)
            .withTopic(T4, 2)
            .build();
        var fixture = new QuotasFixture(spec, describer);
        fixture.addExtras("A", T1, T2, T3);
        fixture.addExtras("B", T1);
        assertEquals(List.of(6, 4, 3), fixture.assignmentSizes());

        balanceAndAssertBalanced(fixture);

        assertEquals(Map.of(T1, List.of("B", "C"), T2, List.of("A"), T3, List.of("A")), fixture.membersWithExtraPartition());
        assertEquals(List.of(5, 4, 4), fixture.assignmentSizes());
    }

    /**
     * A and B subscribe to T1 and T2, of 5 partitions each. A holds T1 [0, 1, 2] and T2 [0, 1], and
     * B holds T1 [3, 4] and T2 [2, 3, 4]. C joins with the same subscription. Each topic has a
     * minimum quota of 1 and 2 extra partitions, all kept by A and B, which own more than 1
     * partition of each. The assignment sizes are 4, 4 and 2. A and B are equally large and own all
     * their extra partitions, so A, first in member order, gives one to C. Giving either of its
     * extra partitions revokes a partition, and its topics are tried from the last, so the one of
     * T2 goes. The assignment sizes end at 3, 4 and 3.
     */
    @Test
    public void testWithinACohortTheLargestGivesToTheSmallest() {
        var topics = Set.of(T1, T2);
        var spec = new GroupSpecFixture()
            .withMember("A", topics, mkAssignment(mkTopicAssignment(T1, 0, 1, 2), mkTopicAssignment(T2, 0, 1)))
            .withMember("B", topics, mkAssignment(mkTopicAssignment(T1, 3, 4), mkTopicAssignment(T2, 2, 3, 4)))
            .withMember("C", topics)
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 5)
            .withTopic(T2, 5)
            .build();
        var fixture = new QuotasFixture(spec, describer);
        fixture.addExtras("A", T1, T2);
        fixture.addExtras("B", T1, T2);

        balanceAndAssertBalanced(fixture);

        assertEquals(Map.of(T1, List.of("A", "B"), T2, List.of("B", "C")), fixture.membersWithExtraPartition());
        assertEquals(List.of(3, 4, 3), fixture.assignmentSizes());
    }

    /**
     * A, B and C subscribe to T1, T2 and T3, of 4 partitions each. Each topic has a minimum quota
     * of 1 and 1 extra partition. A has the three extra partitions and owns none of their
     * partitions. B owns 2 partitions of T2 and C 2 of T1. The assignment sizes are 6, 3 and 3. A
     * first gives B, the first of the smallest in member order, the extra partition of T2, which
     * revokes one partition fewer, since B keeps its 2 partitions. A then gives C the one of T1,
     * for the same reason, and keeps the one of T3.
     */
    @Test
    public void testTheGiverGivesAnExtraPartitionTheReceiverOwnsFirst() {
        var topics = Set.of(T1, T2, T3);
        var spec = new GroupSpecFixture()
            .withMember("A", topics)
            .withMember("B", topics, mkAssignment(mkTopicAssignment(T2, 0, 1)))
            .withMember("C", topics, mkAssignment(mkTopicAssignment(T1, 0, 1)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 4)
            .withTopic(T3, 4)
            .build();
        var fixture = new QuotasFixture(spec, describer);
        fixture.addExtras("A", T1, T2, T3);

        balanceAndAssertBalanced(fixture);

        assertEquals(Map.of(T1, List.of("C"), T2, List.of("B"), T3, List.of("A")), fixture.membersWithExtraPartition());
        assertEquals(List.of(4, 4, 4), fixture.assignmentSizes());
    }

    /**
     * A subscribes to T1, T2 and T3, B to T1 and T2. T1 and T2 have 3 partitions over A and B, so 1
     * extra partition each, both A's. T3 gives A 1 more partition. A owns 2 partitions of T2, so
     * its extra partition of T2 is owned and the one of T1 is free. The assignment sizes are 5 and
     * 2. A gives B the extra partition of T1, which revokes no partition, and keeps the one of T2.
     * The assignment sizes end at 4 and 3.
     */
    @Test
    public void testTheGiverGivesAFreeExtraPartitionBeforeAnOwnedOne() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3), mkAssignment(mkTopicAssignment(T2, 0, 1)))
            .withMember("B", Set.of(T1, T2))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .withTopic(T2, 3)
            .withTopic(T3, 1)
            .build();
        var fixture = new QuotasFixture(spec, describer);
        fixture.addExtras("A", T1, T2);

        balanceAndAssertBalanced(fixture);

        assertEquals(Map.of(T1, List.of("B"), T2, List.of("A")), fixture.membersWithExtraPartition());
        assertEquals(List.of(4, 3), fixture.assignmentSizes());
    }

    /**
     * A, B and C subscribe to T1 to T4, of 4 partitions each. Each topic has a minimum quota of 1
     * and 1 extra partition. A has the extra partitions of T1 and T2 and owns 2 partitions of each.
     * B has those of T3 and T4 and owns 2 partitions of T3 only. The assignment sizes are 6, 6 and
     * 4. A and B are equally large, but B has a free extra partition, the one of T4, whose move
     * revokes no partition. B gives it to C, and A keeps both of its extra partitions.
     */
    @Test
    public void testOnEqualSizesAGiverWithAFreeExtraPartitionGivesFirst() {
        var topics = Set.of(T1, T2, T3, T4);
        var spec = new GroupSpecFixture()
            .withMember("A", topics, mkAssignment(mkTopicAssignment(T1, 0, 1), mkTopicAssignment(T2, 0, 1)))
            .withMember("B", topics, mkAssignment(mkTopicAssignment(T3, 0, 1)))
            .withMember("C", topics)
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 4)
            .withTopic(T3, 4)
            .withTopic(T4, 4)
            .build();
        var fixture = new QuotasFixture(spec, describer);
        fixture.addExtras("A", T1, T2);
        fixture.addExtras("B", T3, T4);

        balanceAndAssertBalanced(fixture);

        assertEquals(Map.of(T1, List.of("A"), T2, List.of("A"), T3, List.of("B"), T4, List.of("C")), fixture.membersWithExtraPartition());
        assertEquals(List.of(6, 5, 5), fixture.assignmentSizes());
    }

    /**
     * A subscribes to T1 and T2, B to T1 and T4, and C to T1 and T3, in cohorts 0, 1 and 2. T1, 4
     * partitions over the three, has 1 extra partition, A's. T2 gives A 2 more partitions, T4 gives
     * B 1, and T3 gives C 1. The assignment sizes are 4, 2 and 2. B and C are equally small and own
     * nothing, so B, first in member order, takes the extra partition of T1.
     */
    @Test
    public void testReceiversOfEqualSizesGoInMemberOrder() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2))
            .withMember("B", Set.of(T1, T4))
            .withMember("C", Set.of(T1, T3))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 2)
            .withTopic(T3, 1)
            .withTopic(T4, 1)
            .build();
        var fixture = new QuotasFixture(spec, describer);
        fixture.addExtras("A", T1);

        balanceAndAssertBalanced(fixture);

        assertEquals(Map.of(T1, List.of("B")), fixture.membersWithExtraPartition());
        assertEquals(List.of(3, 3, 2), fixture.assignmentSizes());
    }

    /**
     * A subscribes to T1 and T2, B and C to T1 only. T1, 4 partitions, has a minimum quota of 1 and
     * 1 extra partition, A's. T2 gives A 1 more partition. The assignment sizes are 3, 1 and 1. C
     * owns 2 partitions of T1, more than the minimum quota, and B owns none. So C, rather than B,
     * receives the extra partition, and then keeps both of its partitions.
     */
    @Test
    public void testOnEqualSizesAReceiverOwningMoreThanTheMinQuotaReceivesFirst() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2))
            .withMember("B", Set.of(T1))
            .withMember("C", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 1, 2)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 1)
            .build();
        var fixture = new QuotasFixture(spec, describer);
        fixture.addExtras("A", T1);

        balanceAndAssertBalanced(fixture);

        assertEquals(Map.of(T1, List.of("C")), fixture.membersWithExtraPartition());
        assertEquals(List.of(2, 1, 2), fixture.assignmentSizes());
    }

    /**
     * A subscribes to T1 to T4, B and C to T1 to T5. T1 to T4, 4 partitions over 3 subscribers
     * each, have 1 extra partition each, all A's. T5 gives B and C 1 more partition. The assignment
     * sizes are 8, 5 and 5. A gives to B, the first of the smallest, then to C, skipping the entry
     * of B with its former assignment size. The assignment sizes end at 6, 6 and 6. Looking at B
     * with its former assignment size would have stopped at 7, 6 and 5.
     */
    @Test
    public void testMembersWhoseSizeChangedAreLookedAtWithTheirNewSize() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3, T4))
            .withMember("B", Set.of(T1, T2, T3, T4, T5))
            .withMember("C", Set.of(T1, T2, T3, T4, T5))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 4)
            .withTopic(T3, 4)
            .withTopic(T4, 4)
            .withTopic(T5, 2)
            .build();
        var fixture = new QuotasFixture(spec, describer);
        fixture.addExtras("A", T1, T2, T3, T4);

        balanceAndAssertBalanced(fixture);

        assertEquals(Map.of(T1, List.of("A"), T2, List.of("A"), T3, List.of("C"), T4, List.of("B")), fixture.membersWithExtraPartition());
        assertEquals(List.of(6, 6, 6), fixture.assignmentSizes());
    }

    /**
     * A subscribes to T1 and T4, and B and C to T1, T2 and T3. T1, 5 partitions over the three, has
     * 2 extra partitions, A's and B's. T2 and T3, 3 partitions over B and C, have 1 extra partition
     * each, both C's. T4 gives A 5 more partitions. The assignment sizes are 7, 4 and 5. B, the
     * smallest, already has an extra partition of T1, the only topic of A's extra partitions, so C,
     * the next, takes it. Then C, at 6, gives B its extra partition of T3. The assignment sizes end
     * at 6, 5 and 5.
     */
    @Test
    public void testTheSmallestMemberWhichCannotTakeIsPassedOver() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T4))
            .withMember("B", Set.of(T1, T2, T3))
            .withMember("C", Set.of(T1, T2, T3))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 5)
            .withTopic(T2, 3)
            .withTopic(T3, 3)
            .withTopic(T4, 5)
            .build();
        var fixture = new QuotasFixture(spec, describer);
        fixture.addExtras("A", T1);
        fixture.addExtras("B", T1);
        fixture.addExtras("C", T2, T3);
        assertEquals(List.of(7, 4, 5), fixture.assignmentSizes());

        balanceAndAssertBalanced(fixture);

        assertEquals(Map.of(T1, List.of("B", "C"), T2, List.of("C"), T3, List.of("B")), fixture.membersWithExtraPartition());
        assertEquals(List.of(6, 5, 5), fixture.assignmentSizes());
    }

    /**
     * A subscribes to T1 and T3, B to T1 and T2, and C to T2 only. T1, 3 partitions over A and B,
     * has 1 extra partition, A's. T2, 3 partitions over B and C, has 1, B's. T3 gives A 2 more
     * partitions. The assignment sizes are 4, 3 and 1. A, the largest, has no receiver, since B is
     * only one partition smaller, so A waits. B gives its extra partition of T2 to C, and is then
     * two partitions smaller than A, which wakes up and gives B the extra partition of T1. The
     * assignment sizes end at 3, 3 and 2. Without waking A, they would end at 4, 2 and 2, and the
     * extra partition of T1 could still move from A to B.
     */
    @Test
    public void testAWaitingGiverWakesUpWhenASubscriberOfItsTopicsGetsSmaller() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T3))
            .withMember("B", Set.of(T1, T2))
            .withMember("C", Set.of(T2))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .withTopic(T2, 3)
            .withTopic(T3, 2)
            .build();
        var fixture = new QuotasFixture(spec, describer);
        fixture.addExtras("A", T1);
        fixture.addExtras("B", T2);
        assertEquals(List.of(4, 3, 1), fixture.assignmentSizes());

        balanceAndAssertBalanced(fixture);

        assertEquals(Map.of(T1, List.of("B"), T2, List.of("C")), fixture.membersWithExtraPartition());
        assertEquals(List.of(3, 3, 2), fixture.assignmentSizes());
    }

    /**
     * A and B subscribe to T1, T2 and T3, and C and D to T4, T5 and T6, in two cohorts that share
     * no topic and are balanced on their own. T1 to T3 have 5 partitions over A and B, so a minimum
     * quota of 2 and 1 extra partition each, all A's. T4 to T6 have 3 partitions over C and D, so a
     * minimum quota of 1 and 1 extra partition each, all C's. The assignment sizes are 9, 6, 6 and
     * 3. A gives an extra partition to B, and at 8 has no receiver, since B is at 7. C gives one to
     * D, and at 5 has no receiver either. The assignment sizes end at 8, 7, 5 and 4.
     */
    @Test
    public void testCohortsSharingNoTopicAreBalancedOnTheirOwn() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3))
            .withMember("B", Set.of(T1, T2, T3))
            .withMember("C", Set.of(T4, T5, T6))
            .withMember("D", Set.of(T4, T5, T6))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 5)
            .withTopic(T2, 5)
            .withTopic(T3, 5)
            .withTopic(T4, 3)
            .withTopic(T5, 3)
            .withTopic(T6, 3)
            .build();
        var fixture = new QuotasFixture(spec, describer);
        fixture.addExtras("A", T1, T2, T3);
        fixture.addExtras("C", T4, T5, T6);
        assertEquals(List.of(9, 6, 6, 3), fixture.assignmentSizes());

        balanceAndAssertBalanced(fixture);

        assertEquals(
            Map.ofEntries(
                Map.entry(T1, List.of("A")),
                Map.entry(T2, List.of("A")),
                Map.entry(T3, List.of("B")),
                Map.entry(T4, List.of("C")),
                Map.entry(T5, List.of("C")),
                Map.entry(T6, List.of("D"))
            ),
            fixture.membersWithExtraPartition()
        );
        assertEquals(List.of(8, 7, 5, 4), fixture.assignmentSizes());
    }

    /**
     * A and B subscribe to T1 to T4, of 3 partitions each: a minimum quota of 1 and 1 extra
     * partition each, all A's. The assignment sizes are 8 and 4. A gives B two extra partitions in
     * a row, the last of its topics first: T4, then T3. The assignment sizes end at 6 and 6.
     */
    @Test
    public void testAGiverGivesSeveralExtraPartitionsInARowToTheSameReceiver() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3, T4))
            .withMember("B", Set.of(T1, T2, T3, T4))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .withTopic(T2, 3)
            .withTopic(T3, 3)
            .withTopic(T4, 3)
            .build();
        var fixture = new QuotasFixture(spec, describer);
        fixture.addExtras("A", T1, T2, T3, T4);
        assertEquals(List.of(8, 4), fixture.assignmentSizes());

        balanceAndAssertBalanced(fixture);

        assertEquals(Map.of(T1, List.of("A"), T2, List.of("A"), T3, List.of("B"), T4, List.of("B")), fixture.membersWithExtraPartition());
        assertEquals(List.of(6, 6), fixture.assignmentSizes());
    }

    /**
     * On random groups, keeping the counts per cohort of every member or of one member at a time
     * give exactly the same quotas, and these have the balance property. The groups have up to 40
     * cohorts, so that topics become full in a cohort and stop being full as extra partitions move.
     */
    @Test
    public void testBothWaysOfCountingGiveTheSameQuotas() {
        var seeds = new Random(7);
        for (int run = 0; run < 1000; run++) {
            long seed = seeds.nextLong();
            Map<Uuid, List<String>> first = null;
            for (boolean keepCounts : new boolean[] {true, false}) {
                var fixture = randomGroup(new Random(seed));
                new BalanceExtraPartitions(fixture.group, fixture.currentAssignment, fixture.quotas, keepCounts).run();
                assertBalanced(fixture);
                var extras = fixture.membersWithExtraPartition();
                if (first == null) {
                    first = extras;
                } else {
                    assertEquals(first, extras, "seed " + seed + ", keepCounts " + keepCounts);
                }
            }
        }
    }

    /**
     * @return A group of 10 to 40 members in 1 to 40 subscriptions of 1 to 10 topics of 1 to 40
     *         partitions, owning random partitions, with the extra partitions of every topic on
     *         random distinct subscribers.
     */
    private static QuotasFixture randomGroup(Random random) {
        int topicCount = 1 + random.nextInt(10);
        var topicIds = new ArrayList<Uuid>();
        var partitionCounts = new HashMap<Uuid, Integer>();
        for (int topic = 0; topic < topicCount; topic++) {
            topicIds.add(new Uuid(topic + 1, 1L));
            partitionCounts.put(topicIds.get(topic), 1 + random.nextInt(40));
        }
        var subscriptions = new ArrayList<Set<Uuid>>();
        for (int i = 1 + random.nextInt(40); i > 0; i--) {
            var topics = new HashSet<Uuid>();
            for (var topicId : topicIds) {
                if (random.nextInt(2) == 0) {
                    topics.add(topicId);
                }
            }
            topics.add(topicIds.get(random.nextInt(topicCount)));
            subscriptions.add(topics);
        }
        var memberSubscriptions = new TreeMap<String, Set<Uuid>>();
        for (int i = 10 + random.nextInt(31); i > 0; i--) {
            memberSubscriptions.put(String.format("M%02d", i), subscriptions.get(random.nextInt(subscriptions.size())));
        }
        var owned = new HashMap<String, Map<Uuid, Set<Integer>>>();
        for (var topicId : topicIds) {
            var subscribers = memberSubscriptions.keySet().stream().filter(m -> memberSubscriptions.get(m).contains(topicId)).toList();
            for (int partition = 0; partition < partitionCounts.get(topicId) && !subscribers.isEmpty(); partition++) {
                if (random.nextInt(5) < 3) {
                    var owner = subscribers.get(random.nextInt(subscribers.size()));
                    owned.computeIfAbsent(owner, m -> new HashMap<>()).computeIfAbsent(topicId, t -> new HashSet<>()).add(partition);
                }
            }
        }
        var spec = new GroupSpecFixture();
        memberSubscriptions.forEach((memberId, topics) -> spec.withMember(memberId, topics, owned.getOrDefault(memberId, Map.of())));
        var describer = new TopicsFixture();
        partitionCounts.forEach(describer::withTopic);
        var fixture = new QuotasFixture(spec.build(), describer.build());
        for (int topic = 0; topic < fixture.group.topicCount(); topic++) {
            var topicId = fixture.group.topicId(topic);
            var subscribers = new ArrayList<>(memberSubscriptions.keySet().stream().filter(m -> memberSubscriptions.get(m).contains(topicId)).toList());
            Collections.shuffle(subscribers, random);
            for (int i = 0; i < fixture.quotas.extraPartitions(topic); i++) {
                fixture.addExtras(subscribers.get(i), topicId);
            }
        }
        return fixture;
    }

    /**
     * Checks that every topic has its number of extra partitions on distinct subscribers, and that
     * no member with one is at least two partitions larger than a subscriber without one.
     */
    private static void assertBalanced(QuotasFixture fixture) {
        var quotas = fixture.quotas;
        // The moves that the balance property rules out, as "topic: giver to receiver".
        var possibleMoves = new ArrayList<String>();
        for (int topic = 0; topic < fixture.group.topicCount(); topic++) {
            var membersWithExtraPartition = new HashSet<Integer>();
            var members = quotas.membersWithExtraPartition(topic);
            for (int i = 0; i < members.size(); i++) {
                assertTrue(fixture.group.subscribes(members.get(i), topic));
                membersWithExtraPartition.add(members.get(i));
            }
            assertEquals(quotas.extraPartitions(topic), membersWithExtraPartition.size());
            for (int giver : membersWithExtraPartition) {
                for (int receiver = 0; receiver < fixture.group.memberCount(); receiver++) {
                    boolean canTake = fixture.group.subscribes(receiver, topic) && !membersWithExtraPartition.contains(receiver);
                    if (canTake && quotas.assignmentSize(giver) - quotas.assignmentSize(receiver) >= 2) {
                        possibleMoves.add(topic + ": " + fixture.group.memberId(giver) + " to " + fixture.group.memberId(receiver));
                    }
                }
            }
        }
        assertEquals(List.of(), possibleMoves, "assignment sizes " + fixture.assignmentSizes());
    }

    /**
     * Runs the balance step on the quotas of the fixture and checks that they have the balance
     * property.
     */
    private static void balanceAndAssertBalanced(QuotasFixture fixture) {
        new BalanceExtraPartitions(fixture.group, fixture.currentAssignment, fixture.quotas).run();
        assertBalanced(fixture);
    }

    private static QuotasFixture homogeneousGroup() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2))
            .withMember("B", Set.of(T1, T2))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .withTopic(T2, 3)
            .build();
        return new QuotasFixture(spec, describer);
    }

    private static QuotasFixture twoCohortGroup() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2))
            .withMember("B", Set.of(T1))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .withTopic(T2, 1)
            .build();
        return new QuotasFixture(spec, describer);
    }
}
