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
 * Tests the balance step of the shares. The topic ids sort as T1 &lt; T2 &lt; T3 &lt; T4 &lt; T5,
 * the members are numbered in member id order, and the cohorts in the order of their first
 * member.
 */
public class ShareBalancerTest {
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
        var fixture = new SharesFixture(new GroupSpecFixture().build(), new TopicsFixture().withTopic(T1, 3).build());
        assertFalse(ShareBalancer.mayMove(fixture.group, fixture.shares));
    }

    /**
     * A and B subscribe to T1 and T2, of 3 partitions each: 1 extra partition each. When A and B
     * have one each, the cohort is within one and nothing may move; when A has both, it may.
     */
    @Test
    public void testMayMoveWhenACohortHasMembersTwoExtraPartitionsApart() {
        var balanced = homogeneousGroup();
        balanced.addExtras("A", T1);
        balanced.addExtras("B", T2);
        assertFalse(ShareBalancer.mayMove(balanced.group, balanced.shares));

        var unbalanced = homogeneousGroup();
        unbalanced.addExtras("A", T1, T2);
        assertTrue(ShareBalancer.mayMove(unbalanced.group, unbalanced.shares));
    }

    /**
     * A subscribes to T1 and T2, B to T1 only. T1, 3 partitions, has 1 extra partition, and T2
     * gives A 1 more partition. With the extra partition, B has the size of A and nothing may
     * move; without it, A is two partitions larger, and it may. Balancing then moves the extra
     * partition to B.
     */
    @Test
    public void testMayMoveWhenSizesAcrossCohortsAreTwoApart() {
        var balanced = twoCohortGroup();
        balanced.addExtras("B", T1);
        assertFalse(ShareBalancer.mayMove(balanced.group, balanced.shares));

        var unbalanced = twoCohortGroup();
        unbalanced.addExtras("A", T1);
        assertTrue(ShareBalancer.mayMove(unbalanced.group, unbalanced.shares));

        balance(unbalanced);

        assertEquals(Map.of(T1, List.of("B")), unbalanced.membersWithExtra());
        assertEquals(List.of(2, 2), unbalanced.sizes());
    }

    /**
     * A subscribes to T1, T2 and T3, B to T1 and T2, C to T2 and T3, and D to T1 and T3: four
     * cohorts, every topic having three of them. T1, T2 and T3, of 5 partitions each, give 1 base
     * partition to their subscribers and have 2 extra partitions each, piled on A and B: A has one
     * of every topic, B of T1 and T2, and C the second one of T3. The sizes are 6, 4, 3 and 2.
     * After the balance, no extra partition can move to a subscriber of its topic at least two
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
        var fixture = new SharesFixture(spec, describer);
        fixture.addExtras("A", T1, T2, T3);
        fixture.addExtras("B", T1, T2);
        fixture.addExtras("C", T3);
        assertEquals(List.of(6, 4, 3, 2), fixture.sizes());

        balance(fixture);

        assertBalanced(fixture);
    }

    /**
     * A subscribes to T1 and T2, B to T1 only, and C to T2 and T3. T1 has no extra partition, T2
     * has 1, A's. The sizes are 3, 1 and 2: A and B are two apart, so something may move, but B
     * cannot take the extra partition of T2, and C is only one partition smaller. Nothing moves.
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
        var fixture = new SharesFixture(spec, describer);
        fixture.addExtras("A", T2);
        assertTrue(ShareBalancer.mayMove(fixture.group, fixture.shares));

        balance(fixture);

        assertEquals(Map.of(T2, List.of("A")), fixture.membersWithExtra());
        assertEquals(List.of(3, 1, 2), fixture.sizes());
    }

    /**
     * A and B subscribe to T1, T2 and T3, and C to T1 and T4, in cohorts 0 and 1. T1, 5
     * partitions, gives 1 base partition to each and has 2 extra partitions, A's and B's; T2 and
     * T3, 3 partitions over A and B, have 1 extra partition each, both A's; T4 gives C 2 more
     * partitions. The sizes are 6, 4 and 3. A gives the extra partition of T1 to C, the smallest,
     * and the sizes end at 5, 4 and 4: one move, where balancing the cohort of A and B first would
     * move an extra partition from A to B, and then one of T1 to C.
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
        var fixture = new SharesFixture(spec, describer);
        fixture.addExtras("A", T1, T2, T3);
        fixture.addExtras("B", T1);
        assertEquals(List.of(6, 4, 3), fixture.sizes());

        balance(fixture);

        assertEquals(Map.of(T1, List.of("B", "C"), T2, List.of("A"), T3, List.of("A")), fixture.membersWithExtra());
        assertEquals(List.of(5, 4, 4), fixture.sizes());
    }

    /**
     * A and B subscribe to T1 and T2, of 5 partitions each, and hold A: T1 [0, 1, 2], T2 [0, 1]
     * and B: T1 [3, 4], T2 [2, 3, 4]; C joins with the same subscription. Each topic has 1 base
     * partition and 2 extra partitions, all kept by A and B, which own more than 1 partition of
     * each. The sizes are 4, 4 and 2. A and B are equally large and own all their extra
     * partitions, so member order lets A give one to C; both cost a move, and A's topics are tried
     * from the last: T2's goes. The sizes end at 3, 4 and 3.
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
        var fixture = new SharesFixture(spec, describer);
        fixture.addExtras("A", T1, T2);
        fixture.addExtras("B", T1, T2);

        balance(fixture);

        assertEquals(Map.of(T1, List.of("A", "B"), T2, List.of("B", "C")), fixture.membersWithExtra());
        assertEquals(List.of(3, 4, 3), fixture.sizes());
    }

    /**
     * A, B and C subscribe to T1, T2 and T3, of 4 partitions each: 1 base partition and 1 extra
     * partition each. A has the three extra partitions, and owns none of their partitions; B owns
     * 2 partitions of T2 and C 2 of T1. The sizes are 6, 3 and 3. A gives to B, first in member
     * order of the smallest, the extra partition of T2, which saves a move since B keeps its 2
     * partitions; then to C the one of T1, for the same reason. A keeps T3's.
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
        var fixture = new SharesFixture(spec, describer);
        fixture.addExtras("A", T1, T2, T3);

        balance(fixture);

        assertEquals(Map.of(T1, List.of("C"), T2, List.of("B"), T3, List.of("A")), fixture.membersWithExtra());
        assertEquals(List.of(4, 4, 4), fixture.sizes());
    }

    /**
     * A subscribes to T1, T2 and T3, B to T1 and T2. T1 and T2 have 3 partitions over A and B,
     * so 1 extra partition each, both A's; T3 gives A 1 more partition. The sizes are 5 and 2. A
     * gives B its cheapest extra partition: the one of T2, since B owns 2 partitions of T2, which
     * it then keeps. The sizes end at 4 and 3.
     */
    @Test
    public void testTheGiverGivesItsCheapestExtraPartitionToAnotherCohort() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3))
            .withMember("B", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T2, 0, 1)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .withTopic(T2, 3)
            .withTopic(T3, 1)
            .build();
        var fixture = new SharesFixture(spec, describer);
        fixture.addExtras("A", T1, T2);

        balance(fixture);

        assertEquals(Map.of(T1, List.of("A"), T2, List.of("B")), fixture.membersWithExtra());
        assertEquals(List.of(4, 3), fixture.sizes());
    }

    /**
     * A, B and C subscribe to T1 to T4, of 4 partitions each: 1 base partition and 1 extra
     * partition each. A has the extra partitions of T1 and T2 and owns 2 partitions of each; B
     * has those of T3 and T4 and owns 2 partitions of T3 only. The sizes are 6, 6 and 4. A and B
     * are equally large, but B has a free extra partition, T4's, which costs no move to give: B
     * gives it to C, and A keeps both of its extra partitions.
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
        var fixture = new SharesFixture(spec, describer);
        fixture.addExtras("A", T1, T2);
        fixture.addExtras("B", T3, T4);

        balance(fixture);

        assertEquals(Map.of(T1, List.of("A"), T2, List.of("A"), T3, List.of("B"), T4, List.of("C")), fixture.membersWithExtra());
        assertEquals(List.of(6, 5, 5), fixture.sizes());
    }

    /**
     * A subscribes to T1 and T2, B to T1 and T4, and C to T1 and T3, in cohorts 0, 1 and 2. T1,
     * 4 partitions over the three, has 1 extra partition, A's; T2 gives A 2 more partitions, T4
     * gives B 1, and T3 gives C 1. The sizes are 4, 2 and 2: B and C are equally small, own
     * nothing, and B, first in member order, takes the extra partition of T1.
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
        var fixture = new SharesFixture(spec, describer);
        fixture.addExtras("A", T1);

        balance(fixture);

        assertEquals(Map.of(T1, List.of("B")), fixture.membersWithExtra());
        assertEquals(List.of(3, 3, 2), fixture.sizes());
    }

    /**
     * A subscribes to T1 and T2, B and C to T1 only. T1, 4 partitions, gives 1 base partition to
     * each and has 1 extra partition, A's; T2 gives A 1 more partition. The sizes are 3, 1 and 1.
     * C owns 2 partitions of T1, more than the base partition, and B none: of the two smallest
     * members, C receives the extra partition, and then keeps both of its partitions.
     */
    @Test
    public void testOnEqualSizesAReceiverOwningMoreThanTheBaseReceivesFirst() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2))
            .withMember("B", Set.of(T1))
            .withMember("C", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 1, 2)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 1)
            .build();
        var fixture = new SharesFixture(spec, describer);
        fixture.addExtras("A", T1);

        balance(fixture);

        assertEquals(Map.of(T1, List.of("C")), fixture.membersWithExtra());
        assertEquals(List.of(2, 1, 2), fixture.sizes());
    }

    /**
     * A subscribes to T1 to T4, B and C to T1 to T5. T1 to T4, 4 partitions over 3 subscribers
     * each, have 1 extra partition each, all A's; T5 gives B and C 1 more partition. The sizes
     * are 8, 5 and 5. A gives to B, the first of the smallest, then again to C: the entry of B
     * with its former size is skipped. The sizes end at 6, 6 and 6, where looking at B with its
     * former size would have stopped at 7, 6 and 5.
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
        var fixture = new SharesFixture(spec, describer);
        fixture.addExtras("A", T1, T2, T3, T4);

        balance(fixture);

        assertEquals(Map.of(T1, List.of("A"), T2, List.of("A"), T3, List.of("C"), T4, List.of("B")), fixture.membersWithExtra());
        assertEquals(List.of(6, 6, 6), fixture.sizes());
    }

    /**
     * A subscribes to T1 and T4, and B and C to T1, T2 and T3. T1, 5 partitions over the three,
     * has 2 extra partitions, A's and B's; T2 and T3, 3 partitions over B and C, have 1 extra
     * partition each, both C's; T4 gives A 5 more partitions. The sizes are 7, 4 and 5. B, the
     * smallest, already has the extra partition of T1, the only one A has: C, the next, takes it.
     * Then C, at 6, gives B its extra partition of T3. The sizes end at 6, 5 and 5.
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
        var fixture = new SharesFixture(spec, describer);
        fixture.addExtras("A", T1);
        fixture.addExtras("B", T1);
        fixture.addExtras("C", T2, T3);
        assertEquals(List.of(7, 4, 5), fixture.sizes());

        balance(fixture);

        assertEquals(Map.of(T1, List.of("B", "C"), T2, List.of("C"), T3, List.of("B")), fixture.membersWithExtra());
        assertEquals(List.of(6, 5, 5), fixture.sizes());
    }

    /**
     * A subscribes to T1 and T3, B to T1 and T2, and C to T2 only. T1, 3 partitions over A and B,
     * has 1 extra partition, A's; T2, 3 partitions over B and C, has 1, B's; T3 gives A 2 more
     * partitions. The sizes are 4, 3 and 1. A, the largest, has no receiver: B is only one
     * partition smaller, and A waits. B gives its extra partition of T2 to C, and is then two
     * partitions smaller than A, which wakes up and gives it the extra partition of T1. The sizes
     * end at 3, 3 and 2; without waking A, they would end at 4, 2 and 2, and the extra partition of
     * T1 could still move from A to B.
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
        var fixture = new SharesFixture(spec, describer);
        fixture.addExtras("A", T1);
        fixture.addExtras("B", T2);
        assertEquals(List.of(4, 3, 1), fixture.sizes());

        balance(fixture);

        assertEquals(Map.of(T1, List.of("B"), T2, List.of("C")), fixture.membersWithExtra());
        assertEquals(List.of(3, 3, 2), fixture.sizes());
    }

    /**
     * A and B subscribe to T1, T2 and T3, and C and D to T4, T5 and T6: two cohorts sharing no
     * topic, each balanced on its own. T1 to T3 have 5 partitions over A and B, so 2 base
     * partitions and 1 extra partition each, all A's; T4 to T6 have 3 partitions over C and D,
     * so 1 base partition and 1 extra each, all C's. The sizes are 9, 6, 6 and 3. A gives an
     * extra partition to B, and at 8 has no receiver: B is at 7, and the cohort of A and B is
     * done. C gives one to D, and at 5 has no receiver either. The sizes end at 8, 7, 5 and 4.
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
        var fixture = new SharesFixture(spec, describer);
        fixture.addExtras("A", T1, T2, T3);
        fixture.addExtras("C", T4, T5, T6);
        assertEquals(List.of(9, 6, 6, 3), fixture.sizes());

        balance(fixture);

        assertEquals(Map.of(
            T1, List.of("A"), T2, List.of("A"), T3, List.of("B"),
            T4, List.of("C"), T5, List.of("C"), T6, List.of("D")
        ), fixture.membersWithExtra());
        assertEquals(List.of(8, 7, 5, 4), fixture.sizes());
    }

    /**
     * On random groups, keeping the counts per cohort of every member or of one member at a time,
     * and testing the subscriptions with bit sets or by searching the topics of the cohorts, give
     * exactly the same shares, which have the balance property. Which ways the balance uses
     * otherwise depends on the size of the group.
     */
    @Test
    public void testTheWaysOfCountingGiveTheSameShares() {
        var seeds = new Random(7);
        for (int run = 0; run < 300; run++) {
            long seed = seeds.nextLong();
            Map<Uuid, List<String>> first = null;
            for (boolean keepCounts : new boolean[] {true, false}) {
                for (boolean bitsFit : new boolean[] {true, false}) {
                    var fixture = randomGroup(new Random(seed));
                    new ShareBalancer(fixture.group, fixture.current, fixture.shares, keepCounts, bitsFit).run();
                    assertBalanced(fixture);
                    var extras = fixture.membersWithExtra();
                    if (first == null) {
                        first = extras;
                    } else {
                        assertEquals(first, extras, "seed " + seed + ", keepCounts " + keepCounts + ", bitsFit " + bitsFit);
                    }
                }
            }
        }
    }

    /**
     * @return A group of 2 to 10 members in 1 to 4 subscriptions of 1 to 6 topics of 1 to 15
     *         partitions, owning random partitions, with the extra partitions of every topic on
     *         random distinct subscribers.
     */
    private static SharesFixture randomGroup(Random random) {
        int topicCount = 1 + random.nextInt(6);
        var topicIds = new ArrayList<Uuid>();
        var partitionCounts = new HashMap<Uuid, Integer>();
        for (int topic = 0; topic < topicCount; topic++) {
            topicIds.add(new Uuid(topic + 1, 1L));
            partitionCounts.put(topicIds.get(topic), 1 + random.nextInt(15));
        }
        var subscriptions = new ArrayList<Set<Uuid>>();
        for (int i = 1 + random.nextInt(4); i > 0; i--) {
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
        for (int i = 2 + random.nextInt(9); i > 0; i--) {
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
        var fixture = new SharesFixture(spec.build(), describer.build());
        for (int topic = 0; topic < fixture.group.topicCount(); topic++) {
            var topicId = fixture.group.topicId(topic);
            var subscribers = new ArrayList<>(memberSubscriptions.keySet().stream().filter(m -> memberSubscriptions.get(m).contains(topicId)).toList());
            Collections.shuffle(subscribers, random);
            for (int i = 0; i < fixture.shares.extraPartitions(topic); i++) {
                fixture.addExtras(subscribers.get(i), topicId);
            }
        }
        return fixture;
    }

    /**
     * Checks that every topic has its number of extra partitions on distinct subscribers, and
     * that no member having one is at least two partitions larger than a subscriber without one.
     */
    private static void assertBalanced(SharesFixture fixture) {
        var shares = fixture.shares;
        for (int topic = 0; topic < fixture.group.topicCount(); topic++) {
            var holders = new HashSet<Integer>();
            var members = shares.membersWithExtra(topic);
            for (int i = 0; i < members.size(); i++) {
                assertTrue(fixture.group.subscribes(members.get(i), topic));
                holders.add(members.get(i));
            }
            assertEquals(shares.extraPartitions(topic), holders.size());
            for (int holder : holders) {
                for (int member = 0; member < fixture.group.memberCount(); member++) {
                    if (fixture.group.subscribes(member, topic) && !holders.contains(member)) {
                        assertTrue(shares.size(holder) - shares.size(member) < 2, "an extra partition of topic " + topic
                            + " could move from " + fixture.group.memberId(holder) + " to " + fixture.group.memberId(member)
                            + ", sizes " + fixture.sizes());
                    }
                }
            }
        }
    }

    private static void balance(SharesFixture fixture) {
        new ShareBalancer(fixture.group, fixture.current, fixture.shares).run();
        assertBalanced(fixture);
    }

    private static SharesFixture homogeneousGroup() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2))
            .withMember("B", Set.of(T1, T2))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .withTopic(T2, 3)
            .build();
        return new SharesFixture(spec, describer);
    }

    private static SharesFixture twoCohortGroup() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2))
            .withMember("B", Set.of(T1))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .withTopic(T2, 1)
            .build();
        return new SharesFixture(spec, describer);
    }
}
