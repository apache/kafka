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

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests the hand out of the extra partitions that no owner kept. The topic ids sort as
 * T1 &lt; T2 &lt; T3 &lt; T4, and the cohorts are numbered in the order of their first member.
 */
public class HandOutTest {
    private static final Uuid T1 = new Uuid(1L, 1L);
    private static final Uuid T2 = new Uuid(2L, 1L);
    private static final Uuid T3 = new Uuid(3L, 1L);
    private static final Uuid T4 = new Uuid(4L, 1L);

    /**
     * A subscribes to T1 and T2, B and D to T1 and T3, and C to T2 and T4. T1, 4 partitions over
     * A, B and D, has 1 extra partition, and T2, 3 partitions over A and C, has 1 too; T3 and T4
     * have none. The base partitions give sizes of 2 to A, B and D, and 3 to C. T2 has the fewest
     * subscribers and goes first: its extra partition goes to A, the smaller of A and C. Then the
     * one of T1 goes to B, since A now has 3. The sizes are within one of each other where the
     * subscriptions allow it, and the balance step has nothing to do. Handing out T1 first would
     * have given both extra partitions to A, the first cohort on ties: 4 partitions for A and 2
     * for B, which subscribes to T1.
     */
    @Test
    public void testTopicsWithTheFewestSubscribersGoFirst() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2))
            .withMember("B", Set.of(T1, T3))
            .withMember("C", Set.of(T2, T4))
            .withMember("D", Set.of(T1, T3))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 3)
            .withTopic(T3, 2)
            .withTopic(T4, 2)
            .build();
        var fixture = new SharesFixture(spec, describer);

        new HandOut(fixture.group, fixture.shares).run();

        assertEquals(Map.of(T1, List.of("B"), T2, List.of("A")), fixture.membersWithExtra());
        assertEquals(List.of(3, 3, 3, 2), fixture.sizes());
        assertFalse(ShareBalancer.mayMove(fixture.group, fixture.shares));
    }

    /**
     * A subscribes to T1 and T2, B to T1 and T3. T1, 3 partitions, has 1 extra partition. T2
     * gives A 2 partitions and T3 gives B 1, so B has the smaller assignment and gets it.
     */
    @Test
    public void testExtraPartitionGoesToTheCohortWithTheSmallestAssignment() {
        var fixture = twoCohortGroup(2, 1);

        new HandOut(fixture.group, fixture.shares).run();

        assertEquals(Map.of(T1, List.of("B")), fixture.membersWithExtra());
        assertEquals(List.of(3, 3), fixture.sizes());
    }

    /**
     * The same group, with 1 partition for T2 and T3: A and B have the same size, and the extra
     * partition of T1 goes to the first cohort, A's.
     */
    @Test
    public void testTiesGoToTheFirstCohort() {
        var fixture = twoCohortGroup(1, 1);

        new HandOut(fixture.group, fixture.shares).run();

        assertEquals(Map.of(T1, List.of("A")), fixture.membersWithExtra());
        assertEquals(List.of(3, 2), fixture.sizes());
    }

    /**
     * A, B and C subscribe to T1, T2 and T3, of 4 partitions each: 1 extra partition each. Each
     * goes to the next member having no extra partition yet, and every member ends with one.
     */
    @Test
    public void testMembersOfACohortAreServedInTurn() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3))
            .withMember("B", Set.of(T1, T2, T3))
            .withMember("C", Set.of(T1, T2, T3))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 4)
            .withTopic(T3, 4)
            .build();
        var fixture = new SharesFixture(spec, describer);

        new HandOut(fixture.group, fixture.shares).run();

        assertEquals(Map.of(T1, List.of("A"), T2, List.of("B"), T3, List.of("C")), fixture.membersWithExtra());
        assertEquals(List.of(4, 4, 4), fixture.sizes());
    }

    /**
     * A, B and C subscribe to T1, of 4 partitions, and T2, of 5: 1 extra partition of T1 and 2
     * of T2. A kept one of T2. The extra partition of T1 skips A, which has more extra partitions
     * than B and C, and goes to B; the remaining one of T2 goes to C, the only member without
     * any.
     */
    @Test
    public void testMembersWithTheFewestExtraPartitionsAreServedFirst() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2))
            .withMember("B", Set.of(T1, T2))
            .withMember("C", Set.of(T1, T2))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 5)
            .build();
        var fixture = new SharesFixture(spec, describer);
        fixture.addExtras("A", T2);

        new HandOut(fixture.group, fixture.shares).run();

        assertEquals(Map.of(T1, List.of("B"), T2, List.of("A", "C")), fixture.membersWithExtra());
        assertEquals(List.of(3, 3, 3), fixture.sizes());
    }

    /**
     * A, B and C subscribe to T1, T2 and T3, of 5 partitions each: 2 extra partitions each. A
     * kept one of T1, and B and C both of T2 and T3. The last extra partition of T1 cannot go to
     * A, the member with the fewest extra partitions, which has one of T1 already: it goes to the
     * member without one having the fewest extra partitions, B before C in member order. The
     * balance step evens the sizes out later.
     */
    @Test
    public void testAMemberWithMoreExtraPartitionsGetsOneWhenTheOthersHaveOneOfTheTopic() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3))
            .withMember("B", Set.of(T1, T2, T3))
            .withMember("C", Set.of(T1, T2, T3))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 5)
            .withTopic(T2, 5)
            .withTopic(T3, 5)
            .build();
        var fixture = new SharesFixture(spec, describer);
        fixture.addExtras("A", T1);
        fixture.addExtras("B", T2, T3);
        fixture.addExtras("C", T2, T3);

        new HandOut(fixture.group, fixture.shares).run();

        assertEquals(Map.of(T1, List.of("A", "B"), T2, List.of("B", "C"), T3, List.of("B", "C")), fixture.membersWithExtra());
        assertEquals(List.of(4, 6, 5), fixture.sizes());
    }

    /**
     * A subscribes to T1 only, B and C to T1 and T2. T1, 5 partitions, has 2 extra partitions,
     * and A kept one. A has the smallest assignment, but its cohort has no member left without an
     * extra partition of T1, and the other one goes to B.
     */
    @Test
    public void testACohortWhoseMembersAllHaveAnExtraPartitionGetsNoMore() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1))
            .withMember("B", Set.of(T1, T2))
            .withMember("C", Set.of(T1, T2))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 5)
            .withTopic(T2, 2)
            .build();
        var fixture = new SharesFixture(spec, describer);
        fixture.addExtras("A", T1);

        new HandOut(fixture.group, fixture.shares).run();

        assertEquals(Map.of(T1, List.of("A", "B")), fixture.membersWithExtra());
        assertEquals(List.of(2, 3, 2), fixture.sizes());
    }

    /**
     * A and B subscribe to T1, of 3 partitions: 1 base partition each and 1 extra partition,
     * which A kept. Nothing is left to hand out, and the shares do not change.
     */
    @Test
    public void testNothingToHandOut() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1))
            .withMember("B", Set.of(T1))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .build();
        var fixture = new SharesFixture(spec, describer);
        fixture.addExtras("A", T1);

        new HandOut(fixture.group, fixture.shares).run();

        assertEquals(Map.of(T1, List.of("A")), fixture.membersWithExtra());
        assertEquals(List.of(2, 1), fixture.sizes());
    }

    /**
     * A subscribes to T1 and T2, B to T1 and T3, with 3 partitions for T1.
     */
    private static SharesFixture twoCohortGroup(int t2Partitions, int t3Partitions) {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2))
            .withMember("B", Set.of(T1, T3))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .withTopic(T2, t2Partitions)
            .withTopic(T3, t3Partitions)
            .build();
        return new SharesFixture(spec, describer);
    }
}
