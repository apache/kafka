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

import java.util.Arrays;
import java.util.Set;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of {@link PartitionPlacement}, on quotas set by hand. A topic with {@code P} partitions and
 * {@code N} subscribers has a minimum quota of {@code P / N}, and each test gives its {@code P % N}
 * extra partitions to chosen subscribers.
 */
public class PartitionPlacementTest {
    private static final Uuid T1 = new Uuid(1L, 1L);
    private static final Uuid T2 = new Uuid(2L, 1L);

    private static final int A = 0;
    private static final int B = 1;
    private static final int C = 2;

    /**
     * The value of every entry of the assignment before placing, so that an entry
     * {@link PartitionPlacement#place} did not write stands out.
     */
    private static final int UNTOUCHED = -1;

    /**
     * T1 has 7 partitions and 3 subscribers, so a minimum quota of 2 and 1 extra partition, A's.
     * The quota of A is 3, and the quotas of B and C are 2. A keeps its 3 lowest partitions 1, 3
     * and 5, B keeps 0 and 2, and C keeps 4. Partition 6 of A goes to C, the only member below its
     * quota.
     */
    @Test
    public void testOwnersKeepTheirLowestPartitionsUpToTheirQuota() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 1, 3, 5, 6)))
            .withMember("B", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 0, 2)))
            .withMember("C", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 4)))
            .build();
        var fixture = new QuotasFixture(spec, new TopicsFixture().withTopic(T1, 7).build());
        fixture.addExtras("A", T1);
        var placement = new PartitionPlacement(fixture.group, fixture.currentAssignment, fixture.quotas);

        assertArrayEquals(new int[] {B, A, B, A, C, A, C}, place(fixture, placement, T1));
    }

    /**
     * T1 has 6 partitions and 3 subscribers, so a minimum quota of 2 and no extra partition. C owns
     * partitions 1 to 4, and keeps 1 and 2. The other partitions, 0 and 5 without an owner and 3
     * and 4 beyond the quota of C, go to A and then B, the lowest ids first.
     */
    @Test
    public void testMovingPartitionsGoToTheMembersBelowTheirQuotaInMemberOrder() {
        var spec = new GroupSpecFixture()
            .withMember("C", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 1, 2, 3, 4)))
            .withMember("B", Set.of(T1))
            .withMember("A", Set.of(T1))
            .build();
        var fixture = new QuotasFixture(spec, new TopicsFixture().withTopic(T1, 6).build());
        var placement = new PartitionPlacement(fixture.group, fixture.currentAssignment, fixture.quotas);

        assertArrayEquals(new int[] {A, C, C, A, B, B}, place(fixture, placement, T1));
    }

    /**
     * A and C subscribe to T1, and B to T1 and T2, so that A and C form cohort 0 and B cohort 1. T1
     * has 6 partitions over its 3 subscribers, so a minimum quota of 2. Nobody owns them, and they
     * go cohort by cohort: to A and C, then to B. T2 has a single partition and a single
     * subscriber, B, which gets it when the same {@code PartitionPlacement} places T2 after T1.
     */
    @Test
    public void testMovingPartitionsGoCohortByCohort() {
        var spec = new GroupSpecFixture()
            .withMember("C", Set.of(T1))
            .withMember("B", Set.of(T1, T2))
            .withMember("A", Set.of(T1))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 6)
            .withTopic(T2, 1)
            .build();
        var fixture = new QuotasFixture(spec, describer);
        assertArrayEquals(new int[] {0, 1}, fixture.group.cohortsOf(fixture.topic(T1)));

        var placement = new PartitionPlacement(fixture.group, fixture.currentAssignment, fixture.quotas);

        assertArrayEquals(new int[] {A, A, C, C, B, B}, place(fixture, placement, T1));
        assertArrayEquals(new int[] {B}, place(fixture, placement, T2));
    }

    /**
     * T1 has 2 partitions and 3 subscribers, so a minimum quota of 0 and 2 extra partitions, C's
     * and A's. C owns both partitions and keeps partition 0, and A gets partition 1, while B, whose
     * quota is 0, gets nothing.
     */
    @Test
    public void testMovingPartitionsWithAMinQuotaOfZeroGoOnlyToTheMembersWithAnExtraPartition() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1))
            .withMember("B", Set.of(T1))
            .withMember("C", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 0, 1)))
            .build();
        var fixture = new QuotasFixture(spec, new TopicsFixture().withTopic(T1, 2).build());
        fixture.addExtras("C", T1);
        fixture.addExtras("A", T1);
        var placement = new PartitionPlacement(fixture.group, fixture.currentAssignment, fixture.quotas);

        assertArrayEquals(new int[] {C, A}, place(fixture, placement, T1));
    }

    /**
     * T1 and T2 have 2 partitions over 3 subscribers each, so a minimum quota of 0 and 2 extra
     * partitions each, and nobody owns them. The extra partitions of T2 are C's and A's, in that
     * order, as the round robin of the second step gives them after T1 took A's and B's. The
     * partitions of T2 go in the order of its extra partitions: 0 to C and 1 to A.
     */
    @Test
    public void testMovingPartitionsWithAMinQuotaOfZeroGoInTheOrderOfTheExtraPartitions() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2))
            .withMember("B", Set.of(T1, T2))
            .withMember("C", Set.of(T1, T2))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 2)
            .withTopic(T2, 2)
            .build();
        var fixture = new QuotasFixture(spec, describer);
        fixture.addExtras("A", T1);
        fixture.addExtras("B", T1);
        fixture.addExtras("C", T2);
        fixture.addExtras("A", T2);
        var placement = new PartitionPlacement(fixture.group, fixture.currentAssignment, fixture.quotas);

        assertArrayEquals(new int[] {C, A}, place(fixture, placement, T2));
    }

    /**
     * T1 has 5 partitions and 2 subscribers, so a minimum quota of 2 and 1 extra partition, A's.
     * The quotas are 3 for A and 2 for B, exactly what they own, and nothing moves.
     */
    @Test
    public void testNothingMovesWhenEveryOwnerOwnsItsQuota() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 0, 2, 4)))
            .withMember("B", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 1, 3)))
            .build();
        var fixture = new QuotasFixture(spec, new TopicsFixture().withTopic(T1, 5).build());
        fixture.addExtras("A", T1);
        var placement = new PartitionPlacement(fixture.group, fixture.currentAssignment, fixture.quotas);

        assertFalse(placement.place(fixture.topic(T1), new int[5]));
    }

    /**
     * A, B and C subscribe to T1 and T2, both placed with the same placement. T1 has 4 partitions,
     * so a minimum quota of 1 and 1 extra partition, A's: A keeps its partitions 0 and 1, and B and
     * C get 2 and 3. T2 has 3 partitions, a minimum quota of 1 and no extra partition, and nobody
     * owns them: A, B and C get one each, as nothing of T1 carries over, neither the extra
     * partition of A nor the partitions it kept.
     */
    @Test
    public void testNothingCarriesOverFromOneTopicToTheNext() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T1, 0, 1)))
            .withMember("B", Set.of(T1, T2))
            .withMember("C", Set.of(T1, T2))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 3)
            .build();
        var fixture = new QuotasFixture(spec, describer);
        fixture.addExtras("A", T1);
        var placement = new PartitionPlacement(fixture.group, fixture.currentAssignment, fixture.quotas);

        assertArrayEquals(new int[] {A, A, B, C}, place(fixture, placement, T1));
        assertArrayEquals(new int[] {A, B, C}, place(fixture, placement, T2));
    }

    /**
     * T1 has 5 partitions and 2 subscribers, so a minimum quota of 2 and 1 extra partition, A's.
     * A owns partitions 0 and 1, its minimum quota, and keeps them, and gets partition 2 to reach
     * its quota of 3. B gets 3 and 4.
     */
    @Test
    public void testAMemberOwningItsMinQuotaGetsItsExtraPartition() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), mkAssignment(mkTopicAssignment(T1, 0, 1)))
            .withMember("B", Set.of(T1))
            .build();
        var fixture = new QuotasFixture(spec, new TopicsFixture().withTopic(T1, 5).build());
        fixture.addExtras("A", T1);
        var placement = new PartitionPlacement(fixture.group, fixture.currentAssignment, fixture.quotas);

        assertArrayEquals(new int[] {A, A, A, B, B}, place(fixture, placement, T1));
    }

    /**
     * Places the topic, expecting partitions to move.
     *
     * @return Per partition of the topic, the member getting it.
     */
    private static int[] place(QuotasFixture fixture, PartitionPlacement placement, Uuid topicId) {
        int topic = fixture.topic(topicId);
        var assignment = new int[fixture.group.partitionCount(topic)];
        Arrays.fill(assignment, UNTOUCHED);
        assertTrue(placement.place(topic, assignment));
        return assignment;
    }
}
