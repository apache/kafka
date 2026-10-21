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

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests of {@link KeepStickyExtraPartitions}.
 */
public class KeepStickyExtraPartitionsTest {
    private static final Uuid T1 = new Uuid(1L, 1L);
    private static final Uuid T2 = new Uuid(2L, 1L);
    private static final Uuid T3 = new Uuid(3L, 1L);

    /**
     * T1, 3 partitions over 3 members, has a minimum quota of 1 and no extra partition. T2, 8
     * partitions, has a minimum quota of 2 and 2 extra partitions. A owns 3 partitions of T2 and C
     * owns 4. Both own more than the minimum quota, so each keeps one extra partition, even C,
     * which owns two partitions more than the minimum quota. The assignment sizes are 4, 3 and 4.
     */
    @Test
    public void testOwnersAboveTheMinQuotaKeepAnExtraPartition() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T1, 0), mkTopicAssignment(T2, 0, 1, 2)))
            .withMember("B", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T1, 1), mkTopicAssignment(T2, 3)))
            .withMember("C", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T1, 2), mkTopicAssignment(T2, 4, 5, 6, 7)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .withTopic(T2, 8)
            .build();
        var fixture = new QuotasFixture(spec, describer);

        new KeepStickyExtraPartitions(fixture.group, fixture.currentAssignment, fixture.quotas).run();

        assertEquals(Map.of(T2, List.of("A", "C")), fixture.membersWithExtraPartition());
        assertEquals(List.of(4, 3, 4), fixture.assignmentSizes());
    }

    /**
     * T1 and T2 have 4 partitions each over A, B and C: a minimum quota of 1 and 1 extra
     * partition each. A owns 2 partitions of both and B 2 of T1. T1 has two owners above the
     * minimum quota for one extra partition, of the same assignment size so far: A keeps it,
     * first in member order. A also keeps the one of T2, of which only A owns more than the
     * minimum quota.
     */
    @Test
    public void testOwnersOfTheSameSizeKeepInMemberOrder() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T1, 0, 1), mkTopicAssignment(T2, 0, 1)))
            .withMember("B", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T1, 2, 3), mkTopicAssignment(T2, 2)))
            .withMember("C", Set.of(T1, T2))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 4)
            .build();
        var fixture = new QuotasFixture(spec, describer);

        new KeepStickyExtraPartitions(fixture.group, fixture.currentAssignment, fixture.quotas).run();

        assertEquals(Map.of(T1, List.of("A"), T2, List.of("A")), fixture.membersWithExtraPartition());
        assertEquals(List.of(4, 2, 2), fixture.assignmentSizes());
    }

    /**
     * Three members own 3 of the 9 partitions of T1, T2 and T3 each; a fourth member joins. Each
     * topic now has a minimum quota of 2 and 1 extra partition, and its 3 owners all own more than
     * the minimum quota. The owners with the smallest assignments keep the extra partitions: A for
     * T1; then B and C have the smaller assignment size for T2, B first in member order; then C
     * for T3. D gets none, and the assignment sizes are within one of each other.
     */
    @Test
    public void testOwnersWithTheSmallestAssignmentKeepTheExtraPartitions() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3), mkAssignment(
                mkTopicAssignment(T1, 0, 1, 2), mkTopicAssignment(T2, 0, 1, 2), mkTopicAssignment(T3, 0, 1, 2)))
            .withMember("B", Set.of(T1, T2, T3), mkAssignment(
                mkTopicAssignment(T1, 3, 4, 5), mkTopicAssignment(T2, 3, 4, 5), mkTopicAssignment(T3, 3, 4, 5)))
            .withMember("C", Set.of(T1, T2, T3), mkAssignment(
                mkTopicAssignment(T1, 6, 7, 8), mkTopicAssignment(T2, 6, 7, 8), mkTopicAssignment(T3, 6, 7, 8)))
            .withMember("D", Set.of(T1, T2, T3))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 9)
            .withTopic(T2, 9)
            .withTopic(T3, 9)
            .build();
        var fixture = new QuotasFixture(spec, describer);

        new KeepStickyExtraPartitions(fixture.group, fixture.currentAssignment, fixture.quotas).run();

        assertEquals(Map.of(T1, List.of("A"), T2, List.of("B"), T3, List.of("C")), fixture.membersWithExtraPartition());
        assertEquals(List.of(7, 7, 7, 6), fixture.assignmentSizes());
    }
}
