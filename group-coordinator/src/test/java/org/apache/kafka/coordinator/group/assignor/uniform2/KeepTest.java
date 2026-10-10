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

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests of the keep step, {@link Keep}. The topic ids sort as T1 &lt; T2 &lt; T3.
 */
public class KeepTest {
    private static final Uuid T1 = new Uuid(1L, 1L);
    private static final Uuid T2 = new Uuid(2L, 1L);
    private static final Uuid T3 = new Uuid(3L, 1L);

    /**
     * T1, 3 partitions over 3 members, gives 1 base partition to each and has no extra
     * partition. T2, 5 partitions, gives 1 base partition to each and has 2 extra ones; A and C
     * own 2 partitions of it, more than the base partitions, so they keep them, and the sizes are
     * 3, 2 and 3.
     */
    @Test
    public void testOwnersAboveTheBasePartitionsKeepAnExtraPartition() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T1, 0), mkTopicAssignment(T2, 0, 1)))
            .withMember("B", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T1, 1), mkTopicAssignment(T2, 2)))
            .withMember("C", Set.of(T1, T2), mkAssignment(mkTopicAssignment(T1, 2), mkTopicAssignment(T2, 3, 4)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .withTopic(T2, 5)
            .build();
        var fixture = new SharesFixture(spec, describer);

        new Keep(fixture.group, fixture.current, fixture.shares).run();

        assertEquals(Map.of(T2, List.of("A", "C")), fixture.membersWithExtra());
        assertEquals(List.of(3, 2, 3), fixture.sizes());
    }

    /**
     * T1 and T2 have 4 partitions each over A, B and C: 1 base partition and 1 extra partition
     * each. A owns 2 partitions of both and B 2 of T1. T1 has two owners above the base
     * partitions for one extra partition, of the same size so far: A keeps it, first in member
     * order. A also keeps the one of T2, of which only A owns more than the base partitions.
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
        var fixture = new SharesFixture(spec, describer);

        new Keep(fixture.group, fixture.current, fixture.shares).run();

        assertEquals(Map.of(T1, List.of("A"), T2, List.of("A")), fixture.membersWithExtra());
        assertEquals(List.of(4, 2, 2), fixture.sizes());
    }

    /**
     * Three members own 3 of the 9 partitions of T1, T2 and T3 each; a fourth member joins. Each
     * topic now has 2 base partitions and 1 extra partition, and its 3 owners all own more than
     * the base partitions. The owners with the smallest assignments keep the extra partitions: A
     * for T1; then B and C have the smaller size for T2, B first in member order; then C for T3.
     * D gets none, and the sizes are within one of each other.
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
        var fixture = new SharesFixture(spec, describer);

        new Keep(fixture.group, fixture.current, fixture.shares).run();

        assertEquals(Map.of(T1, List.of("A"), T2, List.of("B"), T3, List.of("C")), fixture.membersWithExtra());
        assertEquals(List.of(7, 7, 7, 6), fixture.sizes());
    }
}
