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
import org.apache.kafka.coordinator.group.assignor.uniform2.util.IntList;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests of {@link Quotas}.
 */
public class QuotasTest {
    private static final Uuid T1 = new Uuid(1L, 1L);
    private static final Uuid T2 = new Uuid(2L, 1L);
    private static final Uuid T3 = new Uuid(3L, 1L);

    private static final int A = 0;
    private static final int B = 1;

    /**
     * 3 partitions over 2 subscribers give a minimum quota of 1 and 1 extra partition; 7 over 3
     * a minimum quota of 2 and 1 extra partition; 2 over 3 a minimum quota of 0 and 2 extra
     * partitions. The members of cohort 0, A and B, subscribe to all three topics and get at least
     * 1 + 2 + 0 = 3 partitions each; the member of cohort 1, C, subscribes to T2 and T3 and gets
     * at least 2 + 0 = 2.
     */
    @Test
    public void testMinQuotaAndExtraPartitions() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3))
            .withMember("B", Set.of(T1, T2, T3))
            .withMember("C", Set.of(T2, T3))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .withTopic(T2, 7)
            .withTopic(T3, 2)
            .build();
        var group = new IndexedGroup.Builder(spec, describer).build();

        var quotas = new Quotas(group);

        assertEquals(List.of(1, 2, 0), values(group.topicCount(), quotas::minQuota));
        assertEquals(List.of(1, 1, 2), values(group.topicCount(), quotas::extraPartitions));
        assertEquals(List.of(3, 2), values(group.cohortCount(), quotas::minAssignmentSize));
        assertEquals(List.of(3, 3, 2), values(group.memberCount(), quotas::assignmentSize));
        assertEquals(List.of(0, 0, 0), values(group.memberCount(), quotas::extraPartitionCount));
        assertEquals(List.of(List.of(), List.of(), List.of()), membersWithExtraPartition(group, quotas));
    }

    /**
     * A and B subscribe to T1, of 3 partitions, and T2, of 2. T1 has 1 extra partition, which A
     * gets, and T2 has none. A second extra partition of T1 cannot be given, nor one of T2.
     */
    @Test
    public void testGiveExtraPartition() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2))
            .withMember("B", Set.of(T1, T2))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .withTopic(T2, 2)
            .build();
        var group = new IndexedGroup.Builder(spec, describer).build();
        var quotas = new Quotas(group);
        int t1 = group.topicIndex(T1);
        int t2 = group.topicIndex(T2);

        quotas.giveExtraPartition(A, t1);

        assertThrows(IllegalStateException.class, () -> quotas.giveExtraPartition(B, t1));
        assertThrows(IllegalStateException.class, () -> quotas.giveExtraPartition(A, t2));
        assertEquals(List.of(3, 2), values(group.memberCount(), quotas::assignmentSize));
        assertEquals(List.of(1, 0), values(group.memberCount(), quotas::extraPartitionCount));
        assertEquals(List.of(List.of(A), List.of()), membersWithExtraPartition(group, quotas));
    }

    /**
     * A and B subscribe to T1, of 3 partitions, which has 1 extra partition, A's. B has none to
     * give. Moving the extra partition from A to B gives B the larger assignment size.
     */
    @Test
    public void testMoveExtraPartition() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1))
            .withMember("B", Set.of(T1))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .build();
        var group = new IndexedGroup.Builder(spec, describer).build();
        var quotas = new Quotas(group);
        int t1 = group.topicIndex(T1);
        quotas.giveExtraPartition(A, t1);

        assertThrows(IndexOutOfBoundsException.class, () -> quotas.moveExtraPartition(B, t1, A));
        quotas.moveExtraPartition(A, t1, B);

        assertEquals(List.of(1, 2), values(group.memberCount(), quotas::assignmentSize));
        assertEquals(List.of(0, 1), values(group.memberCount(), quotas::extraPartitionCount));
        assertEquals(List.of(List.of(B)), membersWithExtraPartition(group, quotas));
    }

    /**
     * @return The values of the numbers from 0 to the count, in order.
     */
    private static List<Integer> values(int count, IntUnaryOperator value) {
        return IntStream.range(0, count).map(value).boxed().toList();
    }

    /**
     * @return Per topic, the members getting one of its extra partitions, in the order they got
     *         them.
     */
    private static List<List<Integer>> membersWithExtraPartition(IndexedGroup group, Quotas quotas) {
        return IntStream.range(0, group.topicCount())
            .mapToObj(topic -> toList(quotas.membersWithExtraPartition(topic)))
            .toList();
    }

    private static List<Integer> toList(IntList list) {
        return Arrays.stream(list.toArray()).boxed().toList();
    }
}
