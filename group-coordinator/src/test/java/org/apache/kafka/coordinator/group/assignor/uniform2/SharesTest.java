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
 * Tests of {@link Shares}.
 *
 * <p>The member ids sort as A &lt; B &lt; C, so that A is member 0, B is member 1 and C is member
 * 2, and the topic ids as T1 &lt; T2 &lt; T3, so that T1 is topic 0, T2 is topic 1 and T3 is
 * topic 2 when all of them are subscribed.
 */
public class SharesTest {
    private static final Uuid T1 = new Uuid(1L, 1L);
    private static final Uuid T2 = new Uuid(2L, 1L);
    private static final Uuid T3 = new Uuid(3L, 1L);

    private static final int A = 0;
    private static final int B = 1;

    /**
     * 3 partitions over 2 subscribers give 1 base partition and 1 extra partition; 7 over 3 give
     * 2 base partitions and 1 extra one; 2 over 3 give no base partition and 2 extra ones. The
     * members of cohort 0, A and B, subscribe to all three topics and get 1 + 2 + 0 = 3 base
     * partitions each; the member of cohort 1, C, subscribes to T2 and T3 and gets 2 + 0 = 2.
     */
    @Test
    public void testBaseAndExtraPartitions() {
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
        var group = new GroupModel(spec, describer);

        var shares = new Shares(group);

        assertEquals(List.of(1, 2, 0), values(group.topicCount(), shares::basePartitions));
        assertEquals(List.of(1, 1, 2), values(group.topicCount(), shares::extraPartitions));
        assertEquals(List.of(3, 2), values(group.cohortCount(), shares::cohortBaseSize));
        assertEquals(List.of(3, 3, 2), values(group.memberCount(), shares::size));
        assertEquals(List.of(0, 0, 0), values(group.memberCount(), shares::extraCount));
        assertEquals(List.of(List.of(), List.of(), List.of()), membersWithExtra(group, shares));
    }

    /**
     * A and B subscribe to T1, of 3 partitions, and T2, of 2: T1 has 1 extra partition, which A
     * gets, and T2 none. Neither a second extra partition of T1 nor one of T2 can be given, and an
     * extra partition of T1 cannot move from B, which has none: A keeps its only extra partition,
     * and the sizes are 3 and 2.
     */
    @Test
    public void testExtraPartitionsBeyondThoseOfTheTopicAreRejected() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2))
            .withMember("B", Set.of(T1, T2))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .withTopic(T2, 2)
            .build();
        var group = new GroupModel(spec, describer);
        var shares = new Shares(group);
        int t1 = group.topicIndex(T1);
        int t2 = group.topicIndex(T2);
        shares.giveExtraPartition(A, t1);

        assertThrows(IllegalStateException.class, () -> shares.giveExtraPartition(B, t1));
        assertThrows(IllegalStateException.class, () -> shares.giveExtraPartition(A, t2));
        assertThrows(IndexOutOfBoundsException.class, () -> shares.moveExtraPartition(B, t1, A));

        assertEquals(List.of(List.of(A), List.of()), membersWithExtra(group, shares));
        assertEquals(List.of(1, 0), values(group.memberCount(), shares::extraCount));
        assertEquals(List.of(3, 2), values(group.memberCount(), shares::size));
    }

    /**
     * A and B subscribe to T1, of 3 partitions: 1 base partition each and 1 extra partition, A's.
     * Moving it to B gives B the extra partition and the larger size.
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
        var group = new GroupModel(spec, describer);
        var shares = new Shares(group);
        int t1 = group.topicIndex(T1);
        shares.giveExtraPartition(A, t1);

        shares.moveExtraPartition(A, t1, B);

        assertEquals(List.of(List.of(B)), membersWithExtra(group, shares));
        assertEquals(List.of(0, 1), values(group.memberCount(), shares::extraCount));
        assertEquals(List.of(1, 2), values(group.memberCount(), shares::size));
    }

    /**
     * A subscribes to T1 and T2, B to T1 only. T1, 3 partitions, gives 1 base partition to each,
     * and B gets its extra partition; T2, 1 partition, gives its base partition to A. A gets
     * partitions of its 2 topics. B gets partitions of T1 only, but T1 counts twice in the bound,
     * for its base partition and for its extra partition: the bound is 2 for both.
     */
    @Test
    public void testMaxTopicsWithPartitions() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2))
            .withMember("B", Set.of(T1))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .withTopic(T2, 1)
            .build();
        var group = new GroupModel(spec, describer);
        var shares = new Shares(group);
        shares.giveExtraPartition(B, group.topicIndex(T1));

        assertEquals(List.of(2, 2), values(group.memberCount(), shares::maxTopicsWithPartitions));
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
    private static List<List<Integer>> membersWithExtra(GroupModel group, Shares shares) {
        return IntStream.range(0, group.topicCount())
            .mapToObj(topic -> toList(shares.membersWithExtra(topic)))
            .toList();
    }

    private static List<Integer> toList(IntList list) {
        return Arrays.stream(list.toArray()).boxed().toList();
    }
}
