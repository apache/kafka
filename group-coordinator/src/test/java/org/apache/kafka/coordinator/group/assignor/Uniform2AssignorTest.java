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
package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.assignor.uniform2.GroupSpecFixture;
import org.apache.kafka.coordinator.group.assignor.uniform2.TopicsFixture;
import org.apache.kafka.coordinator.group.modern.SubscribedTopicDescriberImpl;
import org.apache.kafka.coordinator.group.modern.TopicIds;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.assertAssignment;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.assertValidAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * End to end tests of the uniform2 assignor, with expectations traced by hand.
 */
public class Uniform2AssignorTest {
    private static final Uuid T1 = new Uuid(1L, 1L);
    private static final Uuid T2 = new Uuid(2L, 1L);
    private static final Uuid T3 = new Uuid(3L, 1L);
    private static final Uuid T4 = new Uuid(4L, 1L);
    private static final Uuid T5 = new Uuid(5L, 1L);
    private static final Uuid MISSING = new Uuid(9L, 1L);

    private final Uniform2Assignor assignor = new Uniform2Assignor();

    @Test
    public void testName() {
        assertEquals("uniform2", assignor.name());
    }

    @Test
    public void testEmptyGroup() {
        var result = assignor.assign(new GroupSpecFixture().build(), new TopicsFixture().withTopic(T1, 3).build());
        assertEquals(Map.of(), result.members());
    }

    @Test
    public void testMissingAndUnsubscribedTopicsAreIgnored() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(MISSING))
            .withMember("B", Set.of(MISSING))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", Map.of(),
            "B", Map.of()
        ), result);
    }

    /**
     * A subscribes to T1 and holds partitions 0 and 1 of T1, partition 0 of T2, which only B
     * subscribes to, and partition 0 of a topic that does not exist. T1, of 4 partitions over A and
     * B, has a minimum quota of 2, which both own, so T1 does not change. A loses its partitions of
     * T2 and of the missing topic, and B gets the single partition of T2.
     */
    @Test
    public void testStalePartitionsAreDropped() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), Map.of(T1, Set.of(0, 1), T2, Set.of(0), MISSING, Set.of(0)))
            .withMember("B", Set.of(T1, T2), Map.of(T1, Set.of(2, 3)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 1)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0, 1)),
            "B", Map.of(T1, Set.of(2, 3), T2, Set.of(0))
        ), result);
    }

    /**
     * T1 has 2 partitions, so a minimum quota of 0 and 2 extra partitions, T2 has 5, so a minimum
     * quota of 1 and 2 extra partitions, and T3 has 7, so a minimum quota of 2 and 1 extra
     * partition. The 5 extra partitions go round robin, T1's to A and B, T2's to C and A, and T3's
     * to B. The members below their quota then take the partitions of every topic in partition
     * order.
     */
    @Test
    public void testHomogeneousSpreadsEveryTopic() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3))
            .withMember("B", Set.of(T1, T2, T3))
            .withMember("C", Set.of(T1, T2, T3))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 2)
            .withTopic(T2, 5)
            .withTopic(T3, 7)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0), T2, Set.of(0, 1), T3, Set.of(0, 1)),
            "B", Map.of(T1, Set.of(1), T2, Set.of(2), T3, Set.of(2, 3, 4)),
            "C", Map.of(T2, Set.of(3, 4), T3, Set.of(5, 6))
        ), result);
    }

    /**
     * A and D subscribe to T2, and B and C to T1 and T2, so that A and D form a cohort and B and C
     * another, although B and C come between A and D in member order. T1, of 2 partitions over B
     * and C, has a minimum quota of 1, and T2, of 8 partitions over the four members, a minimum
     * quota of 2. Nobody owns anything, and the partitions of T2 go cohort by cohort: 0 to 3 to A
     * and D, then 4 to 7 to B and C.
     */
    @Test
    public void testMembersWithTheSameSubscriptionShareACohort() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T2))
            .withMember("B", Set.of(T1, T2))
            .withMember("C", Set.of(T1, T2))
            .withMember("D", Set.of(T2))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 2)
            .withTopic(T2, 8)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", Map.of(T2, Set.of(0, 1)),
            "B", Map.of(T1, Set.of(0), T2, Set.of(4, 5)),
            "C", Map.of(T1, Set.of(1), T2, Set.of(6, 7)),
            "D", Map.of(T2, Set.of(2, 3))
        ), result);
    }

    /**
     * C leaves the assignment of {@link #testHomogeneousSpreadsEveryTopic}. T1 now has a
     * minimum quota of 1, which A and B hold. T2 has a minimum quota of 2 and 1 extra partition, T3
     * has a minimum quota of 3 and 1 extra partition, and neither A nor B owns more than the
     * minimum quota of them, so their extra partitions are assigned, T2's to A and T3's to B. A and
     * B then take the partitions of C in partition order, and nothing moves between them.
     */
    @Test
    public void testLeaveOnlyMovesThePartitionsOfTheLeaver() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3), Map.of(T1, Set.of(0), T2, Set.of(0, 1), T3, Set.of(0, 1)))
            .withMember("B", Set.of(T1, T2, T3), Map.of(T1, Set.of(1), T2, Set.of(2), T3, Set.of(2, 3, 4)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 2)
            .withTopic(T2, 5)
            .withTopic(T3, 7)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0), T2, Set.of(0, 1, 3), T3, Set.of(0, 1, 5)),
            "B", Map.of(T1, Set.of(1), T2, Set.of(2, 4), T3, Set.of(2, 3, 4, 6))
        ), result);
    }

    /**
     * A member joining a stable homogeneous group only takes its quota. With 3 topics of 6
     * partitions and 2 members, A has partitions 0 to 2 of each topic and B 3 to 5. With a third
     * member, each member has 2 partitions of each topic, so A and B keep their 2 lowest partitions
     * of each topic and give the third one to the joiner. Nothing else moves.
     */
    @Test
    public void testJoinOnlyMovesThePartitionsOfTheJoiner() {
        var describer = new TopicsFixture()
            .withTopic(T1, 6)
            .withTopic(T2, 6)
            .withTopic(T3, 6)
            .build();
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3))
            .withMember("B", Set.of(T1, T2, T3))
            .build();
        var before = assignor.assign(spec, describer);
        var joined = GroupSpecFixture.after(spec, before)
            .withMember("C", Set.of(T1, T2, T3))
            .build();

        var after = assignor.assign(joined, describer);

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0, 1), T2, Set.of(0, 1), T3, Set.of(0, 1)),
            "B", Map.of(T1, Set.of(3, 4), T2, Set.of(3, 4), T3, Set.of(3, 4)),
            "C", Map.of(T1, Set.of(2, 5), T2, Set.of(2, 5), T3, Set.of(2, 5))
        ), after);
    }

    /**
     * A and B subscribe to T1 and T2. T1 had 4 partitions, of which A holds 0 and 1 and B holds 2
     * and 3, and now has 6, so a minimum quota of 3. A gets the new partition 4, and B gets 5. T2,
     * of 2 partitions, does not change, and both keep their set of it.
     */
    @Test
    public void testAddedPartitionsGoToTheMembersBelowTheirQuota() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2), Map.of(T1, Set.of(0, 1), T2, Set.of(0)))
            .withMember("B", Set.of(T1, T2), Map.of(T1, Set.of(2, 3), T2, Set.of(1)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 6)
            .withTopic(T2, 2)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0, 1, 4), T2, Set.of(0)),
            "B", Map.of(T1, Set.of(2, 3, 5), T2, Set.of(1))
        ), result);
    }

    /**
     * A, B and C subscribed to T1, of 6 partitions, and hold 2 partitions each. C now subscribes
     * to T2, of 3 partitions, instead. Its partitions 4 and 5 of T1 are dropped and go to A and B,
     * whose minimum quota of T1 is now 3, and C gets all the partitions of T2.
     */
    @Test
    public void testMemberChangingItsSubscriptionGivesUpTheTopicItLeaves() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), Map.of(T1, Set.of(0, 1)))
            .withMember("B", Set.of(T1), Map.of(T1, Set.of(2, 3)))
            .withMember("C", Set.of(T2), Map.of(T1, Set.of(4, 5)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 6)
            .withTopic(T2, 3)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0, 1, 4)),
            "B", Map.of(T1, Set.of(2, 3, 5)),
            "C", Map.of(T2, Set.of(0, 1, 2))
        ), result);
    }

    /**
     * A subscribes to T1, and B and C to T1 and T2. T1, of 5 partitions, has a minimum quota of 1
     * and 2 extra partitions. A owns partitions 0 and 1 and keeps one extra partition. A is the
     * only member of its cohort and already has an extra partition of T1, so its cohort gets no
     * other, and the second one goes to B, the first of the other cohort. B gets partitions 2 and 3
     * of T1, and C gets 4. T2, of 2 partitions over B and C, gives each of them 1.
     */
    @Test
    public void testCohortWhoseMembersAllHaveAnExtraPartitionGetsNoMore() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1), Map.of(T1, Set.of(0, 1)))
            .withMember("B", Set.of(T1, T2))
            .withMember("C", Set.of(T1, T2))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 5)
            .withTopic(T2, 2)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0, 1)),
            "B", Map.of(T1, Set.of(2, 3), T2, Set.of(0)),
            "C", Map.of(T1, Set.of(4), T2, Set.of(1))
        ), result);
    }

    /**
     * A, B and C subscribe to T1 and T2, of 2 partitions each, and T3, of 5 partitions. T1 and T2
     * have a minimum quota of 0 and 2 extra partitions each, which B and C keep, as each owns one
     * partition of each. T3 has a minimum quota of 1 and 2 extra partitions. A owns partitions 0
     * and 1 and keeps one. The second goes to B: A has the fewest extra partitions but already has
     * one of T3, and B is the first of B and C, which have 2 each. B, then at 4 partitions against
     * 2 for A, gives A its extra partition of T2, since A cannot take another one of T3. A gets
     * partition 0 of T2, and B gets partition 4 of T3, which nobody owns.
     */
    @Test
    public void testAnExtraPartitionGoesToAnotherMemberWhenTheSmallestHaveOneOfTheTopic() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3), Map.of(T3, Set.of(0, 1)))
            .withMember("B", Set.of(T1, T2, T3), Map.of(T1, Set.of(0), T2, Set.of(0), T3, Set.of(2)))
            .withMember("C", Set.of(T1, T2, T3), Map.of(T1, Set.of(1), T2, Set.of(1), T3, Set.of(3)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 2)
            .withTopic(T2, 2)
            .withTopic(T3, 5)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", Map.of(T2, Set.of(0), T3, Set.of(0, 1)),
            "B", Map.of(T1, Set.of(0), T3, Set.of(2, 4)),
            "C", Map.of(T1, Set.of(1), T2, Set.of(1), T3, Set.of(3))
        ), result);
    }

    /**
     * A subscribes to T1, B to T1 and T3, and C and D to T2 and T4. T1 and T2, of 1 partition each
     * over 2 subscribers, have a minimum quota of 0 and 1 extra partition each. T3 gives B 1
     * partition, and T4 gives C and D 2 each. Nobody owns anything. The extra partition of T1 goes
     * to A, whose assignment is smaller than that of B. The extra partition of T2 goes to C, the
     * first of its subscribers. B, which is smaller than C and could have taken the extra
     * partition of T1, does not subscribe to T2.
     */
    @Test
    public void testCohortsWaitingForAnExtraPartitionOfOneTopicDoNotCarryOverToTheNext() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1))
            .withMember("B", Set.of(T1, T3))
            .withMember("C", Set.of(T2, T4))
            .withMember("D", Set.of(T2, T4))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 1)
            .withTopic(T2, 1)
            .withTopic(T3, 1)
            .withTopic(T4, 4)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0)),
            "B", Map.of(T3, Set.of(0)),
            "C", Map.of(T2, Set.of(0), T4, Set.of(0, 1)),
            "D", Map.of(T4, Set.of(2, 3))
        ), result);
    }

    /**
     * A subscribes to T1 and T2, B to T2 only. T1, 4 partitions, has a single subscriber, so A has
     * a minimum quota of 4. T2, 3 partitions, has a minimum quota of 1, and its extra partition
     * goes to B, whose assignment is smaller.
     */
    @Test
    public void testHeterogeneousBalancesAcrossTopics() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2))
            .withMember("B", Set.of(T2))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 3)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0, 1, 2, 3), T2, Set.of(0)),
            "B", Map.of(T2, Set.of(1, 2))
        ), result);
    }

    /**
     * A subscribes to T1 and T2, B to T1 and C to T2, of 3 partitions each, so each topic has a
     * minimum quota of 1 and 1 extra partition. A owns 2 partitions of both, and B and C one each,
     * so A keeps both extra partitions and has 4 partitions against 1. The balance step then moves
     * the extra partition of T1 to B and the one of T2 to C. A gives up one partition of each, the
     * fewest revocations for assignment sizes within one.
     */
    @Test
    public void testBalanceMovesExtraPartitionsKeptByTheirOwner() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2), Map.of(T1, Set.of(0, 1), T2, Set.of(0, 1)))
            .withMember("B", Set.of(T1), Map.of(T1, Set.of(2)))
            .withMember("C", Set.of(T2), Map.of(T2, Set.of(2)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .withTopic(T2, 3)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0), T2, Set.of(0)),
            "B", Map.of(T1, Set.of(1, 2)),
            "C", Map.of(T2, Set.of(1, 2))
        ), result);
    }

    /**
     * A subscribes to T1, T2 and T3, B to T1 and T2, C to T2 and T3, and D to T1 and T3, in four
     * cohorts. T1, T2 and T3, of 5 partitions each over 3 subscribers, have a minimum quota of 1
     * and 2 extra partitions each, all kept by owners of 2 partitions: A keeps one of every topic,
     * B one of T1 and one of T2, and C one of T3. The assignment sizes are 6, 4, 3 and 2. A gives
     * the extra partition of T3 to D, the smallest, then the one of T2 to C. The assignment sizes
     * end at 4, 4, 4 and 3, and no extra partition can move to a subscriber of its topic at least
     * two partitions smaller. C gets partition 1 of T2, and D partition 1 of T3.
     */
    @Test
    public void testBalanceMovesUntilNoExtraPartitionCanMove() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3), Map.of(T1, Set.of(0, 1), T2, Set.of(0, 1), T3, Set.of(0, 1)))
            .withMember("B", Set.of(T1, T2), Map.of(T1, Set.of(2, 3), T2, Set.of(2, 3)))
            .withMember("C", Set.of(T2, T3), Map.of(T2, Set.of(4), T3, Set.of(2, 3)))
            .withMember("D", Set.of(T1, T3), Map.of(T1, Set.of(4), T3, Set.of(4)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 5)
            .withTopic(T2, 5)
            .withTopic(T3, 5)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0, 1), T2, Set.of(0), T3, Set.of(0)),
            "B", Map.of(T1, Set.of(2, 3), T2, Set.of(2, 3)),
            "C", Map.of(T2, Set.of(1, 4), T3, Set.of(2, 3)),
            "D", Map.of(T1, Set.of(4), T3, Set.of(1, 4))
        ), result);
    }

    /**
     * A subscribes to T1 and T4, and B and C to T1, T2 and T3. T1, of 5 partitions over the three,
     * has a minimum quota of 1 and 2 extra partitions, kept by A and B, which own 2 partitions
     * each. T2 and T3, of 3 partitions over B and C, have 1 extra partition each, both kept by C.
     * T4 gives A its 5 partitions. The assignment sizes are 7, 4 and 5. B, the smallest, already
     * has an extra partition of T1, the only topic of the extra partitions of A, so C, the next,
     * takes it. C, at 6, then gives B its extra partition of T3. A gives up partition 1 of T1 to
     * C, and C gives up partition 2 of T3 to B.
     */
    @Test
    public void testTheSmallestMemberThatCannotTakeIsPassedOver() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T4), Map.of(T1, Set.of(0, 1), T4, Set.of(0, 1, 2, 3, 4)))
            .withMember("B", Set.of(T1, T2, T3), Map.of(T1, Set.of(2, 3), T2, Set.of(0), T3, Set.of(0)))
            .withMember("C", Set.of(T1, T2, T3), Map.of(T1, Set.of(4), T2, Set.of(1, 2), T3, Set.of(1, 2)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 5)
            .withTopic(T2, 3)
            .withTopic(T3, 3)
            .withTopic(T4, 5)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0), T4, Set.of(0, 1, 2, 3, 4)),
            "B", Map.of(T1, Set.of(2, 3), T2, Set.of(0), T3, Set.of(0, 2)),
            "C", Map.of(T1, Set.of(1, 4), T2, Set.of(1, 2), T3, Set.of(1))
        ), result);
    }

    /**
     * A subscribes to T1 and T3, B to T1 and T2, and C to T2. T1, of 3 partitions over A and B, has
     * 1 extra partition, which A keeps as it owns 2 partitions. T2, of 3 partitions over B and C,
     * has 1, which B keeps for the same reason. T3 gives A its 2 partitions. The assignment sizes
     * are 4, 3 and 1. A, the largest, has no receiver, since B is only one partition smaller, so A
     * waits. B gives its extra partition of T2 to C, and is then two partitions smaller than A,
     * which wakes up and gives B its extra partition of T1. B gets partition 1 of T1, and C
     * partition 1 of T2.
     */
    @Test
    public void testAWaitingGiverWakesUpWhenASubscriberOfItsTopicsGetsSmaller() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T3), Map.of(T1, Set.of(0, 1), T3, Set.of(0, 1)))
            .withMember("B", Set.of(T1, T2), Map.of(T1, Set.of(2), T2, Set.of(0, 1)))
            .withMember("C", Set.of(T2), Map.of(T2, Set.of(2)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .withTopic(T2, 3)
            .withTopic(T3, 2)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0), T3, Set.of(0, 1)),
            "B", Map.of(T1, Set.of(1, 2), T2, Set.of(0)),
            "C", Map.of(T2, Set.of(1, 2))
        ), result);
    }

    /**
     * A and C subscribe to T1 to T4, and B to T1 to T5. T1 to T4, of 4 partitions over the three,
     * have a minimum quota of 1 and 1 extra partition each, and T5, of 1 partition, gives B 1 more.
     * C owns 2 partitions of T1 and keeps its extra partition. A owns 2 partitions of T2, T3 and T4
     * and keeps their extra partitions. C also owns 2 partitions of T3, but A keeps the extra
     * partition of T3, since A and C are then equally large and A comes first. The assignment
     * sizes are 7, 5 and 5. B and C are equally small, and C receives first, since it owns more
     * than the minimum quota of T3. A gives C the extra partition of T3, which revokes no partition
     * from C, rather than that of T4, its last topic. B gets partition 1 of T3, which A gives up.
     */
    @Test
    public void testTheGiverGivesTheExtraPartitionOfATopicTheReceiverOwnsMoreThanTheMinimumQuotaOf() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3, T4), Map.of(T1, Set.of(2), T2, Set.of(0, 1), T3, Set.of(0, 1), T4, Set.of(0, 1)))
            .withMember("B", Set.of(T1, T2, T3, T4, T5), Map.of(T1, Set.of(3), T2, Set.of(2), T4, Set.of(2), T5, Set.of(0)))
            .withMember("C", Set.of(T1, T2, T3, T4), Map.of(T1, Set.of(0, 1), T2, Set.of(3), T3, Set.of(2, 3), T4, Set.of(3)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 4)
            .withTopic(T3, 4)
            .withTopic(T4, 4)
            .withTopic(T5, 1)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(2), T2, Set.of(0, 1), T3, Set.of(0), T4, Set.of(0, 1)),
            "B", Map.of(T1, Set.of(3), T2, Set.of(2), T3, Set.of(1), T4, Set.of(2), T5, Set.of(0)),
            "C", Map.of(T1, Set.of(0, 1), T2, Set.of(3), T3, Set.of(2, 3), T4, Set.of(3))
        ), result);
    }

    /**
     * A and B subscribe to T1 to T4, of 3 partitions each, so a minimum quota of 1 and 1 extra
     * partition each. B owns 2 partitions of every topic and keeps the four extra partitions, so
     * the assignment sizes are 4 and 8. B gives A two extra partitions in a row, of its last topics
     * first: T4, then T3. A gets partition 1 of T3 and of T4, which B gives up.
     */
    @Test
    public void testAGiverGivesSeveralExtraPartitionsInARow() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2, T3, T4), Map.of(T1, Set.of(2), T2, Set.of(2), T3, Set.of(2), T4, Set.of(2)))
            .withMember("B", Set.of(T1, T2, T3, T4), Map.of(T1, Set.of(0, 1), T2, Set.of(0, 1), T3, Set.of(0, 1), T4, Set.of(0, 1)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 3)
            .withTopic(T2, 3)
            .withTopic(T3, 3)
            .withTopic(T4, 3)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(2), T2, Set.of(2), T3, Set.of(1, 2), T4, Set.of(1, 2)),
            "B", Map.of(T1, Set.of(0, 1), T2, Set.of(0, 1), T3, Set.of(0), T4, Set.of(0))
        ), result);
    }

    /**
     * An assignment that already has the properties is kept as it is, even though the assignor
     * would not have computed it from scratch. T1 has a minimum quota of 2. T2 has a minimum quota
     * of 1 and 1 extra partition, which A keeps since it owns 2 partitions of T2. The assignment
     * sizes are 4 and 3. From scratch, A would get partitions 0 and 1 of T1 rather than 2 and 3.
     */
    @Test
    public void testAssignmentWithThePropertiesDoesNotChange() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2), Map.of(T1, Set.of(2, 3), T2, Set.of(1, 2)))
            .withMember("B", Set.of(T1, T2), Map.of(T1, Set.of(0, 1), T2, Set.of(0)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 3)
            .build();

        var result = assignor.assign(spec, describer);

        for (String id : spec.memberIds()) {
            assertSame(spec.memberAssignment(id).partitions(), result.members().get(id).partitions());
        }
    }

    /**
     * A, B and C subscribe to T1 and T2, of 4 partitions each, and hold consecutive ranges of both,
     * the extra partition going to the first member, as the range assignor assigns them: A holds 0
     * and 1, B holds 2 and C holds 3 of each topic. Each topic has a minimum quota of 1 and 1 extra
     * partition, which A keeps for both, so that A has 4 partitions against 2. The balance step
     * moves the extra partition of T2 to B: A gives up partition 1 of T2, which B gets. A and B get
     * new maps sharing their set of T1, and C keeps its map.
     */
    @Test
    public void testFromARangeLikeAssignment() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2), Map.of(T1, Set.of(0, 1), T2, Set.of(0, 1)))
            .withMember("B", Set.of(T1, T2), Map.of(T1, Set.of(2), T2, Set.of(2)))
            .withMember("C", Set.of(T1, T2), Map.of(T1, Set.of(3), T2, Set.of(3)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 4)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0, 1), T2, Set.of(0)),
            "B", Map.of(T1, Set.of(2), T2, Set.of(1, 2)),
            "C", Map.of(T1, Set.of(3), T2, Set.of(3))
        ), result);
    }

    /**
     * C holds all the partitions of a topic. A, B and C subscribe to T1, of 4 partitions, and T2,
     * of 2 partitions, and hold ranges of the partitions of both topics taken together, as the
     * uniform assignor can assign them: A holds 0 and 1 of T1, B holds 2 and 3 of T1, and C holds
     * both partitions of T2. T1 has a minimum quota of 1 and 1 extra partition, which A keeps, as
     * the first of the two owners above the minimum quota. T2 has a minimum quota of 0 and 2 extra
     * partitions: C keeps one, and the other goes to B, the smallest member without one. B gives
     * up partition 3 of T1 to C, and C gives up partition 1 of T2 to B. A keeps its map.
     */
    @Test
    public void testFromATopicHeldByASingleMember() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2), Map.of(T1, Set.of(0, 1)))
            .withMember("B", Set.of(T1, T2), Map.of(T1, Set.of(2, 3)))
            .withMember("C", Set.of(T1, T2), Map.of(T2, Set.of(0, 1)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 4)
            .withTopic(T2, 2)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0, 1)),
            "B", Map.of(T1, Set.of(2), T2, Set.of(1)),
            "C", Map.of(T1, Set.of(3), T2, Set.of(0))
        ), result);
    }

    /**
     * A subscribes to T1 and T2, B to T1 and C to T2, and they hold an arbitrary assignment with
     * partitions held by nobody. T1, of 5 partitions over A and B, has a minimum quota of 2 and 1
     * extra partition, which goes to B, the smallest, since no owner holds more than the minimum
     * quota. T2, of 4 partitions over A and C, has a minimum quota of 2. A keeps partitions 1 and 3
     * of T1, and B keeps 4 and gets 0 and 2, which nobody holds. C keeps its map, and A gets
     * partition 1 of T2, which nobody holds, in a new map sharing its set of T1.
     */
    @Test
    public void testFromAnArbitraryAssignment() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1, T2), Map.of(T1, Set.of(1, 3), T2, Set.of(0)))
            .withMember("B", Set.of(T1), Map.of(T1, Set.of(4)))
            .withMember("C", Set.of(T2), Map.of(T2, Set.of(2, 3)))
            .build();
        var describer = new TopicsFixture()
            .withTopic(T1, 5)
            .withTopic(T2, 4)
            .build();

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(1, 3), T2, Set.of(0, 1)),
            "B", Map.of(T1, Set.of(0, 2, 4)),
            "C", Map.of(T2, Set.of(2, 3))
        ), result);
    }

    /**
     * The subscriptions are given as the coordinator gives them, with topic names resolved lazily
     * against the metadata image and one of them unknown. They are the same for every member, so
     * the group is homogeneous, and the assignment is the same as with the topics given by id.
     */
    @Test
    public void testSubscriptionsAsTopicIdsWithAnUnknownTopic() {
        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(T1, "t1", 2)
            .addTopic(T2, "t2", 5)
            .addTopic(T3, "t3", 7)
            .buildCoordinatorMetadataImage();
        var resolver = new TopicIds.CachedTopicResolver(image);
        var spec = new GroupSpecFixture()
            .withMember("A", new TopicIds(Set.of("t1", "t2", "t3", "unknown"), resolver))
            .withMember("B", new TopicIds(Set.of("t1", "t2", "t3", "unknown"), resolver))
            .withMember("C", new TopicIds(Set.of("t1", "t2", "t3", "unknown"), resolver))
            .build();
        var describer = new SubscribedTopicDescriberImpl(image);

        var result = assignor.assign(spec, describer);

        assertAssignment(Map.of(
            "A", Map.of(T1, Set.of(0), T2, Set.of(0, 1), T3, Set.of(0, 1)),
            "B", Map.of(T1, Set.of(1), T2, Set.of(2), T3, Set.of(2, 3, 4)),
            "C", Map.of(T2, Set.of(3, 4), T3, Set.of(5, 6))
        ), result);
    }

    @ParameterizedTest
    @EnumSource(SubscriptionType.class)
    public void testAssignmentReuse(SubscriptionType subscriptionType) {
        CommonAssignorTests.testAssignmentReuse(assignor, subscriptionType, false);
    }

    @ParameterizedTest
    @EnumSource(SubscriptionType.class)
    public void testReassignmentStickiness(SubscriptionType subscriptionType) {
        CommonAssignorTests.testReassignmentStickiness(assignor, subscriptionType, false);
    }

    /**
     * The assignment does not depend on the order of the members, of the topics of their
     * subscriptions, nor of the entries of their current assignment. The order of the members is
     * also checked by {@link #testReassignmentStickiness}; this test also shuffles the topics of
     * the subscriptions and the entries of the current assignments.
     */
    @Test
    public void testOrderIndependence() {
        var describer = new TopicsFixture()
            .withTopic(T1, 2)
            .withTopic(T2, 5)
            .withTopic(T3, 7)
            .build();
        var forward = new GroupSpecFixture()
            .withMember("A", new LinkedHashSet<>(List.of(T1, T2, T3)), Map.of(T3, Set.of(0, 1, 2, 3)))
            .withMember("B", new LinkedHashSet<>(List.of(T1, T2)))
            .withMember("C", new LinkedHashSet<>(List.of(T2, T3)))
            .build();
        var backward = new GroupSpecFixture()
            .withMember("C", new LinkedHashSet<>(List.of(T3, T2)))
            .withMember("B", new LinkedHashSet<>(List.of(T2, T1)))
            .withMember("A", new LinkedHashSet<>(List.of(T3, T2, T1)), Map.of(T3, Set.of(3, 2, 1, 0)))
            .build();

        var first = assignor.assign(forward, describer);
        var second = assignor.assign(backward, describer);

        assertEquals(first, second);
        assertValidAssignment(forward, describer, first);
    }
}
