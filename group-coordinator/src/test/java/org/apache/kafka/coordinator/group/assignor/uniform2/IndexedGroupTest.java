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
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests of {@link IndexedGroup.Builder}: the numbering of the members, the numbering of the topics,
 * and the cohorts of the {@link IndexedGroup} it builds.
 *
 * <p>The members are added to the specs, and the subscriptions list their topics, out of id order,
 * so that the indices cannot come from the iteration order of the spec.
 */
public class IndexedGroupTest {
    private static final Uuid T1 = new Uuid(1L, 1L);
    private static final Uuid T2 = new Uuid(2L, 1L);
    private static final Uuid T3 = new Uuid(3L, 1L);
    private static final Uuid MISSING = new Uuid(9L, 1L);

    private static final SubscribedTopicDescriber DESCRIBER = new TopicsFixture()
        .withTopic(T1, 2)
        .withTopic(T2, 5)
        .withTopic(T3, 7)
        .build();

    // Numbering of the members.

    /**
     * Member ids are compared as strings, so m10 comes between m1 and m2.
     */
    @Test
    public void testMembersAreNumberedInMemberIdOrder() {
        var spec = new GroupSpecFixture()
            .withMember("m2", Set.of(T1))
            .withMember("m10", Set.of(T1))
            .withMember("m1", Set.of(T1))
            .build();

        var group = new IndexedGroup.Builder(spec, DESCRIBER).build();

        assertEquals(List.of("m1", "m10", "m2"), memberIds(group));
    }

    // Numbering of the topics.

    /**
     * A, read first, sees T3 before T1, and B then sees T2; the topics are still numbered in topic
     * id order, with their partition counts, and so are the topics of the cohorts.
     */
    @Test
    public void testTopicsAreNumberedInTopicIdOrder() {
        var spec = new GroupSpecFixture()
            .withMember("B", Set.of(T2))
            .withMember("A", orderedSet(T3, T1))
            .build();

        var group = new IndexedGroup.Builder(spec, DESCRIBER).build();

        assertEquals(List.of(T1, T2, T3), topicIds(group));
        assertEquals(List.of(0, 1, 2), topicIndices(group, T1, T2, T3));
        assertEquals(List.of(2, 5, 7), partitionCounts(group));
        assertEquals(List.of(List.of(0), List.of(1)), membersOfCohorts(group));
        assertEquals(List.of(List.of(0, 2), List.of(1)), topicsOfCohorts(group));
    }

    /**
     * A subscribes to T1 only: T2 exists, but nobody subscribes to it, so it has no index.
     */
    @Test
    public void testTopicsThatAreNotSubscribedAreIgnored() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1))
            .build();

        var group = new IndexedGroup.Builder(spec, DESCRIBER).build();

        assertEquals(List.of(T1), topicIds(group));
        assertEquals(List.of(0, -1), topicIndices(group, T1, T2));
        assertEquals(List.of(2), partitionCounts(group));
        assertEquals(List.of(List.of(0)), membersOfCohorts(group));
        assertEquals(List.of(List.of(0)), topicsOfCohorts(group));
    }

    /**
     * The describer gives -1 partitions for a topic that does not exist, which is then dropped from
     * the subscriptions: A, subscribing to T1 and the missing topic, and B, subscribing to T1,
     * share cohort 0, and C, subscribing only to the missing topic, has a cohort without topics.
     */
    @Test
    public void testTopicsThatDoNotExistAreIgnored() {
        var spec = new GroupSpecFixture()
            .withMember("C", Set.of(MISSING))
            .withMember("B", Set.of(T1))
            .withMember("A", orderedSet(MISSING, T1))
            .build();

        var group = new IndexedGroup.Builder(spec, DESCRIBER).build();

        assertEquals(List.of(T1), topicIds(group));
        assertEquals(List.of(0, -1), topicIndices(group, T1, MISSING));
        assertEquals(List.of(2), partitionCounts(group));
        assertEquals(List.of(List.of(0, 1), List.of(2)), membersOfCohorts(group));
        assertEquals(List.of(List.of(0), List.of()), topicsOfCohorts(group));
    }

    // Cohorts.

    /**
     * An empty group, homogeneous as the coordinator declares it, has a single cohort without
     * members nor topics.
     */
    @Test
    public void testEmptyGroup() {
        var spec = new GroupSpecFixture().build();

        var group = new IndexedGroup.Builder(spec, DESCRIBER).build();

        assertEquals(List.of(), cohorts(group));
        assertEquals(List.of(List.of()), membersOfCohorts(group));
        assertEquals(List.of(List.of()), topicsOfCohorts(group));
        assertEquals(List.of(), cohortsOfTopics(group));
        assertEquals(List.of(), subscriberCounts(group));
        assertEquals(List.of(), subscribedTopics(group));
    }

    /**
     * A single member makes a single cohort with its topics.
     */
    @Test
    public void testSingleMember() {
        var spec = new GroupSpecFixture()
            .withMember("A", orderedSet(T2, T1))
            .build();

        var group = new IndexedGroup.Builder(spec, DESCRIBER).build();

        assertEquals(List.of(0), cohorts(group));
        assertEquals(List.of(List.of(0)), membersOfCohorts(group));
        assertEquals(List.of(List.of(0, 1)), topicsOfCohorts(group));
        assertEquals(List.of(List.of(0), List.of(0)), cohortsOfTopics(group));
        assertEquals(List.of(1, 1), subscriberCounts(group));
        assertEquals(List.of(List.of(0, 1)), subscribedTopics(group));
    }

    /**
     * A, B and C subscribe to T1 and T2, listing them in different orders: the group is
     * homogeneous, and makes a single cohort with every member and topic.
     */
    @Test
    public void testHomogeneousGroup() {
        var spec = new GroupSpecFixture()
            .withMember("C", orderedSet(T2, T1))
            .withMember("B", orderedSet(T1, T2))
            .withMember("A", orderedSet(T2, T1))
            .build();
        assertEquals(SubscriptionType.HOMOGENEOUS, spec.subscriptionType());

        var group = new IndexedGroup.Builder(spec, DESCRIBER).build();

        assertEquals(List.of(0, 0, 0), cohorts(group));
        assertEquals(List.of(List.of(0, 1, 2)), membersOfCohorts(group));
        assertEquals(List.of(List.of(0, 1)), topicsOfCohorts(group));
        assertEquals(List.of(List.of(0), List.of(0)), cohortsOfTopics(group));
        assertEquals(List.of(3, 3), subscriberCounts(group));
        assertEquals(List.of(List.of(0, 1), List.of(0, 1), List.of(0, 1)), subscribedTopics(group));
    }

    /**
     * A subscribes to T1 and T2, B to T2 and T3, C to T1 and T2 like A, and D to T3. Read in member
     * order, A starts cohort 0, B cohort 1, C joins cohort 0 although it lists its topics in
     * another order, and D starts cohort 2. T1 then has 2 subscribers (A and C), T2 has 3 (A, B
     * and C) and T3 has 2 (B and D).
     */
    @Test
    public void testHeterogeneousGroup() {
        var spec = new GroupSpecFixture()
            .withMember("D", Set.of(T3))
            .withMember("C", orderedSet(T2, T1))
            .withMember("B", orderedSet(T3, T2))
            .withMember("A", orderedSet(T1, T2))
            .build();
        assertEquals(SubscriptionType.HETEROGENEOUS, spec.subscriptionType());

        var group = new IndexedGroup.Builder(spec, DESCRIBER).build();

        assertEquals(List.of(0, 1, 0, 2), cohorts(group));
        assertEquals(List.of(List.of(0, 2), List.of(1), List.of(3)), membersOfCohorts(group));
        assertEquals(List.of(List.of(0, 1), List.of(1, 2), List.of(2)), topicsOfCohorts(group));
        assertEquals(List.of(List.of(0), List.of(0, 1), List.of(1, 2)), cohortsOfTopics(group));
        assertEquals(List.of(2, 3, 2), subscriberCounts(group));
        assertEquals(List.of(List.of(0, 1), List.of(1, 2), List.of(0, 1), List.of(2)), subscribedTopics(group));
    }

    /**
     * With every hash masked to 0, all the subscriptions have the same hash, and the cohorts are
     * told apart by comparing their topics: A {T1} and B {T1, T2} have different numbers of
     * topics, C {T2} and A different topics, D {T1} and F {T2, T1} find the cohorts of A and B,
     * and E {} starts a cohort without topics.
     */
    @Test
    public void testCohortsWithTheSameHashAreComparedExactly() {
        var spec = new GroupSpecFixture()
            .withMember("F", orderedSet(T2, T1))
            .withMember("E", Set.of())
            .withMember("D", Set.of(T1))
            .withMember("C", Set.of(T2))
            .withMember("B", orderedSet(T1, T2))
            .withMember("A", Set.of(T1))
            .build();
        assertEquals(SubscriptionType.HETEROGENEOUS, spec.subscriptionType());

        var group = new IndexedGroup.Builder(spec, DESCRIBER, 0L).build();

        assertEquals(List.of(0, 1, 2, 0, 3, 1), cohorts(group));
        assertEquals(List.of(List.of(0), List.of(0, 1), List.of(1), List.of()), topicsOfCohorts(group));
    }

    /**
     * A builder builds a single group: the second build throws.
     */
    @Test
    public void testSecondBuildThrows() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1))
            .build();
        var builder = new IndexedGroup.Builder(spec, DESCRIBER);
        builder.build();

        assertThrows(IllegalStateException.class, builder::build);
    }

    private static List<String> memberIds(IndexedGroup group) {
        return IntStream.range(0, group.memberCount()).mapToObj(group::memberId).toList();
    }

    private static List<Uuid> topicIds(IndexedGroup group) {
        return IntStream.range(0, group.topicCount()).mapToObj(group::topicId).toList();
    }

    private static List<Integer> topicIndices(IndexedGroup group, Uuid... topicIds) {
        return Arrays.stream(topicIds).map(group::topicIndex).toList();
    }

    private static List<Integer> partitionCounts(IndexedGroup group) {
        return IntStream.range(0, group.topicCount()).mapToObj(group::partitionCount).toList();
    }

    /**
     * @return The cohort of every member.
     */
    private static List<Integer> cohorts(IndexedGroup group) {
        return IntStream.range(0, group.memberCount()).mapToObj(group::cohortOf).toList();
    }

    private static List<List<Integer>> membersOfCohorts(IndexedGroup group) {
        return IntStream.range(0, group.cohortCount()).mapToObj(cohort -> boxed(group.membersOf(cohort))).toList();
    }

    private static List<List<Integer>> topicsOfCohorts(IndexedGroup group) {
        return IntStream.range(0, group.cohortCount()).mapToObj(cohort -> boxed(group.topicsOf(cohort))).toList();
    }

    private static List<List<Integer>> cohortsOfTopics(IndexedGroup group) {
        return IntStream.range(0, group.topicCount()).mapToObj(topic -> boxed(group.cohortsOf(topic))).toList();
    }

    private static List<Integer> subscriberCounts(IndexedGroup group) {
        return IntStream.range(0, group.topicCount()).mapToObj(group::subscriberCount).toList();
    }

    /**
     * @return Per member, the topics it subscribes to.
     */
    private static List<List<Integer>> subscribedTopics(IndexedGroup group) {
        return IntStream.range(0, group.memberCount())
            .mapToObj(member -> IntStream.range(0, group.topicCount()).filter(topic -> group.subscribes(member, topic)).boxed().toList())
            .toList();
    }

    private static List<Integer> boxed(int[] values) {
        return Arrays.stream(values).boxed().toList();
    }

    private static Set<Uuid> orderedSet(Uuid... topicIds) {
        return new LinkedHashSet<>(List.of(topicIds));
    }
}
