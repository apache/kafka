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
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.modern.SubscribedTopicDescriberImpl;
import org.apache.kafka.coordinator.group.modern.TopicIds;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests of {@link GroupModel}: the numbering of the members and the topics, and the cohorts.
 *
 * <p>The topic ids sort as T1 &lt; T2 &lt; T3, so that when all of them are subscribed T1 is
 * topic 0, T2 is topic 1 and T3 is topic 2; the member ids sort as A &lt; B &lt; C &lt; D, so
 * that A is member 0, B is member 1, and so on. The members are put in the specs in another
 * order, and the subscriptions list their topics in another order, so that the numbers cannot
 * come from the iteration order of the spec.
 */
public class GroupModelTest {
    private static final Uuid T1 = new Uuid(1L, 1L);
    private static final Uuid T2 = new Uuid(2L, 1L);
    private static final Uuid T3 = new Uuid(3L, 1L);
    private static final Uuid MISSING = new Uuid(9L, 1L);

    private static final SubscribedTopicDescriber DESCRIBER = new TopicsFixture()
        .withTopic(T1, 2)
        .withTopic(T2, 5)
        .withTopic(T3, 7)
        .build();

    /**
     * Member ids are compared as strings, so m10 comes between m1 and m2.
     */
    @Test
    public void testMembersAreNumberedInMemberIdOrder() {
        var spec = new GroupSpecFixture()
            .withMember("m2", Set.of(T1))
            .withMember("m10", Set.of(T1))
            .withMember("m1", Set.of(T1));

        var group = new GroupModel(spec.build(), DESCRIBER);

        assertEquals(List.of("m1", "m10", "m2"), memberIds(group));
    }

    /**
     * A, read first, meets T3 before T1, and B then meets T2; the topics are still numbered in
     * topic id order, with their partition counts.
     */
    @Test
    public void testTopicsAreNumberedInTopicIdOrder() {
        var spec = new GroupSpecFixture()
            .withMember("B", Set.of(T2))
            .withMember("A", ordered(T3, T1));

        var group = new GroupModel(spec.build(), DESCRIBER);

        assertEquals(List.of(T1, T2, T3), topicIds(group));
        assertEquals(List.of(0, 1, 2), topicIndexes(group, T1, T2, T3));
        assertEquals(List.of(2, 5, 7), partitionCounts(group));
        assertEquals(List.of(List.of(0, 2), List.of(1)), topicsOfCohorts(group));
    }

    /**
     * A subscribes to T1 and T2, B to T2 and T3, C to T1 and T2 like A, and D to T3. Read in
     * member order, A starts cohort 0, B cohort 1, C joins cohort 0 although it lists its topics
     * in another order, and D starts cohort 2. T1 then has 2 subscribers (A and C), T2 has 3 (A,
     * B and C) and T3 has 2 (B and D).
     */
    @Test
    public void testCohorts() {
        var spec = new GroupSpecFixture()
            .withMember("D", Set.of(T3))
            .withMember("C", ordered(T2, T1))
            .withMember("B", ordered(T3, T2))
            .withMember("A", ordered(T1, T2));

        var group = new GroupModel(spec.build(), DESCRIBER);

        assertEquals(List.of(0, 1, 0, 2), cohorts(group));
        assertEquals(List.of(List.of(0, 2), List.of(1), List.of(3)), membersOfCohorts(group));
        assertEquals(List.of(List.of(0, 1), List.of(1, 2), List.of(2)), topicsOfCohorts(group));
        assertEquals(List.of(List.of(0), List.of(0, 1), List.of(1, 2)), cohortsOfTopics(group));
        assertEquals(List.of(2, 3, 2), subscriberCounts(group));
        assertEquals(List.of(List.of(0, 1), List.of(1, 2), List.of(0, 1), List.of(2)), subscribedTopics(group));
    }

    /**
     * The describer gives -1 partitions for a topic that does not exist, which is then dropped
     * from the subscriptions: A, subscribing to T1 and the missing topic, and B, subscribing to T1,
     * share cohort 0, and C, subscribing only to the missing topic, has a cohort without topics.
     */
    @Test
    public void testTopicsThatDoNotExistAreIgnored() {
        var spec = new GroupSpecFixture()
            .withMember("C", Set.of(MISSING))
            .withMember("B", Set.of(T1))
            .withMember("A", ordered(MISSING, T1));

        var group = new GroupModel(spec.build(), DESCRIBER);

        assertEquals(List.of(T1), topicIds(group));
        assertEquals(List.of(0, -1), topicIndexes(group, T1, MISSING));
        assertEquals(List.of(2), subscriberCounts(group));
        assertEquals(List.of(List.of(0, 1), List.of(2)), membersOfCohorts(group));
        assertEquals(List.of(List.of(0), List.of()), topicsOfCohorts(group));
        assertEquals(List.of(List.of(0)), cohortsOfTopics(group));
        assertEquals(List.of(List.of(0), List.of(0), List.of()), subscribedTopics(group));
    }

    /**
     * A, B and C subscribe to T1 and T2, listing them in different orders: the group is
     * homogeneous, and makes a single cohort with every member and topic.
     */
    @Test
    public void testHomogeneousGroup() {
        var spec = new GroupSpecFixture()
            .withMember("C", ordered(T2, T1))
            .withMember("B", ordered(T1, T2))
            .withMember("A", ordered(T2, T1))
            .build();
        assertEquals(SubscriptionType.HOMOGENEOUS, spec.subscriptionType());

        var group = new GroupModel(spec, DESCRIBER);

        assertEquals(List.of("A", "B", "C"), memberIds(group));
        assertEquals(List.of(T1, T2), topicIds(group));
        assertEquals(List.of(3, 3), subscriberCounts(group));
        assertEquals(List.of(0, 0, 0), cohorts(group));
        assertEquals(List.of(List.of(0, 1, 2)), membersOfCohorts(group));
        assertEquals(List.of(List.of(0, 1)), topicsOfCohorts(group));
        assertEquals(List.of(List.of(0), List.of(0)), cohortsOfTopics(group));
        assertEquals(List.of(List.of(0, 1), List.of(0, 1), List.of(0, 1)), subscribedTopics(group));
    }

    /**
     * The coordinator gives the subscriptions as {@link TopicIds}, resolving topic names against
     * the metadata image when iterated; an unknown name is skipped, although it counts in their
     * size. A subscribes to t1, t3 and an unknown topic, B to t1 and t2, and C to t1 and t3: A and
     * C have the same topics, so they share cohort 0. Three members subscribing to all the topics
     * and the unknown one make a homogeneous group, a single cohort with the three topics.
     */
    @Test
    public void testSubscriptionsAsTopicIds() {
        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(T1, "t1", 2)
            .addTopic(T2, "t2", 5)
            .addTopic(T3, "t3", 7)
            .buildCoordinatorMetadataImage();
        var describer = new SubscribedTopicDescriberImpl(image);
        var resolver = new TopicIds.CachedTopicResolver(image);
        var subscriptionOfA = new TopicIds(Set.of("t1", "t3", "unknown"), resolver);
        assertEquals(3, subscriptionOfA.size());
        var spec = new GroupSpecFixture()
            .withMember("A", subscriptionOfA)
            .withMember("B", new TopicIds(Set.of("t1", "t2"), resolver))
            .withMember("C", new TopicIds(Set.of("t3", "t1"), resolver))
            .build();

        var group = new GroupModel(spec, describer);

        assertEquals(List.of(T1, T2, T3), topicIds(group));
        assertEquals(List.of(2, 5, 7), partitionCounts(group));
        assertEquals(List.of(List.of(0, 2), List.of(1)), membersOfCohorts(group));
        assertEquals(List.of(List.of(0, 2), List.of(0, 1)), topicsOfCohorts(group));
        assertEquals(List.of(3, 1, 2), subscriberCounts(group));

        var homogeneousSpec = new GroupSpecFixture();
        for (String id : List.of("C", "B", "A")) {
            homogeneousSpec.withMember(id, new TopicIds(Set.of("t1", "t2", "t3", "unknown"), resolver));
        }

        var homogeneous = new GroupModel(homogeneousSpec.build(), describer);

        assertEquals(List.of(T1, T2, T3), topicIds(homogeneous));
        assertEquals(List.of(List.of(0, 1, 2)), membersOfCohorts(homogeneous));
        assertEquals(List.of(List.of(0, 1, 2)), topicsOfCohorts(homogeneous));
        assertEquals(List.of(3, 3, 3), subscriberCounts(homogeneous));
    }

    /**
     * T2 exists but nobody subscribes to it, and the missing topic does not exist: neither has a
     * number.
     */
    @Test
    public void testTopicIndex() {
        var spec = new GroupSpecFixture()
            .withMember("A", Set.of(T1));

        var group = new GroupModel(spec.build(), DESCRIBER);

        assertEquals(List.of(0, -1, -1), topicIndexes(group, T1, T2, MISSING));
    }

    /**
     * An empty group, homogeneous as the coordinator declares it, has no member nor topic, and
     * its single cohort has neither.
     */
    @Test
    public void testEmptyGroup() {
        var group = new GroupModel(new GroupSpecFixture().build(), DESCRIBER);

        assertEquals(List.of(), memberIds(group));
        assertEquals(List.of(), topicIds(group));
        assertEquals(List.of(-1), topicIndexes(group, T1));
        assertEquals(List.of(List.of()), membersOfCohorts(group));
        assertEquals(List.of(List.of()), topicsOfCohorts(group));
    }

    /**
     * A single member makes a single cohort with its topics.
     */
    @Test
    public void testSingleMember() {
        var spec = new GroupSpecFixture()
            .withMember("A", ordered(T2, T1))
            .build();

        var group = new GroupModel(spec, DESCRIBER);

        assertEquals(List.of("A"), memberIds(group));
        assertEquals(List.of(T1, T2), topicIds(group));
        assertEquals(List.of(List.of(0)), membersOfCohorts(group));
        assertEquals(List.of(List.of(0, 1)), topicsOfCohorts(group));
        assertEquals(List.of(1, 1), subscriberCounts(group));
    }

    private static List<String> memberIds(GroupModel group) {
        return IntStream.range(0, group.memberCount()).mapToObj(group::memberId).toList();
    }

    private static List<Uuid> topicIds(GroupModel group) {
        return IntStream.range(0, group.topicCount()).mapToObj(group::topicId).toList();
    }

    private static List<Integer> topicIndexes(GroupModel group, Uuid... topicIds) {
        return Arrays.stream(topicIds).map(group::topicIndex).toList();
    }

    private static List<Integer> partitionCounts(GroupModel group) {
        return IntStream.range(0, group.topicCount()).mapToObj(group::partitionCount).toList();
    }

    /**
     * @return The cohort of every member.
     */
    private static List<Integer> cohorts(GroupModel group) {
        return IntStream.range(0, group.memberCount()).mapToObj(group::cohortOf).toList();
    }

    private static List<List<Integer>> membersOfCohorts(GroupModel group) {
        return IntStream.range(0, group.cohortCount()).mapToObj(cohort -> boxed(group.membersOf(cohort))).toList();
    }

    private static List<List<Integer>> topicsOfCohorts(GroupModel group) {
        return IntStream.range(0, group.cohortCount()).mapToObj(cohort -> boxed(group.topicsOf(cohort))).toList();
    }

    private static List<List<Integer>> cohortsOfTopics(GroupModel group) {
        return IntStream.range(0, group.topicCount()).mapToObj(topic -> boxed(group.cohortsOf(topic))).toList();
    }

    private static List<Integer> subscriberCounts(GroupModel group) {
        return IntStream.range(0, group.topicCount()).mapToObj(group::subscriberCount).toList();
    }

    /**
     * @return Per member, the topics it subscribes to.
     */
    private static List<List<Integer>> subscribedTopics(GroupModel group) {
        return IntStream.range(0, group.memberCount())
            .mapToObj(member -> IntStream.range(0, group.topicCount()).filter(topic -> group.subscribes(member, topic)).boxed().toList())
            .toList();
    }

    private static List<Integer> boxed(int[] values) {
        return Arrays.stream(values).boxed().toList();
    }

    private static Set<Uuid> ordered(Uuid... topicIds) {
        return new LinkedHashSet<>(List.of(topicIds));
    }
}
