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
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.assignor.Uniform2FuzzScenario.Event;
import org.apache.kafka.coordinator.group.assignor.Uniform2FuzzScenario.Kind;
import org.apache.kafka.coordinator.group.assignor.uniform2.CurrentAssignment;
import org.apache.kafka.coordinator.group.assignor.uniform2.GroupSpecFixture;
import org.apache.kafka.coordinator.group.assignor.uniform2.IndexedGroup;
import org.apache.kafka.coordinator.group.assignor.uniform2.Quotas;
import org.apache.kafka.coordinator.group.assignor.uniform2.steps.AssignRemainingExtraPartitions;
import org.apache.kafka.coordinator.group.assignor.uniform2.steps.BalanceExtraPartitions;
import org.apache.kafka.coordinator.group.assignor.uniform2.steps.KeepStickyExtraPartitions;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.assertValidAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Fuzzer of the uniform2 assignor. Random groups and clusters, see {@link Uniform2FuzzScenario},
 * go through random sequences of events, and every resulting assignment is checked for the
 * properties the assignor guarantees:
 * <ul>
 *     <li>complete, spread and balanced, with
 *     {@link AssignmentTestUtils#assertValidAssignment};</li>
 *     <li>reusing the maps: a member whose partitions do not change gets back the same map,
 *     whatever the other members get;</li>
 *     <li>deterministic: the members, the topics of their subscriptions and the entries of their
 *     current assignments given in a random order, drawn from the seed and the step, produce the
 *     same assignment;</li>
 *     <li>a fixed point: assigning the result again returns the same map and set instances;</li>
 *     <li>independent of the way of counting of the balance step: keeping the counts per cohort of
 *     every member or of one member at a time, which the assignor picks from the shape of the
 *     group, gives the same quotas;</li>
 *     <li>sticky: with a single subscription, a single leave or join moves at most the extra
 *     partitions of the group, the sum over the topics of the partition count modulo the subscriber
 *     count, among the members present before and after it. The partitions the joiner takes are not
 *     counted. For groups of up to {@link #ORACLE_MAX_MEMBERS} members and
 *     {@link #ORACLE_MAX_TOPICS} topics, the number of moved partitions is at most one above the
 *     fewest of all the assignments with the properties, found by brute force.</li>
 * </ul>
 *
 * <p>The scenarios give the subscriptions as the coordinator does, and include stale inputs:
 * subscriptions to topic names without a topic, partitions of deleted or re-created topics, and
 * empty subscriptions. They have no racks, neither for the members nor for the replicas.
 *
 * <p>Environment variables, which Gradle passes on to the test JVM: {@code UNIFORM2_FUZZ_SEEDS}
 * sets the number of seeds (2000 by default), {@code UNIFORM2_FUZZ_FIRST_SEED} the first of them (0
 * by default), and {@code UNIFORM2_FUZZ_EVENTS} the number of events per seed (12 by default).
 * Gradle does not see them as inputs of the test task, so pass {@code --rerun}, for instance:
 * <pre>
 * UNIFORM2_FUZZ_SEEDS=20000 ./gradlew :group-coordinator:test --rerun \
 *     --tests org.apache.kafka.coordinator.group.assignor.Uniform2AssignorFuzzTest
 * </pre>
 * Every failure message carries the seed, the step, the event and the dump of the scenario as the
 * assignor saw it at that step, after the event and before the assignment was applied. A seed that
 * does not finish within {@link #SEED_TIMEOUT} fails the same way, at the step it was at. A failing
 * seed runs alone with {@code UNIFORM2_FUZZ_FIRST_SEED=<seed> UNIFORM2_FUZZ_SEEDS=1}.
 */
public class Uniform2AssignorFuzzTest {
    /**
     * What the checks of a step need to know about the group: its subscription type, and the
     * subscribed topics that exist, in topic id order, with their numbers of subscribers and
     * partitions. The subscriptions only give the topics that exist, see
     * {@link Uniform2FuzzScenario}.
     */
    private static final class GroupFacts {
        /**
         * The subscription type of the group.
         */
        final SubscriptionType type;

        /**
         * The subscribed topics that exist, in topic id order.
         */
        final List<Uuid> topics = new ArrayList<>();

        /**
         * Per subscribed topic that exists, its number of subscribers.
         */
        final Map<Uuid, Integer> subscribers = new HashMap<>();

        /**
         * Per subscribed topic that exists, its number of partitions.
         */
        final Map<Uuid, Integer> partitions = new HashMap<>();

        GroupFacts(GroupSpec spec, SubscribedTopicDescriber describer) {
            type = spec.subscriptionType();
            Map<Uuid, Integer> counts = new TreeMap<>();
            for (String id : spec.memberIds()) {
                subscribedTopicIds(spec, id).forEach(topicId -> counts.merge(topicId, 1, Integer::sum));
            }
            counts.forEach((topicId, count) -> {
                topics.add(topicId);
                subscribers.put(topicId, count);
                partitions.put(topicId, describer.numPartitions(topicId));
            });
        }

        /**
         * @return The number of extra partitions of the group, the sum over the subscribed topics
         *         that exist of the partition count modulo the number of subscribers.
         */
        int extraPartitions() {
            int total = 0;
            for (Uuid topicId : topics) {
                total += partitions.get(topicId) % subscribers.get(topicId);
            }
            return total;
        }
    }

    /**
     * Brute force computation of the smallest number of partitions that an assignment with the
     * spread and balance properties must move from the current assignment.
     *
     * <p>The spread fixes the quota of every subscriber of a topic to the minimum quota or one
     * more, so an assignment is characterized, up to partition ids, by which subscribers get the
     * extra partitions of each topic. For given quotas, the fewest moves are made when every member
     * keeps as many of its current partitions of each topic as its quota allows, which is always
     * possible without racks. The oracle enumerates all the ways to assign the extra partitions,
     * keeps the balanced ones and takes the cheapest. Current partitions of topics a member is not
     * subscribed to or of topics that do not exist are lost whatever the quotas. Balance means that
     * no extra partition could move to a subscriber at least two below its owner, which with a
     * single subscription means that all assignment sizes are within one of each other.
     */
    private static final class MovementOracle {
        /**
         * The number of members, numbered in the order of the group spec.
         */
        private final int memberCount;

        /**
         * The number of subscribed topics that exist, numbered in topic id order.
         */
        private final int topicCount;

        /**
         * Per topic, its subscribers as a bit mask over the members.
         */
        private final int[] subscribers;

        /**
         * Per topic, its minimum quota.
         */
        private final int[] minQuotas;

        /**
         * Per topic, its extra partitions.
         */
        private final int[] extras;

        /**
         * Per topic and member, the number of current partitions of the member that its quota can
         * keep.
         */
        private final int[][] current;

        /**
         * The number of current partitions lost whatever the quotas.
         */
        private final int lost;

        /**
         * Per topic, the members getting one of its extra partitions in the search, as a bit mask.
         */
        private final int[] extraMasks;

        /**
         * Per member, its assignment size from the topics assigned so far in the search.
         */
        private final int[] sizes;

        /**
         * The fewest moves of the balanced assignments found so far.
         */
        private int best = Integer.MAX_VALUE;

        /**
         * @return The smallest number of moved partitions, or null when the group is too large.
         */
        static Integer minimalMovement(GroupFacts facts, GroupSpec spec) {
            int memberCount = spec.memberIds().size();
            if (memberCount == 0 || memberCount > ORACLE_MAX_MEMBERS || facts.topics.size() > ORACLE_MAX_TOPICS) {
                return null;
            }
            MovementOracle oracle = new MovementOracle(spec, facts);
            oracle.search(0, 0);
            if (oracle.best == Integer.MAX_VALUE) {
                fail("the oracle found no assignment with the properties");
            }
            return oracle.lost + oracle.best;
        }

        private MovementOracle(GroupSpec spec, GroupFacts facts) {
            List<String> ids = new ArrayList<>(spec.memberIds());
            List<Uuid> topics = facts.topics;
            memberCount = ids.size();
            topicCount = topics.size();
            subscribers = new int[topicCount];
            minQuotas = new int[topicCount];
            extras = new int[topicCount];
            current = new int[topicCount][memberCount];
            extraMasks = new int[topicCount];
            sizes = new int[memberCount];
            int lostPartitions = 0;
            for (int t = 0; t < topicCount; t++) {
                Uuid topicId = topics.get(t);
                int partitions = facts.partitions.get(topicId);
                for (int m = 0; m < memberCount; m++) {
                    boolean subscribed = subscribedTopicIds(spec, ids.get(m)).contains(topicId);
                    subscribers[t] |= subscribed ? 1 << m : 0;
                    int held = spec.memberAssignment(ids.get(m)).partitions().getOrDefault(topicId, Set.of()).size();
                    if (subscribed) {
                        current[t][m] += held;
                    } else {
                        lostPartitions += held;
                    }
                }
                int subscriberCount = Integer.bitCount(subscribers[t]);
                minQuotas[t] = partitions / subscriberCount;
                extras[t] = partitions % subscriberCount;
            }
            for (String id : ids) {
                for (Map.Entry<Uuid, Set<Integer>> entry : spec.memberAssignment(id).partitions().entrySet()) {
                    if (!topics.contains(entry.getKey())) {
                        lostPartitions += entry.getValue().size();
                    }
                }
            }
            lost = lostPartitions;
        }

        /**
         * Tries every way to assign the extra partitions of the topics from {@code t} on.
         */
        private void search(int t, int moves) {
            if (t == topicCount) {
                if (balanced()) {
                    best = Math.min(best, moves);
                }
                return;
            }
            for (int mask = 0; mask < 1 << memberCount; mask++) {
                if (Integer.bitCount(mask) != extras[t] || (mask & ~subscribers[t]) != 0) {
                    continue;
                }
                extraMasks[t] = mask;
                int topicMoves = 0;
                for (int m = 0; m < memberCount; m++) {
                    if ((subscribers[t] & (1 << m)) != 0) {
                        int quota = minQuotas[t] + ((mask >> m) & 1);
                        sizes[m] += quota;
                        topicMoves += Math.max(0, current[t][m] - quota);
                    }
                }
                search(t + 1, moves + topicMoves);
                for (int m = 0; m < memberCount; m++) {
                    if ((subscribers[t] & (1 << m)) != 0) {
                        sizes[m] -= minQuotas[t] + ((mask >> m) & 1);
                    }
                }
            }
        }

        private boolean balanced() {
            for (int t = 0; t < topicCount; t++) {
                for (int giver = 0; giver < memberCount; giver++) {
                    if ((extraMasks[t] & (1 << giver)) == 0) {
                        continue;
                    }
                    for (int receiver = 0; receiver < memberCount; receiver++) {
                        boolean eligible = (subscribers[t] & (1 << receiver)) != 0 && (extraMasks[t] & (1 << receiver)) == 0;
                        if (eligible && sizes[giver] >= sizes[receiver] + 2) {
                            return false;
                        }
                    }
                }
            }
            return true;
        }
    }

    /**
     * The number of seeds of a run.
     */
    private static final long SEEDS = fromEnvironment("UNIFORM2_FUZZ_SEEDS", 2000);

    /**
     * The first seed of a run.
     */
    private static final long FIRST_SEED = fromEnvironment("UNIFORM2_FUZZ_FIRST_SEED", 0);

    /**
     * The number of events of every seed, after its first assignment.
     */
    private static final long EVENTS = fromEnvironment("UNIFORM2_FUZZ_EVENTS", 12);

    /**
     * The most members of a group the brute force oracle handles.
     */
    private static final int ORACLE_MAX_MEMBERS = 4;

    /**
     * The most subscribed topics of a group the brute force oracle handles.
     */
    private static final int ORACLE_MAX_TOPICS = 4;

    /**
     * The time a seed may take, far above what any seed needs, so that a step that never ends
     * fails with its context instead of hanging the run.
     */
    private static final Duration SEED_TIMEOUT = Duration.ofSeconds(60);

    /**
     * The assignor under test.
     */
    private final Uniform2Assignor assignor = new Uniform2Assignor();

    /**
     * The assignments checked.
     */
    private long assignments;

    /**
     * The partitions moved by all the assignments checked.
     */
    private long moved;

    /**
     * The assignments compared with the brute force oracle.
     */
    private long oracleChecks;

    /**
     * The seed, the step and the event being checked, which prefix the failure messages. It is
     * volatile since the steps run in the thread of {@code assertTimeoutPreemptively}, and the
     * message of a timeout reads it from the test thread.
     */
    private volatile String context = "";

    /**
     * @return The value of the environment variable as a long, or the default when it is not set.
     */
    private static long fromEnvironment(String name, long defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : Long.parseLong(value.trim());
    }

    @Test
    public void testRandomScenarios() {
        Set<Kind> kinds = EnumSet.noneOf(Kind.class);
        for (long seed = FIRST_SEED; seed < FIRST_SEED + SEEDS; seed++) {
            Uniform2FuzzScenario scenario = new Uniform2FuzzScenario(seed);
            assertTimeoutPreemptively(SEED_TIMEOUT, () -> {
                check(scenario, 0, Event.INIT);
                for (int step = 1; step <= EVENTS; step++) {
                    Event event = scenario.mutate();
                    if (event.kind() != Kind.NOTHING) {
                        kinds.add(event.kind());
                    }
                    check(scenario, step, event);
                }
            }, () -> context + ": the seed did not finish within " + SEED_TIMEOUT + System.lineSeparator() + scenario.dump());
        }
        System.out.printf(
            "uniform2 fuzz: %d assignments (%d seeds from %d, 1 initial + %d events each), moved partitions=%d, oracle checks=%d%n",
            assignments,
            SEEDS,
            FIRST_SEED,
            EVENTS,
            moved,
            oracleChecks
        );
        // A short run may not reach every kind of event.
        if (SEEDS * EVENTS >= 2000) {
            assertEquals(EnumSet.complementOf(EnumSet.of(Kind.INIT, Kind.NOTHING)), kinds, "every kind of event happens");
        }
    }

    /**
     * Checks the assignment of the scenario at the step. A failure message starts with the seed,
     * the step and the event, and ends with the dump of the scenario.
     */
    private void check(Uniform2FuzzScenario scenario, int step, Event event) {
        context = "seed=" + scenario.seed() + " step=" + step + " event=\"" + event + "\"";
        try {
            checkAssignment(scenario, event, new Random(31L * scenario.seed() + step));
        } catch (Throwable e) {
            fail(context + ": " + e.getMessage() + System.lineSeparator() + scenario.dump(), e);
        }
    }

    /**
     * Assigns the group of the scenario, checks the properties of the assignment, and makes it the
     * current assignment of the scenario.
     *
     * @param random The source of the orders of the shuffled group spec.
     */
    private void checkAssignment(Uniform2FuzzScenario scenario, Event event, Random random) {
        GroupSpec spec = scenario.spec();
        SubscribedTopicDescriber describer = scenario.describer();
        GroupAssignment result = assignor.assign(spec, describer);
        assignments++;

        // The properties of the assignment itself.
        assertValidAssignment(spec, describer, result);
        assertUnchangedMembersKeepTheirMaps(spec, result);

        // The properties of assigning the same group again.
        assertOrderIndependent(scenario.shuffledSpec(random), describer, result);
        assertFixedPoint(spec, describer, result);
        assertWaysOfCountingAgree(spec, describer);

        // The properties of the moves from the current assignment.
        GroupFacts facts = new GroupFacts(spec, describer);
        int movedNow = revocations(spec, result);
        moved += movedNow;
        assertMovementBounds(facts, result, event, movedNow);
        assertMinimalMovement(facts, spec, movedNow);

        scenario.apply(result);
    }

    /**
     * A member whose partitions do not change gets back the same map, whatever the other members
     * get, so that the coordinator sees it as unchanged.
     */
    private static void assertUnchangedMembersKeepTheirMaps(GroupSpec spec, GroupAssignment result) {
        for (String id : spec.memberIds()) {
            Map<Uuid, Set<Integer>> current = spec.memberAssignment(id).partitions();
            Map<Uuid, Set<Integer>> partitions = result.members().get(id).partitions();
            if (partitions.equals(current)) {
                assertSame(current, partitions, id + " keeps its partitions in a new map");
            }
        }
    }

    /**
     * The same group spec with its members, the topics of their subscriptions and the entries of
     * their current assignments in another order produces the same assignment.
     */
    private void assertOrderIndependent(GroupSpec shuffledSpec, SubscribedTopicDescriber describer, GroupAssignment result) {
        assertEquals(result, assignor.assign(shuffledSpec, describer),
            "the order of the members, topics or partitions changed the assignment");
    }

    /**
     * Assigning the result again returns the same maps, which the coordinator relies on to
     * recognize unchanged members.
     */
    private void assertFixedPoint(GroupSpec spec, SubscribedTopicDescriber describer, GroupAssignment result) {
        GroupSpec stable = GroupSpecFixture.after(spec, result).build();
        GroupAssignment again = assignor.assign(stable, describer);
        for (String id : stable.memberIds()) {
            assertSame(stable.memberAssignment(id).partitions(), again.members().get(id).partitions(),
                "the assignment of " + id + " is not a fixed point");
        }
    }

    /**
     * The two ways of counting of the balance step, keeping the counts per cohort of every member
     * or of one member at a time, give the same quotas.
     */
    private static void assertWaysOfCountingAgree(GroupSpec spec, SubscribedTopicDescriber describer) {
        var group = new IndexedGroup.Builder(spec, describer).build();
        var currentAssignment = new CurrentAssignment(spec, group);
        assertEquals(
            membersWithExtraPartition(group, currentAssignment, true),
            membersWithExtraPartition(group, currentAssignment, false),
            "the ways of counting of the balance step disagree"
        );
    }

    /**
     * @param keepCounts Whether the balance step keeps the counts per cohort of every member.
     * @return Per topic, the members with an extra partition of it, in member order, as the steps
     *         decide them.
     */
    private static Map<Uuid, List<Integer>> membersWithExtraPartition(
        IndexedGroup group,
        CurrentAssignment currentAssignment,
        boolean keepCounts
    ) {
        var quotas = new Quotas(group);
        new KeepStickyExtraPartitions(group, currentAssignment, quotas).run();
        new AssignRemainingExtraPartitions(group, quotas).run();
        new BalanceExtraPartitions(group, currentAssignment, quotas, keepCounts).run();
        var membersWithExtraPartition = new HashMap<Uuid, List<Integer>>();
        for (int topic = 0; topic < group.topicCount(); topic++) {
            int[] members = quotas.membersWithExtraPartition(topic).toArray();
            Arrays.sort(members);
            membersWithExtraPartition.put(group.topicId(topic), Arrays.stream(members).boxed().toList());
        }
        return membersWithExtraPartition;
    }

    /**
     * With a single subscription, the partitions moved among the members present both before
     * and after a single leave or join are at most the extra partitions of the group. A leave
     * only frees the partitions of the leaver and a join only takes what the joiner is owed,
     * except that the change of quotas can leave the other members uneven, and levelling
     * them costs at most one move per extra partition of the group.
     */
    private static void assertMovementBounds(GroupFacts facts, GroupAssignment result, Event event, int movedNow) {
        if (facts.type != SubscriptionType.HOMOGENEOUS) {
            return;
        }
        if (event.kind() == Kind.LEAVE) {
            int bound = facts.extraPartitions();
            assertTrue(movedNow <= bound, "a single leave moved " + movedNow
                + " partitions among the remaining members, more than the " + bound + " extra partitions");
        } else if (event.kind() == Kind.JOIN) {
            int intake = assignmentSize(result, event.members().get(0));
            int bound = facts.extraPartitions();
            assertTrue(movedNow - intake <= bound, "a single join moved " + movedNow
                + " partitions for an intake of " + intake + ", more than the " + bound + " extra partitions beyond it");
        }
    }

    /**
     * For tiny groups, the assignment moves at most one partition more than the fewest moves of
     * any assignment with the properties. The tolerance of one covers ties settled without
     * looking ahead. The keep step settles its conflicts by the assignment sizes over the
     * topics already seen, and the balance step moves one extra partition at a time, never
     * along a chain through a third member that could make a move free.
     */
    private void assertMinimalMovement(GroupFacts facts, GroupSpec spec, int movedNow) {
        Integer minimum = MovementOracle.minimalMovement(facts, spec);
        if (minimum == null) {
            return;
        }
        oracleChecks++;
        assertTrue(movedNow >= minimum, movedNow + " partitions moved but the oracle needs " + minimum);
        assertTrue(movedNow - minimum <= 1, movedNow + " partitions moved where " + minimum + " suffice");
    }

    private static Set<Uuid> subscribedTopicIds(GroupSpec spec, String memberId) {
        return spec.memberSubscription(memberId).subscribedTopicIds();
    }

    /**
     * @return The number of current partitions of the members that they do not have in the
     *         assignment.
     */
    private static int revocations(GroupSpec spec, GroupAssignment assignment) {
        int revocations = 0;
        for (String id : spec.memberIds()) {
            var newPartitions = assignment.members().get(id).partitions();
            for (Map.Entry<Uuid, Set<Integer>> topicEntry : spec.memberAssignment(id).partitions().entrySet()) {
                var kept = newPartitions.getOrDefault(topicEntry.getKey(), Set.of());
                for (int partition : topicEntry.getValue()) {
                    if (!kept.contains(partition)) {
                        revocations++;
                    }
                }
            }
        }
        return revocations;
    }

    /**
     * @return The number of partitions assigned to the member.
     */
    private static int assignmentSize(GroupAssignment assignment, String memberId) {
        return assignment.members().get(memberId).partitions().values().stream().mapToInt(Set::size).sum();
    }
}
