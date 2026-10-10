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
import org.apache.kafka.coordinator.group.assignor.uniform2.GroupSpecFixture;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.assertStable;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.assertValidAssignment;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.assignmentSize;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.revocations;
import static org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentTestUtils.sharesPerWayOfCounting;
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
 *     <li>deterministic: the members, the topics of their subscriptions and the entries of their
 *     current assignments given in a random order, drawn from the seed and the step, produce the
 *     same assignment;</li>
 *     <li>a fixed point: feeding the assignment back returns the very same assignment maps, hence
 *     the very same partition set instances;</li>
 *     <li>reusing the maps: a member whose partitions do not change gets back the very map it had,
 *     whatever the other members get;</li>
 *     <li>independent of the way of counting of the balance step: the four ways of
 *     {@code ExtraPartitionMoves}, which the assignor picks from the shape of the group, give the
 *     same shares;</li>
 *     <li>sticky: with a single subscription, the partitions moved among the members present both
 *     before and after a single leave or join, that is every moved partition for a leave and every
 *     one beyond the joiner's intake for a join, are at most the extra partitions of the group, the
 *     sum over the topics of the partition count modulo the subscriber count. For groups of up to
 *     {@link #ORACLE_MAX_MEMBERS} members and {@link #ORACLE_MAX_TOPICS} topics, the number of
 *     moved partitions is at most one above the smallest of all the assignments having the
 *     properties, found by brute force, and the summary line counts the cases one above it per
 *     subscription kind.</li>
 * </ul>
 * The summary line also counts, without asserting anything, the assignments with several
 * subscriptions which are not the exact optimum of the balance: those where an extra partition
 * could move along a chain of members, each giving one to the next, to a member at least two below
 * the first.
 *
 * <p>The scenarios include stale inputs: subscriptions to topics that do not exist, partitions of
 * deleted topics or beyond the partition count, empty subscriptions. They have no racks: no member
 * has one, and the describer returns no replica racks.
 *
 * <p>Environment variables, which Gradle passes on to the test JVM: {@code UNIFORM2_FUZZ_SEEDS}
 * sets the number of seeds (2000 by default), {@code UNIFORM2_FUZZ_SEED} runs a single seed,
 * {@code UNIFORM2_FUZZ_EVENTS} the number of events per seed (12 by default),
 * {@code UNIFORM2_FUZZ_ORACLE=false} skips the brute force oracle, and
 * {@code UNIFORM2_FUZZ_REFERENCE=true} also runs the {@link UniformAssignor} on the same inputs,
 * only to compare the moved partitions in the summary line. Gradle does not see them as inputs of
 * the test task, so pass {@code --rerun}, for instance:
 * <pre>
 * UNIFORM2_FUZZ_SEEDS=20000 ./gradlew :group-coordinator:test --rerun \
 *     --tests org.apache.kafka.coordinator.group.assignor.Uniform2AssignorFuzzTest
 * </pre>
 * Every failure message carries the seed, the step, the event and the dump of the scenario as the
 * assignor saw it at that step: after the event, before the assignment was applied. A seed that does
 * not finish within {@link #SEED_TIMEOUT} fails the same way, at the step it was at.
 */
public class Uniform2AssignorFuzzTest {
    /**
     * The number of seeds of a run.
     */
    private static final int SEEDS = intFromEnvironment("UNIFORM2_FUZZ_SEEDS", 2000);

    /**
     * The single seed to run, or null to run {@link #SEEDS} seeds from 0.
     */
    private static final Integer SEED = System.getenv("UNIFORM2_FUZZ_SEED") == null ? null : intFromEnvironment("UNIFORM2_FUZZ_SEED", 0);

    /**
     * The number of events of every seed, after its first assignment.
     */
    private static final int EVENTS = intFromEnvironment("UNIFORM2_FUZZ_EVENTS", 12);

    /**
     * The most members of a group the brute force oracle handles.
     */
    private static final int ORACLE_MAX_MEMBERS = 4;

    /**
     * The most subscribed topics of a group the brute force oracle handles.
     */
    private static final int ORACLE_MAX_TOPICS = 4;

    /**
     * Lets a run skip the brute force oracle, which dominates the run time of tiny groups.
     */
    private static final boolean ORACLE = booleanFromEnvironment("UNIFORM2_FUZZ_ORACLE", true);

    /**
     * Whether the {@link UniformAssignor} runs on the same inputs, for the summary line only.
     */
    private static final boolean REFERENCE = booleanFromEnvironment("UNIFORM2_FUZZ_REFERENCE", false);

    /**
     * The time a seed may take, far above what any seed needs, so that a step that never ends
     * fails with its context instead of hanging the run.
     */
    private static final Duration SEED_TIMEOUT = Duration.ofSeconds(60);

    /**
     * @return The value of the environment variable as an int, or the default when it is not set.
     */
    private static int intFromEnvironment(String name, int defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : Integer.parseInt(value.trim());
    }

    /**
     * @return The value of the environment variable as a boolean, or the default when it is not
     *         set.
     */
    private static boolean booleanFromEnvironment(String name, boolean defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : Boolean.parseBoolean(value.trim());
    }

    @Test
    public void testRandomScenarios() {
        Harness harness = new Harness();
        long first = SEED == null ? 0 : SEED;
        long last = SEED == null ? SEEDS - 1 : SEED;
        for (long seed = first; seed <= last; seed++) {
            Uniform2FuzzScenario scenario = new Uniform2FuzzScenario(seed);
            assertTimeoutPreemptively(SEED_TIMEOUT, () -> {
                harness.check(scenario, 0, Event.INIT);
                for (int step = 1; step <= EVENTS; step++) {
                    harness.check(scenario, step, scenario.mutate());
                }
            }, () -> harness.context() + ": the seed did not finish within " + SEED_TIMEOUT + System.lineSeparator() + scenario.dump());
        }
        System.out.println(harness.summary());
    }

    /**
     * Runs the checks of one step and accumulates the statistics of the run.
     */
    private static final class Harness {
        /**
         * The assignor under test.
         */
        private final Uniform2Assignor assignor = new Uniform2Assignor();

        /**
         * The uniform assignor, run on the same inputs for the summary line only.
         */
        private final UniformAssignor reference = new UniformAssignor();

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
         * Oracle checks that needed one move more than the minimum, with a single subscription.
         */
        private long oracleExcessHomogeneous;

        /**
         * Oracle checks that needed one move more than the minimum, with several subscriptions.
         */
        private long oracleExcessHeterogeneous;

        /**
         * The assignments with several subscriptions.
         */
        private long heterogeneousAssignments;

        /**
         * The assignments with several subscriptions which are not the exact optimum of the
         * balance.
         */
        private long notOptimallyBalanced;

        /**
         * The partitions moved by the uniform assignor on the same inputs.
         */
        private long referenceMoved;

        /**
         * The inputs on which the uniform assignor failed.
         */
        private long referenceFailures;

        /**
         * The context of the step being checked, for a seed that does not finish.
         */
        private volatile String context = "";

        String context() {
            return context;
        }

        void check(Uniform2FuzzScenario scenario, int step, Event event) {
            String context = "seed=" + scenario.seed() + " step=" + step + " event=\"" + event + "\"";
            this.context = context;
            try {
                step(scenario, step, event, context);
            } catch (Throwable e) {
                String message = String.valueOf(e.getMessage());
                String prefix = message.startsWith(context) ? "" : context + ": ";
                fail(prefix + message + System.lineSeparator() + scenario.dump(), e);
            }
        }

        private void step(Uniform2FuzzScenario scenario, int step, Event event, String context) {
            GroupSpec spec = scenario.spec();
            SubscribedTopicDescriber describer = scenario.describer();
            GroupFacts facts = new GroupFacts(spec, describer);
            GroupAssignment result = assignor.assign(spec, describer);
            assignments++;

            assertValidAssignment(spec, describer, result, context);
            countOptimalBalance(facts, spec, result);
            assertOrderIndependent(spec, describer, result, new Random(31L * scenario.seed() + step), context);
            assertStable(spec, describer, result, assignor, context);
            assertUnchangedMembersKeepTheirMaps(spec, result, context);
            assertWaysOfCountingAgree(spec, describer, context);

            int movedNow = revocations(spec, result);
            moved += movedNow;
            assertMovementBounds(facts, result, event, movedNow, context);
            assertMinimalMovement(facts, spec, movedNow, context);
            if (REFERENCE) {
                compareWithReference(spec, describer);
            }
            scenario.apply(result);
        }

        /**
         * With several subscriptions, the balance property only rules out single moves of an
         * extra partition to a subscriber at least two below. The exact optimum, the sizes which
         * are least majorized, also rules out chains: an extra partition moving from a member to a
         * subscriber of its topic, which gives one of another topic to a third member, and so on,
         * to a member at least two below the first. Counts the assignments with such a chain.
         */
        private void countOptimalBalance(GroupFacts facts, GroupSpec spec, GroupAssignment result) {
            if (facts.type == SubscriptionType.HOMOGENEOUS) {
                return;
            }
            heterogeneousAssignments++;
            if (hasImprovingChain(facts, spec, result)) {
                notOptimallyBalanced++;
            }
        }

        /**
         * The members, the topics of their subscriptions and the entries of their current
         * assignments given in a random order produce the same assignment. The scenario hands
         * them out in ascending order.
         */
        private void assertOrderIndependent(
            GroupSpec spec,
            SubscribedTopicDescriber describer,
            GroupAssignment result,
            Random random,
            String context
        ) {
            List<String> ids = new ArrayList<>(spec.memberIds());
            Collections.shuffle(ids, random);
            var shuffledSpec = new GroupSpecFixture();
            for (String id : ids) {
                shuffledSpec.withMember(id, shuffled(subscribedTopicIds(spec, id), random),
                    shuffled(spec.memberAssignment(id).partitions(), random));
            }
            GroupAssignment shuffledResult = assignor.assign(shuffledSpec.build(), describer);
            assertEquals(result, shuffledResult,
                context + ": the order of the members, topics or partitions changed the assignment");
        }

        /**
         * A member whose partitions do not change gets back the very map it had, whatever the
         * other members get, so that the coordinator sees it as unchanged.
         */
        private void assertUnchangedMembersKeepTheirMaps(GroupSpec spec, GroupAssignment result, String context) {
            for (String id : spec.memberIds()) {
                Map<Uuid, Set<Integer>> current = spec.memberAssignment(id).partitions();
                Map<Uuid, Set<Integer>> partitions = result.members().get(id).partitions();
                if (partitions.equals(current)) {
                    assertSame(current, partitions, context + ": " + id + " keeps its partitions in a new map");
                }
            }
        }

        /**
         * The four ways of counting of the balance step give the same shares.
         */
        private void assertWaysOfCountingAgree(GroupSpec spec, SubscribedTopicDescriber describer, String context) {
            List<String> ways = sharesPerWayOfCounting(spec, describer);
            for (int way = 1; way < ways.size(); way++) {
                assertEquals(ways.get(0), ways.get(way), context + ": the ways of counting of the balance step disagree");
            }
        }

        /**
         * With a single subscription, the partitions moved among the members present both before
         * and after a single leave or join are at most the extra partitions of the group. A leave
         * only frees the partitions of the leaver and a join only takes what the joiner is owed,
         * except that the change of shares can leave the other members uneven, and levelling
         * them costs at most one move per extra partition of the group.
         */
        private void assertMovementBounds(
            GroupFacts facts,
            GroupAssignment result,
            Event event,
            int movedNow,
            String context
        ) {
            if (facts.type != SubscriptionType.HOMOGENEOUS) {
                return;
            }
            if (event.kind() == Kind.LEAVE) {
                int bound = facts.extraPartitions();
                assertTrue(movedNow <= bound, context + ": a single leave moved " + movedNow
                    + " partitions among the remaining members, more than the " + bound + " extra partitions");
            } else if (event.kind() == Kind.JOIN) {
                int intake = assignmentSize(result, event.members().get(0));
                int bound = facts.extraPartitions();
                assertTrue(movedNow - intake <= bound, context + ": a single join moved " + movedNow
                    + " partitions for an intake of " + intake + ", more than the " + bound + " extra partitions beyond it");
            }
        }

        /**
         * For tiny groups, at most one partition more than the fewest moves of any assignment
         * with the properties. The tolerance of one covers ties settled without looking ahead:
         * the keep step settles its conflicts by the sizes over the topics already seen, and the
         * balance step moves one extra partition at a time, never along a chain through a third
         * member which could make a move free.
         */
        private void assertMinimalMovement(GroupFacts facts, GroupSpec spec, int movedNow, String context) {
            Integer minimum = MovementOracle.minimalMovement(facts, spec);
            if (minimum == null) {
                return;
            }
            oracleChecks++;
            assertTrue(movedNow >= minimum, context + ": " + movedNow + " partitions moved but the oracle needs " + minimum);
            assertTrue(movedNow - minimum <= 1, context + ": " + movedNow + " partitions moved where " + minimum + " suffice");
            if (movedNow > minimum) {
                if (facts.type == SubscriptionType.HOMOGENEOUS) {
                    oracleExcessHomogeneous++;
                } else {
                    oracleExcessHeterogeneous++;
                }
            }
        }

        /**
         * Runs the {@link UniformAssignor} on the same input and accumulates its moved partitions
         * for the summary line. Nothing is asserted: it is only a point of comparison, and its
         * failures are counted rather than reported.
         */
        private void compareWithReference(GroupSpec spec, SubscribedTopicDescriber describer) {
            try {
                GroupAssignment referenceResult = reference.assign(spec, describer);
                referenceMoved += revocations(spec, referenceResult);
            } catch (RuntimeException e) {
                referenceFailures++;
            }
        }

        String summary() {
            String summary = String.format("uniform2 fuzz: %d assignments (%d seeds, 1 initial + %d events each), "
                    + "moved partitions=%d, oracle checks=%d (one move above the minimum: homogeneous=%d heterogeneous=%d)",
                assignments, SEED == null ? SEEDS : 1, EVENTS, moved, oracleChecks, oracleExcessHomogeneous,
                oracleExcessHeterogeneous);
            summary += String.format(", heterogeneous assignments=%d (not the exact optimum of the balance=%d)",
                heterogeneousAssignments, notOptimallyBalanced);
            if (REFERENCE) {
                summary += String.format(", uniform: moved partitions=%d failures=%d",
                    referenceMoved, referenceFailures);
            }
            return summary;
        }
    }

    /**
     * What the checks of a step need to know of the group: its subscription type, and the
     * subscribed topics that exist, in topic id order, with their numbers of subscribers and of
     * partitions.
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
                int partitionCount = describer.numPartitions(topicId);
                if (partitionCount >= 0) {
                    topics.add(topicId);
                    subscribers.put(topicId, count);
                    partitions.put(topicId, partitionCount);
                }
            });
        }

        /**
         * @return The number of extra partitions of the group: for every subscribed topic that
         *         exists, its partition count modulo its number of subscribers.
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
     * @return True if an extra partition can move along a chain of members, each giving one to the
     *         next, from a member to another at least two below it: a breadth first search from
     *         every member over the moves of an extra partition to a subscriber of its topic
     *         without one.
     */
    private static boolean hasImprovingChain(GroupFacts facts, GroupSpec spec, GroupAssignment result) {
        List<String> ids = new ArrayList<>(spec.memberIds());
        List<Uuid> topics = facts.topics;
        int[] sizes = new int[ids.size()];
        boolean[][] extra = new boolean[ids.size()][topics.size()];
        boolean[][] subscribes = new boolean[ids.size()][topics.size()];
        for (int t = 0; t < topics.size(); t++) {
            Uuid topicId = topics.get(t);
            int base = facts.partitions.get(topicId) / facts.subscribers.get(topicId);
            for (int m = 0; m < ids.size(); m++) {
                subscribes[m][t] = subscribedTopicIds(spec, ids.get(m)).contains(topicId);
                extra[m][t] = result.members().get(ids.get(m)).partitions().getOrDefault(topicId, Set.of()).size() > base;
            }
        }
        for (int m = 0; m < ids.size(); m++) {
            sizes[m] = assignmentSize(result, ids.get(m));
        }
        for (int start = 0; start < ids.size(); start++) {
            boolean[] reached = new boolean[ids.size()];
            ArrayDeque<Integer> queue = new ArrayDeque<>();
            reached[start] = true;
            queue.add(start);
            while (!queue.isEmpty()) {
                int giver = queue.poll();
                for (int receiver = 0; receiver < ids.size(); receiver++) {
                    if (reached[receiver]) {
                        continue;
                    }
                    for (int t = 0; t < topics.size(); t++) {
                        if (extra[giver][t] && subscribes[receiver][t] && !extra[receiver][t]) {
                            if (sizes[receiver] <= sizes[start] - 2) {
                                return true;
                            }
                            reached[receiver] = true;
                            queue.add(receiver);
                            break;
                        }
                    }
                }
            }
        }
        return false;
    }

    private static Set<Uuid> subscribedTopicIds(GroupSpec spec, String memberId) {
        return spec.memberSubscription(memberId).subscribedTopicIds();
    }

    /**
     * @return The elements in a random order.
     */
    private static <T> Set<T> shuffled(Set<T> elements, Random random) {
        List<T> list = new ArrayList<>(elements);
        Collections.shuffle(list, random);
        return new LinkedHashSet<>(list);
    }

    /**
     * @return The assignment with its topics and the partitions of every topic in a random order.
     */
    private static Map<Uuid, Set<Integer>> shuffled(Map<Uuid, Set<Integer>> partitions, Random random) {
        Map<Uuid, Set<Integer>> result = new LinkedHashMap<>();
        for (Uuid topicId : shuffled(partitions.keySet(), random)) {
            result.put(topicId, shuffled(partitions.get(topicId), random));
        }
        return result;
    }

    /**
     * Brute force computation of the smallest number of partitions that an assignment with the
     * spread and balance properties must move from the current assignment.
     *
     * <p>The spread fixes the share of every subscriber of a topic to the base partitions or one
     * more, so an assignment is characterized, up to partition ids, by which subscribers get the
     * extra partitions of each topic. For given shares, the fewest moves are made when every
     * member keeps as many of its current partitions of each topic as its share allows, which is
     * always possible without racks. The oracle enumerates all the ways to hand out the extra
     * partitions, keeps the balanced ones and takes the cheapest. Current partitions of topics a
     * member is not subscribed to, of topics that do not exist, or beyond the partition count, are
     * lost whatever the shares. Balance means that no extra partition could move to a subscriber at
     * least two below its owner, which with a single subscription is all sizes within one of each
     * other.
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
         * Per topic, its base partitions.
         */
        private final int[] base;

        /**
         * Per topic, its extra partitions.
         */
        private final int[] extras;

        /**
         * Per topic and member, the current partitions of the member that its share can keep.
         */
        private final int[][] current;

        /**
         * The current partitions lost whatever the shares.
         */
        private final int lost;

        /**
         * Per topic, the members getting one of its extra partitions in the search, as a bit mask.
         */
        private final int[] extraMasks;

        /**
         * Per member, its size from the topics handed out so far in the search.
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
            if (!ORACLE || memberCount == 0 || memberCount > ORACLE_MAX_MEMBERS || facts.topics.size() > ORACLE_MAX_TOPICS) {
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
            base = new int[topicCount];
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
                    for (int partition : spec.memberAssignment(ids.get(m)).partitions().getOrDefault(topicId, Set.of())) {
                        if (subscribed && partition < partitions) {
                            current[t][m]++;
                        } else {
                            lostPartitions++;
                        }
                    }
                }
                int subscriberCount = Integer.bitCount(subscribers[t]);
                base[t] = partitions / subscriberCount;
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
         * Tries every way to hand out the extra partitions of the topics from {@code t} on.
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
                        int share = base[t] + ((mask >> m) & 1);
                        sizes[m] += share;
                        topicMoves += Math.max(0, current[t][m] - share);
                    }
                }
                search(t + 1, moves + topicMoves);
                for (int m = 0; m < memberCount; m++) {
                    if ((subscribers[t] & (1 << m)) != 0) {
                        sizes[m] -= base[t] + ((mask >> m) & 1);
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
}
