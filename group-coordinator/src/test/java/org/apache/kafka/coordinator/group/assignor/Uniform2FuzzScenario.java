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
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.assignor.uniform2.GroupSpecFixture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A random consumer group and cluster for the uniform2 fuzzer, with the events that change them.
 *
 * <p>A scenario is entirely determined by its seed. It has topics, each with a number of
 * partitions, cohorts, which are the subscriptions that joining members copy, and members with a
 * subscription and the partitions they currently hold. {@link #mutate()} applies one random event:
 * members join or leave, alone or several at once, a topic gains partitions, a cohort subscribes to
 * or drops a topic, a member toggles a topic of its own. Other events make the input stale, as the
 * coordinator can hand it to the assignor: a topic is deleted while members still subscribe to it
 * and hold its partitions, a deleted topic is created again under a new id, a topic has fewer
 * partitions than the members hold, a member has an empty subscription. The current partitions are
 * only changed through {@link #apply(GroupAssignment)}, so the events are independent of the
 * assignor under test.
 *
 * <p>Scenarios come in three sizes: tiny groups where the edge cases live, medium groups, and large
 * skewed groups with many single partition topics and a few large ones, more than 64 topics for some
 * of them.
 *
 * <p>{@link #dump()} prints the whole scenario, so that a failure can be reproduced in a unit
 * test from the seed, the step and the dump alone.
 */
final class Uniform2FuzzScenario {
    /**
     * How big the group and the topics are.
     */
    enum Size {
        /**
         * One to three members and topics of one to four partitions: the edge cases.
         */
        TINY,

        /**
         * Up to twelve members and six topics of up to twenty partitions.
         */
        MEDIUM,

        /**
         * Five to thirty members, many single partition topics and a few large ones: 8 to 24
         * topics, or 64 to 96 for a quarter of them.
         */
        LARGE
    }

    /**
     * The kinds of events.
     */
    enum Kind {
        /**
         * The initial assignment.
         */
        INIT,

        /**
         * A single member joins.
         */
        JOIN,

        /**
         * A single member leaves.
         */
        LEAVE,

        /**
         * Several members join, or nobody when the group is full.
         */
        JOIN_MANY,

        /**
         * Several members leave, or nobody when the group is empty.
         */
        LEAVE_MANY,

        /**
         * A topic gains partitions.
         */
        GROW_TOPIC,

        /**
         * A cohort subscribes to one more topic, possibly a new one.
         */
        ADD_TOPIC_TO_COHORT,

        /**
         * A cohort drops one of its topics.
         */
        REMOVE_TOPIC_FROM_COHORT,

        /**
         * A single member subscribes to a topic, or drops one.
         */
        TOGGLE_TOPIC,

        /**
         * A topic is deleted. The members keep subscribing to it and holding its partitions until
         * the next assignment.
         */
        DELETE_TOPIC,

        /**
         * A deleted topic is created again, under a new id: the subscriptions follow the new id,
         * and the members still hold the partitions of the old one.
         */
        RECREATE_TOPIC,

        /**
         * A topic has fewer partitions than before: the members still hold those beyond its
         * partition count.
         */
        SHRINK_TOPIC,

        /**
         * A single member drops all its topics.
         */
        EMPTY_SUBSCRIPTION
    }

    /**
     * An event, with the ids of the members it added or removed. A join or leave of exactly one
     * member has the kind {@link Kind#JOIN} or {@link Kind#LEAVE}, whatever the number asked; one
     * adding or removing nobody, because the group is full or empty, has the kind
     * {@link Kind#JOIN_MANY} or {@link Kind#LEAVE_MANY} and says so.
     */
    record Event(Kind kind, String description, List<String> members) {
        /**
         * The event of the first assignment, before any change.
         */
        static final Event INIT = new Event(Kind.INIT, "init", List.of());

        @Override
        public String toString() {
            return description;
        }
    }

    /**
     * The kinds of the random events, each as many times as its weight.
     */
    private static final Kind[] EVENT_WEIGHTS = {
        Kind.JOIN, Kind.JOIN, Kind.JOIN,
        Kind.LEAVE, Kind.LEAVE, Kind.LEAVE,
        Kind.JOIN_MANY, Kind.LEAVE_MANY,
        Kind.GROW_TOPIC, Kind.GROW_TOPIC,
        Kind.ADD_TOPIC_TO_COHORT, Kind.REMOVE_TOPIC_FROM_COHORT, Kind.TOGGLE_TOPIC,
        Kind.DELETE_TOPIC, Kind.RECREATE_TOPIC, Kind.SHRINK_TOPIC, Kind.EMPTY_SUBSCRIPTION
    };
    /**
     * The most members of a group, which keeps long event sequences bounded.
     */
    private static final int MAX_MEMBERS = 64;

    /**
     * The most topics created, deleted ones included, which keeps long event sequences bounded.
     */
    private static final int MAX_TOPICS = 128;

    /**
     * The most partitions of a topic, which keeps long event sequences bounded.
     */
    private static final int MAX_PARTITIONS = 256;

    /**
     * A topic of the cluster. A deleted topic is kept, so that the members can still subscribe to
     * it and hold its partitions, and so that it can be re-created under a new id.
     */
    private static final class Topic {
        /**
         * The name of the topic in the dump and in the events.
         */
        final String name;

        /**
         * The topic id.
         */
        final Uuid id;

        /**
         * The number of partitions of the topic.
         */
        private int partitionCount;

        /**
         * Whether the topic is deleted: the describer does not know it any more.
         */
        private boolean deleted;

        Topic(String name, Uuid id, int partitionCount) {
            this.name = name;
            this.id = id;
            this.partitionCount = partitionCount;
        }

        int partitions() {
            return partitionCount;
        }

        boolean deleted() {
            return deleted;
        }

        /**
         * Changes the partition count by the delta, positive or negative.
         */
        void resize(int delta) {
            partitionCount += delta;
        }

        void delete() {
            deleted = true;
        }
    }

    /**
     * A member of the group.
     */
    private static final class Member {
        /**
         * The member id.
         */
        final String id;

        /**
         * The cohort of the member, whose changes of subscription it follows. Toggling a topic or
         * emptying the subscription of the member only changes its own.
         */
        final int cohort;

        /**
         * The topics the member subscribes to.
         */
        final Set<Uuid> subscription = new HashSet<>();

        /**
         * The partitions the member holds, the last assignment applied.
         */
        Map<Uuid, Set<Integer>> current = new TreeMap<>();

        Member(String id, int cohort) {
            this.id = id;
            this.cohort = cohort;
        }
    }

    /**
     * A snapshot of the topics, as the assignor sees them.
     */
    private record Describer(Map<Uuid, Integer> partitionCounts) implements SubscribedTopicDescriber {
        @Override
        public int numPartitions(Uuid topicId) {
            return partitionCounts.getOrDefault(topicId, -1);
        }

        @Override
        public Set<String> racksForPartition(Uuid topicId, int partition) {
            return Set.of();
        }
    }

    /**
     * The seed of the scenario.
     */
    private final long seed;

    /**
     * The source of every random choice, from the seed.
     */
    private final Random random;

    /**
     * The size of the scenario.
     */
    private final Size size;

    /**
     * The topics, deleted ones included, in creation order.
     */
    private final List<Topic> topics = new ArrayList<>();

    /**
     * The topics, by id.
     */
    private final Map<Uuid, Topic> topicsById = new HashMap<>();

    /**
     * Per cohort, its subscription. Members copy it when they join and follow its changes.
     */
    private final List<Set<Uuid>> cohorts = new ArrayList<>();

    /**
     * The members, by id.
     */
    private final TreeMap<String, Member> members = new TreeMap<>();

    Uniform2FuzzScenario(long seed) {
        this.seed = seed;
        this.random = new Random(seed);
        this.size = pick(Size.TINY, Size.TINY, Size.TINY, Size.TINY, Size.TINY, Size.TINY, Size.TINY,
            Size.MEDIUM, Size.MEDIUM, Size.MEDIUM, Size.MEDIUM, Size.MEDIUM, Size.MEDIUM, Size.MEDIUM, Size.MEDIUM,
            Size.MEDIUM, Size.MEDIUM, Size.LARGE, Size.LARGE, Size.LARGE);
        createTopics();
        createCohorts();
        int memberCount = switch (size) {
            case TINY -> 1 + random.nextInt(3);
            case MEDIUM -> 1 + random.nextInt(12);
            case LARGE -> 5 + random.nextInt(26);
        };
        for (int i = 0; i < memberCount; i++) {
            addMember(i % cohorts.size());
        }
    }

    long seed() {
        return seed;
    }

    Size size() {
        return size;
    }

    int memberCount() {
        return members.size();
    }

    /**
     * @return The group spec as the assignor receives it: the members in id order, their
     *         subscription with the topics in ascending order, and their current partitions with
     *         the topics and partitions in ascending order, so that the input does not depend on
     *         hash iteration orders and a seed reproduces exactly. The fuzzer shuffles these orders
     *         itself to check that they do not matter. The current partitions are the maps of the
     *         scenario, which the assignor must not change.
     */
    GroupSpec spec() {
        var spec = new GroupSpecFixture();
        for (Member member : members.values()) {
            spec.withMember(member.id, new TreeSet<>(member.subscription), member.current);
        }
        return spec.build();
    }

    /**
     * @return A snapshot of the topics that exist: their partition counts.
     */
    SubscribedTopicDescriber describer() {
        Map<Uuid, Integer> snapshot = new HashMap<>();
        for (Topic topic : topics) {
            if (!topic.deleted()) {
                snapshot.put(topic.id, topic.partitions());
            }
        }
        return new Describer(snapshot);
    }

    /**
     * Makes the assignment the current one. Members absent from it hold nothing.
     */
    void apply(GroupAssignment assignment) {
        for (Member member : members.values()) {
            MemberAssignment memberAssignment = assignment.members().get(member.id);
            member.current = memberAssignment == null ? new TreeMap<>() : copy(memberAssignment.partitions());
        }
    }

    /**
     * Applies one random event.
     *
     * @return The event.
     */
    Event mutate() {
        Kind kind = EVENT_WEIGHTS[random.nextInt(EVENT_WEIGHTS.length)];
        boolean tiny = size == Size.TINY;
        return switch (kind) {
            case JOIN -> join(1);
            case JOIN_MANY -> join(2 + random.nextInt(tiny ? 1 : 3));
            case LEAVE -> leave(1);
            case LEAVE_MANY -> leave(2 + random.nextInt(tiny ? 1 : 3));
            case GROW_TOPIC -> growTopic();
            case ADD_TOPIC_TO_COHORT -> addTopicToCohort();
            case REMOVE_TOPIC_FROM_COHORT -> removeTopicFromCohort();
            case TOGGLE_TOPIC -> toggleTopic();
            case DELETE_TOPIC -> deleteTopic();
            case RECREATE_TOPIC -> recreateTopic();
            case SHRINK_TOPIC -> shrinkTopic();
            case EMPTY_SUBSCRIPTION -> emptySubscription();
            default -> throw new IllegalStateException("Unexpected event kind " + kind);
        };
    }

    /**
     * @return The whole scenario: topics with their partitions, cohorts, and members with their
     *         subscription and current partitions.
     */
    String dump() {
        StringBuilder out = new StringBuilder();
        out.append("scenario seed=").append(seed).append(" size=").append(size)
            .append(" members=").append(members.size()).append(" topics=").append(topics.size()).append('\n');
        for (Topic topic : topics) {
            out.append("  topic ").append(topic.name).append(" id=").append(topic.id)
                .append(" partitions=").append(topic.partitions()).append(topic.deleted() ? " deleted" : "").append('\n');
        }
        for (int c = 0; c < cohorts.size(); c++) {
            out.append("  cohort ").append(c).append(" topics=").append(topicNames(cohorts.get(c))).append('\n');
        }
        for (Member member : members.values()) {
            out.append("  member ").append(member.id)
                .append(" cohort=").append(member.cohort).append(" topics=").append(topicNames(member.subscription))
                .append(" current=").append(currentPartitions(member.current)).append('\n');
        }
        return out.toString();
    }

    @Override
    public String toString() {
        return dump();
    }

    // Construction.

    private void createTopics() {
        int topicCount = switch (size) {
            case TINY -> 1 + random.nextInt(3);
            case MEDIUM -> 1 + random.nextInt(6);
            case LARGE -> random.nextInt(4) == 0 ? 64 + random.nextInt(33) : 8 + random.nextInt(17);
        };
        int largeTopics = size == Size.LARGE ? 1 + random.nextInt(3) : 0;
        for (int i = 0; i < topicCount; i++) {
            createTopic(i < largeTopics ? 20 + random.nextInt(41) : initialPartitions());
        }
    }

    private int initialPartitions() {
        return switch (size) {
            case TINY -> 1 + random.nextInt(4);
            case MEDIUM -> 1 + random.nextInt(20);
            case LARGE -> 1;
        };
    }

    private Topic createTopic(int partitions) {
        Topic topic = new Topic(
            "T" + topics.size(),
            new Uuid(random.nextLong(), random.nextLong()),
            partitions
        );
        topics.add(topic);
        topicsById.put(topic.id, topic);
        return topic;
    }

    /**
     * Creates one cohort, or two or three whose subscriptions overlap, nest or are disjoint.
     */
    private void createCohorts() {
        boolean homogeneous = random.nextBoolean();
        int cohortCount = homogeneous ? 1 : 2 + random.nextInt(2);
        Set<Uuid> first = homogeneous || random.nextBoolean() ? allTopics() : randomSubscription(allTopics());
        cohorts.add(first);
        for (int c = 1; c < cohortCount; c++) {
            Set<Uuid> others = allTopics();
            others.removeAll(first);
            cohorts.add(switch (random.nextInt(3)) {
                case 0 -> randomSubscription(allTopics());
                case 1 -> randomSubscription(first);
                default -> others.isEmpty() ? randomSubscription(allTopics()) : randomSubscription(others);
            });
        }
    }

    private Set<Uuid> allTopics() {
        Set<Uuid> all = new HashSet<>();
        for (Topic topic : topics) {
            all.add(topic.id);
        }
        return all;
    }

    /**
     * @return A random non-empty subset of the topics.
     */
    private Set<Uuid> randomSubscription(Set<Uuid> from) {
        List<Uuid> shuffled = sorted(from);
        Collections.shuffle(shuffled, random);
        return new HashSet<>(shuffled.subList(0, 1 + random.nextInt(shuffled.size())));
    }

    private Member addMember(int cohort) {
        Member member = new Member(newMemberId(), cohort);
        member.subscription.addAll(cohorts.get(cohort));
        members.put(member.id, member);
        return member;
    }

    /**
     * @return An unused member id. Ids are drawn at random so that a joining member sorts
     *         anywhere among the existing ones.
     */
    private String newMemberId() {
        int bound = 100;
        while (true) {
            String id = String.format(Locale.ROOT, "m%02d", random.nextInt(bound));
            if (!members.containsKey(id)) {
                return id;
            }
            bound *= 10;
        }
    }

    // Events.

    private Event join(int count) {
        List<String> joined = new ArrayList<>();
        for (int i = 0; i < count && members.size() < MAX_MEMBERS; i++) {
            joined.add(addMember(random.nextInt(cohorts.size())).id);
        }
        if (joined.isEmpty()) {
            return new Event(Kind.JOIN_MANY, "nobody joins, the group is full", joined);
        }
        Kind kind = joined.size() == 1 ? Kind.JOIN : Kind.JOIN_MANY;
        return new Event(kind, "join " + joined, joined);
    }

    /**
     * Removes members, keeping at least one, so that the group does not restart from scratch.
     */
    private Event leave(int count) {
        List<String> left = new ArrayList<>();
        for (int i = 0; i < count && members.size() > 1; i++) {
            List<String> ids = new ArrayList<>(members.keySet());
            String id = ids.get(random.nextInt(ids.size()));
            members.remove(id);
            left.add(id);
        }
        if (left.isEmpty()) {
            return new Event(Kind.LEAVE_MANY, "nobody leaves, a single member is left", left);
        }
        Kind kind = left.size() == 1 ? Kind.LEAVE : Kind.LEAVE_MANY;
        return new Event(kind, "leave " + left, left);
    }

    private Event growTopic() {
        List<Topic> candidates = new ArrayList<>();
        for (Topic topic : topics) {
            if (!topic.deleted() && topic.partitions() < MAX_PARTITIONS) {
                candidates.add(topic);
            }
        }
        if (candidates.isEmpty()) {
            return new Event(Kind.GROW_TOPIC, "grow nothing", List.of());
        }
        Topic topic = candidates.get(random.nextInt(candidates.size()));
        int added = switch (size) {
            case TINY -> 1 + random.nextInt(2);
            case MEDIUM -> 1 + random.nextInt(10);
            case LARGE -> 1 + random.nextInt(20);
        };
        added = Math.min(added, MAX_PARTITIONS - topic.partitions());
        topic.resize(added);
        return new Event(Kind.GROW_TOPIC, "grow " + topic.name + " by " + added + " to " + topic.partitions(), List.of());
    }

    private Event addTopicToCohort() {
        int cohort = random.nextInt(cohorts.size());
        Set<Uuid> subscription = cohorts.get(cohort);
        List<Uuid> candidates = sorted(allTopics());
        candidates.removeAll(subscription);
        Topic topic;
        if (topics.size() < MAX_TOPICS && (candidates.isEmpty() || random.nextInt(3) == 0)) {
            topic = createTopic(initialPartitions());
        } else if (candidates.isEmpty()) {
            return new Event(Kind.ADD_TOPIC_TO_COHORT, "cohort " + cohort + " adds nothing", List.of());
        } else {
            topic = topicsById.get(candidates.get(random.nextInt(candidates.size())));
        }
        subscription.add(topic.id);
        for (Member member : members.values()) {
            if (member.cohort == cohort) {
                member.subscription.add(topic.id);
            }
        }
        return new Event(Kind.ADD_TOPIC_TO_COHORT, "cohort " + cohort + " adds " + topic.name, List.of());
    }

    /**
     * Drops a topic from a cohort keeping at least one. Members of the cohort follow, unless
     * that would leave them without any topic.
     */
    private Event removeTopicFromCohort() {
        int cohort = random.nextInt(cohorts.size());
        Set<Uuid> subscription = cohorts.get(cohort);
        if (subscription.size() <= 1) {
            return new Event(Kind.REMOVE_TOPIC_FROM_COHORT, "cohort " + cohort + " drops nothing", List.of());
        }
        List<Uuid> candidates = sorted(subscription);
        Uuid topicId = candidates.get(random.nextInt(candidates.size()));
        subscription.remove(topicId);
        for (Member member : members.values()) {
            if (member.cohort == cohort && member.subscription.size() > 1) {
                member.subscription.remove(topicId);
            }
        }
        return new Event(Kind.REMOVE_TOPIC_FROM_COHORT, "cohort " + cohort + " drops " + topicsById.get(topicId).name, List.of());
    }

    /**
     * A member subscribes to a topic it does not have, or drops one it has, keeping at least one.
     */
    private Event toggleTopic() {
        if (members.isEmpty()) {
            return new Event(Kind.TOGGLE_TOPIC, "nobody toggles", List.of());
        }
        Member member = randomMember();
        Topic topic = topics.get(random.nextInt(topics.size()));
        if (!member.subscription.contains(topic.id)) {
            member.subscription.add(topic.id);
            return new Event(Kind.TOGGLE_TOPIC, member.id + " subscribes to " + topic.name, List.of());
        }
        if (member.subscription.size() == 1) {
            return new Event(Kind.TOGGLE_TOPIC, member.id + " keeps " + topic.name, List.of());
        }
        member.subscription.remove(topic.id);
        return new Event(Kind.TOGGLE_TOPIC, member.id + " drops " + topic.name, List.of());
    }

    /**
     * Deletes a topic, keeping at least one topic that exists.
     */
    private Event deleteTopic() {
        List<Topic> live = liveTopics();
        if (live.size() <= 1) {
            return new Event(Kind.DELETE_TOPIC, "delete nothing", List.of());
        }
        Topic topic = live.get(random.nextInt(live.size()));
        topic.delete();
        return new Event(Kind.DELETE_TOPIC, "delete " + topic.name, List.of());
    }

    /**
     * Creates a deleted topic again under a new id, which replaces the old one in every
     * subscription.
     */
    private Event recreateTopic() {
        List<Topic> deleted = new ArrayList<>();
        for (Topic topic : topics) {
            if (topic.deleted() && subscribed(topic.id)) {
                deleted.add(topic);
            }
        }
        if (deleted.isEmpty() || topics.size() >= MAX_TOPICS) {
            return new Event(Kind.RECREATE_TOPIC, "re-create nothing", List.of());
        }
        Topic old = deleted.get(random.nextInt(deleted.size()));
        Topic topic = createTopic(initialPartitions());
        for (Set<Uuid> subscription : cohorts) {
            if (subscription.remove(old.id)) {
                subscription.add(topic.id);
            }
        }
        for (Member member : members.values()) {
            if (member.subscription.remove(old.id)) {
                member.subscription.add(topic.id);
            }
        }
        return new Event(Kind.RECREATE_TOPIC, "re-create " + old.name + " as " + topic.name, List.of());
    }

    /**
     * Removes partitions from a topic, keeping at least one.
     */
    private Event shrinkTopic() {
        List<Topic> candidates = new ArrayList<>();
        for (Topic topic : liveTopics()) {
            if (topic.partitions() > 1) {
                candidates.add(topic);
            }
        }
        if (candidates.isEmpty()) {
            return new Event(Kind.SHRINK_TOPIC, "shrink nothing", List.of());
        }
        Topic topic = candidates.get(random.nextInt(candidates.size()));
        int removed = 1 + random.nextInt(topic.partitions() - 1);
        topic.resize(-removed);
        return new Event(Kind.SHRINK_TOPIC, "shrink " + topic.name + " by " + removed + " to " + topic.partitions(), List.of());
    }

    /**
     * A member drops all its topics. It stays in its cohort, whose changes it follows again.
     */
    private Event emptySubscription() {
        if (members.isEmpty()) {
            return new Event(Kind.EMPTY_SUBSCRIPTION, "nobody empties its subscription", List.of());
        }
        Member member = randomMember();
        member.subscription.clear();
        return new Event(Kind.EMPTY_SUBSCRIPTION, member.id + " drops all its topics", List.of());
    }

    private List<Topic> liveTopics() {
        List<Topic> live = new ArrayList<>();
        for (Topic topic : topics) {
            if (!topic.deleted()) {
                live.add(topic);
            }
        }
        return live;
    }

    /**
     * @return True if a cohort or a member subscribes to the topic.
     */
    private boolean subscribed(Uuid topicId) {
        for (Set<Uuid> subscription : cohorts) {
            if (subscription.contains(topicId)) {
                return true;
            }
        }
        for (Member member : members.values()) {
            if (member.subscription.contains(topicId)) {
                return true;
            }
        }
        return false;
    }

    private Member randomMember() {
        List<String> ids = new ArrayList<>(members.keySet());
        return members.get(ids.get(random.nextInt(ids.size())));
    }

    // Helpers.

    @SafeVarargs
    private <T> T pick(T... choices) {
        return choices[random.nextInt(choices.length)];
    }

    private static List<Uuid> sorted(Set<Uuid> topicIds) {
        return new ArrayList<>(new TreeSet<>(topicIds));
    }

    /**
     * @return A copy of the partitions with the topics and the partitions in ascending order.
     */
    private static Map<Uuid, Set<Integer>> copy(Map<Uuid, Set<Integer>> partitions) {
        Map<Uuid, Set<Integer>> copy = new TreeMap<>();
        partitions.forEach((topicId, topicPartitions) -> copy.put(topicId, new TreeSet<>(topicPartitions)));
        return copy;
    }

    private List<String> topicNames(Set<Uuid> topicIds) {
        List<String> names = new ArrayList<>();
        for (Topic topic : topics) {
            if (topicIds.contains(topic.id)) {
                names.add(topic.name);
            }
        }
        return names;
    }

    private String currentPartitions(Map<Uuid, Set<Integer>> current) {
        StringBuilder out = new StringBuilder("{");
        for (Topic topic : topics) {
            Set<Integer> partitions = current.get(topic.id);
            if (partitions != null) {
                out.append(out.length() == 1 ? "" : ", ").append(topic.name).append('=').append(new TreeSet<>(partitions));
            }
        }
        return out.append('}').toString();
    }
}
