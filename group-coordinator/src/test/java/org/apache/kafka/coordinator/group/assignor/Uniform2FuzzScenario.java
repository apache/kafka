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
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.assignor.uniform2.GroupSpecFixture;
import org.apache.kafka.coordinator.group.modern.SubscribedTopicDescriberImpl;
import org.apache.kafka.coordinator.group.modern.TopicIds;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
 * <p>A scenario is entirely determined by its seed. It has topics, each with a name, an id and a
 * number of partitions, cohorts, which are the subscriptions that joining members copy, and members
 * with a subscription and the partitions they currently hold. {@link #mutate()} applies one random
 * event. Members join or leave, alone or several at once, a topic gains partitions, a cohort
 * subscribes to or drops a topic, or a member toggles a topic of its own. Other events make the
 * input stale. A topic is deleted while members still subscribe to it and hold its partitions, a
 * topic is created again under a new id while members hold the partitions of the old one, or a
 * member has an empty subscription. Some groups start from a random assignment instead of
 * nothing. The current partitions are only changed through {@link #apply(GroupAssignment)}, so
 * the events are independent of the assignor under test.
 * Several of these events can also happen between two assignments.
 *
 * <p>The subscriptions are topic names. The group spec gives them as the coordinator does, as
 * {@link TopicIds} resolved against a metadata image of the topics that exist, from which the
 * describer reads the partition counts. A subscribed name without a topic is then not among the
 * topic ids of the subscription, but its {@code size()} still counts it.
 *
 * <p>Scenarios come in three sizes: tiny groups where the edge cases live, medium groups, and large
 * skewed groups with many single partition topics and a few large ones, 64 topics or more for some
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
         * One to three members and topics of one to four partitions, where the edge cases are.
         */
        TINY,

        /**
         * Up to twelve members and six topics of up to twenty partitions.
         */
        MEDIUM,

        /**
         * Five to thirty members, with many single partition topics and a few large ones, 8 to 24
         * topics in all, or 64 to 96 for a quarter of these scenarios.
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
         * An event that changes nothing, for example a leave when a single member is left.
         */
        NOTHING,

        /**
         * A single member joins.
         */
        JOIN,

        /**
         * A single member leaves.
         */
        LEAVE,

        /**
         * Several members join.
         */
        JOIN_MANY,

        /**
         * Several members leave.
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
         * A topic is deleted. The members keep subscribing to its name and holding its partitions
         * until the next assignment.
         */
        DELETE_TOPIC,

        /**
         * A subscribed topic is created again with the same name and a new id, which the
         * subscriptions then resolve to. When the topic still existed, it is deleted at the same
         * time, and its members still hold the partitions of the old id until the next assignment.
         */
        RECREATE_TOPIC,

        /**
         * A single member drops all its topics.
         */
        EMPTY_SUBSCRIPTION,

        /**
         * Two or three of the events above, applied before the same assignment.
         */
        SEVERAL
    }

    /**
     * An event, with the ids of the members it added or removed. A join or leave of exactly one
     * member has the kind {@link Kind#JOIN} or {@link Kind#LEAVE}, whatever the number asked. Only
     * a join reads the members, for the intake of the joiner, since the bound of a leave does not
     * depend on who leaves.
     */
    record Event(Kind kind, String description, List<String> members) {
        /**
         * The event of the first assignment, before any change.
         */
        static final Event INIT = new Event(Kind.INIT, "init", List.of());

        /**
         * @return An event that changes nothing, with the description of what it did not do.
         */
        static Event nothing(String description) {
            return new Event(Kind.NOTHING, description, List.of());
        }

        @Override
        public String toString() {
            return description;
        }
    }

    /**
     * A topic of the cluster. A deleted topic is kept, so that the members can still hold its
     * partitions and the dump shows them.
     */
    private static final class Topic {
        /**
         * The name of the topic, which the subscriptions refer to.
         */
        final String name;

        /**
         * The topic id.
         */
        final Uuid id;

        /**
         * The number of partitions of the topic.
         */
        int partitions;

        /**
         * Whether the topic is deleted, so that the metadata image no longer has it.
         */
        boolean deleted;

        Topic(String name, Uuid id, int partitions) {
            this.name = name;
            this.id = id;
            this.partitions = partitions;
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
         * The names of the topics the member subscribes to, in name order.
         */
        final Set<String> subscription = new TreeSet<>();

        /**
         * The partitions the member holds, from the last assignment applied.
         */
        Map<Uuid, Set<Integer>> current = new TreeMap<>();

        Member(String id, int cohort) {
            this.id = id;
            this.cohort = cohort;
        }
    }

    /**
     * The weights of the sizes of the scenarios.
     */
    private static final Map<Size, Integer> SIZE_WEIGHTS = new EnumMap<>(Map.of(
        Size.TINY, 7,
        Size.MEDIUM, 10,
        Size.LARGE, 3
    ));

    /**
     * The weights of the kinds of the random events.
     */
    private static final Map<Kind, Integer> EVENT_WEIGHTS = new EnumMap<>(Map.ofEntries(
        Map.entry(Kind.JOIN, 3),
        Map.entry(Kind.LEAVE, 3),
        Map.entry(Kind.JOIN_MANY, 1),
        Map.entry(Kind.LEAVE_MANY, 1),
        Map.entry(Kind.GROW_TOPIC, 2),
        Map.entry(Kind.ADD_TOPIC_TO_COHORT, 1),
        Map.entry(Kind.REMOVE_TOPIC_FROM_COHORT, 1),
        Map.entry(Kind.TOGGLE_TOPIC, 1),
        Map.entry(Kind.DELETE_TOPIC, 1),
        Map.entry(Kind.RECREATE_TOPIC, 1),
        Map.entry(Kind.EMPTY_SUBSCRIPTION, 1),
        Map.entry(Kind.SEVERAL, 2)
    ));

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
     * Per topic name, the last topic created with it. The other topics with the name are deleted.
     */
    private final TreeMap<String, Topic> latestTopics = new TreeMap<>();

    /**
     * Per cohort, the names of the topics of its subscription, in name order. Members copy it when
     * they join and follow its changes.
     */
    private final List<Set<String>> cohorts = new ArrayList<>();

    /**
     * The members, by id.
     */
    private final TreeMap<String, Member> members = new TreeMap<>();

    /**
     * The metadata image of the topics that exist, updated after every event.
     */
    private CoordinatorMetadataImage image;

    Uniform2FuzzScenario(long seed) {
        this.seed = seed;
        this.random = new Random(seed);
        this.size = pick(SIZE_WEIGHTS);
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
        // A quarter of the groups start from an assignment not made by the assignor under test, as
        // after a change of assignor or of the subscriptions of the group.
        if (random.nextInt(4) == 0) {
            startFromRandomAssignment();
        }
        updateImage();
    }

    long seed() {
        return seed;
    }

    /**
     * Gives every partition of the topics to a random member subscribed to its topic, so that the
     * members start with assignments that are neither balanced nor sticky. A partition of a topic
     * nobody subscribes to is not given.
     */
    private void startFromRandomAssignment() {
        var assigned = new TreeMap<String, Map<Uuid, Set<Integer>>>();
        for (var topic : topics) {
            var subscribers = members.values().stream()
                .filter(member -> member.subscription.contains(topic.name))
                .toList();
            if (subscribers.isEmpty()) {
                continue;
            }
            for (int partition = 0; partition < topic.partitions; partition++) {
                var owner = subscribers.get(random.nextInt(subscribers.size()));
                assigned.computeIfAbsent(owner.id, id -> new TreeMap<>())
                    .computeIfAbsent(topic.id, id -> new TreeSet<>())
                    .add(partition);
            }
        }
        assigned.forEach((id, partitions) -> members.get(id).current = copy(partitions));
    }

    /**
     * @return The group spec as the assignor receives it. The members, the topics of their
     *         subscriptions and their current partitions are in ascending order, so that the input
     *         does not depend on hash iteration orders and a seed reproduces exactly. The current
     *         partitions are the maps of the scenario, and their sets cannot be changed.
     */
    GroupSpec spec() {
        var resolver = new TopicIds.CachedTopicResolver(image);
        var spec = new GroupSpecFixture();
        for (var member : members.values()) {
            spec.withMember(
                member.id,
                new TopicIds(new TreeSet<>(member.subscription), resolver),
                member.current
            );
        }
        return spec.build();
    }

    /**
     * @param random The source of the orders.
     * @return The same group spec as {@link #spec()}, with the members, the topics of their
     *         subscriptions and their current partitions in a random order.
     */
    GroupSpec shuffledSpec(Random random) {
        var resolver = new TopicIds.CachedTopicResolver(image);
        var shuffledMembers = new ArrayList<>(members.values());
        Collections.shuffle(shuffledMembers, random);
        var spec = new GroupSpecFixture();
        for (var member : shuffledMembers) {
            spec.withMember(
                member.id,
                new TopicIds(shuffled(member.subscription, random), resolver),
                shuffled(member.current, random)
            );
        }
        return spec.build();
    }

    /**
     * @return The describer of the topics that exist, from the metadata image of the group spec.
     */
    SubscribedTopicDescriber describer() {
        return new SubscribedTopicDescriberImpl(image);
    }

    /**
     * Makes the assignment the current one. Members absent from it hold nothing.
     */
    void apply(GroupAssignment assignment) {
        for (var member : members.values()) {
            var memberAssignment = assignment.members().get(member.id);
            member.current = memberAssignment == null
                ? new TreeMap<>()
                : copy(memberAssignment.partitions());
        }
    }

    /**
     * @return A key of the weights, with the probability of its weight over their sum.
     */
    private <T> T pick(Map<T, Integer> weights) {
        int remaining = random.nextInt(weights.values().stream().mapToInt(Integer::intValue).sum());
        for (var entry : weights.entrySet()) {
            remaining -= entry.getValue();
            if (remaining < 0) {
                return entry.getKey();
            }
        }
        throw new IllegalStateException("The weights are empty");
    }

    /**
     * Applies one random event.
     *
     * @return The event.
     */
    Event mutate() {
        var kind = pick(EVENT_WEIGHTS);
        var event = kind == Kind.SEVERAL ? several() : applyEvent(kind);
        updateImage();
        return event;
    }

    /**
     * Applies two or three events before the next assignment, since several things can change
     * between two assignments of the coordinator: members join or leave while others still hold
     * the partitions of a deleted topic, or change their subscription as a topic grows.
     */
    private Event several() {
        var descriptions = new ArrayList<String>();
        int count = 2 + random.nextInt(2);
        for (int i = 0; i < count; i++) {
            Kind kind;
            do {
                kind = pick(EVENT_WEIGHTS);
            } while (kind == Kind.SEVERAL);
            descriptions.add(applyEvent(kind).description());
        }
        return new Event(Kind.SEVERAL, String.join(", then ", descriptions), List.of());
    }

    /**
     * Applies one event of the kind, without updating the metadata image.
     */
    private Event applyEvent(Kind kind) {
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
            case EMPTY_SUBSCRIPTION -> emptySubscription();
            default -> throw new IllegalStateException("Unexpected event kind " + kind);
        };
    }

    /**
     * @return The whole scenario: topics with their partitions, cohorts, and members with their
     *         subscription and current partitions.
     */
    String dump() {
        var out = new StringBuilder();
        out.append("scenario seed=").append(seed)
            .append(" size=").append(size)
            .append(" members=").append(members.size())
            .append(" topics=").append(topics.size())
            .append('\n');
        for (var topic : topics) {
            out.append("  topic ").append(topic.name)
                .append(" id=").append(topic.id)
                .append(" partitions=").append(topic.partitions)
                .append(topic.deleted ? " deleted" : "")
                .append('\n');
        }
        for (int c = 0; c < cohorts.size(); c++) {
            out.append("  cohort ").append(c)
                .append(" topics=").append(cohorts.get(c))
                .append('\n');
        }
        for (var member : members.values()) {
            out.append("  member ").append(member.id)
                .append(" cohort=").append(member.cohort)
                .append(" topics=").append(member.subscription)
                .append(" current=").append(currentPartitions(member.current))
                .append('\n');
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

    /**
     * Creates a topic with a new name.
     */
    private Topic createTopic(int partitions) {
        return createTopic("T" + latestTopics.size(), partitions);
    }

    /**
     * Creates a topic with a new id. The name must not be used by a topic that exists.
     */
    private Topic createTopic(String name, int partitions) {
        var topic = new Topic(name, new Uuid(random.nextLong(), random.nextLong()), partitions);
        topics.add(topic);
        latestTopics.put(name, topic);
        return topic;
    }

    /**
     * Creates one cohort, or two or three whose subscriptions overlap, nest or are disjoint.
     */
    private void createCohorts() {
        boolean homogeneous = random.nextBoolean();
        int cohortCount = homogeneous ? 1 : 2 + random.nextInt(2);
        var first = homogeneous || random.nextBoolean()
            ? allTopics()
            : randomSubscription(allTopics());
        cohorts.add(first);
        for (int c = 1; c < cohortCount; c++) {
            var others = allTopics();
            others.removeAll(first);
            cohorts.add(switch (random.nextInt(3)) {
                case 0 -> randomSubscription(allTopics());
                case 1 -> randomSubscription(first);
                default -> randomSubscription(others.isEmpty() ? allTopics() : others);
            });
        }
    }

    /**
     * @return The names of all the topics, in name order.
     */
    private Set<String> allTopics() {
        return new TreeSet<>(latestTopics.keySet());
    }

    /**
     * @return A random non-empty subset of the topic names, in name order.
     */
    private Set<String> randomSubscription(Set<String> from) {
        var shuffled = new ArrayList<>(new TreeSet<>(from));
        Collections.shuffle(shuffled, random);
        return new TreeSet<>(shuffled.subList(0, 1 + random.nextInt(shuffled.size())));
    }

    private Member addMember(int cohort) {
        var member = new Member(newMemberId(), cohort);
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
            var id = String.format(Locale.ROOT, "m%02d", random.nextInt(bound));
            if (!members.containsKey(id)) {
                return id;
            }
            bound *= 10;
        }
    }

    /**
     * Builds the metadata image of the topics that exist.
     */
    private void updateImage() {
        var builder = new MetadataImageBuilder();
        for (var topic : latestTopics.values()) {
            if (!topic.deleted) {
                builder.addTopic(topic.id, topic.name, topic.partitions);
            }
        }
        image = builder.buildCoordinatorMetadataImage();
    }

    // Events.

    private Event join(int count) {
        var joined = new ArrayList<String>();
        for (int i = 0; i < count && members.size() < MAX_MEMBERS; i++) {
            joined.add(addMember(random.nextInt(cohorts.size())).id);
        }
        if (joined.isEmpty()) {
            return Event.nothing("nobody joins, the group is full");
        }
        var kind = joined.size() == 1 ? Kind.JOIN : Kind.JOIN_MANY;
        return new Event(kind, "join " + joined, joined);
    }

    /**
     * Removes members, keeping at least one, so that the group does not restart from scratch.
     */
    private Event leave(int count) {
        var left = new ArrayList<String>();
        for (int i = 0; i < count && members.size() > 1; i++) {
            var ids = new ArrayList<>(members.keySet());
            var id = ids.get(random.nextInt(ids.size()));
            members.remove(id);
            left.add(id);
        }
        if (left.isEmpty()) {
            return Event.nothing("nobody leaves, a single member is left");
        }
        var kind = left.size() == 1 ? Kind.LEAVE : Kind.LEAVE_MANY;
        return new Event(kind, "leave " + left, left);
    }

    private Event growTopic() {
        var candidates = new ArrayList<Topic>();
        for (var topic : liveTopics()) {
            if (topic.partitions < MAX_PARTITIONS) {
                candidates.add(topic);
            }
        }
        if (candidates.isEmpty()) {
            return Event.nothing("grow nothing");
        }
        var topic = candidates.get(random.nextInt(candidates.size()));
        int added = switch (size) {
            case TINY -> 1 + random.nextInt(2);
            case MEDIUM -> 1 + random.nextInt(10);
            case LARGE -> 1 + random.nextInt(20);
        };
        added = Math.min(added, MAX_PARTITIONS - topic.partitions);
        topic.partitions += added;
        return new Event(
            Kind.GROW_TOPIC,
            "grow " + topic.name + " by " + added + " to " + topic.partitions,
            List.of()
        );
    }

    /**
     * Adds a topic to a cohort and its members: a new topic, or one the cohort does not subscribe
     * to, which may be deleted.
     */
    private Event addTopicToCohort() {
        int cohort = random.nextInt(cohorts.size());
        var subscription = cohorts.get(cohort);
        var candidates = new ArrayList<>(allTopics());
        candidates.removeAll(subscription);
        String name;
        if (topics.size() < MAX_TOPICS && (candidates.isEmpty() || random.nextInt(3) == 0)) {
            name = createTopic(initialPartitions()).name;
        } else if (candidates.isEmpty()) {
            return Event.nothing("cohort " + cohort + " adds nothing");
        } else {
            name = candidates.get(random.nextInt(candidates.size()));
        }
        subscription.add(name);
        for (var member : members.values()) {
            if (member.cohort == cohort) {
                member.subscription.add(name);
            }
        }
        return new Event(Kind.ADD_TOPIC_TO_COHORT, "cohort " + cohort + " adds " + name, List.of());
    }

    /**
     * Drops a topic from a cohort keeping at least one. Members of the cohort follow, unless
     * that would leave them without any topic.
     */
    private Event removeTopicFromCohort() {
        int cohort = random.nextInt(cohorts.size());
        var subscription = cohorts.get(cohort);
        if (subscription.size() <= 1) {
            return Event.nothing("cohort " + cohort + " drops nothing");
        }
        var candidates = new ArrayList<>(subscription);
        var name = candidates.get(random.nextInt(candidates.size()));
        subscription.remove(name);
        for (var member : members.values()) {
            if (member.cohort == cohort && member.subscription.size() > 1) {
                member.subscription.remove(name);
            }
        }
        return new Event(
            Kind.REMOVE_TOPIC_FROM_COHORT,
            "cohort " + cohort + " drops " + name,
            List.of()
        );
    }

    /**
     * A member subscribes to a topic it does not have, or drops one it has, keeping at least one.
     */
    private Event toggleTopic() {
        var member = randomMember();
        var names = new ArrayList<>(allTopics());
        var name = names.get(random.nextInt(names.size()));
        if (!member.subscription.contains(name)) {
            member.subscription.add(name);
            return new Event(Kind.TOGGLE_TOPIC, member.id + " subscribes to " + name, List.of());
        }
        if (member.subscription.size() == 1) {
            return Event.nothing(member.id + " keeps " + name + ", its only topic");
        }
        member.subscription.remove(name);
        return new Event(Kind.TOGGLE_TOPIC, member.id + " drops " + name, List.of());
    }

    /**
     * Deletes a topic that members subscribe to, keeping at least one topic that exists.
     */
    private Event deleteTopic() {
        var live = liveTopics();
        var subscribed = live.stream().filter(topic -> subscribed(topic.name)).toList();
        if (live.size() <= 1 || subscribed.isEmpty()) {
            return Event.nothing("delete nothing");
        }
        var topic = subscribed.get(random.nextInt(subscribed.size()));
        topic.deleted = true;
        return new Event(Kind.DELETE_TOPIC, "delete " + topic.name, List.of());
    }

    /**
     * Creates a subscribed topic again under a new id, deleting it first if it exists.
     */
    private Event recreateTopic() {
        var subscribed = new ArrayList<Topic>();
        for (var topic : latestTopics.values()) {
            if (subscribed(topic.name)) {
                subscribed.add(topic);
            }
        }
        if (subscribed.isEmpty() || topics.size() >= MAX_TOPICS) {
            return Event.nothing("re-create nothing");
        }
        var old = subscribed.get(random.nextInt(subscribed.size()));
        var description = old.deleted
            ? "re-create " + old.name
            : "delete and re-create " + old.name;
        old.deleted = true;
        createTopic(old.name, initialPartitions());
        return new Event(Kind.RECREATE_TOPIC, description, List.of());
    }

    /**
     * A member drops all its topics. It stays in its cohort, whose changes it follows again.
     */
    private Event emptySubscription() {
        var member = randomMember();
        if (member.subscription.isEmpty()) {
            return Event.nothing(member.id + " has no topics to drop");
        }
        member.subscription.clear();
        return new Event(Kind.EMPTY_SUBSCRIPTION, member.id + " drops all its topics", List.of());
    }

    /**
     * @return The topics that exist, in name order.
     */
    private List<Topic> liveTopics() {
        var live = new ArrayList<Topic>();
        for (var topic : latestTopics.values()) {
            if (!topic.deleted) {
                live.add(topic);
            }
        }
        return live;
    }

    /**
     * @return {@code true} if a cohort or a member subscribes to the topic name.
     */
    private boolean subscribed(String name) {
        for (var subscription : cohorts) {
            if (subscription.contains(name)) {
                return true;
            }
        }
        for (var member : members.values()) {
            if (member.subscription.contains(name)) {
                return true;
            }
        }
        return false;
    }

    private Member randomMember() {
        var ids = new ArrayList<>(members.keySet());
        return members.get(ids.get(random.nextInt(ids.size())));
    }

    // Helpers.

    /**
     * @return A copy of the partitions with the topics and the partitions in ascending order, in
     *         sets that cannot be changed.
     */
    private static Map<Uuid, Set<Integer>> copy(Map<Uuid, Set<Integer>> partitions) {
        var copy = new TreeMap<Uuid, Set<Integer>>();
        partitions.forEach((topicId, topicPartitions) -> copy.put(
            topicId,
            Collections.unmodifiableSet(new TreeSet<>(topicPartitions))
        ));
        return copy;
    }

    /**
     * @return The elements in a random order.
     */
    private static <T extends Comparable<T>> Set<T> shuffled(Set<T> elements, Random random) {
        var list = new ArrayList<>(new TreeSet<>(elements));
        Collections.shuffle(list, random);
        return Collections.unmodifiableSet(new LinkedHashSet<>(list));
    }

    /**
     * @return The partitions with their topics and the partitions of every topic in a random order.
     */
    private static Map<Uuid, Set<Integer>> shuffled(
        Map<Uuid, Set<Integer>> partitions,
        Random random
    ) {
        var result = new LinkedHashMap<Uuid, Set<Integer>>();
        for (var topicId : shuffled(partitions.keySet(), random)) {
            result.put(topicId, shuffled(partitions.get(topicId), random));
        }
        return result;
    }

    /**
     * @return The partitions by topic name, in creation order, with deleted topics marked.
     */
    private String currentPartitions(Map<Uuid, Set<Integer>> current) {
        var out = new StringBuilder("{");
        for (var topic : topics) {
            var partitions = current.get(topic.id);
            if (partitions != null) {
                out.append(out.length() == 1 ? "" : ", ")
                    .append(topic.name)
                    .append(topic.deleted ? " (deleted)" : "")
                    .append('=')
                    .append(new TreeSet<>(partitions));
            }
        }
        return out.append('}').toString();
    }
}
