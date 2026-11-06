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
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.assignor.uniform2.GroupSpecFixture;
import org.apache.kafka.coordinator.group.modern.SubscribedTopicDescriberImpl;
import org.apache.kafka.coordinator.group.modern.TopicIds;

import java.util.ArrayList;
import java.util.Collections;
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
 * member has an empty subscription. The current partitions are only changed through
 * {@link #apply(GroupAssignment)}, so the events are independent of the assignor under test.
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
        EMPTY_SUBSCRIPTION
    }

    /**
     * An event, with the ids of the members it added or removed. A join or leave of exactly one
     * member has the kind {@link Kind#JOIN} or {@link Kind#LEAVE}, whatever the number asked.
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
     * The sizes of the scenarios, each as many times as its weight.
     */
    private static final Size[] SIZE_WEIGHTS = {
        Size.TINY, Size.TINY, Size.TINY, Size.TINY, Size.TINY, Size.TINY, Size.TINY,
        Size.MEDIUM, Size.MEDIUM, Size.MEDIUM, Size.MEDIUM, Size.MEDIUM, Size.MEDIUM, Size.MEDIUM, Size.MEDIUM,
        Size.MEDIUM, Size.MEDIUM,
        Size.LARGE, Size.LARGE, Size.LARGE
    };

    /**
     * The kinds of the random events, each as many times as its weight.
     */
    private static final Kind[] EVENT_WEIGHTS = {
        Kind.JOIN, Kind.JOIN, Kind.JOIN,
        Kind.LEAVE, Kind.LEAVE, Kind.LEAVE,
        Kind.JOIN_MANY, Kind.LEAVE_MANY,
        Kind.GROW_TOPIC, Kind.GROW_TOPIC,
        Kind.ADD_TOPIC_TO_COHORT, Kind.REMOVE_TOPIC_FROM_COHORT, Kind.TOGGLE_TOPIC,
        Kind.DELETE_TOPIC, Kind.RECREATE_TOPIC, Kind.EMPTY_SUBSCRIPTION
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
        this.size = SIZE_WEIGHTS[random.nextInt(SIZE_WEIGHTS.length)];
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
        updateImage();
    }

    long seed() {
        return seed;
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
        for (Member member : members.values()) {
            spec.withMember(member.id, new TopicIds(new TreeSet<>(member.subscription), resolver), member.current);
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
        List<Member> shuffledMembers = new ArrayList<>(members.values());
        Collections.shuffle(shuffledMembers, random);
        var spec = new GroupSpecFixture();
        for (Member member : shuffledMembers) {
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
        Event event = switch (kind) {
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
        updateImage();
        return event;
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
                .append(" partitions=").append(topic.partitions).append(topic.deleted ? " deleted" : "").append('\n');
        }
        for (int c = 0; c < cohorts.size(); c++) {
            out.append("  cohort ").append(c).append(" topics=").append(cohorts.get(c)).append('\n');
        }
        for (Member member : members.values()) {
            out.append("  member ").append(member.id)
                .append(" cohort=").append(member.cohort).append(" topics=").append(member.subscription)
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
        Topic topic = new Topic(name, new Uuid(random.nextLong(), random.nextLong()), partitions);
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
        Set<String> first = homogeneous || random.nextBoolean() ? allTopics() : randomSubscription(allTopics());
        cohorts.add(first);
        for (int c = 1; c < cohortCount; c++) {
            Set<String> others = allTopics();
            others.removeAll(first);
            cohorts.add(switch (random.nextInt(3)) {
                case 0 -> randomSubscription(allTopics());
                case 1 -> randomSubscription(first);
                default -> others.isEmpty() ? randomSubscription(allTopics()) : randomSubscription(others);
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
        List<String> shuffled = new ArrayList<>(new TreeSet<>(from));
        Collections.shuffle(shuffled, random);
        return new TreeSet<>(shuffled.subList(0, 1 + random.nextInt(shuffled.size())));
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

    /**
     * Builds the metadata image of the topics that exist.
     */
    private void updateImage() {
        var builder = new MetadataImageBuilder();
        for (Topic topic : latestTopics.values()) {
            if (!topic.deleted) {
                builder.addTopic(topic.id, topic.name, topic.partitions);
            }
        }
        image = builder.buildCoordinatorMetadataImage();
    }

    // Events.

    private Event join(int count) {
        List<String> joined = new ArrayList<>();
        for (int i = 0; i < count && members.size() < MAX_MEMBERS; i++) {
            joined.add(addMember(random.nextInt(cohorts.size())).id);
        }
        if (joined.isEmpty()) {
            return Event.nothing("nobody joins, the group is full");
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
            return Event.nothing("nobody leaves, a single member is left");
        }
        Kind kind = left.size() == 1 ? Kind.LEAVE : Kind.LEAVE_MANY;
        return new Event(kind, "leave " + left, left);
    }

    private Event growTopic() {
        List<Topic> candidates = new ArrayList<>();
        for (Topic topic : liveTopics()) {
            if (topic.partitions < MAX_PARTITIONS) {
                candidates.add(topic);
            }
        }
        if (candidates.isEmpty()) {
            return Event.nothing("grow nothing");
        }
        Topic topic = candidates.get(random.nextInt(candidates.size()));
        int added = switch (size) {
            case TINY -> 1 + random.nextInt(2);
            case MEDIUM -> 1 + random.nextInt(10);
            case LARGE -> 1 + random.nextInt(20);
        };
        added = Math.min(added, MAX_PARTITIONS - topic.partitions);
        topic.partitions += added;
        return new Event(Kind.GROW_TOPIC, "grow " + topic.name + " by " + added + " to " + topic.partitions, List.of());
    }

    /**
     * Adds a topic to a cohort and its members: a new topic, or one the cohort does not subscribe
     * to, which may be deleted.
     */
    private Event addTopicToCohort() {
        int cohort = random.nextInt(cohorts.size());
        Set<String> subscription = cohorts.get(cohort);
        List<String> candidates = new ArrayList<>(allTopics());
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
        for (Member member : members.values()) {
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
        Set<String> subscription = cohorts.get(cohort);
        if (subscription.size() <= 1) {
            return Event.nothing("cohort " + cohort + " drops nothing");
        }
        List<String> candidates = new ArrayList<>(subscription);
        String name = candidates.get(random.nextInt(candidates.size()));
        subscription.remove(name);
        for (Member member : members.values()) {
            if (member.cohort == cohort && member.subscription.size() > 1) {
                member.subscription.remove(name);
            }
        }
        return new Event(Kind.REMOVE_TOPIC_FROM_COHORT, "cohort " + cohort + " drops " + name, List.of());
    }

    /**
     * A member subscribes to a topic it does not have, or drops one it has, keeping at least one.
     */
    private Event toggleTopic() {
        Member member = randomMember();
        List<String> names = new ArrayList<>(allTopics());
        String name = names.get(random.nextInt(names.size()));
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
     * Deletes a topic, keeping at least one topic that exists.
     */
    private Event deleteTopic() {
        List<Topic> live = liveTopics();
        if (live.size() <= 1) {
            return Event.nothing("delete nothing");
        }
        Topic topic = live.get(random.nextInt(live.size()));
        topic.deleted = true;
        return new Event(Kind.DELETE_TOPIC, "delete " + topic.name, List.of());
    }

    /**
     * Creates a subscribed topic again under a new id, deleting it first if it exists.
     */
    private Event recreateTopic() {
        List<Topic> subscribed = new ArrayList<>();
        for (Topic topic : latestTopics.values()) {
            if (subscribed(topic.name)) {
                subscribed.add(topic);
            }
        }
        if (subscribed.isEmpty() || topics.size() >= MAX_TOPICS) {
            return Event.nothing("re-create nothing");
        }
        Topic old = subscribed.get(random.nextInt(subscribed.size()));
        String description = old.deleted ? "re-create " + old.name : "delete and re-create " + old.name;
        old.deleted = true;
        createTopic(old.name, initialPartitions());
        return new Event(Kind.RECREATE_TOPIC, description, List.of());
    }

    /**
     * A member drops all its topics. It stays in its cohort, whose changes it follows again.
     */
    private Event emptySubscription() {
        Member member = randomMember();
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
        List<Topic> live = new ArrayList<>();
        for (Topic topic : latestTopics.values()) {
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
        for (Set<String> subscription : cohorts) {
            if (subscription.contains(name)) {
                return true;
            }
        }
        for (Member member : members.values()) {
            if (member.subscription.contains(name)) {
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

    /**
     * @return A copy of the partitions with the topics and the partitions in ascending order, in
     *         sets that cannot be changed.
     */
    private static Map<Uuid, Set<Integer>> copy(Map<Uuid, Set<Integer>> partitions) {
        Map<Uuid, Set<Integer>> copy = new TreeMap<>();
        partitions.forEach((topicId, topicPartitions) -> copy.put(topicId, Collections.unmodifiableSet(new TreeSet<>(topicPartitions))));
        return copy;
    }

    /**
     * @return The elements in a random order.
     */
    private static <T extends Comparable<T>> Set<T> shuffled(Set<T> elements, Random random) {
        List<T> list = new ArrayList<>(new TreeSet<>(elements));
        Collections.shuffle(list, random);
        return Collections.unmodifiableSet(new LinkedHashSet<>(list));
    }

    /**
     * @return The partitions with their topics and the partitions of every topic in a random order.
     */
    private static Map<Uuid, Set<Integer>> shuffled(Map<Uuid, Set<Integer>> partitions, Random random) {
        Map<Uuid, Set<Integer>> result = new LinkedHashMap<>();
        for (Uuid topicId : shuffled(partitions.keySet(), random)) {
            result.put(topicId, shuffled(partitions.get(topicId), random));
        }
        return result;
    }

    /**
     * @return The partitions by topic name, in creation order, with deleted topics marked.
     */
    private String currentPartitions(Map<Uuid, Set<Integer>> current) {
        StringBuilder out = new StringBuilder("{");
        for (Topic topic : topics) {
            Set<Integer> partitions = current.get(topic.id);
            if (partitions != null) {
                out.append(out.length() == 1 ? "" : ", ").append(topic.name).append(topic.deleted ? " (deleted)" : "")
                    .append('=').append(new TreeSet<>(partitions));
            }
        }
        return out.append('}').toString();
    }
}
