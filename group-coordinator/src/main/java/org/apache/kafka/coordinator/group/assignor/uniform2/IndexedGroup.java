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
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.assignor.uniform2.util.IntList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The members, the subscribed topics and the cohorts of a group, numbered by small integers. It is
 * built by {@link Builder}.
 *
 * <p>Members are numbered in member id order and topics in topic id order, so that the assignment
 * is deterministic and does not depend on the iteration order of the collections in the spec.
 *
 * <p>A cohort is a set of members having the same subscription. Cohorts are numbered in the order
 * of their first member. Grouping the members by subscription lets the assignor store and process
 * every distinct subscription once, however many members share it. A group with a homogeneous
 * subscription type is a single cohort; an empty group is a single cohort without members.
 *
 * <p>Only topics subscribed by at least one member and which exist are retained.
 */
public final class IndexedGroup {
    /**
     * Builds the {@link IndexedGroup} of a group spec.
     *
     * <p>With a homogeneous subscription type, only the subscription of the first member is read,
     * as the coordinator guarantees that every member has the same one. Otherwise every
     * subscription is read once and looked up by a hash of its topics that does not depend on their
     * order. The cohorts with the same hash are compared exactly, so members are never assigned to
     * the wrong cohort.
     *
     * <p>A subscription may be a {@code TopicIds}, whose {@code size()} also counts the topic names
     * that do not resolve, so the builder only uses the topics it iterates, never {@code size()}.
     * Subscribed topics that do not exist are ignored.
     *
     * <p>A builder builds a single group: a second call to {@link #build()} throws.
     */
    public static final class Builder {
        /**
         * The group spec, which gives the subscription of every member.
         */
        private final GroupSpec groupSpec;

        /**
         * The describer of the subscribed topics, which gives their partition counts.
         */
        private final SubscribedTopicDescriber subscribedTopicDescriber;

        /**
         * The mask applied to the hash of every subscription: all the bits in the assignor, none in
         * a test that makes every subscription collide.
         */
        private final long hashMask;

        /**
         * The member ids, in ascending order.
         */
        private final String[] memberIds;

        /**
         * Per member, its cohort.
         */
        private final int[] cohortOfMember;

        /**
         * Per topic id seen in a subscription, its index, or -1 if the topic does not exist, so
         * that the describer is asked once per topic. Indexed in the order seen, then in topic id
         * order by {@link #build()}.
         */
        private final Map<Uuid, Integer> topicIndices = new HashMap<>();

        /**
         * The topics seen that exist, in the order seen.
         */
        private final List<Uuid> seenTopicIds = new ArrayList<>();

        /**
         * The partition counts of the topics seen that exist, in the order seen.
         */
        private final IntList seenPartitionCounts = new IntList(16);

        /**
         * Per cohort, its subscribed topics. Indexed in the order seen, then in topic id order by
         * {@link #build()}.
         */
        private final List<int[]> subscriptions = new ArrayList<>();

        /**
         * Per hash of a subscription, the cohorts whose subscription has that hash. There is
         * usually only one cohort per hash.
         */
        private final Map<Long, IntList> cohortsByHash = new HashMap<>();

        /**
         * Per topic, the lookup of a subscription that marked it last, or 0 if none did.
         */
        private final IntList marks = new IntList(16);

        /**
         * The number of lookups of a subscription so far, which is also the mark of the current
         * one.
         */
        private int lookups;

        /**
         * Whether the group is built.
         */
        private boolean built;

        /**
         * @param groupSpec                The group spec.
         * @param subscribedTopicDescriber The describer of the subscribed topics.
         */
        public Builder(GroupSpec groupSpec, SubscribedTopicDescriber subscribedTopicDescriber) {
            this(groupSpec, subscribedTopicDescriber, -1L);
        }

        /**
         * @param groupSpec                The group spec.
         * @param subscribedTopicDescriber The describer of the subscribed topics.
         * @param hashMask                 The mask applied to the hash of every subscription.
         */
        public Builder(
            GroupSpec groupSpec,
            SubscribedTopicDescriber subscribedTopicDescriber,
            long hashMask
        ) {
            this.groupSpec = groupSpec;
            this.subscribedTopicDescriber = subscribedTopicDescriber;
            this.hashMask = hashMask;
            memberIds = groupSpec.memberIds().toArray(new String[0]);
            Arrays.sort(memberIds);
            cohortOfMember = new int[memberIds.length];
        }

        /**
         * Reads the members and their subscriptions.
         *
         * @return The group.
         * @throws IllegalStateException If the group is already built.
         */
        public IndexedGroup build() {
            if (built) {
                throw new IllegalStateException("The group is already built");
            }
            built = true;

            if (groupSpec.subscriptionType() == SubscriptionType.HOMOGENEOUS ||
                memberIds.length <= 1) {
                readHomogeneousSubscription();
            } else {
                readHeterogeneousSubscriptions();
            }

            // The topics are indexed in the order seen, as they are only known once every
            // subscription is read. They are renumbered in topic id order, so that the indices only
            // depend on the content of the input. The map is renumbered in place, its topics that
            // do not exist keeping -1.
            var topicIds = seenTopicIds.toArray(new Uuid[0]);
            Arrays.sort(topicIds);
            var partitionCounts = new int[topicIds.length];
            // Per index in the order seen, the index in topic id order.
            var renumbered = new int[topicIds.length];
            for (int topic = 0; topic < topicIds.length; topic++) {
                int seenIndex = topicIndices.put(topicIds[topic], topic);
                renumbered[seenIndex] = topic;
                partitionCounts[topic] = seenPartitionCounts.get(seenIndex);
            }

            // The topics of the cohorts are renumbered in place too, which is cheaper than reading
            // the subscriptions again, and the subscribers of every topic are counted.
            var cohortMembers = membersByCohort();
            var cohortTopics = new int[subscriptions.size()][];
            var subscriberCounts = new int[topicIds.length];
            for (int cohort = 0; cohort < subscriptions.size(); cohort++) {
                var topics = subscriptions.get(cohort);
                for (int i = 0; i < topics.length; i++) {
                    topics[i] = renumbered[topics[i]];
                    subscriberCounts[topics[i]] += cohortMembers[cohort].length;
                }
                Arrays.sort(topics);
                cohortTopics[cohort] = topics;
            }

            return new IndexedGroup(
                memberIds,
                cohortOfMember,
                topicIds,
                topicIndices,
                partitionCounts,
                subscriberCounts,
                cohortsByTopic(topicIds.length, cohortTopics),
                cohortMembers,
                cohortTopics
            );
        }

        /**
         * Reads the subscription of the first member, which every member is assumed to share, as
         * the subscription of the single cohort.
         */
        private void readHomogeneousSubscription() {
            // Every member is already in cohort 0, the initial value of cohortOfMember.
            var topics = new IntList(16);
            if (memberIds.length > 0) {
                readSubscribedTopics(memberIds[0], topics);
            }
            subscriptions.add(topics.toArray());
        }

        /**
         * Reads the subscription of every member and groups the members with the same one into
         * cohorts.
         */
        private void readHeterogeneousSubscriptions() {
            var topics = new IntList(16);
            for (int member = 0; member < memberIds.length; member++) {
                topics.clear();
                readSubscribedTopics(memberIds[member], topics);
                cohortOfMember[member] = getOrCreateCohort(topics);
            }
        }

        /**
         * Reads the subscription of the member, indexing its topics that exist in the order they
         * are seen, and remembering their partition counts.
         *
         * @param memberId The member id.
         * @param topics   Receives the indices of its subscribed topics that exist.
         */
        private void readSubscribedTopics(String memberId, IntList topics) {
            // Only the iteration is used: the subscription may be a TopicIds, whose size() also
            // counts the names that do not resolve.
            for (Uuid topicId : groupSpec.memberSubscription(memberId).subscribedTopicIds()) {
                var index = topicIndices.get(topicId);
                if (index == null) {
                    // Seen for the first time: assigned the next index, unless it does not exist.
                    int partitionCount = subscribedTopicDescriber.numPartitions(topicId);
                    index = partitionCount < 0 ? -1 : seenTopicIds.size();
                    if (index >= 0) {
                        seenTopicIds.add(topicId);
                        seenPartitionCounts.add(partitionCount);
                    }
                    topicIndices.put(topicId, index);
                }
                if (index >= 0) {
                    topics.add(index);
                }
            }
        }

        /**
         * Looks up the subscription by its hash. Subscription equality is tested by marking the
         * topics of the subscription in {@link #marks}, then checking that a cohort has as many
         * topics, all of them marked. Unlike a {@code HashMap} keyed by the subscription, this
         * needs no sorted copy of every member's subscription.
         *
         * @param subscribedTopics The subscribed topics of a member, in any order, without
         *                         duplicates.
         * @return The cohort with the exact same subscription, created if there is none.
         */
        private int getOrCreateCohort(IntList subscribedTopics) {
            int mark = ++lookups;
            markTopics(subscribedTopics, mark);
            long hash = subscriptionHash(subscribedTopics) & hashMask;
            var cohorts = cohortsByHash.get(hash);
            if (cohorts == null) {
                cohorts = new IntList(1);
                cohortsByHash.put(hash, cohorts);
            }
            for (int i = 0; i < cohorts.size(); i++) {
                int cohort = cohorts.get(i);
                if (hasSameSubscription(cohort, subscribedTopics.size(), mark)) {
                    return cohort;
                }
            }
            int cohort = subscriptions.size();
            subscriptions.add(subscribedTopics.toArray());
            cohorts.add(cohort);
            return cohort;
        }

        /**
         * Sets the entry of every subscribed topic in {@link #marks} to the mark, growing the marks
         * as needed.
         */
        private void markTopics(IntList subscribedTopics, int mark) {
            for (int i = 0; i < subscribedTopics.size(); i++) {
                int topic = subscribedTopics.get(i);
                while (marks.size() <= topic) {
                    marks.add(0);
                }
                marks.set(topic, mark);
            }
        }

        /**
         * @return {@code true} if the cohort has the exact same subscription as the one marked with
         *         the mark, which has {@code topicCount} topics.
         */
        private boolean hasSameSubscription(int cohort, int topicCount, int mark) {
            int[] topics = subscriptions.get(cohort);
            if (topics.length != topicCount) {
                return false;
            }
            // The topics of the cohort were marked when it was created, so the marks cover them.
            for (int topic : topics) {
                if (marks.get(topic) != mark) {
                    return false;
                }
            }
            return true;
        }

        /**
         * @return A hash of the topics that does not depend on their order: their number plus the
         *         sum of a well spread hash of every topic, so that two different subscriptions
         *         rarely have the same hash.
         */
        private static long subscriptionHash(IntList topics) {
            long hash = topics.size();
            for (int i = 0; i < topics.size(); i++) {
                hash += topicHash(topics.get(i));
            }
            return hash;
        }

        /**
         * @return A well spread hash of the topic: the SplitMix64 finalizer of its index plus one.
         */
        private static long topicHash(int topic) {
            long z = (topic + 1) * 0x9E3779B97F4A7C15L;
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            return z ^ (z >>> 31);
        }

        /**
         * @return The members of every cohort, in member order.
         */
        private int[][] membersByCohort() {
            int cohortCount = subscriptions.size();
            // Count the members of every cohort, so that every array is created with the right
            // size.
            var sizes = new int[cohortCount];
            for (int cohort : cohortOfMember) {
                sizes[cohort]++;
            }
            var members = new int[cohortCount][];
            for (int cohort = 0; cohort < cohortCount; cohort++) {
                members[cohort] = new int[sizes[cohort]];
                sizes[cohort] = 0;
            }
            for (int member = 0; member < memberIds.length; member++) {
                int cohort = cohortOfMember[member];
                members[cohort][sizes[cohort]++] = member;
            }
            return members;
        }

        /**
         * @param topicCount   The number of topics.
         * @param cohortTopics Per cohort, its subscribed topics.
         * @return The cohorts subscribing to every topic, in cohort order.
         */
        private static int[][] cohortsByTopic(int topicCount, int[][] cohortTopics) {
            var cohorts = new int[topicCount][];
            if (cohortTopics.length == 1) {
                // A single cohort subscribes to all the topics, which share the same array.
                Arrays.fill(cohorts, new int[] {0});
                return cohorts;
            }
            // Count the subscribing cohorts of every topic, so that every array is created with the
            // right size.
            var counts = new int[topicCount];
            for (int[] topics : cohortTopics) {
                for (int topic : topics) {
                    counts[topic]++;
                }
            }
            for (int topic = 0; topic < topicCount; topic++) {
                cohorts[topic] = new int[counts[topic]];
                counts[topic] = 0;
            }
            for (int cohort = 0; cohort < cohortTopics.length; cohort++) {
                for (int topic : cohortTopics[cohort]) {
                    cohorts[topic][counts[topic]++] = cohort;
                }
            }
            return cohorts;
        }
    }

    /**
     * The member ids, in ascending order.
     */
    private final String[] memberIds;

    /**
     * Per member, its cohort.
     */
    private final int[] cohortOfMember;

    /**
     * The topic ids, in ascending order.
     */
    private final Uuid[] topicIds;

    /**
     * Per topic id seen in a subscription, its index, or -1 if the topic does not exist.
     */
    private final Map<Uuid, Integer> topicIndices;

    /**
     * Per topic, its number of partitions.
     */
    private final int[] partitionCounts;

    /**
     * Per topic, its number of subscribers.
     */
    private final int[] subscriberCounts;

    /**
     * Per topic, the list of cohorts subscribing to it, in cohort order.
     */
    private final int[][] cohortsOfTopic;

    /**
     * Per cohort, its members, in member order.
     */
    private final int[][] cohortMembers;

    /**
     * Per cohort, its subscribed topics, in topic order.
     */
    private final int[][] cohortTopics;

    /**
     * Holds the arrays and the map without copying them: they must not be changed afterwards.
     *
     * @param memberIds        The member ids, in ascending order.
     * @param cohortOfMember   Per member, its cohort.
     * @param topicIds         The topic ids, in ascending order.
     * @param topicIndices     Per topic id seen in a subscription, its index, or -1 if the topic
     *                         does not exist.
     * @param partitionCounts  Per topic, its number of partitions.
     * @param subscriberCounts Per topic, its number of subscribers.
     * @param cohortsOfTopic   Per topic, the cohorts subscribing to it, in cohort order.
     * @param cohortMembers    Per cohort, its members, in member order.
     * @param cohortTopics     Per cohort, its subscribed topics, in topic order.
     */
    private IndexedGroup(
        String[] memberIds,
        int[] cohortOfMember,
        Uuid[] topicIds,
        Map<Uuid, Integer> topicIndices,
        int[] partitionCounts,
        int[] subscriberCounts,
        int[][] cohortsOfTopic,
        int[][] cohortMembers,
        int[][] cohortTopics
    ) {
        this.memberIds = memberIds;
        this.cohortOfMember = cohortOfMember;
        this.topicIds = topicIds;
        this.topicIndices = topicIndices;
        this.partitionCounts = partitionCounts;
        this.subscriberCounts = subscriberCounts;
        this.cohortsOfTopic = cohortsOfTopic;
        this.cohortMembers = cohortMembers;
        this.cohortTopics = cohortTopics;
    }

    /**
     * @return The number of members.
     */
    public int memberCount() {
        return memberIds.length;
    }

    /**
     * @return The id of the member.
     */
    public String memberId(int member) {
        return memberIds[member];
    }

    /**
     * @return The cohort of the member.
     */
    public int cohortOf(int member) {
        return cohortOfMember[member];
    }

    /**
     * @return The number of topics.
     */
    public int topicCount() {
        return topicIds.length;
    }

    /**
     * @return The id of the topic.
     */
    public Uuid topicId(int topic) {
        return topicIds[topic];
    }

    /**
     * @return The index of the topic, or -1 if nobody subscribes to it or it does not exist.
     */
    public int topicIndex(Uuid topicId) {
        var topic = topicIndices.get(topicId);
        return topic == null ? -1 : topic;
    }

    /**
     * @return The number of partitions of the topic.
     */
    public int partitionCount(int topic) {
        return partitionCounts[topic];
    }

    /**
     * @return The number of members subscribing to the topic. Always at least one for topics
     *         retained by the class.
     */
    public int subscriberCount(int topic) {
        return subscriberCounts[topic];
    }

    /**
     * @return The cohorts subscribing to the topic, in cohort order. The array must not be
     *         changed.
     */
    public int[] cohortsOf(int topic) {
        return cohortsOfTopic[topic];
    }

    /**
     * @return The number of cohorts.
     */
    public int cohortCount() {
        return cohortMembers.length;
    }

    /**
     * @return The members of the cohort, in member order. The array must not be changed.
     */
    public int[] membersOf(int cohort) {
        return cohortMembers[cohort];
    }

    /**
     * @return The topics of the cohort, in topic order. The array must not be changed.
     */
    public int[] topicsOf(int cohort) {
        return cohortTopics[cohort];
    }

    /**
     * @return {@code true} if the member subscribes to the topic.
     */
    public boolean subscribes(int member, int topic) {
        return Arrays.binarySearch(cohortTopics[cohortOfMember[member]], topic) >= 0;
    }
}
