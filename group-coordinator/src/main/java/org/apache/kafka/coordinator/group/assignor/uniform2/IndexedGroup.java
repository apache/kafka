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
 * The members, the subscribed topics and the cohorts of a group, numbered by small integers.
 *
 * <p>Members are numbered in member id order and topics in topic id order, so that the assignment
 * is deterministic and does not depend on the iteration order of the collections in the spec.
 *
 * <p>A cohort is a set of members having the same subscription. Cohorts are numbered in the order
 * of their first member. Grouping the members by subscription lets the assignor read, store and
 * process every subscription once, however many members share it.
 *
 * <p>Only topics subscribed by at least one member and which exist are retained.
 *
 * <p>With a homogeneous subscription type, only the subscription of the first member is read, as
 * the coordinator guarantees that every member has the same one, and the group is a single cohort.
 * Otherwise every subscription is read once and looked up by a hash of its topics that does not
 * depend on their order, see {@link CohortsBySubscription}. The cohorts with the same hash are
 * compared exactly, so a collision only costs a comparison, never a wrong cohort.
 */
final class IndexedGroup {
    /**
     * Reads the subscriptions of the members, indexing their topics that exist in the order they
     * are seen, and remembering their partition counts.
     */
    private static final class SubscriptionReader {
        /**
         * The group spec, which gives the subscription of every member.
         */
        private final GroupSpec groupSpec;

        /**
         * The describer of the subscribed topics, which gives their partition counts.
         */
        private final SubscribedTopicDescriber subscribedTopicDescriber;

        /**
         * Per topic seen, its index in the order seen, or -1 if it does not exist, so that the
         * describer is asked once per topic.
         */
        private final Map<Uuid, Integer> topicIndices = new HashMap<>();

        /**
         * The topics seen that exist, in the order seen.
         */
        private final List<Uuid> topicIds = new ArrayList<>();

        /**
         * The partition counts of the topics seen that exist, in the order seen.
         */
        private final IntList partitionCounts = new IntList(16);

        SubscriptionReader(GroupSpec groupSpec, SubscribedTopicDescriber subscribedTopicDescriber) {
            this.groupSpec = groupSpec;
            this.subscribedTopicDescriber = subscribedTopicDescriber;
        }

        /**
         * Reads the subscription of the member.
         *
         * @param memberId The member id.
         * @param topics   Receives the indices of its subscribed topics that exist.
         */
        void readSubscribedTopics(String memberId, IntList topics) {
            for (Uuid topicId : groupSpec.memberSubscription(memberId).subscribedTopicIds()) {
                var index = topicIndices.get(topicId);
                if (index == null) {
                    // Seen for the first time: assigned the next index, unless it does not exist.
                    int partitionCount = subscribedTopicDescriber.numPartitions(topicId);
                    index = partitionCount < 0 ? -1 : topicIds.size();
                    if (index >= 0) {
                        topicIds.add(topicId);
                        partitionCounts.add(partitionCount);
                    }
                    topicIndices.put(topicId, index);
                }
                if (index >= 0) {
                    topics.add(index);
                }
            }
        }
    }

    /**
     * A hash map from subscriptions to cohorts: {@link #getOrCreateCohort} returns the cohort with
     * the exact same subscribed topics, creating it when there is none.
     *
     * <p>A subscription is summarized by a hash that does not depend on the order of its topics,
     * and the cohorts with the same hash are listed together, usually one per hash.
     *
     * <p>Subscription equality is tested by marking the topics of the subscription being looked
     * up in an array, then checking that a cohort has as many topics, all of them marked. Unlike a
     * {@code HashMap} keyed by the subscription, this needs no sorted copy of every member's
     * subscription.
     */
    private static final class CohortsBySubscription {
        /**
         * Per cohort, its subscribed topics, indexed in the order they were seen.
         */
        private final List<int[]> subscriptions = new ArrayList<>();

        /**
         * Per hash of a subscription, the cohorts whose subscription has that hash.
         */
        private final Map<Long, IntList> cohortsByHash = new HashMap<>();

        /**
         * Per topic, the lookup that marked it last, or 0 if none did.
         */
        private final IntList marks = new IntList(16);

        /**
         * The number of lookups so far, which is also the mark of the current one.
         */
        private int lookups;

        /**
         * @param subscribedTopics The subscribed topics of a member, in any order, without
         *                         duplicates.
         * @return The cohort with the exact same subscription, created if there is none.
         */
        int getOrCreateCohort(IntList subscribedTopics) {
            int mark = ++lookups;
            markTopics(subscribedTopics, mark);
            long hash = hashOf(subscribedTopics);
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
         * @return Per cohort, its subscribed topics. The list must not be changed.
         */
        List<int[]> subscriptions() {
            return subscriptions;
        }

        /**
         * Marks the topics with the mark, growing the marks to cover them.
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
         *         the mark, which has that many topics.
         */
        private boolean hasSameSubscription(int cohort, int topicCount, int mark) {
            int[] topics = subscriptions.get(cohort);
            if (topics.length != topicCount) {
                return false;
            }
            // Its topics were marked when it was created, so the marks cover them.
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
        private static long hashOf(IntList topics) {
            long hash = topics.size();
            for (int i = 0; i < topics.size(); i++) {
                hash += mix(topics.get(i));
            }
            return hash;
        }

        /**
         * @return A well spread hash of the topic: the SplitMix64 finalizer of its index plus one.
         */
        private static long mix(int topic) {
            long z = (topic + 1) * 0x9E3779B97F4A7C15L;
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            return z ^ (z >>> 31);
        }
    }

    /**
     * The member ids, in member id order: the id of every member.
     */
    private final String[] memberIds;

    /**
     * Per member, its cohort.
     */
    private final int[] cohortOfMember;

    /**
     * The topic ids, in topic id order: the id of every topic.
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
     * Per topic, the cohorts subscribing to it, in cohort order: an array of cohorts, as long as
     * the number of such cohorts.
     */
    private final int[][] cohortsOfTopic;

    /**
     * Per cohort, its members, in member order: an array of members, as long as the cohort.
     */
    private final int[][] cohortMembers;

    /**
     * Per cohort, its subscribed topics, in topic order: an array of topics, as long as the
     * subscription.
     */
    private final int[][] cohortTopics;

    /**
     * Reads the members and their subscriptions.
     *
     * @param groupSpec                The group spec.
     * @param subscribedTopicDescriber The describer of the subscribed topics.
     */
    IndexedGroup(GroupSpec groupSpec, SubscribedTopicDescriber subscribedTopicDescriber) {
        memberIds = groupSpec.memberIds().toArray(new String[0]);
        Arrays.sort(memberIds);
        cohortOfMember = new int[memberIds.length];

        var reader = new SubscriptionReader(groupSpec, subscribedTopicDescriber);
        List<int[]> cohortSubscriptions = groupSpec.subscriptionType() == SubscriptionType.HOMOGENEOUS || memberIds.length <= 1
            ? readHomogeneousCohorts(reader)
            : readHeterogeneousCohorts(reader);

        // The reader indexes the topics in the order seen, as the topics are only known once every
        // subscription is read. They are indexed in topic id order instead, so that the indices
        // only depend on the content of the input. The map of the reader is renumbered in place,
        // its topics that do not exist keeping -1.
        topicIds = reader.topicIds.toArray(new Uuid[0]);
        Arrays.sort(topicIds);
        topicIndices = reader.topicIndices;
        partitionCounts = new int[topicIds.length];
        // Per index in the order seen, the index in topic id order.
        var renumbered = new int[topicIds.length];
        for (int topic = 0; topic < topicIds.length; topic++) {
            int seenIndex = topicIndices.put(topicIds[topic], topic);
            renumbered[seenIndex] = topic;
            partitionCounts[topic] = reader.partitionCounts.get(seenIndex);
        }

        // The topics of the cohorts are renumbered too, which is cheaper than reading the
        // subscriptions again, and the subscribers of every topic are counted.
        cohortMembers = membersByCohort(cohortSubscriptions.size());
        cohortTopics = new int[cohortSubscriptions.size()][];
        subscriberCounts = new int[topicIds.length];
        for (int cohort = 0; cohort < cohortSubscriptions.size(); cohort++) {
            var topics = cohortSubscriptions.get(cohort);
            for (int i = 0; i < topics.length; i++) {
                topics[i] = renumbered[topics[i]];
                subscriberCounts[topics[i]] += cohortMembers[cohort].length;
            }
            Arrays.sort(topics);
            cohortTopics[cohort] = topics;
        }
        cohortsOfTopic = cohortsByTopic();
    }

    /**
     * Reads the subscription of the first member, which every member has.
     *
     * @return The subscribed topics of the single cohort, indexed in the order they were seen.
     */
    private List<int[]> readHomogeneousCohorts(SubscriptionReader reader) {
        // Every member is already in cohort 0, the initial value of cohortOfMember.
        var topics = new IntList(16);
        if (memberIds.length > 0) {
            reader.readSubscribedTopics(memberIds[0], topics);
        }
        return List.of(topics.toArray());
    }

    /**
     * Reads the subscription of every member and groups the members with the same one into
     * cohorts.
     *
     * @return Per cohort, its subscribed topics, indexed in the order they were seen.
     */
    private List<int[]> readHeterogeneousCohorts(SubscriptionReader reader) {
        var cohorts = new CohortsBySubscription();
        var topics = new IntList(16);
        for (int member = 0; member < memberIds.length; member++) {
            topics.clear();
            reader.readSubscribedTopics(memberIds[member], topics);
            cohortOfMember[member] = cohorts.getOrCreateCohort(topics);
        }
        return cohorts.subscriptions();
    }

    /**
     * @return The members of every cohort, in member order.
     */
    private int[][] membersByCohort(int cohortCount) {
        // Count the members of every cohort, so that every array is created with the right size.
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
     * @return The cohorts subscribing to every topic, in cohort order.
     */
    private int[][] cohortsByTopic() {
        var cohorts = new int[topicIds.length][];
        if (cohortTopics.length == 1) {
            // A single cohort subscribes to all the topics, which share the same array.
            Arrays.fill(cohorts, new int[] {0});
            return cohorts;
        }
        // Count the subscribing cohorts of every topic, so that every array is created with the
        // right size.
        var counts = new int[topicIds.length];
        for (int[] topics : cohortTopics) {
            for (int topic : topics) {
                counts[topic]++;
            }
        }
        for (int topic = 0; topic < topicIds.length; topic++) {
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

    /**
     * @return The number of members.
     */
    int memberCount() {
        return memberIds.length;
    }

    /**
     * @return The id of the member.
     */
    String memberId(int member) {
        return memberIds[member];
    }

    /**
     * @return The cohort of the member.
     */
    int cohortOf(int member) {
        return cohortOfMember[member];
    }

    /**
     * @return The number of topics.
     */
    int topicCount() {
        return topicIds.length;
    }

    /**
     * @return The id of the topic.
     */
    Uuid topicId(int topic) {
        return topicIds[topic];
    }

    /**
     * @return The index of the topic, or -1 if nobody subscribes to it or it does not exist.
     */
    int topicIndex(Uuid topicId) {
        var topic = topicIndices.get(topicId);
        return topic == null ? -1 : topic;
    }

    /**
     * @return The number of partitions of the topic.
     */
    int partitionCount(int topic) {
        return partitionCounts[topic];
    }

    /**
     * @return The number of members subscribing to the topic. Always at least one for topics
     *         retained by the class.
     */
    int subscriberCount(int topic) {
        return subscriberCounts[topic];
    }

    /**
     * @return The cohorts subscribing to the topic, in cohort order. The array must not be
     *         changed.
     */
    int[] cohortsOf(int topic) {
        return cohortsOfTopic[topic];
    }

    /**
     * @return The number of cohorts.
     */
    int cohortCount() {
        return cohortMembers.length;
    }

    /**
     * @return The members of the cohort, in member order. The array must not be changed.
     */
    int[] membersOf(int cohort) {
        return cohortMembers[cohort];
    }

    /**
     * @return The topics of the cohort, in topic order. The array must not be changed.
     */
    int[] topicsOf(int cohort) {
        return cohortTopics[cohort];
    }

    /**
     * @return {@code true} if the member subscribes to the topic.
     */
    boolean subscribes(int member, int topic) {
        return Arrays.binarySearch(cohortTopics[cohortOfMember[member]], topic) >= 0;
    }
}
