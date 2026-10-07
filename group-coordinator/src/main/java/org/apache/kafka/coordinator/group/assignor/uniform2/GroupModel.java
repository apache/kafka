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
 * <p>Members are numbered in member id order and topics in topic id order, so that every
 * decision taken on these numbers depends on the content of the input only, never on the
 * iteration order of the collections of the spec. A cohort is a set of members having the same
 * subscription; cohorts are numbered in the order of their first member. The topics are those
 * subscribed by at least one member and which exist.
 *
 * <p>With a homogeneous subscription type, only the subscription of the first member is read,
 * as the coordinator guarantees that every member has the same one, and the group is a single
 * cohort. Otherwise every subscription is read once, and summarized by a hash of its topics
 * that does not depend on their order, so that grouping the members into cohorts is linear in
 * the size of the subscriptions.
 */
final class GroupModel {
    /**
     * Reads the subscriptions of the members, numbering their topics that exist in the order they
     * are met, and remembering their partition counts.
     */
    private static final class SubscriptionReader {
        private final GroupSpec groupSpec;
        private final SubscribedTopicDescriber describer;

        /**
         * Per topic met, its number in the order met, or -1 if it does not exist, so that the
         * describer is asked once per topic.
         */
        private final Map<Uuid, Integer> indices = new HashMap<>();

        /**
         * The topics met that exist, in the order met.
         */
        private final List<Uuid> ids = new ArrayList<>();

        /**
         * The partition counts of the topics met that exist, in the order met.
         */
        private final IntList partitionCounts = new IntList(16);

        SubscriptionReader(GroupSpec groupSpec, SubscribedTopicDescriber describer) {
            this.groupSpec = groupSpec;
            this.describer = describer;
        }

        /**
         * Reads the subscription of the member.
         *
         * @param memberId The member id.
         * @param topics   Receives the numbers of its subscribed topics that exist.
         */
        void subscribedTopics(String memberId, IntList topics) {
            for (Uuid topicId : groupSpec.memberSubscription(memberId).subscribedTopicIds()) {
                var index = indices.get(topicId);
                if (index == null) {
                    // Met for the first time: numbered next, unless it does not exist.
                    int partitionCount = describer.numPartitions(topicId);
                    index = partitionCount < 0 ? -1 : ids.size();
                    if (index >= 0) {
                        ids.add(topicId);
                        partitionCounts.add(partitionCount);
                    }
                    indices.put(topicId, index);
                }
                if (index >= 0) {
                    topics.add(index);
                }
            }
        }
    }

    /**
     * The cohorts found so far, looked up by the topics of their subscription.
     *
     * <p>A subscription is summarized by a hash that does not depend on the order of its topics,
     * and the cohorts with the same hash form a chain. A subscription equals a cohort of its
     * chain when it has as many topics and its member subscribes to all the topics of the cohort,
     * which the last subscriber of every topic tells without sorting the subscription nor copying
     * it.
     */
    private static final class CohortsByTopics {
        /**
         * The topics of every cohort, numbered in the order they were met.
         */
        private final List<int[]> topics = new ArrayList<>();

        /**
         * Per hash, the first cohort of the chain of the cohorts with that hash.
         */
        private final Map<Long, Integer> firstWithHash = new HashMap<>();

        /**
         * Per cohort, the next cohort of the chain of its hash, the previous one created with
         * that hash, or -1.
         */
        private final IntList nextWithSameHash = new IntList(16);

        /**
         * Per topic, the last member read that subscribes to it, or -1. A member writes its own
         * number, so the entries left by the previous members never need to be cleared.
         */
        private final IntList lastSubscriber = new IntList(16);

        /**
         * @param member       The member, read after the members before it.
         * @param memberTopics The topics of its subscription.
         * @return The cohort with the same topics, a new one if there is none.
         */
        int cohortOf(int member, IntList memberTopics) {
            // Marks the topics of the member, to compare them with a cohort without sorting.
            for (int i = 0; i < memberTopics.size(); i++) {
                int topic = memberTopics.get(i);
                while (lastSubscriber.size() <= topic) {
                    lastSubscriber.add(-1);
                }
                lastSubscriber.set(topic, member);
            }
            long hash = hashOf(memberTopics);
            int first = firstWithHash.getOrDefault(hash, -1);
            // Only the cohorts with the same hash can have the same topics.
            for (int cohort = first; cohort >= 0; cohort = nextWithSameHash.get(cohort)) {
                if (hasTopicsOf(cohort, memberTopics.size(), member)) {
                    return cohort;
                }
            }
            // None has: a new cohort, first in the chain of its hash, before the previous first.
            int cohort = topics.size();
            topics.add(memberTopics.toArray());
            nextWithSameHash.add(first);
            firstWithHash.put(hash, cohort);
            return cohort;
        }

        /**
         * @return True if the cohort has exactly the topics of the member, which has that many.
         */
        private boolean hasTopicsOf(int cohort, int memberTopicCount, int member) {
            int[] cohortTopics = topics.get(cohort);
            if (cohortTopics.length != memberTopicCount) {
                return false;
            }
            for (int topic : cohortTopics) {
                if (lastSubscriber.get(topic) != member) {
                    return false;
                }
            }
            return true;
        }

        /**
         * @return A hash of the topics that does not depend on their order: their number plus the
         *         sum of a well spread hash of every topic, so that two subscriptions rarely
         *         collide.
         */
        private static long hashOf(IntList topics) {
            long hash = topics.size();
            for (int i = 0; i < topics.size(); i++) {
                hash += mix(topics.get(i));
            }
            return hash;
        }

        private static long mix(int topic) {
            long z = (topic + 1) * 0x9E3779B97F4A7C15L;
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            return z ^ (z >>> 31);
        }
    }

    private final String[] memberIds;
    private final int[] cohortOfMember;

    private final Uuid[] topicIds;
    private final Map<Uuid, Integer> topicIndices;
    private final int[] partitionCounts;
    private final int[] subscriberCounts;
    private final int[][] cohortsOfTopic;

    private final int[][] cohortMembers;
    private final int[][] cohortTopics;

    /**
     * Reads the members and their subscriptions.
     *
     * @param groupSpec The group spec.
     * @param describer The describer of the subscribed topics.
     */
    GroupModel(GroupSpec groupSpec, SubscribedTopicDescriber describer) {
        memberIds = groupSpec.memberIds().toArray(new String[0]);
        Arrays.sort(memberIds);
        cohortOfMember = new int[memberIds.length];

        var reader = new SubscriptionReader(groupSpec, describer);
        List<int[]> cohorts;
        if (groupSpec.subscriptionType() == SubscriptionType.HOMOGENEOUS || memberIds.length <= 1) {
            // Every member has the subscription of the first one: a single cohort.
            var topics = new IntList(16);
            if (memberIds.length > 0) {
                reader.subscribedTopics(memberIds[0], topics);
            }
            cohorts = List.of(topics.toArray());
        } else {
            cohorts = readCohorts(reader);
        }

        // The reader numbers the topics in the order met, as the topics are only known once every
        // subscription is read. They are numbered in topic id order instead, so that the numbers
        // only depend on the content of the input. The map of the reader is renumbered in place,
        // its topics that do not exist keeping -1.
        topicIds = reader.ids.toArray(new Uuid[0]);
        Arrays.sort(topicIds);
        topicIndices = reader.indices;
        partitionCounts = new int[topicIds.length];
        // Per number in the order met, the number in topic id order.
        var renumbered = new int[topicIds.length];
        for (int topic = 0; topic < topicIds.length; topic++) {
            int readIndex = topicIndices.put(topicIds[topic], topic);
            renumbered[readIndex] = topic;
            partitionCounts[topic] = reader.partitionCounts.get(readIndex);
        }

        // The topics of the cohorts are renumbered too, which is cheaper than reading the
        // subscriptions again, and the subscribers of every topic are counted.
        cohortMembers = membersByCohort(cohorts.size());
        cohortTopics = new int[cohorts.size()][];
        subscriberCounts = new int[topicIds.length];
        for (int cohort = 0; cohort < cohorts.size(); cohort++) {
            var topics = cohorts.get(cohort);
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
     * Reads the subscription of every member and groups the members with the same one into
     * cohorts.
     *
     * @return The topics of every cohort, numbered in the order they were met.
     */
    private List<int[]> readCohorts(SubscriptionReader reader) {
        var cohorts = new CohortsByTopics();
        var topics = new IntList(16);
        for (int member = 0; member < memberIds.length; member++) {
            topics.clear();
            reader.subscribedTopics(memberIds[member], topics);
            cohortOfMember[member] = cohorts.cohortOf(member, topics);
        }
        return cohorts.topics;
    }

    /**
     * @return The members of every cohort, in member order.
     */
    private int[][] membersByCohort(int cohortCount) {
        // Counted first, so that every array is created with its size.
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
        // Counted first, so that every array is created with its size.
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
     * @return The number of the topic, or -1 if nobody subscribes to it or it does not exist.
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
     * @return The number of members subscribing to the topic, at least one.
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
     * @return True if the member subscribes to the topic.
     */
    boolean subscribes(int member, int topic) {
        return Arrays.binarySearch(cohortTopics[cohortOfMember[member]], topic) >= 0;
    }
}
