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

import org.apache.kafka.coordinator.group.assignor.uniform2.util.IntList;

/**
 * The shares of the members: for every topic, how many of its partitions each subscriber gets.
 *
 * <p>A topic with {@code P} partitions and {@code N} subscribers gives its {@code P / N} base
 * partitions to every subscriber, and its {@code P % N} extra partitions to as many distinct
 * subscribers, one each. The share of a subscriber is thus the base partitions, or one more when
 * it gets an extra partition, and the assignment size of a member is the sum of its shares.
 *
 * <p>The shares start with the base partitions only. The steps deciding the shares give the extra
 * partitions with {@link #giveExtraPartition} and move them with {@link #moveExtraPartition},
 * every extra partition going to a distinct subscriber of its topic.
 */
final class Shares {
    /**
     * The empty list of members with an extra partition shared by all the topics without extra
     * partitions. It never changes: {@link #giveExtraPartition} rejects such a topic, and
     * {@link #moveExtraPartition} a giver without an extra partition of the topic.
     */
    private static final IntList NONE = new IntList(0);

    /**
     * The members and topics of the group.
     */
    private final GroupModel group;

    /**
     * Per topic, the number of partitions that every subscriber gets.
     */
    private final int[] basePartitions;

    /**
     * Per topic, the number of subscribers getting one partition more than the base partitions.
     */
    private final int[] extraPartitions;

    /**
     * Per cohort, the number of base partitions that every member gets over all its topics.
     */
    private final int[] cohortBaseSizes;

    /**
     * Per topic, the members getting one of its extra partitions.
     */
    private final IntList[] membersWithExtra;

    /**
     * Per member, the number of topics of which it gets an extra partition.
     */
    private final int[] extraCounts;

    /**
     * Creates the shares with the base partitions only: no member has an extra partition yet.
     *
     * @param group The members and topics of the group.
     */
    Shares(GroupModel group) {
        this.group = group;
        int topicCount = group.topicCount();
        basePartitions = new int[topicCount];
        extraPartitions = new int[topicCount];
        membersWithExtra = new IntList[topicCount];
        for (int topic = 0; topic < topicCount; topic++) {
            basePartitions[topic] = group.partitionCount(topic) / group.subscriberCount(topic);
            extraPartitions[topic] = group.partitionCount(topic) % group.subscriberCount(topic);
            membersWithExtra[topic] = extraPartitions[topic] == 0 ? NONE : new IntList(extraPartitions[topic]);
        }
        cohortBaseSizes = new int[group.cohortCount()];
        for (int cohort = 0; cohort < group.cohortCount(); cohort++) {
            for (int topic : group.topicsOf(cohort)) {
                cohortBaseSizes[cohort] += basePartitions[topic];
            }
        }
        extraCounts = new int[group.memberCount()];
    }

    /**
     * @return The number of partitions of the topic that every subscriber gets.
     */
    int basePartitions(int topic) {
        return basePartitions[topic];
    }

    /**
     * @return The number of subscribers of the topic getting one partition more than the base
     *         partitions.
     */
    int extraPartitions(int topic) {
        return extraPartitions[topic];
    }

    /**
     * @return The members getting an extra partition of the topic. The list must not be changed.
     */
    IntList membersWithExtra(int topic) {
        return membersWithExtra[topic];
    }

    /**
     * @return The number of topics of which the member gets an extra partition.
     */
    int extraCount(int member) {
        return extraCounts[member];
    }

    /**
     * @return The assignment size of the member: the number of partitions it gets over all its
     *         topics.
     */
    int size(int member) {
        return cohortBaseSizes[group.cohortOf(member)] + extraCounts[member];
    }

    /**
     * @return The number of partitions each member of the cohort gets as base partitions.
     */
    int cohortBaseSize(int cohort) {
        return cohortBaseSizes[cohort];
    }

    /**
     * Gives an extra partition of the topic to the member, which does not have one yet.
     *
     * @throws IllegalStateException If every extra partition of the topic is given already.
     */
    void giveExtraPartition(int member, int topic) {
        if (membersWithExtra[topic].size() == extraPartitions[topic]) {
            throw new IllegalStateException("No extra partition of topic " + group.topicId(topic) + " left for member "
                + group.memberId(member));
        }
        membersWithExtra[topic].add(member);
        extraCounts[member]++;
    }

    /**
     * Moves an extra partition of the topic from the giver, which has one, to the receiver,
     * which does not.
     *
     * @throws IndexOutOfBoundsException If the giver has no extra partition of the topic.
     */
    void moveExtraPartition(int giver, int topic, int receiver) {
        var members = membersWithExtra[topic];
        members.set(members.indexOf(giver), receiver);
        extraCounts[giver]--;
        extraCounts[receiver]++;
    }
}
