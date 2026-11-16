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
 * The quotas of the members: for every topic, how many of its partitions each subscriber gets.
 *
 * <p>A topic with {@code P} partitions and {@code N} subscribers has a minimum quota of
 * {@code P / N} partitions and {@code P % N} extra partitions, which go to as many distinct
 * subscribers, one each. The quota of a subscriber is thus the minimum quota, or one more when it
 * gets an extra partition, and the assignment size of a member is the sum of its quotas.
 *
 * <p>The quotas start at the minimum quota. The steps deciding the quotas give the extra
 * partitions with {@link #giveExtraPartition} and move them with {@link #moveExtraPartition},
 * every extra partition going to a distinct subscriber of its topic.
 */
public final class Quotas {
    /**
     * The empty list of members with an extra partition shared by all the topics without extra
     * partitions. It never changes: {@link #giveExtraPartition} rejects such a topic, and
     * {@link #moveExtraPartition} a giver without an extra partition of the topic.
     */
    private static final IntList NONE = new IntList(0);

    /**
     * The members and topics of the group.
     */
    private final IndexedGroup group;

    /**
     * Per topic, the minimum number of partitions that every subscriber gets.
     */
    private final int[] minQuotas;

    /**
     * Per topic, the number of subscribers getting one partition more than the minimum quota.
     */
    private final int[] extraPartitions;

    /**
     * Per cohort, the sum of the minimum quotas of its topics: the smallest assignment size of
     * its members.
     */
    private final int[] minAssignmentSizes;

    /**
     * Per topic, the members getting one of its extra partitions.
     */
    private final IntList[] membersWithExtraPartition;

    /**
     * Per member, the number of topics of which it gets an extra partition.
     */
    private final int[] extraPartitionCounts;

    /**
     * Creates the quotas at the minimum quota: no member has an extra partition yet.
     *
     * @param group The members and topics of the group.
     */
    public Quotas(IndexedGroup group) {
        this.group = group;
        int topicCount = group.topicCount();
        minQuotas = new int[topicCount];
        extraPartitions = new int[topicCount];
        membersWithExtraPartition = new IntList[topicCount];
        for (int topic = 0; topic < topicCount; topic++) {
            minQuotas[topic] = group.partitionCount(topic) / group.subscriberCount(topic);
            extraPartitions[topic] = group.partitionCount(topic) % group.subscriberCount(topic);
            membersWithExtraPartition[topic] = extraPartitions[topic] == 0 ? NONE : new IntList(extraPartitions[topic]);
        }
        minAssignmentSizes = new int[group.cohortCount()];
        for (int cohort = 0; cohort < group.cohortCount(); cohort++) {
            for (int topic : group.topicsOf(cohort)) {
                minAssignmentSizes[cohort] += minQuotas[topic];
            }
        }
        extraPartitionCounts = new int[group.memberCount()];
    }

    /**
     * @return The minimum number of partitions of the topic that every subscriber gets.
     */
    public int minQuota(int topic) {
        return minQuotas[topic];
    }

    /**
     * @return The number of subscribers of the topic getting one partition more than the minimum
     *         quota.
     */
    public int extraPartitions(int topic) {
        return extraPartitions[topic];
    }

    /**
     * @return The members getting an extra partition of the topic. The list must not be changed.
     */
    public IntList membersWithExtraPartition(int topic) {
        return membersWithExtraPartition[topic];
    }

    /**
     * @return The number of topics of which the member gets an extra partition.
     */
    public int extraPartitionCount(int member) {
        return extraPartitionCounts[member];
    }

    /**
     * @return The assignment size of the member: the number of partitions it gets over all its
     *         topics.
     */
    public int assignmentSize(int member) {
        return minAssignmentSizes[group.cohortOf(member)] + extraPartitionCounts[member];
    }

    /**
     * @return The sum of the minimum quotas of the topics of the cohort: the smallest assignment
     *         size of its members.
     */
    public int minAssignmentSize(int cohort) {
        return minAssignmentSizes[cohort];
    }

    /**
     * Gives an extra partition of the topic to the member. The member must not already have an
     * extra partition of the topic.
     *
     * @throws IllegalStateException If every extra partition of the topic is given already.
     */
    public void giveExtraPartition(int member, int topic) {
        if (membersWithExtraPartition[topic].size() == extraPartitions[topic]) {
            throw new IllegalStateException("No extra partition of topic " + group.topicId(topic) + " left for member "
                + group.memberId(member));
        }
        membersWithExtraPartition[topic].add(member);
        extraPartitionCounts[member]++;
    }

    /**
     * Moves an extra partition of the topic from the giver to the receiver. The giver must have an
     * extra partition of the topic and the receiver must not already have one.
     *
     * @throws IndexOutOfBoundsException If the giver has no extra partition of the topic.
     */
    public void moveExtraPartition(int giver, int topic, int receiver) {
        var members = membersWithExtraPartition[topic];
        members.set(members.indexOf(giver), receiver);
        extraPartitionCounts[giver]--;
        extraPartitionCounts[receiver]++;
    }
}
