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
package org.apache.kafka.coordinator.group.assignor.uniform2.steps;

import org.apache.kafka.coordinator.group.assignor.uniform2.CurrentAssignment;
import org.apache.kafka.coordinator.group.assignor.uniform2.IndexedGroup;
import org.apache.kafka.coordinator.group.assignor.uniform2.Quotas;
import org.apache.kafka.coordinator.group.assignor.uniform2.util.IntList;
import org.apache.kafka.coordinator.group.assignor.uniform2.util.LongHeap;

import java.util.Arrays;

/**
 * The third step deciding the {@link Quotas}: moves extra partitions until none can move from its
 * member to another subscriber of its topic whose assignment is at least two partitions smaller.
 *
 * <p>An extra partition of a member is owned when the member owns more partitions of its topic
 * than the minimum quota, so that giving it away revokes one of them. It is free otherwise, and
 * giving it away revokes no partition.
 *
 * <p>The step applies a single rule until it no longer applies. The member with the largest
 * assignment, among those with an extra partition that can move, gives one to the smallest member
 * that can take one of its extra partitions. A member can take an extra partition when it
 * subscribes to the topic, has no extra partition of the topic, and is at least two partitions
 * smaller. On equal assignment sizes, a giver with a free extra partition gives first, and a
 * receiver owning more than the minimum quota of a topic with extra partitions receives first,
 * since only such a receiver can keep one more of its partitions. Member order breaks the
 * remaining ties. The giver gives the extra partition whose move revokes the fewest partitions. It
 * tries its free extra partitions first, then its owned ones, and in both lists prefers a topic
 * that the receiver owns more than the minimum quota of. Ties are broken by taking the last such
 * topic in the list of the giver.
 *
 * <p>Every move goes from a member to one with at least two partitions fewer, so it lowers the sum
 * of the squares of the assignment sizes by at least two, and the step ends. It ends when no extra
 * partition can move to a subscriber with at least two partitions fewer, which is the balance
 * property. With every member subscribed to the same topics, all assignment sizes are then within
 * one of each other, since a member with two partitions more than another has an extra partition
 * of a topic the other does not have. Nothing moves when the assignment already has the property.
 *
 * <p>Pairing the largest member with the smallest one across cohorts lets a member give an extra
 * partition to a member of another cohort that needs one. Balancing each cohort on its own could
 * instead take a move in each of the two cohorts.
 *
 * <p>The members with extra partitions wait in a heap, the largest first. A member without a
 * receiver is parked until a move may give it one, see {@link #wake}. Receiving an extra partition
 * also wakes a member, since it is then larger and has a new topic to give. The step ends when the
 * largest giver is less than two partitions above the smallest member.
 *
 * <p>Receivers are searched cohort by cohort, starting with the cohort of the smallest member, and
 * within a cohort from its smallest member. A cohort is skipped when each of its members already
 * has an extra partition of every topic of the giver that the cohort subscribes to. A member is
 * known to be able to take an extra partition of the giver when the member has fewer extra
 * partitions in all than the giver has of topics of the member's cohort. Otherwise, their topics
 * are compared.
 */
public final class BalanceExtraPartitions {
    /**
     * The offset of the free extra partitions in {@link #resumePositions}.
     */
    private static final int FREE = 0;

    /**
     * The offset of the owned extra partitions in {@link #resumePositions}.
     */
    private static final int OWNED = 2;

    /**
     * The members and topics of the group.
     */
    private final IndexedGroup group;

    /**
     * The current assignment, which tells which partitions a move revokes.
     */
    private final CurrentAssignment currentAssignment;

    /**
     * The quotas, which the step updates.
     */
    private final Quotas quotas;

    /**
     * Whether the counts per cohort of every member are kept, in the fields ending in {@code Of},
     * see {@link #countsFit}. Otherwise, only those of {@link #countedMember} are, in the fields
     * starting with {@code counted}. The counts per cohort of a member are, per cohort, the number
     * of its extra partitions of topics that the cohort subscribes to, and the number of those
     * topics that are full in the cohort, see {@link #membersWithExtraPartitionPerCohort}.
     */
    private final boolean keepCounts;

    /**
     * Per member, the topics of its free extra partitions.
     */
    private IntList[] freeExtraPartitions;

    /**
     * Per member, the topics of its owned extra partitions.
     */
    private IntList[] ownedExtraPartitions;

    /**
     * Per member, the topics with extra partitions of which it owns more than the minimum quota.
     */
    private IntList[] ownedAboveMinQuota;

    /**
     * Per topic, {@link #ownedMark} when {@link #ownedMarkedMember} owns more than the minimum
     * quota of the topic.
     */
    private int[] ownedMarks;

    /**
     * The current mark of {@link #ownedMarks}.
     */
    private int ownedMark;

    /**
     * The member whose entries in {@link #ownedMarks} are current, or -1.
     */
    private int ownedMarkedMember;

    /**
     * Per topic with extra partitions, per cohort subscribing to it in the order of
     * {@link IndexedGroup#cohortsOf}, the number of members of the cohort with an extra partition
     * of the topic. The entry of a topic without extra partitions is null. The topic is full in the
     * cohort when every member of the cohort has an extra partition of it, and then no member of
     * the cohort can take another.
     */
    private int[][] membersWithExtraPartitionPerCohort;

    /**
     * Per cohort, its number of members.
     */
    private int[] cohortSizes;

    /**
     * The members that may give, the largest first, see {@link #giverKey}. The heap holds each
     * member with the key it had when it was added. A member whose key changes is added again, and
     * the stale entries are skipped.
     */
    private LongHeap givers;

    /**
     * Per cohort, its members, the smallest first, see {@link #receiverKey}, with the same kind of
     * entries as {@link #givers}.
     */
    private LongHeap[] receivers;

    /**
     * The number of members of each assignment size, at the index of the assignment size minus
     * {@link #initialSmallestSize}. A move from a member of size {@code L} to one of size
     * {@code R <= L - 2} leaves them at {@code L - 1} and {@code R + 1}, so the assignment sizes
     * stay within their range at the start.
     */
    private int[] sizeCounts;

    /**
     * The smallest assignment size at the start.
     */
    private int initialSmallestSize;

    /**
     * The smallest assignment size of any member. It never decreases.
     */
    private int smallestAssignmentSize;

    /**
     * Per member, whether it is parked. A member is parked when no member can take any of its
     * extra partitions, see {@link #wake}.
     */
    private boolean[] isParked;

    /**
     * The parked members by assignment size, at the index of the assignment size minus
     * {@link #initialSmallestSize}, created when the first member is parked. A member is listed
     * under the assignment size it had when it was parked, and the entry only holds while the
     * member stays parked at that size.
     */
    private IntList[] parkedBySize;

    /**
     * When the counts per cohort are kept, per member, per cohort, the number of its extra
     * partitions of topics that the cohort subscribes to. The entry of a member is null until a
     * receiver is first searched for it.
     */
    private int[][] extraPartitionsPerCohortOf;

    /**
     * When the counts per cohort are kept, per member, per cohort, the number of the topics of its
     * extra partitions that are full in the cohort. The entry of a member is null until a receiver
     * is first searched for it. When it equals the entry of {@link #extraPartitionsPerCohortOf}, no
     * member of the cohort can take an extra partition of the member.
     */
    private int[][] fullTopicsPerCohortOf;

    /**
     * When the counts per cohort are kept, per member, the cohorts subscribing to a topic of its
     * extra partitions. The entry of a member is null until a receiver is first searched for it.
     * It may also hold cohorts whose count dropped to zero since the last search, and may hold a
     * cohort more than once.
     */
    private IntList[] subscribingCohortsOf;

    /**
     * When the counts per cohort are not kept, the number of extra partitions per cohort of
     * {@link #countedMember}, see {@link #extraPartitionsPerCohortOf}.
     */
    private int[] countedExtraPartitionsPerCohort;

    /**
     * When the counts per cohort are not kept, the number of full topics per cohort of
     * {@link #countedMember}, see {@link #fullTopicsPerCohortOf}.
     */
    private int[] countedFullTopicsPerCohort;

    /**
     * When the counts per cohort are not kept, the cohorts subscribing to a topic of the extra
     * partitions of {@link #countedMember}. It may also hold cohorts whose count dropped to zero
     * since the last search.
     */
    private IntList countedSubscribingCohorts;

    /**
     * When the counts per cohort are not kept, the member whose counts are held, or -1.
     */
    private int countedMember;

    /**
     * Per topic, {@link #mark} when {@link #markedMember} has an extra partition of the topic.
     */
    private int[] marks;

    /**
     * The current mark. A topic is marked when its entry in {@link #marks} holds it, so that
     * marking another member needs no clearing.
     */
    private int mark;

    /**
     * The member whose entries in {@link #marks} are current, or -1.
     */
    private int markedMember;

    /**
     * Per member as a giver, the receiver of its last move, or -1. For that receiver, the topics
     * after the positions of {@link #resumePositions} in the lists of the giver were already looked
     * at. They stay so while the receiver gives nothing and the giver receives nothing, since
     * removing a topic from a list moves the last topic, already looked at, into its slot.
     */
    private int[] lastReceiver;

    /**
     * Per member as a giver, the number of extra partitions that {@link #lastReceiver} had given
     * when the member last gave to it.
     */
    private int[] lastReceiverGiven;

    /**
     * Per member, the number of extra partitions it gave.
     */
    private int[] given;

    /**
     * Per member as a giver, the positions in its free ({@link #FREE}) and owned ({@link #OWNED})
     * extra partitions from which the next move to {@link #lastReceiver} searches for a topic. The
     * position at {@code 4 * member + list} is for a topic the receiver can take, and the one at
     * {@code 4 * member + list + 1} for a topic the receiver can take and owns more than the
     * minimum quota of. The topics after a position were already looked at, and the moves do not
     * change why they were passed over.
     */
    private int[] resumePositions;

    /**
     * The members taken out of a heap of receivers while searching it for a receiver. They are put
     * back after the search.
     */
    private IntList skipped;

    /**
     * The receiver keys of the smallest member of each cohort searched for a receiver, the
     * smallest first, see {@link #receiverKey}.
     */
    private LongHeap smallestMembersOfCohorts;

    /**
     * Creates the step, which keeps the counts per cohort of every member when they fit, see
     * {@link #countsFit}.
     *
     * @param group             The members and topics of the group.
     * @param currentAssignment The current assignment.
     * @param quotas            The quotas to balance, as the previous steps left them.
     */
    public BalanceExtraPartitions(IndexedGroup group, CurrentAssignment currentAssignment, Quotas quotas) {
        this(group, currentAssignment, quotas, countsFit(group));
    }

    /**
     * Creates the step, keeping the counts per cohort of every member or of one member at a time.
     * Tests use it to run the step both ways on the same group.
     *
     * @param group             The members and topics of the group.
     * @param currentAssignment The current assignment.
     * @param quotas            The quotas to balance, as the previous steps left them.
     * @param keepCounts        Whether the counts per cohort of every member are kept.
     */
    public BalanceExtraPartitions(
        IndexedGroup group,
        CurrentAssignment currentAssignment,
        Quotas quotas,
        boolean keepCounts
    ) {
        this.group = group;
        this.currentAssignment = currentAssignment;
        this.quotas = quotas;
        this.keepCounts = keepCounts;
    }

    /**
     * Tells whether the counts per cohort of every member fit, which is when they take no more
     * entries than the subscriptions of the members, one per member and subscribed topic. The
     * counts take one entry per member and cohort.
     *
     * @return {@code true} if the number of members times the number of cohorts is at most the sum
     *         of the sizes of the subscriptions of the members.
     */
    static boolean countsFit(IndexedGroup group) {
        long subscribedTopics = 0;
        for (int cohort = 0; cohort < group.cohortCount(); cohort++) {
            subscribedTopics += (long) group.membersOf(cohort).length * group.topicsOf(cohort).length;
        }
        return (long) group.memberCount() * group.cohortCount() <= subscribedTopics;
    }

    /**
     * Balances the quotas, see the class documentation. When all assignment sizes are within one
     * of each other, nothing can move, and it returns at once without building any state.
     */
    public void run() {
        if (!mayMove(group, quotas)) {
            return;
        }
        createState();
        balance();
    }

    /**
     * Tells whether an extra partition may move, which needs a member at least two partitions
     * larger than another. With every member subscribed to the same topics, an extra partition can
     * then always move. Otherwise it may not, for example when the two members subscribe to
     * different topics. The step then builds its state and moves nothing.
     *
     * @return {@code false} when all assignment sizes are within one of each other, in which case
     *         nothing can move.
     */
    static boolean mayMove(IndexedGroup group, Quotas quotas) {
        int smallest = Integer.MAX_VALUE;
        // Assignment sizes are not negative, so an empty group returns false.
        int largest = 0;
        for (int member = 0; member < group.memberCount(); member++) {
            smallest = Math.min(smallest, quotas.assignmentSize(member));
            largest = Math.max(largest, quotas.assignmentSize(member));
        }
        return largest - smallest >= 2;
    }

    /**
     * Creates the state of the step from the quotas as the previous steps left them.
     */
    private void createState() {
        int memberCount = group.memberCount();
        cohortSizes = new int[group.cohortCount()];
        for (int cohort = 0; cohort < group.cohortCount(); cohort++) {
            cohortSizes[cohort] = group.membersOf(cohort).length;
        }
        freeExtraPartitions = new IntList[memberCount];
        ownedExtraPartitions = new IntList[memberCount];
        ownedAboveMinQuota = new IntList[memberCount];
        for (int member = 0; member < memberCount; member++) {
            freeExtraPartitions[member] = new IntList(quotas.extraPartitionCount(member));
            ownedExtraPartitions[member] = new IntList(0);
            ownedAboveMinQuota[member] = new IntList(0);
        }
        membersWithExtraPartitionPerCohort = new int[group.topicCount()][];
        recordExtraPartitions();
        createHeaps();
        computeSizeCounts();
        isParked = new boolean[memberCount];
        if (keepCounts) {
            extraPartitionsPerCohortOf = new int[memberCount][];
            fullTopicsPerCohortOf = new int[memberCount][];
            subscribingCohortsOf = new IntList[memberCount];
        } else {
            countedExtraPartitionsPerCohort = new int[group.cohortCount()];
            countedFullTopicsPerCohort = new int[group.cohortCount()];
            countedSubscribingCohorts = new IntList(16);
            countedMember = -1;
        }
        marks = new int[group.topicCount()];
        markedMember = -1;
        ownedMarks = new int[group.topicCount()];
        ownedMarkedMember = -1;
        lastReceiver = new int[memberCount];
        Arrays.fill(lastReceiver, -1);
        lastReceiverGiven = new int[memberCount];
        given = new int[memberCount];
        resumePositions = new int[4 * memberCount];
        skipped = new IntList(16);
        smallestMembersOfCohorts = new LongHeap(group.cohortCount());
    }

    /**
     * Records the free and owned extra partitions of every member,
     * {@link #membersWithExtraPartitionPerCohort}, and the topics with extra partitions that each
     * member owns more than the minimum quota of.
     */
    private void recordExtraPartitions() {
        // Per member, the number of partitions of the topic that it owns.
        var ownedPartitionCounts = new int[group.memberCount()];
        var owners = new IntList(16);
        for (int topic = 0; topic < group.topicCount(); topic++) {
            if (quotas.extraPartitions(topic) == 0) {
                continue;
            }
            currentAssignment.countOwned(topic, ownedPartitionCounts, owners);
            membersWithExtraPartitionPerCohort[topic] = new int[group.cohortsOf(topic).length];
            var members = quotas.membersWithExtraPartition(topic);
            for (int i = 0; i < members.size(); i++) {
                int member = members.get(i);
                boolean owned = ownedPartitionCounts[member] > quotas.minQuota(topic);
                (owned ? ownedExtraPartitions : freeExtraPartitions)[member].add(topic);
                membersWithExtraPartitionPerCohort[topic][cohortIndex(topic, group.cohortOf(member))]++;
            }
            for (int i = 0; i < owners.size(); i++) {
                int owner = owners.get(i);
                if (ownedPartitionCounts[owner] > quotas.minQuota(topic)) {
                    ownedAboveMinQuota[owner].add(topic);
                }
                // Reset for the next topic, as countOwned requires.
                ownedPartitionCounts[owner] = 0;
            }
        }
    }

    /**
     * Creates the heap of the givers, the members with extra partitions, and the heaps of the
     * receivers, every cohort with all its members.
     */
    private void createHeaps() {
        var giverKeys = new long[group.memberCount()];
        int giverCount = 0;
        receivers = new LongHeap[group.cohortCount()];
        for (int cohort = 0; cohort < group.cohortCount(); cohort++) {
            var members = group.membersOf(cohort);
            // The heap keeps this array as its storage. Every move adds its giver and its receiver
            // again to the heaps of their cohorts, so the array has room for as many entries again.
            var receiverKeys = new long[2 * members.length];
            for (int i = 0; i < members.length; i++) {
                receiverKeys[i] = receiverKey(members[i]);
                if (quotas.extraPartitionCount(members[i]) > 0) {
                    giverKeys[giverCount++] = giverKey(members[i]);
                }
            }
            receivers[cohort] = new LongHeap(receiverKeys, members.length);
        }
        givers = new LongHeap(giverKeys, giverCount);
    }

    /**
     * Computes {@link #sizeCounts} and the smallest assignment size at the start.
     */
    private void computeSizeCounts() {
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (int member = 0; member < group.memberCount(); member++) {
            min = Math.min(min, quotas.assignmentSize(member));
            max = Math.max(max, quotas.assignmentSize(member));
        }
        initialSmallestSize = min;
        smallestAssignmentSize = min;
        sizeCounts = new int[max - min + 1];
        for (int member = 0; member < group.memberCount(); member++) {
            sizeCounts[quotas.assignmentSize(member) - min]++;
        }
    }

    /**
     * Moves extra partitions from the largest giver to its receiver until no member can give.
     */
    private void balance() {
        while (!givers.isEmpty()) {
            long key = givers.poll();
            int giver = member(key);
            if (key != giverKey(giver) || isParked[giver] || quotas.extraPartitionCount(giver) == 0) {
                continue;
            }
            // The other givers are no larger, and the parked members have no receiver either. Once
            // the largest giver is less than two partitions above the smallest member, no member
            // can give any more.
            if (quotas.assignmentSize(giver) - 2 < smallestAssignmentSize) {
                return;
            }
            int receiver = findReceiver(giver);
            if (receiver < 0) {
                park(giver);
                continue;
            }
            move(giver, receiver);
            // The receiver has a new extra partition to give, and both members changed size.
            isParked[receiver] = false;
            givers.add(giverKey(receiver));
            receivers[group.cohortOf(receiver)].add(receiverKey(receiver));
            if (quotas.extraPartitionCount(giver) > 0) {
                givers.add(giverKey(giver));
            }
            receivers[group.cohortOf(giver)].add(receiverKey(giver));
            wake(giver);
        }
    }

    /**
     * Finds the receiver of an extra partition of the giver. It is the first member in the order of
     * {@link #receiverKey} among the members at least two partitions smaller that can take an
     * extra partition of the giver. The cohorts are searched from the one with the smallest member,
     * so that the members of the other cohorts are mostly compared by assignment size only.
     *
     * @return The receiver, or -1 if there is none.
     */
    private int findReceiver(int giver) {
        // The largest assignment size that a receiver may have.
        int maxSize = quotas.assignmentSize(giver) - 2;
        // Per cohort, the number of extra partitions of the giver of topics that the cohort
        // subscribes to, and the number of those topics that are full in the cohort.
        int[] extraPartitions = extraPartitionsPerCohort(giver);
        int[] fullTopics = fullTopicsPerCohort(giver);
        var cohorts = subscribingCohorts(giver);
        smallestMembersOfCohorts.clear();
        int kept = 0;
        for (int i = 0; i < cohorts.size(); i++) {
            int cohort = cohorts.get(i);
            // The cohorts no longer subscribing to a topic of the giver are dropped on the way.
            if (extraPartitions[cohort] == 0) {
                continue;
            }
            cohorts.set(kept++, cohort);
            // In a cohort where every topic of the giver is full, no member can take one.
            if (fullTopics[cohort] == extraPartitions[cohort]) {
                continue;
            }
            long key = peekSmallestKey(cohort);
            if (key >= 0 && quotas.assignmentSize(member(key)) <= maxSize) {
                smallestMembersOfCohorts.add(key);
            }
        }
        cohorts.truncate(kept);
        // The first receiver found so far in the order of receiverKey, or -1.
        int receiver = -1;
        while (!smallestMembersOfCohorts.isEmpty()) {
            long key = smallestMembersOfCohorts.poll();
            if (receiver >= 0 && key > receiverKey(receiver)) {
                break;
            }
            int cohortReceiver = findReceiverInCohort(giver, group.cohortOf(member(key)), maxSize, receiver);
            if (cohortReceiver >= 0) {
                receiver = cohortReceiver;
            }
        }
        return receiver;
    }

    /**
     * Drops the stale entries at the top of the heap of receivers of the cohort.
     *
     * @return The key of the first member of the cohort in the order of {@link #receiverKey}, or -1
     *         if there is none.
     */
    private long peekSmallestKey(int cohort) {
        var heap = receivers[cohort];
        while (!heap.isEmpty()) {
            long key = heap.peek();
            if (key == receiverKey(member(key))) {
                return key;
            }
            heap.poll();
        }
        return -1;
    }

    /**
     * Finds the first member of the cohort in the order of {@link #receiverKey} that can take an
     * extra partition of the giver. The member must have at most {@code maxSize} partitions and,
     * if there is a best receiver so far, come before it.
     *
     * @return The member, or -1 if there is none.
     */
    private int findReceiverInCohort(int giver, int cohort, int maxSize, int bestReceiver) {
        var heap = receivers[cohort];
        int receiver = -1;
        long key;
        while ((key = peekSmallestKey(cohort)) >= 0) {
            int member = member(key);
            if (quotas.assignmentSize(member) > maxSize || bestReceiver >= 0 && key > receiverKey(bestReceiver)) {
                break;
            }
            if (canTake(giver, member)) {
                receiver = member;
                break;
            }
            skipped.add(member);
            heap.poll();
        }
        for (int i = 0; i < skipped.size(); i++) {
            heap.add(receiverKey(skipped.get(i)));
        }
        skipped.clear();
        return receiver;
    }

    /**
     * @return {@code true} if the receiver subscribes to a topic of an extra partition of the giver
     *         and has no extra partition of it.
     */
    private boolean canTake(int giver, int receiver) {
        // The receiver only has extra partitions of topics of its cohort. When the giver has more
        // of these topics than the receiver has extra partitions, the receiver lacks one of them.
        if ((keepCounts || giver == countedMember)
                && extraPartitionsPerCohort(giver)[group.cohortOf(receiver)] > quotas.extraPartitionCount(receiver)) {
            return true;
        }
        markExtraPartitions(receiver);
        return canTakeOne(freeExtraPartitions[giver], receiver) || canTakeOne(ownedExtraPartitions[giver], receiver);
    }

    /**
     * Tells whether the receiver can take an extra partition of one of the topics. The extra
     * partitions of the receiver must be marked.
     *
     * @return {@code true} if the receiver subscribes to one of the topics and has no extra
     *         partition of it.
     */
    private boolean canTakeOne(IntList topics, int receiver) {
        for (int i = 0; i < topics.size(); i++) {
            int topic = topics.get(i);
            if (marks[topic] != mark && subscribes(receiver, topic)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Moves an extra partition from the giver to the receiver. The receiver must be able to take
     * an extra partition of the giver. The giver gives a free extra partition if the receiver can
     * take one, and an owned one otherwise. In both lists, it prefers a topic that the receiver
     * owns more than the minimum quota of, which revokes one partition fewer from the receiver.
     */
    private void move(int giver, int receiver) {
        markExtraPartitions(receiver);
        markOwnedAboveMinQuota(receiver);
        // The topics already looked at for the same receiver need not be looked at again.
        if (lastReceiver[giver] != receiver || lastReceiverGiven[giver] != given[receiver]) {
            lastReceiver[giver] = receiver;
            lastReceiverGiven[giver] = given[receiver];
            Arrays.fill(resumePositions, 4 * giver, 4 * giver + 4, Integer.MAX_VALUE);
        }
        var topics = freeExtraPartitions[giver];
        int chosen = chooseTopicIndex(topics, receiver, 4 * giver + FREE);
        if (chosen < 0) {
            topics = ownedExtraPartitions[giver];
            chosen = chooseTopicIndex(topics, receiver, 4 * giver + OWNED);
        }
        int topic = topics.get(chosen);
        updateCountsForMove(giver, topic, receiver);
        quotas.moveExtraPartition(giver, topic, receiver);
        topics.remove(chosen);
        given[giver]++;
        // The extra partitions of the receiver grow, so what it looked at as a giver no longer
        // holds.
        lastReceiver[receiver] = -1;
        (ownsAboveMinQuota(topic) ? ownedExtraPartitions : freeExtraPartitions)[receiver].add(topic);
        // The receiver is the marked member, so its marks stay exact.
        marks[topic] = mark;
        updateSizeCounts(giver, receiver);
    }

    /**
     * Chooses the topic to move among the topics, the free or the owned extra partitions of the
     * giver. It prefers a topic that the receiver owns more than the minimum quota of, and breaks
     * ties by taking the last one in the list. The extra partitions of the receiver and its topics
     * owned more than the minimum quota of must be marked. The search starts from the resume
     * positions at the position, which it updates.
     *
     * @return The index of the chosen topic in the topics, or -1 if the receiver can take none.
     */
    private int chooseTopicIndex(IntList topics, int receiver, int position) {
        if (!ownedAboveMinQuota[receiver].isEmpty()) {
            int chosen = findLastTopicIndex(topics, receiver, resumePositions[position + 1], true);
            // The topic moved into the slot of the chosen one, if any, was already looked at.
            resumePositions[position + 1] = chosen < 0 ? -1 : chosen - 1;
            if (chosen >= 0) {
                return chosen;
            }
        }
        int chosen = findLastTopicIndex(topics, receiver, resumePositions[position], false);
        resumePositions[position] = chosen < 0 ? -1 : chosen - 1;
        return chosen;
    }

    /**
     * Finds the last topic, at most at the index {@code from}, that the receiver can take and, if
     * {@code ownedByReceiver}, owns more than the minimum quota of. The extra partitions of the
     * receiver must be marked, and so must its topics owned more than the minimum quota of if
     * {@code ownedByReceiver}.
     *
     * @return The index of the topic in the topics, or -1 if there is none.
     */
    private int findLastTopicIndex(IntList topics, int receiver, int from, boolean ownedByReceiver) {
        for (int i = Math.min(topics.size() - 1, from); i >= 0; i--) {
            int topic = topics.get(i);
            if (marks[topic] != mark && subscribes(receiver, topic) && (!ownedByReceiver || ownsAboveMinQuota(topic))) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Updates {@link #membersWithExtraPartitionPerCohort} and the counts per cohort for the move of
     * an extra partition of the topic from the giver to the receiver. It must be called before the
     * move.
     */
    private void updateCountsForMove(int giver, int topic, int receiver) {
        int[] cohorts = group.cohortsOf(topic);
        // The search for a receiver counted the giver just before the move. The giver loses the
        // topic, as it was before the move.
        int[] extraPartitions = extraPartitionsPerCohort(giver);
        int[] fullTopics = fullTopicsPerCohort(giver);
        for (int index = 0; index < cohorts.length; index++) {
            extraPartitions[cohorts[index]]--;
            fullTopics[cohorts[index]] -= isFull(topic, index) ? 1 : 0;
        }
        updateMembersWithExtraPartitionPerCohort(giver, topic, receiver);
        // The receiver gets the topic, as it is after the move.
        if (keepCounts && extraPartitionsPerCohortOf[receiver] != null) {
            for (int index = 0; index < cohorts.length; index++) {
                if (extraPartitionsPerCohortOf[receiver][cohorts[index]]++ == 0) {
                    subscribingCohortsOf[receiver].add(cohorts[index]);
                }
                fullTopicsPerCohortOf[receiver][cohorts[index]] += isFull(topic, index) ? 1 : 0;
            }
        }
    }

    /**
     * Moves a member with an extra partition of the topic from the cohort of the giver to the
     * cohort of the receiver, in {@link #membersWithExtraPartitionPerCohort}. When the topic stops
     * being full in the first cohort or becomes full in the second, the counts of full topics kept
     * for the other members with an extra partition of the topic are updated.
     */
    private void updateMembersWithExtraPartitionPerCohort(int giver, int topic, int receiver) {
        int giverIndex = cohortIndex(topic, group.cohortOf(giver));
        int receiverIndex = cohortIndex(topic, group.cohortOf(receiver));
        if (giverIndex == receiverIndex) {
            return;
        }
        boolean wasFull = isFull(topic, giverIndex);
        membersWithExtraPartitionPerCohort[topic][giverIndex]--;
        membersWithExtraPartitionPerCohort[topic][receiverIndex]++;
        boolean becomesFull = isFull(topic, receiverIndex);
        if (!keepCounts || !wasFull && !becomesFull) {
            return;
        }
        int giverCohort = group.cohortOf(giver);
        int receiverCohort = group.cohortOf(receiver);
        var members = quotas.membersWithExtraPartition(topic);
        for (int i = 0; i < members.size(); i++) {
            int member = members.get(i);
            if (member != giver && fullTopicsPerCohortOf[member] != null) {
                fullTopicsPerCohortOf[member][giverCohort] -= wasFull ? 1 : 0;
                fullTopicsPerCohortOf[member][receiverCohort] += becomesFull ? 1 : 0;
            }
        }
    }

    /**
     * @return {@code true} if every member of the cohort at the index of the cohorts of the topic
     *         has an extra partition of the topic.
     */
    private boolean isFull(int topic, int index) {
        return membersWithExtraPartitionPerCohort[topic][index] == cohortSizes[group.cohortsOf(topic)[index]];
    }

    /**
     * Returns the number of full topics per cohort of the member, see
     * {@link #fullTopicsPerCohortOf}. It must be called after {@link #extraPartitionsPerCohort} for
     * the member.
     */
    private int[] fullTopicsPerCohort(int member) {
        return keepCounts ? fullTopicsPerCohortOf[member] : countedFullTopicsPerCohort;
    }

    /**
     * Returns the number of extra partitions per cohort of the member, see
     * {@link #extraPartitionsPerCohortOf}. The counts per cohort of the member are computed when
     * asked for the first time or, when they are not kept, when asked for another member than the
     * last one.
     */
    private int[] extraPartitionsPerCohort(int member) {
        if (keepCounts) {
            if (extraPartitionsPerCohortOf[member] == null) {
                extraPartitionsPerCohortOf[member] = new int[group.cohortCount()];
                fullTopicsPerCohortOf[member] = new int[group.cohortCount()];
                subscribingCohortsOf[member] = new IntList(4);
                computeCounts(
                    member,
                    extraPartitionsPerCohortOf[member],
                    fullTopicsPerCohortOf[member],
                    subscribingCohortsOf[member]
                );
            }
            return extraPartitionsPerCohortOf[member];
        }
        if (member != countedMember) {
            for (int i = 0; i < countedSubscribingCohorts.size(); i++) {
                countedExtraPartitionsPerCohort[countedSubscribingCohorts.get(i)] = 0;
                countedFullTopicsPerCohort[countedSubscribingCohorts.get(i)] = 0;
            }
            countedSubscribingCohorts.clear();
            computeCounts(member, countedExtraPartitionsPerCohort, countedFullTopicsPerCohort, countedSubscribingCohorts);
            countedMember = member;
        }
        return countedExtraPartitionsPerCohort;
    }

    /**
     * Returns the cohorts subscribing to a topic of the extra partitions of the member, see
     * {@link #subscribingCohortsOf}. It must be called after {@link #extraPartitionsPerCohort} for
     * the member.
     */
    private IntList subscribingCohorts(int member) {
        return keepCounts ? subscribingCohortsOf[member] : countedSubscribingCohorts;
    }

    /**
     * Computes the counts per cohort of the member, see {@link #keepCounts}. The counts must be
     * zeroed and the list of cohorts empty.
     */
    private void computeCounts(int member, int[] extraPartitions, int[] fullTopics, IntList cohorts) {
        addToCounts(freeExtraPartitions[member], extraPartitions, fullTopics, cohorts);
        addToCounts(ownedExtraPartitions[member], extraPartitions, fullTopics, cohorts);
    }

    /**
     * Adds the topics to the counts per cohort, and the cohorts they reach for the first time to
     * the list of cohorts.
     */
    private void addToCounts(IntList topics, int[] extraPartitions, int[] fullTopics, IntList cohorts) {
        for (int i = 0; i < topics.size(); i++) {
            int topic = topics.get(i);
            int[] topicCohorts = group.cohortsOf(topic);
            int[] membersPerCohort = membersWithExtraPartitionPerCohort[topic];
            for (int index = 0; index < topicCohorts.length; index++) {
                int cohort = topicCohorts[index];
                if (extraPartitions[cohort]++ == 0) {
                    cohorts.add(cohort);
                }
                fullTopics[cohort] += membersPerCohort[index] == cohortSizes[cohort] ? 1 : 0;
            }
        }
    }

    /**
     * Updates {@link #sizeCounts} and {@link #smallestAssignmentSize} after a move from the giver,
     * now one partition smaller, to the receiver, now one partition larger. It must be called after
     * the move.
     */
    private void updateSizeCounts(int giver, int receiver) {
        int giverIndex = quotas.assignmentSize(giver) - initialSmallestSize;
        int receiverIndex = quotas.assignmentSize(receiver) - initialSmallestSize;
        sizeCounts[giverIndex + 1]--;
        sizeCounts[giverIndex]++;
        sizeCounts[receiverIndex - 1]--;
        sizeCounts[receiverIndex]++;
        while (sizeCounts[smallestAssignmentSize - initialSmallestSize] == 0) {
            smallestAssignmentSize++;
        }
    }

    /**
     * Parks the giver when no receiver is found for it. A giver at most two partitions above the
     * smallest assignment size is not listed, since no move can wake it. A move leaves its giver
     * above the smallest assignment size, which never decreases, and only wakes members two
     * partitions above its giver, see {@link #wake}.
     */
    private void park(int giver) {
        isParked[giver] = true;
        if (quotas.assignmentSize(giver) - 2 <= smallestAssignmentSize) {
            return;
        }
        if (parkedBySize == null) {
            parkedBySize = new IntList[sizeCounts.length];
        }
        int index = quotas.assignmentSize(giver) - initialSmallestSize;
        if (parkedBySize[index] == null) {
            parkedBySize[index] = new IntList(4);
        }
        parkedBySize[index].add(giver);
    }

    /**
     * Wakes the parked members that can now give to the giver of the last move. These are the
     * members exactly two partitions larger than the giver from which the giver can take an extra
     * partition.
     *
     * <p>A parked member has no receiver, and only a move can give it one. The receiver of the move
     * gets larger and gets an extra partition, which makes it no better a receiver. The giver gets
     * one partition smaller, at an assignment size of {@code s}, and lacks the topic it gave. A
     * parked member can then give to it only through:
     * <ul>
     *     <li>the topic it gave, if the parked member has an extra partition of it and has at least
     *     {@code s + 2} partitions. This never happens, since the receiver of the move lacked the
     *     topic and had at most {@code s - 1} partitions, so it was already a receiver of the
     *     parked member;</li>
     *     <li>another topic that the giver lacks. The giver lacked it before the move too, when it
     *     had {@code s + 1} partitions, and was then no receiver of the parked member. So the
     *     parked member has at most {@code s + 2} partitions, and exactly {@code s + 2} if the
     *     giver is now its receiver.</li>
     * </ul>
     */
    private void wake(int lastGiver) {
        int size = quotas.assignmentSize(lastGiver);
        int index = size + 2 - initialSmallestSize;
        if (parkedBySize == null || index >= parkedBySize.length || parkedBySize[index] == null) {
            return;
        }
        var parked = parkedBySize[index];
        int kept = 0;
        for (int i = 0; i < parked.size(); i++) {
            int parkedMember = parked.get(i);
            if (!isParked[parkedMember] || quotas.assignmentSize(parkedMember) != size + 2) {
                continue;
            }
            if (canTake(parkedMember, lastGiver)) {
                isParked[parkedMember] = false;
                givers.add(giverKey(parkedMember));
            } else {
                parked.set(kept++, parkedMember);
            }
        }
        parked.truncate(kept);
    }

    /**
     * Marks the topics of the extra partitions of the member, unless they are marked already.
     */
    private void markExtraPartitions(int member) {
        if (member == markedMember) {
            return;
        }
        mark++;
        markTopics(freeExtraPartitions[member]);
        markTopics(ownedExtraPartitions[member]);
        markedMember = member;
    }

    /**
     * Marks the topics with the current mark.
     */
    private void markTopics(IntList topics) {
        for (int i = 0; i < topics.size(); i++) {
            marks[topics.get(i)] = mark;
        }
    }

    /**
     * Marks the topics with extra partitions of which the member owns more than the minimum quota,
     * unless they are marked already.
     */
    private void markOwnedAboveMinQuota(int member) {
        if (member == ownedMarkedMember) {
            return;
        }
        ownedMark++;
        var topics = ownedAboveMinQuota[member];
        for (int i = 0; i < topics.size(); i++) {
            ownedMarks[topics.get(i)] = ownedMark;
        }
        ownedMarkedMember = member;
    }

    /**
     * @return {@code true} if the member marked by {@link #markOwnedAboveMinQuota} owns more than
     *         the minimum quota of the topic.
     */
    private boolean ownsAboveMinQuota(int topic) {
        return ownedMarks[topic] == ownedMark;
    }

    /**
     * @return {@code true} if the member subscribes to the topic.
     */
    private boolean subscribes(int member, int topic) {
        // A topic usually has far fewer subscribing cohorts than a cohort has topics, so the
        // cohorts of the topic are searched.
        return Arrays.binarySearch(group.cohortsOf(topic), group.cohortOf(member)) >= 0;
    }

    /**
     * @return The index of the cohort among the cohorts subscribing to the topic.
     */
    private int cohortIndex(int topic, int cohort) {
        return Arrays.binarySearch(group.cohortsOf(topic), cohort);
    }

    /**
     * Returns the key of the member in the min heap of givers. The high 32 bits hold its
     * assignment size, complemented so that the largest comes first. The next bit is set unless
     * the member has a free extra partition, and the low 31 bits hold the member.
     */
    private long giverKey(int member) {
        long notFree = freeExtraPartitions[member].isEmpty() ? 1 : 0;
        return ((long) (Integer.MAX_VALUE - quotas.assignmentSize(member)) << 32) | notFree << 31 | member;
    }

    /**
     * Returns the key of the member in the min heaps of receivers. The high 32 bits hold its
     * assignment size. The next bit is set unless the member owns more than the minimum quota of a
     * topic with extra partitions, and the low 31 bits hold the member.
     */
    private long receiverKey(int member) {
        long notOwning = ownedAboveMinQuota[member].isEmpty() ? 1 : 0;
        return ((long) quotas.assignmentSize(member) << 32) | notOwning << 31 | member;
    }

    /**
     * @return The member of a key of {@link #giverKey} or {@link #receiverKey}.
     */
    private static int member(long key) {
        return (int) (key & Integer.MAX_VALUE);
    }
}
