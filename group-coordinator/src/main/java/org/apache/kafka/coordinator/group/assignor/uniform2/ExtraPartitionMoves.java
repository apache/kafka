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

import java.util.Arrays;

/**
 * The extra partitions of every member, as the balance step moves them.
 *
 * <p>Moving an extra partition of a topic from a giver to a receiver costs one move when the
 * giver owns more than the base partitions of the topic, since it gives up one of the partitions
 * it owns, and saves one when the receiver does, since it keeps one of its partitions that it
 * would have given up. The cost of moving an extra partition of topic {@code t} is thus
 * {@code [giver owns more than the base partitions of t] - [receiver owns more than the base
 * partitions of t]}: -1 saves a move, 0 costs none and 1 costs one. When a giver has to give a
 * receiver an extra partition, any extra partition of a topic that the receiver subscribes to and
 * has no extra partition of balances the sizes the same way, so the cheapest goes. The extra
 * partitions of every member are kept in two lists, those of topics it owns more than the base
 * partitions of, and the free ones, which it can give without giving up any partition.
 *
 * <p>To find receivers fast, the extra partitions of a giver are also counted per cohort, those of
 * topics that the cohort subscribes to: a member of the cohort having fewer extra partitions than
 * this count lacks one of these topics, and can take one of them, see {@link #keepCounts}.
 */
final class ExtraPartitionMoves {
    /**
     * The members and topics of the group.
     */
    private final GroupModel group;

    /**
     * The shares, whose extra partitions move.
     */
    private final Shares shares;

    /**
     * Per member, the topics of which it gets an extra partition and owns more than the base
     * partitions: giving one of them costs a move.
     */
    private final IntList[] ownedExtras;

    /**
     * Per member, the other topics of which it gets an extra partition: giving one of them costs
     * no move.
     */
    private final IntList[] freeExtras;

    /**
     * Per member, the topics having extra partitions of which it owns more than the base
     * partitions: receiving an extra partition of one of them saves a move.
     */
    private final IntList[] ownedAboveBase;

    /**
     * Per cohort, the number of its topics having extra partitions: a member with an extra
     * partition of every one of them cannot take another.
     */
    private final int[] topicsWithExtras;

    /**
     * Whether every member keeps its counts per cohort: per cohort, the number of its extra
     * partitions of topics that the cohort subscribes to, and the cohorts for which this number
     * is positive.
     *
     * <p>When they fit, that is when the number of members times the number of cohorts is at most
     * the number of topics in the subscriptions of all the members, every member keeps its counts:
     * built the first time they are asked for, and kept exact by every move of one of its extra
     * partitions, and by every change of the topics closed in a cohort, see {@link #holders}.
     * Otherwise only the counts of one member at a time are held, in {@link #scratchCounts},
     * computed when they are asked for another member or after any move.
     */
    private final boolean keepCounts;

    /**
     * When the counts are kept, per member, its counts per cohort, or null until asked for.
     */
    private final int[][] cohortCounts;

    /**
     * When the counts are kept, per member, per cohort, the number of its extra partitions of
     * topics closed in the cohort, see {@link #holders}, or null until asked for.
     */
    private final int[][] closedCounts;

    /**
     * When the counts are kept, per member, the cohorts for which its count is positive, in no
     * particular order, or null until asked for.
     */
    private final IntList[] cohortsReached;

    /**
     * When the counts are not kept, the counts per cohort of {@link #scratchMember}.
     */
    private final int[] scratchCounts;

    /**
     * When the counts are not kept, per cohort, the number of extra partitions of
     * {@link #scratchMember} of topics closed in the cohort.
     */
    private final int[] scratchClosed;

    /**
     * When the counts are not kept, the cohorts for which the count of {@link #scratchMember} is
     * positive, in no particular order.
     */
    private final IntList scratchReached = new IntList(16);

    /**
     * The member whose counts the scratch state holds, or -1 when it holds none.
     */
    private int scratchMember = -1;

    /**
     * Per topic having extra partitions, per cohort subscribing to it in the order of
     * {@link GroupModel#cohortsOf}: the number of members of the cohort having an extra partition
     * of the topic, kept exact by every move. The topic is closed in the cohort when every member
     * of the cohort has one: no member of the cohort can take another. Next to the counts per
     * cohort of a member, {@link #closedCounts} counts those of its extra partitions of topics
     * closed in the cohort: when the two are equal, no member of the cohort can take any of its
     * extra partitions.
     */
    private final int[][] holders;

    /**
     * Per cohort, its number of members.
     */
    private final int[] cohortSizes;

    /**
     * Per member, the number of extra partitions it gave.
     */
    private final int[] giveCounts;

    /**
     * Per member as a giver, at {@code 4 * giver + pass}, where each of the four passes of
     * {@link #transfer} resumes in its lists, for the receiver of its last transfer. The topics
     * after the position were passed over: the receiver has an extra partition of them, does not
     * subscribe to them, or does not match the pass. They stay so while the receiver gives nothing
     * and the giver receives nothing, since removing a topic from a list moves the last topic,
     * already passed over, into its slot.
     */
    private final int[] passPositions;

    /**
     * Per member as a giver, the receiver of its last transfer, for which the pass positions
     * hold, or -1.
     */
    private final int[] passReceivers;

    /**
     * Per member as a giver, the number of extra partitions that the receiver of its last
     * transfer had given then: the pass positions only hold while that receiver gives nothing.
     */
    private final int[] passReceiverGiven;

    /**
     * Per cohort, the topics it subscribes to as a bit set, built the first time it is needed,
     * so that testing a subscription costs one array read; only when the bit sets of all the
     * cohorts take no more longs than the subscriptions of all the members hold topics, and
     * otherwise the topics of the cohort are searched.
     */
    private final long[][] subscribedTopics;

    /**
     * Per topic, {@link #mark} when the marked receiver has an extra partition of the topic.
     */
    private final int[] receiverExtraMarks;

    /**
     * Per topic, {@link #mark} when the marked receiver owns more than the base partitions of the
     * topic.
     */
    private final int[] receiverOwnedMarks;

    /**
     * The member whose topics are marked, or -1: the member that last received or was checked as
     * a receiver, until it gives an extra partition.
     */
    private int markedReceiver = -1;

    /**
     * The current mark: a topic is marked when its entry holds it, so that marking another
     * receiver needs no clearing.
     */
    private int mark;

    /**
     * @param group   The members and topics of the group.
     * @param current The current assignment.
     * @param shares  The shares to balance.
     */
    ExtraPartitionMoves(GroupModel group, CurrentAssignment current, Shares shares) {
        this(group, current, shares, countsFit(group), bitsFit(group));
    }

    /**
     * @param group      The members and topics of the group.
     * @param current    The current assignment.
     * @param shares     The shares to balance.
     * @param keepCounts Whether every member keeps its counts per cohort, see {@link #keepCounts}.
     * @param bitsFit    Whether the subscriptions are kept as bit sets, see
     *                   {@link #subscribedTopics}.
     */
    ExtraPartitionMoves(GroupModel group, CurrentAssignment current, Shares shares, boolean keepCounts, boolean bitsFit) {
        this.group = group;
        this.shares = shares;
        int memberCount = group.memberCount();
        int topicCount = group.topicCount();
        ownedExtras = new IntList[memberCount];
        freeExtras = new IntList[memberCount];
        ownedAboveBase = new IntList[memberCount];
        for (int member = 0; member < memberCount; member++) {
            ownedExtras[member] = new IntList(0);
            freeExtras[member] = new IntList(shares.extraCount(member));
            ownedAboveBase[member] = new IntList(0);
        }
        holders = new int[topicCount][];
        cohortSizes = new int[group.cohortCount()];
        for (int cohort = 0; cohort < group.cohortCount(); cohort++) {
            cohortSizes[cohort] = group.membersOf(cohort).length;
        }
        recordExtras(current);
        topicsWithExtras = new int[group.cohortCount()];
        for (int cohort = 0; cohort < group.cohortCount(); cohort++) {
            for (int topic : group.topicsOf(cohort)) {
                topicsWithExtras[cohort] += shares.extraPartitions(topic) > 0 ? 1 : 0;
            }
        }
        giveCounts = new int[memberCount];
        passPositions = new int[4 * memberCount];
        passReceivers = new int[memberCount];
        Arrays.fill(passReceivers, -1);
        passReceiverGiven = new int[memberCount];
        this.keepCounts = keepCounts;
        cohortCounts = new int[memberCount][];
        closedCounts = new int[memberCount][];
        cohortsReached = new IntList[memberCount];
        scratchCounts = new int[group.cohortCount()];
        scratchClosed = new int[group.cohortCount()];
        subscribedTopics = bitsFit ? new long[group.cohortCount()][] : null;
        receiverExtraMarks = new int[topicCount];
        receiverOwnedMarks = new int[topicCount];
    }

    /**
     * Records the extra partitions of every member, and the topics with extra partitions of which
     * it owns more than the base partitions.
     */
    private void recordExtras(CurrentAssignment current) {
        var ownedCounts = new int[group.memberCount()];
        var owners = new IntList(16);
        for (int topic = 0; topic < group.topicCount(); topic++) {
            if (shares.extraPartitions(topic) == 0) {
                continue;
            }
            current.countOwned(topic, ownedCounts, owners);
            var members = shares.membersWithExtra(topic);
            holders[topic] = new int[group.cohortsOf(topic).length];
            for (int i = 0; i < members.size(); i++) {
                int member = members.get(i);
                (ownedCounts[member] > shares.basePartitions(topic) ? ownedExtras : freeExtras)[member].add(topic);
                holders[topic][cohortIndex(topic, group.cohortOf(member))]++;
            }
            for (int i = 0; i < owners.size(); i++) {
                int owner = owners.get(i);
                if (ownedCounts[owner] > shares.basePartitions(topic)) {
                    ownedAboveBase[owner].add(topic);
                }
                // Reset for the next topic, as countOwned requires.
                ownedCounts[owner] = 0;
            }
        }
    }

    /**
     * @return True if the counts per cohort of every member fit: the number of members times the
     *         number of cohorts is at most the number of topics in the subscriptions of all the
     *         members, see {@link #keepCounts}.
     */
    static boolean countsFit(GroupModel group) {
        return (long) group.memberCount() * group.cohortCount() <= subscriptionTopics(group);
    }

    /**
     * @return True if the bit sets of the subscriptions of all the cohorts take no more longs than
     *         the subscriptions of all the members hold topics, see {@link #subscribedTopics}.
     */
    static boolean bitsFit(GroupModel group) {
        // A bit set of the topics takes one long per 64 topics.
        return (long) group.cohortCount() * ((group.topicCount() + 63) >>> 6) <= subscriptionTopics(group);
    }

    /**
     * @return The number of topics in the subscriptions of all the members.
     */
    private static long subscriptionTopics(GroupModel group) {
        long topics = 0;
        for (int cohort = 0; cohort < group.cohortCount(); cohort++) {
            topics += (long) group.membersOf(cohort).length * group.topicsOf(cohort).length;
        }
        return topics;
    }

    /**
     * @return True if the member has a free extra partition, of a topic it does not own more
     *         than the base partitions of: giving it costs no move.
     */
    boolean hasFreeExtra(int member) {
        return !freeExtras[member].isEmpty();
    }

    /**
     * @return True if the member owns more than the base partitions of a topic having extra
     *         partitions: receiving an extra partition of such a topic saves a move.
     */
    boolean ownsAboveBase(int member) {
        return !ownedAboveBase[member].isEmpty();
    }

    /**
     * @return True if the member gets an extra partition of every topic of its cohort having
     *         extra partitions, so that it cannot take another one.
     */
    boolean isFull(int member) {
        return shares.extraCount(member) == topicsWithExtras[group.cohortOf(member)];
    }

    /**
     * @return The cohorts subscribing to a topic of an extra partition of the member, in no
     *         particular order. The list must not be changed.
     */
    IntList cohortsReached(int member) {
        countPerCohort(member);
        return keepCounts ? cohortsReached[member] : scratchReached;
    }

    /**
     * @return The number of extra partitions of the member of topics that the cohort subscribes
     *         to.
     */
    int extrasFor(int member, int cohort) {
        if (keepCounts || member == scratchMember) {
            return countPerCohort(member)[cohort];
        }
        // One count of another member than the scratch one: counted directly.
        return subscribedCount(cohort, ownedExtras[member]) + subscribedCount(cohort, freeExtras[member]);
    }

    /**
     * @return The number of the topics that the cohort subscribes to.
     */
    private int subscribedCount(int cohort, IntList topics) {
        int count = 0;
        for (int i = 0; i < topics.size(); i++) {
            count += cohortSubscribes(cohort, topics.get(i)) ? 1 : 0;
        }
        return count;
    }

    /**
     * @return True if every member of the cohort has an extra partition of every topic of an
     *         extra partition of the member that the cohort subscribes to: no member of the cohort
     *         can take one of them.
     */
    boolean allHeldBy(int member, int cohort) {
        int count = countPerCohort(member)[cohort];
        return (keepCounts ? closedCounts[member] : scratchClosed)[cohort] == count;
    }

    /**
     * @return True if the receiver subscribes to a topic of an extra partition of the giver and
     *         has no extra partition of it. The receiver has extra partitions only of topics of
     *         its cohort, so it surely can when the giver has more extra partitions of these
     *         topics than the receiver has in all; otherwise, the topics are compared.
     */
    boolean canTake(int giver, int receiver) {
        if (extrasFor(giver, group.cohortOf(receiver)) > shares.extraCount(receiver)) {
            return true;
        }
        markReceiver(receiver);
        return canTakeOne(ownedExtras[giver], receiver) || canTakeOne(freeExtras[giver], receiver);
    }

    /**
     * @return True if the receiver, whose topics are marked, subscribes to one of the topics and
     *         has no extra partition of it.
     */
    private boolean canTakeOne(IntList topics, int receiver) {
        for (int i = 0; i < topics.size(); i++) {
            int topic = topics.get(i);
            if (receiverExtraMarks[topic] != mark && subscribes(receiver, topic)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Moves extra partitions from the giver to the receiver, of topics that the receiver
     * subscribes to and has no extra partition of, the cheapest first, see the class
     * documentation. For each cost, the lists of the giver are tried from their last topic.
     *
     * @throws IllegalStateException If the giver has fewer such extra partitions than the amount.
     */
    void transfer(int giver, int receiver, int amount) {
        if (giver == markedReceiver) {
            markedReceiver = -1;
        }
        markReceiver(receiver);
        // The passed over topics only stay so for the same receiver, which gave nothing since.
        if (passReceivers[giver] != receiver || passReceiverGiven[giver] != giveCounts[receiver]) {
            passReceivers[giver] = receiver;
            passReceiverGiven[giver] = giveCounts[receiver];
            Arrays.fill(passPositions, 4 * giver, 4 * giver + 4, Integer.MAX_VALUE);
        }
        // The passes, the cheapest first: the free extra partitions of topics the receiver owns more
        // than the base partitions of, which save a move, the other free ones and the owned ones of
        // such topics, which cost none, then the other owned ones, which cost one.
        boolean receiverOwns = !ownedAboveBase[receiver].isEmpty();
        if (receiverOwns) {
            amount = give(giver, freeExtras[giver], 4 * giver, receiver, true, amount);
        }
        amount = give(giver, freeExtras[giver], 4 * giver + 1, receiver, false, amount);
        if (receiverOwns) {
            amount = give(giver, ownedExtras[giver], 4 * giver + 2, receiver, true, amount);
        }
        amount = give(giver, ownedExtras[giver], 4 * giver + 3, receiver, false, amount);
        if (amount > 0) {
            throw new IllegalStateException("Member " + group.memberId(giver) + " has too few extra partitions to give.");
        }
    }

    /**
     * Moves up to the amount of extra partitions from the list of the giver to the receiver,
     * whose topics are marked, of topics that the receiver subscribes to, has no extra partition
     * of, and owns more than the base partitions of or not, as asked. The list is tried from the
     * position of the pass, see {@link #passPositions}.
     *
     * @return The amount left to move.
     */
    private int give(int giver, IntList topics, int pass, int receiver, boolean receiverOwns, int amount) {
        boolean sameCohort = group.cohortOf(giver) == group.cohortOf(receiver);
        // Backwards, since remove moves the last topic, already seen, into the slot.
        int i = Math.min(topics.size() - 1, passPositions[pass]);
        for (; i >= 0 && amount > 0; i--) {
            int topic = topics.get(i);
            if (receiverExtraMarks[topic] == mark || (receiverOwnedMarks[topic] == mark) != receiverOwns
                    || !sameCohort && !subscribes(receiver, topic)) {
                continue;
            }
            shares.moveExtraPartition(giver, topic, receiver);
            giveCounts[giver]++;
            passReceivers[receiver] = -1;
            topics.remove(i);
            (receiverOwns ? ownedExtras : freeExtras)[receiver].add(topic);
            receiverExtraMarks[topic] = mark;
            countMove(giver, topic, receiver);
            amount--;
        }
        passPositions[pass] = i;
        return amount;
    }

    /**
     * Marks the topics of the receiver, unless they are marked already.
     */
    private void markReceiver(int receiver) {
        if (receiver == markedReceiver) {
            return;
        }
        mark++;
        markTopics(ownedExtras[receiver], receiverExtraMarks);
        markTopics(freeExtras[receiver], receiverExtraMarks);
        markTopics(ownedAboveBase[receiver], receiverOwnedMarks);
        markedReceiver = receiver;
    }

    /**
     * @return The counts per cohort of the member, see {@link #keepCounts}.
     */
    private int[] countPerCohort(int member) {
        if (!keepCounts) {
            // Only the counts of one member are held: recounted for another member.
            if (member != scratchMember) {
                for (int i = 0; i < scratchReached.size(); i++) {
                    scratchCounts[scratchReached.get(i)] = 0;
                    scratchClosed[scratchReached.get(i)] = 0;
                }
                scratchReached.clear();
                count(member, scratchCounts, scratchClosed, scratchReached);
                scratchMember = member;
            }
            return scratchCounts;
        }
        var counts = cohortCounts[member];
        if (counts == null) {
            counts = new int[group.cohortCount()];
            var closed = new int[group.cohortCount()];
            var reached = new IntList(4);
            count(member, counts, closed, reached);
            cohortCounts[member] = counts;
            closedCounts[member] = closed;
            cohortsReached[member] = reached;
        }
        return counts;
    }

    /**
     * Counts the extra partitions of the member per cohort, and those of topics closed in the
     * cohort, into counts all zero and an empty list.
     */
    private void count(int member, int[] counts, int[] closed, IntList reached) {
        countTopics(ownedExtras[member], counts, closed, reached);
        countTopics(freeExtras[member], counts, closed, reached);
    }

    /**
     * Adds the extra partitions of the topics to the counts per cohort.
     */
    private void countTopics(IntList topics, int[] counts, int[] closed, IntList reached) {
        for (int i = 0; i < topics.size(); i++) {
            int topic = topics.get(i);
            int[] cohorts = group.cohortsOf(topic);
            int[] topicHolders = holders[topic];
            for (int index = 0; index < cohorts.length; index++) {
                int cohort = cohorts[index];
                if (counts[cohort]++ == 0) {
                    reached.add(cohort);
                }
                closed[cohort] += topicHolders[index] == cohortSizes[cohort] ? 1 : 0;
            }
        }
    }

    /**
     * Drops from the cohorts reached by the member, in one pass, those its extra partitions no
     * longer reach. The order of the cohorts reached does not matter.
     */
    private void dropUnreachedCohorts(int member) {
        var reached = cohortsReached[member];
        int[] counts = cohortCounts[member];
        int kept = 0;
        for (int i = 0; i < reached.size(); i++) {
            int cohort = reached.get(i);
            if (counts[cohort] > 0) {
                reached.set(kept++, cohort);
            }
        }
        reached.truncate(kept);
    }

    /**
     * @return True if every member of the cohort at the index of the cohorts of the topic has an
     *         extra partition of the topic.
     */
    private boolean isClosed(int topic, int index) {
        return holders[topic][index] == cohortSizes[group.cohortsOf(topic)[index]];
    }

    /**
     * @return The index of the cohort among the cohorts subscribing to the topic.
     */
    private int cohortIndex(int topic, int cohort) {
        return Arrays.binarySearch(group.cohortsOf(topic), cohort);
    }

    /**
     * Keeps the counts per cohort, see {@link #keepCounts}, and the holders of the topic, see
     * {@link #holders}, exact for the move of an extra partition of the topic from the giver to
     * the receiver, called once the move is in the shares.
     */
    private void countMove(int giver, int topic, int receiver) {
        int[] cohorts = group.cohortsOf(topic);
        // The giver no longer has the topic, as the topics were before the move.
        if (keepCounts && cohortCounts[giver] != null) {
            boolean unreached = false;
            for (int index = 0; index < cohorts.length; index++) {
                int cohort = cohorts[index];
                closedCounts[giver][cohort] -= isClosed(topic, index) ? 1 : 0;
                unreached |= --cohortCounts[giver][cohort] == 0;
            }
            if (unreached) {
                dropUnreachedCohorts(giver);
            }
        }
        int giverIndex = cohortIndex(topic, group.cohortOf(giver));
        int receiverIndex = cohortIndex(topic, group.cohortOf(receiver));
        if (giverIndex != receiverIndex) {
            boolean wasClosed = isClosed(topic, giverIndex);
            holders[topic][giverIndex]--;
            if (wasClosed) {
                countClosed(topic, giverIndex, receiver, -1);
            }
            holders[topic][receiverIndex]++;
            if (isClosed(topic, receiverIndex)) {
                countClosed(topic, receiverIndex, receiver, 1);
            }
        }
        // The receiver has the topic, as the topics are after the move.
        if (keepCounts && cohortCounts[receiver] != null) {
            for (int index = 0; index < cohorts.length; index++) {
                int cohort = cohorts[index];
                closedCounts[receiver][cohort] += isClosed(topic, index) ? 1 : 0;
                if (cohortCounts[receiver][cohort]++ == 0) {
                    cohortsReached[receiver].add(cohort);
                }
            }
        }
        // The scratch counts may no longer hold after any move.
        scratchMember = -1;
    }

    /**
     * Adds the delta to the counts of topics closed in the cohort at the index of the cohorts of
     * the topic, for the members other than the receiver having an extra partition of the topic,
     * which closed or opened in the cohort.
     */
    private void countClosed(int topic, int index, int receiver, int delta) {
        if (!keepCounts) {
            return;
        }
        int cohort = group.cohortsOf(topic)[index];
        var members = shares.membersWithExtra(topic);
        for (int i = 0; i < members.size(); i++) {
            int member = members.get(i);
            if (member != receiver && closedCounts[member] != null) {
                closedCounts[member][cohort] += delta;
            }
        }
    }

    /**
     * @return True if the member subscribes to the topic.
     */
    private boolean subscribes(int member, int topic) {
        return cohortSubscribes(group.cohortOf(member), topic);
    }

    /**
     * @return True if the cohort subscribes to the topic.
     */
    private boolean cohortSubscribes(int cohort, int topic) {
        if (subscribedTopics == null) {
            return Arrays.binarySearch(group.topicsOf(cohort), topic) >= 0;
        }
        var topics = subscribedTopics[cohort];
        if (topics == null) {
            topics = new long[(group.topicCount() + 63) >>> 6];
            for (int cohortTopic : group.topicsOf(cohort)) {
                topics[cohortTopic >>> 6] |= 1L << cohortTopic;
            }
            subscribedTopics[cohort] = topics;
        }
        // The bit of the topic is in the long at topic / 64, at topic % 64: a shift of a long only uses
        // the low 6 bits of its distance.
        return (topics[topic >>> 6] & (1L << topic)) != 0;
    }

    /**
     * Marks the topics with the current mark.
     */
    private void markTopics(IntList topics, int[] marks) {
        for (int i = 0; i < topics.size(); i++) {
            marks[topics.get(i)] = mark;
        }
    }
}
