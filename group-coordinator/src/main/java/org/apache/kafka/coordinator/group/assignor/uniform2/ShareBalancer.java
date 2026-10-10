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
import org.apache.kafka.coordinator.group.assignor.uniform2.util.LongHeap;

/**
 * The balance step of the {@link Shares}: moves extra partitions until none can move from its
 * member to another subscriber of its topic whose assignment is at least two partitions smaller.
 *
 * <p>The step applies a single rule until it no longer applies: the member with the largest
 * assignment among those having an extra partition that can move gives one to the member with
 * the smallest assignment among the subscribers of the topics of its extra partitions that have
 * no extra partition of the topic and are at least two partitions smaller. The giver gives its
 * cheapest extra partition that the receiver can take, see {@link ExtraPartitionMoves}. On equal
 * sizes, a giver having a free extra partition, of a topic it does not own more than the base
 * partitions of, gives first, and a receiver owning more than the base partitions of a topic with
 * extra partitions receives first, since only such a receiver can save a move; then member order
 * decides.
 *
 * <p>Every move goes from a member to one with at least two partitions fewer, which lowers the
 * sum of the squares of the sizes by at least two, so the step ends. It ends when no extra
 * partition can move to a subscriber with at least two partitions fewer, which is the balance
 * property. With a single subscription, all sizes are then within one of each other, since a
 * member with two partitions more than another has an extra partition of a topic the other does
 * not have. Nothing moves when the assignment already has the property.
 *
 * <p>Pairing the largest member with the smallest one, whatever their cohorts, lets a member that
 * has to give up an extra partition give it to a member of another cohort needing one, where
 * balancing every cohort on its own could make both move a partition.
 *
 * <p>The members having extra partitions wait in a heap, the largest first. A member with no
 * receiver is parked, until a move may give it one, see {@link #wake}; receiving an extra
 * partition also wakes a member, since it then has a new topic to give. In a cohort sharing no
 * topic with another, the first giver without a receiver ends the balance of the cohort, see
 * {@link #balance}. The step ends when every member having extra partitions is parked or in a
 * cohort done.
 *
 * <p>The receivers are looked for per cohort, from the cohort with the smallest member, and in a
 * cohort from the smallest member. A cohort of which every member has an extra partition of every
 * topic of the giver that it subscribes to is skipped. A member of the cohort can take an extra
 * partition of the giver when it has fewer extra partitions in all than the giver has of topics
 * of the cohort; otherwise their topics are compared. A member having an extra partition of every
 * topic of its cohort that has extra partitions cannot take any: it is left out of the heap of
 * its cohort until it gives one.
 */
final class ShareBalancer {
    /**
     * The members and topics of the group.
     */
    private final GroupModel group;

    /**
     * The current assignment, which tells the cost of the moves.
     */
    private final CurrentAssignment current;

    /**
     * The shares to balance.
     */
    private final Shares shares;

    /**
     * Whether every member keeps its counts per cohort in the moves, see
     * {@link ExtraPartitionMoves}.
     */
    private final boolean keepCounts;

    /**
     * Whether the moves keep the subscriptions as bit sets, see {@link ExtraPartitionMoves}.
     */
    private final boolean bitsFit;

    /**
     * The extra partitions of the members, as they move.
     */
    private ExtraPartitionMoves moves;

    /**
     * The members that may give, the largest first, see {@link #giverKey}. The heap holds members
     * with the key they had when added: a member whose key changes is added again, and the entries
     * whose key is no longer the member's are skipped.
     */
    private LongHeap givers;

    /**
     * Per cohort, its members, the smallest first, see {@link #receiverKey}, with the same kind of
     * entries as {@link #givers}.
     */
    private LongHeap[] receivers;

    /**
     * The number of members of every size, at the index of the size minus {@link #firstSize}.
     * Every move goes from a size {@code L} to a size {@code R <= L - 2} and leaves the two
     * members at {@code L - 1} and {@code R + 1}, so the sizes stay within those at the start.
     */
    private int[] sizeCounts;

    /**
     * The smallest size at the start.
     */
    private int firstSize;

    /**
     * The smallest size of any member.
     */
    private int smallestSize;

    /**
     * Per cohort, whether it is isolated, none of its topics having a subscriber in another
     * cohort, once known: 0 unknown, 1 isolated, 2 not.
     */
    private byte[] isolation;

    /**
     * Per cohort, whether it is an isolated cohort done, see {@link #balance}.
     */
    private boolean[] isDone;

    /**
     * Per member, whether it is parked: it has extra partitions, and no member could receive one
     * of them, see {@link #wake}.
     */
    private boolean[] isParked;

    /**
     * The parked members by size, at the index of the size minus {@link #firstSize}, created at
     * the first park, with entries that may no longer hold: a member is listed under the size it
     * had when parked, and its entry only holds while it is parked with that size.
     */
    private IntList[] parkedBySize;

    /**
     * Scratch state of the search of a receiver: the members taken out of a heap of receivers.
     */
    private IntList skipped;

    /**
     * Scratch state of the search of a receiver: the keys of the smallest members of the cohorts
     * looked at, the smallest first.
     */
    private LongHeap smallestKeys;

    /**
     * @param group   The members and topics of the group.
     * @param current The current assignment.
     * @param shares  The shares to balance, as the previous steps left them.
     */
    ShareBalancer(GroupModel group, CurrentAssignment current, Shares shares) {
        this(group, current, shares, ExtraPartitionMoves.countsFit(group), ExtraPartitionMoves.bitsFit(group));
    }

    /**
     * @param group      The members and topics of the group.
     * @param current    The current assignment.
     * @param shares     The shares to balance, as the previous steps left them.
     * @param keepCounts Whether every member keeps its counts per cohort, see
     *                   {@link ExtraPartitionMoves}.
     * @param bitsFit    Whether the subscriptions are kept as bit sets, see
     *                   {@link ExtraPartitionMoves}.
     */
    ShareBalancer(GroupModel group, CurrentAssignment current, Shares shares, boolean keepCounts, boolean bitsFit) {
        this.group = group;
        this.current = current;
        this.shares = shares;
        this.keepCounts = keepCounts;
        this.bitsFit = bitsFit;
    }

    /**
     * Balances the shares, see the class documentation. When the shares are balanced for sure,
     * see {@link #mayMove}, returns at once without building any state.
     */
    void run() {
        if (!mayMove(group, shares)) {
            return;
        }
        moves = new ExtraPartitionMoves(group, current, shares, keepCounts, bitsFit);
        createState();
        balance();
    }

    /**
     * Creates the heaps of the givers and the receivers, and the counts of the sizes, from the
     * shares as the previous steps left them.
     */
    private void createState() {
        receivers = new LongHeap[group.cohortCount()];
        var giverKeys = new long[group.memberCount()];
        int giverCount = 0;
        for (int cohort = 0; cohort < group.cohortCount(); cohort++) {
            var members = group.membersOf(cohort);
            var receiverKeys = new long[members.length];
            for (int i = 0; i < members.length; i++) {
                receiverKeys[i] = receiverKey(members[i]);
                if (shares.extraCount(members[i]) > 0) {
                    giverKeys[giverCount++] = giverKey(members[i]);
                }
            }
            receivers[cohort] = new LongHeap(receiverKeys, members.length);
        }
        givers = new LongHeap(giverKeys, giverCount);
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (int member = 0; member < group.memberCount(); member++) {
            min = Math.min(min, shares.size(member));
            max = Math.max(max, shares.size(member));
        }
        firstSize = min;
        smallestSize = min;
        sizeCounts = new int[group.memberCount() == 0 ? 0 : max - min + 1];
        for (int member = 0; member < group.memberCount(); member++) {
            sizeCounts[shares.size(member) - min]++;
        }
        isParked = new boolean[group.memberCount()];
        skipped = new IntList(16);
        smallestKeys = new LongHeap(group.cohortCount());
        isolation = new byte[group.cohortCount()];
        isDone = new boolean[group.cohortCount()];
    }

    /**
     * @return True if no other cohort subscribes to a topic of the cohort, computed the first
     *         time.
     */
    private boolean isIsolated(int cohort) {
        if (isolation[cohort] == 0) {
            isolation[cohort] = 1;
            for (int topic : group.topicsOf(cohort)) {
                if (group.cohortsOf(topic).length > 1) {
                    isolation[cohort] = 2;
                    break;
                }
            }
        }
        return isolation[cohort] == 1;
    }

    /**
     * @return False if the shares are balanced for sure: the members of every cohort have
     *         numbers of extra partitions within one of each other, and all sizes are within
     *         one of each other when there are several cohorts. Then the balance step would move
     *         nothing, and its state need not be built.
     */
    static boolean mayMove(GroupModel group, Shares shares) {
        int globalMin = Integer.MAX_VALUE;
        int globalMax = Integer.MIN_VALUE;
        for (int cohort = 0; cohort < group.cohortCount(); cohort++) {
            var members = group.membersOf(cohort);
            if (members.length == 0) {
                continue;
            }
            int min = Integer.MAX_VALUE;
            int max = Integer.MIN_VALUE;
            for (int member : members) {
                min = Math.min(min, shares.extraCount(member));
                max = Math.max(max, shares.extraCount(member));
            }
            if (max - min > 1) {
                return true;
            }
            globalMin = Math.min(globalMin, shares.cohortBaseSize(cohort) + min);
            globalMax = Math.max(globalMax, shares.cohortBaseSize(cohort) + max);
        }
        return group.cohortCount() > 1 && globalMax - globalMin >= 2;
    }

    /**
     * Balances the shares, see the class documentation.
     *
     * <p>An isolated cohort, none of whose topics another cohort subscribes to, is done at its
     * first giver without a receiver. Its extra partitions can only move between its members. A
     * member at least two partitions smaller than a giver of the same cohort has fewer extra
     * partitions, so it lacks one of the topics of the giver and can take it: a giver without a
     * receiver means that every member of the cohort has at least its size minus one. The other
     * givers of the cohort are no larger, since the givers come largest first, so none of them
     * has a receiver either, and nothing can move in the cohort any more. With a single cohort,
     * the step then ends.
     */
    private void balance() {
        while (!givers.isEmpty()) {
            long key = givers.poll();
            int giver = member(key);
            int cohort = group.cohortOf(giver);
            if (key != giverKey(giver) || isParked[giver] || shares.extraCount(giver) == 0 || isDone[cohort]) {
                continue;
            }
            int receiver = receiver(giver);
            if (receiver < 0 && isIsolated(cohort)) {
                isDone[cohort] = true;
                if (group.cohortCount() == 1) {
                    return;
                }
                continue;
            }
            if (receiver < 0) {
                park(giver);
                continue;
            }
            moves.transfer(giver, receiver, 1);
            countSizes(giver, receiver);
            isParked[receiver] = false;
            givers.add(giverKey(receiver));
            receivers[group.cohortOf(receiver)].add(receiverKey(receiver));
            if (shares.extraCount(giver) > 0) {
                givers.add(giverKey(giver));
            }
            receivers[group.cohortOf(giver)].add(receiverKey(giver));
            wake(giver);
        }
    }

    /**
     * @return The receiver of an extra partition of the giver: the first member in the order of
     *         {@link #receiverKey}, among the members at least two partitions
     *         smaller that can take one of its extra partitions; -1 if there is none. The cohorts
     *         are looked at from the one with the smallest member, so that the members of the
     *         other cohorts are mostly compared by size only.
     */
    private int receiver(int giver) {
        int limit = shares.size(giver) - 2;
        if (smallestSize > limit) {
            return -1;
        }
        var cohorts = moves.cohortsReached(giver);
        smallestKeys.clear();
        for (int i = 0; i < cohorts.size(); i++) {
            int cohort = cohorts.get(i);
            if (moves.allHeldBy(giver, cohort)) {
                continue;
            }
            long key = smallestKey(cohort);
            if (key >= 0 && shares.size(member(key)) <= limit) {
                smallestKeys.add(key);
            }
        }
        int best = -1;
        while (!smallestKeys.isEmpty()) {
            long key = smallestKeys.poll();
            if (best >= 0 && key > receiverKey(best)) {
                break;
            }
            int taker = smallestTaker(giver, group.cohortOf(member(key)), limit, best);
            if (taker >= 0) {
                best = taker;
            }
        }
        return best;
    }

    /**
     * @return The key of the first member of the cohort in the order of {@link #receiverKey}
     *         which is not full, dropping the entries before it; -1 if there is none. A full
     *         member is added back when it gives an extra partition.
     */
    private long smallestKey(int cohort) {
        var heap = receivers[cohort];
        while (!heap.isEmpty()) {
            long key = heap.peek();
            int member = member(key);
            if (key == receiverKey(member) && !moves.isFull(member)) {
                return key;
            }
            heap.poll();
        }
        return -1;
    }

    /**
     * @return The first member of the cohort in the order of {@link #receiverKey} that can take
     *         an extra partition of the giver, has a size of at most the limit, and comes before
     *         the best receiver so far if there is one; -1 if there is none.
     */
    private int smallestTaker(int giver, int cohort, int limit, int best) {
        var heap = receivers[cohort];
        int taker = -1;
        long key;
        while ((key = smallestKey(cohort)) >= 0) {
            int member = member(key);
            if (shares.size(member) > limit || best >= 0 && key > receiverKey(best)) {
                break;
            }
            if (moves.canTake(giver, member)) {
                taker = member;
                break;
            }
            skipped.add(member);
            heap.poll();
        }
        for (int i = 0; i < skipped.size(); i++) {
            heap.add(receiverKey(skipped.get(i)));
        }
        skipped.clear();
        return taker;
    }

    /**
     * Counts the sizes after a move from the giver, one partition smaller, to the receiver, one
     * partition larger.
     */
    private void countSizes(int giver, int receiver) {
        int giverSize = shares.size(giver) - firstSize;
        int receiverSize = shares.size(receiver) - firstSize;
        sizeCounts[giverSize + 1]--;
        sizeCounts[giverSize]++;
        sizeCounts[receiverSize - 1]--;
        sizeCounts[receiverSize]++;
        smallestSize = Math.min(smallestSize, giverSize + firstSize);
        while (sizeCounts[smallestSize - firstSize] == 0) {
            smallestSize++;
        }
    }

    /**
     * Parks the giver, which has no receiver. A giver at most two partitions above the smallest
     * size is not listed: a move ends its giver above the smallest size, which never decreases,
     * and only wakes members two partitions above its giver, see {@link #wake}, so no move can
     * wake it.
     */
    private void park(int giver) {
        isParked[giver] = true;
        if (shares.size(giver) - 2 <= smallestSize) {
            return;
        }
        if (parkedBySize == null) {
            parkedBySize = new IntList[sizeCounts.length];
        }
        int index = shares.size(giver) - firstSize;
        if (parkedBySize[index] == null) {
            parkedBySize[index] = new IntList(4);
        }
        parkedBySize[index].add(giver);
    }

    /**
     * Wakes the parked members which can now give to the giver of a move: those exactly two
     * partitions larger than it that it can take from.
     *
     * <p>A parked member has no receiver, and only a move can give it one. The receiver of the
     * move gets larger and gets an extra partition, which makes it no better a receiver. The giver
     * gets one partition smaller, at size {@code s}, and lacks the topic it gave. A parked member
     * can then give to it only through:
     * <ul>
     *     <li>the topic it gave, if the parked member has an extra partition of it and has at
     *     least {@code s + 2} partitions. This never happens: the receiver of the move lacked the
     *     topic and had at most {@code s - 1} partitions, so it was already a receiver of the
     *     parked member;</li>
     *     <li>another topic that the giver lacks. The giver lacked it before the move too, when
     *     it had {@code s + 1} partitions, and was then no receiver of the parked member: so the
     *     parked member has at most {@code s + 2} partitions, and exactly {@code s + 2} if the
     *     giver is now its receiver.</li>
     * </ul>
     *
     * <p>A wake looks at every member listed at that size, each look costing up to the extra
     * partitions of the member, so the wakes cost at worst the number of moves times the number
     * of members parked two partitions above their givers times their extra partitions: for
     * instance when every member of a cohort is parked two partitions above the size where the
     * moves of another cohort end.
     */
    private void wake(int giver) {
        int size = shares.size(giver);
        int index = size + 2 - firstSize;
        if (parkedBySize == null || index >= parkedBySize.length || parkedBySize[index] == null) {
            return;
        }
        var parked = parkedBySize[index];
        int kept = 0;
        for (int i = 0; i < parked.size(); i++) {
            int member = parked.get(i);
            if (!isParked[member] || shares.size(member) != size + 2) {
                continue;
            }
            if (moves.canTake(member, giver)) {
                unpark(member);
            } else {
                parked.set(kept++, member);
            }
        }
        parked.truncate(kept);
    }

    /**
     * Makes the parked member a giver again.
     */
    private void unpark(int member) {
        isParked[member] = false;
        givers.add(giverKey(member));
    }

    /**
     * @return The key of the member in the min heap of givers: its size,
     *         complemented so that the largest comes first, in the high 32 bits, then a bit set
     *         unless it has a free extra partition, then the member.
     */
    private long giverKey(int member) {
        long notFree = moves.hasFreeExtra(member) ? 0 : 1;
        return ((long) (Integer.MAX_VALUE - shares.size(member)) << 32) | notFree << 31 | member;
    }

    /**
     * @return The member of a key of {@link #giverKey} or {@link #receiverKey}.
     */
    private static int member(long giverKey) {
        return (int) (giverKey & Integer.MAX_VALUE);
    }

    /**
     * @return The key of the member in the min heaps of receivers: its size in the high 32 bits,
     *         then a bit set unless it owns more than the base partitions of a topic with extra
     *         partitions, then the member.
     */
    private long receiverKey(int member) {
        long notOwning = moves.ownsAboveBase(member) ? 0 : 1;
        return ((long) shares.size(member) << 32) | notOwning << 31 | member;
    }
}
