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

import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.assignor.uniform2.AssignmentBuilder;

/**
 * The uniform2 assignor spreads every topic evenly over its subscribers, balances the members,
 * and moves as few partitions as possible.
 *
 * <h2>Properties</h2>
 * Every assignment has the following properties, in priority order:
 * <ol>
 *     <li><b>Validity.</b> Every partition of every subscribed topic that exists is assigned to
 *     exactly one of its subscribers. Every member gets an assignment, possibly empty, which
 *     never holds an empty set of partitions.</li>
 *     <li><b>Spread.</b> A topic with {@code P} partitions and {@code N} subscribers gives each
 *     of them its {@code P / N} base partitions, and its {@code P % N} extra partitions to as
 *     many distinct subscribers, one each. The share of a subscriber in the topic is the base
 *     partitions, or one more with an extra partition.</li>
 *     <li><b>Balance.</b> The assignment size of a member, or size for short, is its number of
 *     partitions, the sum of its shares. No extra partition could move from its member to another
 *     subscriber of its topic whose size is at least two smaller. With a single subscription,
 *     this means that all sizes are within one of each other.</li>
 *     <li><b>Stickiness.</b> Partitions stay with their owners where the properties above allow:
 *     the placement moves the fewest partitions for the shares, an assignment that already has
 *     the properties does not change, and a member whose partitions do not change gets back the
 *     very map it had.</li>
 * </ol>
 * The result only depends on the content of the input: members and topics are numbered in id
 * order, and every tie is broken by these numbers.
 *
 * <h2>Algorithm</h2>
 * The spread leaves two choices: which subscribers get the extra partitions, which decides the
 * sizes, and which partitions each member gets. A member owning more than the base partitions of
 * a topic saves one move when it gets one of its extra partitions, since it keeps one more of
 * its partitions; an extra partition going to anyone else costs no move at that point. So the
 * assignor first lets the owners keep extra partitions, and moves them only for the balance:
 * <ol>
 *     <li><b>Read</b> the members, the topics and the current owner of every partition. Members
 *     with the same subscription form a cohort. Partitions held by a member which does not own
 *     them, because it does not subscribe to their topic, the topic does not exist or the
 *     partition is beyond the partition count, are dropped.</li>
 *     <li><b>Keep.</b> Every owner of more than the base partitions of a topic keeps one of its
 *     extra partitions; when the owners outnumber the extra partitions, the ones with the
 *     smallest assignments keep them.</li>
 *     <li><b>Hand out</b> the remaining extra partitions, one at a time, to subscribers not having
 *     one of the topic yet, among the ones with the smallest assignments, the topics with the
 *     fewest subscribers first, round robin within a cohort.</li>
 *     <li><b>Balance.</b> While an extra partition could move to a subscriber of its topic with
 *     at least two partitions fewer than its member, the member with the largest size having
 *     such an extra partition gives one to the smallest such subscriber, preferring the moves
 *     which do not move owned partitions. Each move lowers the sum of the squares of the sizes,
 *     so this ends, and it ends exactly when the balance property holds.</li>
 *     <li><b>Place</b> the partitions of every topic. The owners keep their partitions up to their
 *     share, and the other partitions go to the members below their share.</li>
 * </ol>
 * When the current assignment already has the properties, every owner owns its base
 * partitions or one more, so exactly the extra partitions are kept; nothing is handed out,
 * nothing moves in the balance, and every owner keeps its partitions.
 *
 * <h2>Example</h2>
 * Members A, B and C subscribe to T1 with 2 partitions, T2 with 5 and T3 with 7, and hold
 * nothing. T1 has no base partition and 2 extra partitions, T2 has 1 base partition and 2 extra
 * ones, and T3 has 2 base partitions and 1 extra one. Nobody owns anything, so the 5 extra
 * partitions are handed out round robin: T1's to A and B, T2's to C and A, and T3's to B. The
 * sizes are 5, 5 and 4. The members then take the partitions of every topic in partition order:
 * <pre>
 *     A: T1 [0],    T2 [0, 1], T3 [0, 1]
 *     B: T1 [1],    T2 [2],    T3 [2, 3, 4]
 *     C:            T2 [3, 4], T3 [5, 6]
 * </pre>
 * When C leaves, T1 gives 1 base partition to A and B, which they hold. T2 now has 2 base
 * partitions and 1 extra one, T3 has 3 base partitions and 1 extra one, and neither A nor B owns
 * more than the base partitions, so both extra partitions are handed out, T2's to A and T3's to
 * B. A and B keep all their partitions, and take those of C:
 * <pre>
 *     A: T1 [0], T2 [0, 1, 3], T3 [0, 1, 5]
 *     B: T1 [1], T2 [2, 4],    T3 [2, 3, 4, 6]
 * </pre>
 *
 * <p>The assignor does not use the racks of the members and of the replicas.
 */
public class Uniform2Assignor implements ConsumerGroupPartitionAssignor {
    /**
     * The name of the assignor.
     */
    public static final String NAME = "uniform2";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public GroupAssignment assign(
        GroupSpec groupSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) throws PartitionAssignorException {
        return new AssignmentBuilder(groupSpec, subscribedTopicDescriber).build();
    }
}
