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
 * The uniform2 assignor spreads every topic evenly over its subscribers, balances the assignment
 * sizes of the members, and moves as few partitions as possible.
 *
 * <h2>Properties</h2>
 * Every assignment has the following properties, in priority order:
 * <ol>
 *     <li><b>Validity.</b> Every partition of every subscribed topic that exists is assigned to
 *     exactly one of its subscribers. Every member gets an assignment, possibly empty, which
 *     never holds an empty set of partitions.</li>
 *     <li><b>Spread.</b> A topic with {@code P} partitions and {@code N} subscribers has a
 *     minimum quota of {@code P / N} partitions, and its {@code P % N} extra partitions go to as
 *     many distinct subscribers, one each. The quota of a subscriber in the topic is the minimum
 *     quota, or one more with an extra partition.</li>
 *     <li><b>Balance.</b> The assignment size of a member is its number of partitions, the sum of
 *     its quotas. No extra partition could move from its member to another subscriber of its topic
 *     whose assignment size is at least two smaller. With a single subscription, this means that
 *     all assignment sizes are within one of each other.</li>
 *     <li><b>Stickiness.</b> Partitions stay with their owners where the properties above allow.
 *     The placement moves the fewest partitions for the quotas, an assignment that already has the
 *     properties does not change, and a member whose partitions do not change gets back the same
 *     map.</li>
 * </ol>
 * The result is deterministic, since members and topics are numbered in id order and every tie is
 * broken by these numbers.
 *
 * <h2>Algorithm</h2>
 * The spread leaves two choices: which subscribers get the extra partitions, and which partitions
 * each member gets. The first decides the assignment sizes. A member owning more than the minimum
 * quota of a topic saves a move when it gets an extra partition of the topic, since it keeps one
 * more of its partitions. An extra partition going to any other member costs no move at that point.
 * So the assignor first lets the owners keep extra partitions, and moves them only for the balance:
 * <ol>
 *     <li><b>Read</b> the members, the topics and the current owner of every partition. Members
 *     with the same subscription form a cohort. Partitions held by a member which does not own
 *     them, because it does not subscribe to their topic or the topic does not exist, are
 *     dropped.</li>
 *     <li><b>Keep the sticky extra partitions.</b> Every owner of more than the minimum quota of a
 *     topic keeps one of its extra partitions. When more owners qualify than the topic has extra
 *     partitions, the ones with the smallest assignment sizes keep them.</li>
 *     <li><b>Assign the remaining extra partitions</b> one at a time, each to a subscriber without
 *     one of the topic yet and with one of the smallest assignment sizes. The topics with the
 *     fewest subscribers go first, and the members of a cohort are served round robin.</li>
 *     <li><b>Balance the extra partitions.</b> While an extra partition could move to a subscriber
 *     of its topic with at least two partitions fewer than its member, the largest member with such
 *     an extra partition gives one to the smallest member that can take it, choosing the extra
 *     partition whose move revokes the fewest partitions. Each move lowers the sum of the squares
 *     of the assignment sizes, so the step ends, and it ends exactly when the balance property
 *     holds.</li>
 *     <li><b>Place</b> the partitions of every topic. Every owner keeps its partitions with the
 *     lowest ids, up to its quota, and the other partitions go to the members below their
 *     quota.</li>
 * </ol>
 * When the current assignment already has the properties, every owner owns its minimum quota or one
 * more, so the owners keep exactly the extra partitions. Nothing is then assigned, nothing moves in
 * the balance, and every owner keeps its partitions.
 *
 * <h2>Example</h2>
 * Members A, B and C subscribe to T1 with 2 partitions, T2 with 5 and T3 with 7, and hold nothing.
 * T1 has a minimum quota of 0 and 2 extra partitions, T2 a minimum quota of 1 and 2 extra
 * partitions, and T3 a minimum quota of 2 and 1 extra partition. Nobody owns anything, so the 5
 * extra partitions are assigned round robin: T1's to A and B, T2's to C and A, and T3's to B. The
 * assignment sizes are 5, 5 and 4. The members then take the partitions of every topic in partition
 * order:
 * <pre>
 *     A: T1 [0],    T2 [0, 1], T3 [0, 1]
 *     B: T1 [1],    T2 [2],    T3 [2, 3, 4]
 *     C:            T2 [3, 4], T3 [5, 6]
 * </pre>
 * When C leaves, T1 has a minimum quota of 1, which A and B hold. T2 now has a minimum quota of 2
 * and 1 extra partition, T3 a minimum quota of 3 and 1 extra partition, and neither A nor B owns
 * more than the minimum quota, so both extra partitions are assigned, T2's to A and T3's to B. A
 * and B keep all their partitions, and take those of C:
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
