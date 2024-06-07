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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.List;
import java.util.Map;

/**
 * Describes new partitions for a particular topic in a call to {@link Admin#createPartitions(Map)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class NewPartitions {

    private final int totalCount;

    private final List<List<Integer>> newAssignments;

    private NewPartitions(int totalCount, List<List<Integer>> newAssignments) {
        this.totalCount = totalCount;
        this.newAssignments = newAssignments;
    }

    /**
     * Increase the partition count for a topic to the given {@code totalCount}.
     * The assignment of new replicas to brokers will be decided by the broker.
     *
     * @param totalCount The total number of partitions after the operation succeeds.
     */
    public static NewPartitions increaseTo(int totalCount) {
        return new NewPartitions(totalCount, null);
    }

    /**
     * <p>Increase the partition count for a topic to the given {@code totalCount}
     * assigning the new partitions according to the given {@code newAssignments}.
     * The length of the given {@code newAssignments} should equal {@code totalCount - oldCount}, since
     * the assignment of existing partitions are not changed.
     * Each inner list of {@code newAssignments} should have a length equal to
     * the topic's replication factor.
     * The first broker id in each inner list is the "preferred replica".</p>
     *
     * <p>For example, suppose a topic currently has a replication factor of 2, and
     * has 3 partitions. The number of partitions can be increased to 6 using a
     * {@code NewPartition} constructed like this:</p>
     *
     * <pre><code>
     * NewPartitions.increaseTo(6, asList(asList(1, 2),
     *                                    asList(2, 3),
     *                                    asList(3, 1)))
     * </code></pre>
     * <p>In this example partition 3's preferred leader will be broker 1, partition 4's preferred leader will be
     * broker 2 and partition 5's preferred leader will be broker 3.</p>
     *
     * @param totalCount The total number of partitions after the operation succeeds.
     * @param newAssignments The replica assignments for the new partitions.
     */
    public static NewPartitions increaseTo(int totalCount, List<List<Integer>> newAssignments) {
        return new NewPartitions(totalCount, newAssignments);
    }

    /**
     * The total number of partitions after the operation succeeds.
     */
    public int totalCount() {
        return totalCount;
    }

    /**
     * The replica assignments for the new partitions, or null if the assignment will be done by the controller.
     */
    public List<List<Integer>> assignments() {
        return newAssignments;
    }

    @Override
    public String toString() {
        return "(totalCount=" + totalCount() + ", newAssignments=" + assignments() + ")";
    }

}
