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

package org.apache.kafka.common;

import org.apache.kafka.common.utils.Utils;

import java.util.Collections;
import java.util.List;

/**
 * A class containing leadership, replicas and ISR information for a topic partition.
 */
public class TopicPartitionInfo {
    private final int partition;
    private final Node leader;
    private final List<Node> replicas;
    private final List<Node> isr;

    /**
     * Create an instance of this class with the provided parameters.
     *
     * @param partition the partition id
     * @param leader the leader of the partition or {@link Node#noNode()} if there is none.
     * @param replicas the replicas of the partition in the same order as the replica assignment (the preferred replica
     *                 is the head of the list)
     * @param isr the in-sync replicas
     */
    public TopicPartitionInfo(int partition, Node leader, List<Node> replicas, List<Node> isr) {
        this.partition = partition;
        this.leader = leader;
        this.replicas = Collections.unmodifiableList(replicas);
        this.isr = Collections.unmodifiableList(isr);
    }

    /**
     * Return the partition id.
     */
    public int partition() {
        return partition;
    }

    /**
     * Return the leader of the partition or null if there is none.
     */
    public Node leader() {
        return leader;
    }

    /**
     * Return the replicas of the partition in the same order as the replica assignment. The preferred replica is the
     * head of the list.
     *
     * Brokers with version lower than 0.11.0.0 return the replicas in unspecified order due to a bug.
     */
    public List<Node> replicas() {
        return replicas;
    }

    /**
     * Return the in-sync replicas of the partition. Note that the ordering of the result is unspecified.
     */
    public List<Node> isr() {
        return isr;
    }

    public String toString() {
        return "(partition=" + partition + ", leader=" + leader + ", replicas=" +
            Utils.join(replicas, ", ") + ", isr=" + Utils.join(isr, ", ") + ")";
    }
}
