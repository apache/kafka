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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A class containing leadership, replicas and ISR information for a topic partition.
 */
public class TopicPartitionInfo {
    private final int partition;
    private final Node leader;
    private final List<Node> replicas;
    private final List<Node> isr;
    private final List<Node> elr;
    private final List<Node> lastKnownElr;

    /**
     * Create an instance of this class with the provided parameters.
     *
     * @param partition the partition id
     * @param leader the leader of the partition or {@link Node#noNode()} if there is none.
     * @param replicas the replicas of the partition in the same order as the replica assignment (the preferred replica
     *                 is the head of the list)
     * @param isr the in-sync replicas
     * @param elr the eligible leader replicas
     * @param lastKnownElr the last known eligible leader replicas.
     */
    public TopicPartitionInfo(
        int partition,
        Node leader,
        List<Node> replicas,
        List<Node> isr,
        List<Node> elr,
        List<Node> lastKnownElr
    ) {
        this.partition = partition;
        this.leader = leader;
        this.replicas = Collections.unmodifiableList(replicas);
        this.isr = Collections.unmodifiableList(isr);
        this.elr = Collections.unmodifiableList(elr);
        this.lastKnownElr = Collections.unmodifiableList(lastKnownElr);
    }

    public TopicPartitionInfo(int partition, Node leader, List<Node> replicas, List<Node> isr) {
        this.partition = partition;
        this.leader = leader;
        this.replicas = Collections.unmodifiableList(replicas);
        this.isr = Collections.unmodifiableList(isr);
        this.elr = null;
        this.lastKnownElr = null;
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

    /**
     * Return the eligible leader replicas of the partition. Note that the ordering of the result is unspecified.
     */
    public List<Node> elr() {
        return elr;
    }

    /**
     * Return the last known eligible leader replicas of the partition. Note that the ordering of the result is unspecified.
     */
    public List<Node> lastKnownElr() {
        return lastKnownElr;
    }

    public String toString() {
        String elrString = elr != null ? elr.stream().map(Node::toString).collect(Collectors.joining(", ")) : "N/A";
        String lastKnownElrString = lastKnownElr != null ? lastKnownElr.stream().map(Node::toString).collect(Collectors.joining(", ")) : "N/A";
        return "(partition=" + partition + ", leader=" + leader + ", replicas=" +
            replicas.stream().map(Node::toString).collect(Collectors.joining(", ")) + ", isr=" + isr.stream().map(Node::toString).collect(Collectors.joining(", ")) +
            ", elr=" + elrString + ", lastKnownElr=" + lastKnownElrString + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopicPartitionInfo that = (TopicPartitionInfo) o;

        return partition == that.partition &&
            Objects.equals(leader, that.leader) &&
            Objects.equals(replicas, that.replicas) &&
            Objects.equals(isr, that.isr) &&
            Objects.equals(elr, that.elr) &&
            Objects.equals(lastKnownElr, that.lastKnownElr);
    }

    @Override
    public int hashCode() {
        int result = partition;
        result = 31 * result + (leader != null ? leader.hashCode() : 0);
        result = 31 * result + (replicas != null ? replicas.hashCode() : 0);
        result = 31 * result + (isr != null ? isr.hashCode() : 0);
        result = 31 * result + (elr != null ? elr.hashCode() : 0);
        result = 31 * result + (lastKnownElr != null ? lastKnownElr.hashCode() : 0);
        return result;
    }
}
