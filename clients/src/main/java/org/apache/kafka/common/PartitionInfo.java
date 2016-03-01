/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common;

/**
 * Information about a topic-partition.
 */
public class PartitionInfo {

    private final String topic;
    private final int partition;
    private final Node leader;
    private final Node[] replicas;
    private final Node[] inSyncReplicas;

    public PartitionInfo(String topic, int partition, Node leader, Node[] replicas, Node[] inSyncReplicas) {
        this.topic = topic;
        this.partition = partition;
        this.leader = leader;
        this.replicas = replicas;
        this.inSyncReplicas = inSyncReplicas;
    }

    /**
     * The topic name
     */
    public String topic() {
        return topic;
    }

    /**
     * The partition id
     */
    public int partition() {
        return partition;
    }

    /**
     * The node id of the node currently acting as a leader for this partition or null if there is no leader
     */
    public Node leader() {
        return leader;
    }

    /**
     * The complete set of replicas for this partition regardless of whether they are alive or up-to-date
     */
    public Node[] replicas() {
        return replicas;
    }

    /**
     * The subset of the replicas that are in sync, that is caught-up to the leader and ready to take over as leader if
     * the leader should fail
     */
    public Node[] inSyncReplicas() {
        return inSyncReplicas;
    }

    @Override
    public String toString() {
        return String.format("Partition(topic = %s, partition = %d, leader = %s, replicas = %s, isr = %s",
                             topic,
                             partition,
                             leader == null ? "none" : leader.id(),
                             fmtNodeIds(replicas),
                             fmtNodeIds(inSyncReplicas));
    }

    /* Extract the node ids from each item in the array and format for display */
    private String fmtNodeIds(Node[] nodes) {
        StringBuilder b = new StringBuilder("[");
        for (int i = 0; i < nodes.length - 1; i++) {
            b.append(Integer.toString(nodes[i].id()));
            b.append(',');
        }
        if (nodes.length > 0) {
            b.append(Integer.toString(nodes[nodes.length - 1].id()));
            b.append(',');
        }
        b.append("]");
        return b.toString();
    }

}
