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

package org.apache.kafka.controller;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.metadata.Replicas;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER;
import static org.apache.kafka.metadata.Replicas.NONE;


/**
 * Associates brokers with their in-sync partitions.
 *
 * This is useful when we need to remove a broker from all the ISRs, or move all leaders
 * away from a broker.
 *
 * We also track all the partitions that currently have no leader.
 *
 * The core data structure is a map from broker IDs to topic maps.  Each topic map relates
 * topic UUIDs to arrays of partition IDs.
 *
 * Each entry in the array has a high bit which indicates that the broker is the leader
 * for the given partition, as well as 31 low bits which contain the partition id.  This
 * works because partition IDs cannot be negative.
 */
public class BrokersToIsrs {
    private final static int LEADER_FLAG = 0x8000_0000;

    private final static int REPLICA_MASK = 0x7fff_ffff;

    static class PartitionsOnReplicaIterator implements Iterator<TopicIdPartition> {
        private final Iterator<Entry<Uuid, int[]>> iterator;
        private final boolean leaderOnly;
        private int offset = 0;
        Uuid uuid = Uuid.ZERO_UUID;
        int[] replicas = NONE;
        private TopicIdPartition next = null;

        PartitionsOnReplicaIterator(Map<Uuid, int[]> topicMap, boolean leaderOnly) {
            this.iterator = topicMap.entrySet().iterator();
            this.leaderOnly = leaderOnly;
        }

        @Override
        public boolean hasNext() {
            if (next != null) return true;
            while (true) {
                if (offset >= replicas.length) {
                    if (!iterator.hasNext()) return false;
                    offset = 0;
                    Entry<Uuid, int[]> entry = iterator.next();
                    uuid = entry.getKey();
                    replicas = entry.getValue();
                }
                int replica = replicas[offset++];
                if ((!leaderOnly) || (replica & LEADER_FLAG) != 0) {
                    next = new TopicIdPartition(uuid, replica & REPLICA_MASK);
                    return true;
                }
            }
        }

        @Override
        public TopicIdPartition next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            TopicIdPartition result = next;
            next = null;
            return result;
        }
    }

    private final SnapshotRegistry snapshotRegistry;

    /**
     * A map of broker IDs to the partitions that the broker is in the ISR for.
     * Partitions with no isr members appear in this map under id NO_LEADER.
     */
    private final TimelineHashMap<Integer, TimelineHashMap<Uuid, int[]>> isrMembers;
    
    BrokersToIsrs(SnapshotRegistry snapshotRegistry) {
        this.snapshotRegistry = snapshotRegistry;
        this.isrMembers = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    /**
     * Update our records of a partition's ISR.
     *
     * @param topicId       The topic ID of the partition.
     * @param partitionId   The partition ID of the partition.
     * @param prevIsr       The previous ISR, or null if the partition is new.
     * @param nextIsr       The new ISR, or null if the partition is being removed.
     * @param prevLeader    The previous leader, or NO_LEADER if the partition had no leader.
     * @param nextLeader    The new leader, or NO_LEADER if the partition now has no leader.
     */
    void update(Uuid topicId, int partitionId, int[] prevIsr, int[] nextIsr,
                int prevLeader, int nextLeader) {
        int[] prev;
        if (prevIsr == null) {
            prev = NONE;
        } else {
            if (prevLeader == NO_LEADER) {
                prev = Replicas.copyWith(prevIsr, NO_LEADER);
            } else {
                prev = Replicas.clone(prevIsr);
            }
            Arrays.sort(prev);
        }
        int[] next;
        if (nextIsr == null) {
            next = NONE;
        } else {
            if (nextLeader == NO_LEADER) {
                next = Replicas.copyWith(nextIsr, NO_LEADER);
            } else {
                next = Replicas.clone(nextIsr);
            }
            Arrays.sort(next);
        }
        int i = 0, j = 0;
        while (true) {
            if (i == prev.length) {
                if (j == next.length) {
                    break;
                }
                int newReplica = next[j];
                add(newReplica, topicId, partitionId, newReplica == nextLeader);
                j++;
            } else if (j == next.length) {
                int prevReplica = prev[i];
                remove(prevReplica, topicId, partitionId, prevReplica == prevLeader);
                i++;
            } else {
                int prevReplica = prev[i];
                int newReplica = next[j];
                if (prevReplica < newReplica) {
                    remove(prevReplica, topicId, partitionId, prevReplica == prevLeader);
                    i++;
                } else if (prevReplica > newReplica) {
                    add(newReplica, topicId, partitionId, newReplica == nextLeader);
                    j++;
                } else {
                    boolean wasLeader = prevReplica == prevLeader;
                    boolean isLeader = prevReplica == nextLeader;
                    if (wasLeader != isLeader) {
                        change(prevReplica, topicId, partitionId, wasLeader, isLeader);
                    }
                    i++;
                    j++;
                }
            }
        }
    }

    void removeTopicEntryForBroker(Uuid topicId, int brokerId) {
        Map<Uuid, int[]> topicMap = isrMembers.get(brokerId);
        if (topicMap != null) {
            topicMap.remove(topicId);
        }
    }

    private void add(int brokerId, Uuid topicId, int newPartition, boolean leader) {
        if (leader) {
            newPartition = newPartition | LEADER_FLAG;
        }
        TimelineHashMap<Uuid, int[]> topicMap = isrMembers.get(brokerId);
        if (topicMap == null) {
            topicMap = new TimelineHashMap<>(snapshotRegistry, 0);
            isrMembers.put(brokerId, topicMap);
        }
        int[] partitions = topicMap.get(topicId);
        int[] newPartitions;
        if (partitions == null) {
            newPartitions = new int[1];
        } else {
            newPartitions = new int[partitions.length + 1];
            System.arraycopy(partitions, 0, newPartitions, 0, partitions.length);
        }
        newPartitions[newPartitions.length - 1] = newPartition;
        topicMap.put(topicId, newPartitions);
    }

    private void change(int brokerId, Uuid topicId, int partition,
                        boolean wasLeader, boolean isLeader) {
        TimelineHashMap<Uuid, int[]> topicMap = isrMembers.get(brokerId);
        if (topicMap == null) {
            throw new RuntimeException("Broker " + brokerId + " has no isrMembers " +
                "entry, so we can't change " + topicId + ":" + partition);
        }
        int[] partitions = topicMap.get(topicId);
        if (partitions == null) {
            throw new RuntimeException("Broker " + brokerId + " has no " +
                "entry in isrMembers for topic " + topicId);
        }
        int[] newPartitions = new int[partitions.length];
        int target = wasLeader ? partition | LEADER_FLAG : partition;
        for (int i = 0; i < partitions.length; i++) {
            int cur = partitions[i];
            if (cur == target) {
                newPartitions[i] = isLeader ? partition | LEADER_FLAG : partition;
            } else {
                newPartitions[i] = cur;
            }
        }
        topicMap.put(topicId, newPartitions);
    }

    private void remove(int brokerId, Uuid topicId, int removedPartition, boolean leader) {
        if (leader) {
            removedPartition = removedPartition | LEADER_FLAG;
        }
        TimelineHashMap<Uuid, int[]> topicMap = isrMembers.get(brokerId);
        if (topicMap == null) {
            throw new RuntimeException("Broker " + brokerId + " has no isrMembers " +
                "entry, so we can't remove " + topicId + ":" + removedPartition);
        }
        int[] partitions = topicMap.get(topicId);
        if (partitions == null) {
            throw new RuntimeException("Broker " + brokerId + " has no " +
                "entry in isrMembers for topic " + topicId);
        }
        if (partitions.length == 1) {
            if (partitions[0] != removedPartition) {
                throw new RuntimeException("Broker " + brokerId + " has no " +
                    "entry in isrMembers for " + topicId + ":" + removedPartition);
            }
            topicMap.remove(topicId);
            if (topicMap.isEmpty()) {
                isrMembers.remove(brokerId);
            }
        } else {
            int[] newPartitions = new int[partitions.length - 1];
            int j = 0;
            for (int i = 0; i < partitions.length; i++) {
                int partition = partitions[i];
                if (partition != removedPartition) {
                    newPartitions[j++] = partition;
                }
            }
            topicMap.put(topicId, newPartitions);
        }
    }

    PartitionsOnReplicaIterator iterator(int brokerId, boolean leadersOnly) {
        Map<Uuid, int[]> topicMap = isrMembers.get(brokerId);
        if (topicMap == null) {
            topicMap = Collections.emptyMap();
        }
        return new PartitionsOnReplicaIterator(topicMap, leadersOnly);
    }

    PartitionsOnReplicaIterator partitionsWithNoLeader() {
        return iterator(NO_LEADER, true);
    }

    PartitionsOnReplicaIterator partitionsLedByBroker(int brokerId) {
        return iterator(brokerId, true);
    }

    PartitionsOnReplicaIterator partitionsWithBrokerInIsr(int brokerId) {
        return iterator(brokerId, false);
    }

    boolean hasLeaderships(int brokerId) {
        return iterator(brokerId, true).hasNext();
    }
}
