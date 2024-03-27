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
import java.util.Map;

import static org.apache.kafka.metadata.Replicas.NONE;

public class BrokersToElrs {
    private final SnapshotRegistry snapshotRegistry;

    // It maps from the broker id to the topic id partitions if the partition has ELR.
    private final TimelineHashMap<Integer, TimelineHashMap<Uuid, int[]>> elrMembers;

    BrokersToElrs(SnapshotRegistry snapshotRegistry) {
        this.snapshotRegistry = snapshotRegistry;
        this.elrMembers = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    /**
     * Update our records of a partition's ELR.
     *
     * @param topicId       The topic ID of the partition.
     * @param partitionId   The partition ID of the partition.
     * @param prevElr       The previous ELR, or null if the partition is new.
     * @param nextElr       The new ELR, or null if the partition is being removed.
     */

    void update(Uuid topicId, int partitionId, int[] prevElr, int[] nextElr) {
        int[] prev;
        if (prevElr == null) {
            prev = NONE;
        } else {
            prev = Replicas.clone(prevElr);
            Arrays.sort(prev);
        }
        int[] next;
        if (nextElr == null) {
            next = NONE;
        } else {
            next = Replicas.clone(nextElr);
            Arrays.sort(next);
        }

        int i = 0, j = 0;
        while (true) {
            if (i == prev.length) {
                if (j == next.length) {
                    break;
                }
                int newReplica = next[j];
                add(newReplica, topicId, partitionId);
                j++;
            } else if (j == next.length) {
                int prevReplica = prev[i];
                remove(prevReplica, topicId, partitionId);
                i++;
            } else {
                int prevReplica = prev[i];
                int newReplica = next[j];
                if (prevReplica < newReplica) {
                    remove(prevReplica, topicId, partitionId);
                    i++;
                } else if (prevReplica > newReplica) {
                    add(newReplica, topicId, partitionId);
                    j++;
                } else {
                    i++;
                    j++;
                }
            }
        }
    }

    void removeTopicEntryForBroker(Uuid topicId, int brokerId) {
        Map<Uuid, int[]> topicMap = elrMembers.get(brokerId);
        if (topicMap != null) {
            topicMap.remove(topicId);
        }
    }

    private void add(int brokerId, Uuid topicId, int newPartition) {
        TimelineHashMap<Uuid, int[]> topicMap = elrMembers.get(brokerId);
        if (topicMap == null) {
            topicMap = new TimelineHashMap<>(snapshotRegistry, 0);
            elrMembers.put(brokerId, topicMap);
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

    private void remove(int brokerId, Uuid topicId, int removedPartition) {
        TimelineHashMap<Uuid, int[]> topicMap = elrMembers.get(brokerId);
        if (topicMap == null) {
            throw new RuntimeException("Broker " + brokerId + " has no elrMembers " +
                    "entry, so we can't remove " + topicId + ":" + removedPartition);
        }
        int[] partitions = topicMap.get(topicId);
        if (partitions == null) {
            throw new RuntimeException("Broker " + brokerId + " has no " +
                    "entry in elrMembers for topic " + topicId);
        }
        if (partitions.length == 1) {
            if (partitions[0] != removedPartition) {
                throw new RuntimeException("Broker " + brokerId + " has no " +
                        "entry in elrMembers for " + topicId + ":" + removedPartition);
            }
            topicMap.remove(topicId);
            if (topicMap.isEmpty()) {
                elrMembers.remove(brokerId);
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

    BrokersToIsrs.PartitionsOnReplicaIterator partitionsWithBrokerInElr(int brokerId) {
        Map<Uuid, int[]> topicMap = elrMembers.get(brokerId);
        if (topicMap == null) {
            topicMap = Collections.emptyMap();
        }
        return new BrokersToIsrs.PartitionsOnReplicaIterator(topicMap, false);
    }
}
