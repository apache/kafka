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

package org.apache.kafka.image;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.Replicas;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents changes to a topic in the metadata image.
 */
public final class TopicDelta {
    private final TopicImage image;
    private final Map<Integer, PartitionRegistration> partitionChanges = new HashMap<>();

    public TopicDelta(TopicImage image) {
        this.image = image;
    }

    public TopicImage image() {
        return image;
    }

    public Map<Integer, PartitionRegistration> partitionChanges() {
        return partitionChanges;
    }

    public Map<Integer, PartitionRegistration> newPartitions() {
        return partitionChanges
            .entrySet()
            .stream()
            .filter(entry -> !image.partitions().containsKey(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public String name() {
        return image.name();
    }

    public Uuid id() {
        return image.id();
    }

    public void replay(PartitionRecord record) {
        partitionChanges.put(record.partitionId(), new PartitionRegistration(record));
    }

    public void replay(PartitionChangeRecord record) {
        PartitionRegistration partition = partitionChanges.get(record.partitionId());
        if (partition == null) {
            partition = image.partitions().get(record.partitionId());
            if (partition == null) {
                throw new RuntimeException("Unable to find partition " +
                    record.topicId() + ":" + record.partitionId());
            }
        }
        partitionChanges.put(record.partitionId(), partition.merge(record));
    }

    public TopicImage apply() {
        Map<Integer, PartitionRegistration> newPartitions = new HashMap<>();
        for (Entry<Integer, PartitionRegistration> entry : image.partitions().entrySet()) {
            int partitionId = entry.getKey();
            PartitionRegistration changedPartition = partitionChanges.get(partitionId);
            if (changedPartition == null) {
                newPartitions.put(partitionId, entry.getValue());
            } else {
                newPartitions.put(partitionId, changedPartition);
            }
        }
        for (Entry<Integer, PartitionRegistration> entry : partitionChanges.entrySet()) {
            if (!newPartitions.containsKey(entry.getKey())) {
                newPartitions.put(entry.getKey(), entry.getValue());
            }
        }
        return new TopicImage(image.name(), image.id(), newPartitions);
    }

    public boolean hasPartitionsWithAssignmentChanges() {
        for (Entry<Integer, PartitionRegistration> entry : partitionChanges.entrySet()) {
            int partitionId = entry.getKey();
            // New Partition.
            if (!image.partitions().containsKey(partitionId))
                return true;
            PartitionRegistration previousPartition = image.partitions().get(partitionId);
            PartitionRegistration currentPartition = entry.getValue();
            if (!previousPartition.hasSameAssignment(currentPartition))
                return true;
        }
        return false;
    }

    /**
     * Find the partitions that have change based on the replica given.
     *
     * The changes identified are:
     *   1. deletes: partitions for which the broker is not a replica anymore
     *   2. electedLeaders: partitions for which the broker is now a leader (leader epoch bump on the leader)
     *   3. leaders: partitions for which the isr or replicas change if the broker is a leader (partition epoch bump on the leader)
     *   4. followers: partitions for which the broker is now a follower or follower with isr or replica updates (partition epoch bump on follower)
     *
     * Leader epoch bumps are a strict subset of all partition epoch bumps, so all partitions in electedLeaders will be in leaders.
     *
     * @param brokerId the broker id
     * @return the list of partitions which the broker should remove, become leader, become/update leader, or become/update follower.
     */
    @SuppressWarnings("checkstyle:cyclomaticComplexity")
    public LocalReplicaChanges localChanges(int brokerId) {
        Set<TopicPartition> deletes = new HashSet<>();
        Map<TopicPartition, LocalReplicaChanges.PartitionInfo> electedLeaders = new HashMap<>();
        Map<TopicPartition, LocalReplicaChanges.PartitionInfo> leaders = new HashMap<>();
        Map<TopicPartition, LocalReplicaChanges.PartitionInfo> followers = new HashMap<>();
        Map<String, Uuid> topicIds = new HashMap<>();
        Map<TopicIdPartition, Uuid> directoryIds = new HashMap<>();

        for (Entry<Integer, PartitionRegistration> entry : partitionChanges.entrySet()) {
            if (!Replicas.contains(entry.getValue().replicas, brokerId)) {
                PartitionRegistration prevPartition = image.partitions().get(entry.getKey());
                if (prevPartition != null && Replicas.contains(prevPartition.replicas, brokerId)) {
                    deletes.add(new TopicPartition(name(), entry.getKey()));
                }
            } else if (entry.getValue().leader == brokerId) {
                PartitionRegistration prevPartition = image.partitions().get(entry.getKey());
                if (prevPartition == null || prevPartition.partitionEpoch != entry.getValue().partitionEpoch) {
                    TopicPartition tp = new TopicPartition(name(), entry.getKey());
                    LocalReplicaChanges.PartitionInfo partitionInfo = new LocalReplicaChanges.PartitionInfo(id(), entry.getValue());
                    leaders.put(tp, partitionInfo);
                    if (prevPartition == null || prevPartition.leaderEpoch != entry.getValue().leaderEpoch) {
                        electedLeaders.put(tp, partitionInfo);
                    }
                    topicIds.putIfAbsent(name(), id());
                }
            } else if (
                entry.getValue().leader != brokerId &&
                Replicas.contains(entry.getValue().replicas, brokerId)
            ) {
                PartitionRegistration prevPartition = image.partitions().get(entry.getKey());
                if (prevPartition == null || prevPartition.partitionEpoch != entry.getValue().partitionEpoch) {
                    followers.put(
                        new TopicPartition(name(), entry.getKey()),
                        new LocalReplicaChanges.PartitionInfo(id(), entry.getValue())
                    );
                    topicIds.putIfAbsent(name(), id());
                }
            }

            try {
                PartitionRegistration prevPartition = image.partitions().get(entry.getKey());
                if (
                        prevPartition == null ||
                        prevPartition.directories == null ||
                        prevPartition.directory(brokerId) != entry.getValue().directory(brokerId)
                ) {
                    directoryIds.put(
                        new TopicIdPartition(id(), new TopicPartition(name(), entry.getKey())),
                        entry.getValue().directory(brokerId)
                    );
                }
            } catch (IllegalArgumentException e) {
                // Do nothing if broker isn't part of the replica set.
            }
        }

        return new LocalReplicaChanges(deletes, electedLeaders, leaders, followers, topicIds, directoryIds);
    }

    @Override
    public String toString() {
        return "TopicDelta(" +
            "partitionChanges=" + partitionChanges +
            ')';
    }
}
