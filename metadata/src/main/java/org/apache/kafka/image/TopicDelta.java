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

    /**
     * TODO: write documentation
     * TODO
     */
    /**
     * Find the partitions that we are now leading, whose partition epoch has changed.
     *
     * @param replicaId The replica id.
     * @return          A list of (partition ID, partition registration) entries.
     */
    /**
     * Find the partitions that we are now following, whose partition epoch has changed.
     *
     * @param replicaId The replica id.
     * @return          A list of (partition ID, partition registration) entries.
     */
    public LocalReplicaChanges newLocalChanges(int replicaId) {
        Set<TopicPartition> deletes = new HashSet<>();
        Map<TopicPartition, PartitionInfo> leaders = new HashMap<>();
        Map<TopicPartition, PartitionInfo> followers = new HashMap<>();

        for (Entry<Integer, PartitionRegistration> entry : partitionChanges.entrySet()) {
            if (!Replicas.contains(entry.getValue().replicas, replicaId)) {
                PartitionRegistration prevPartition = image.partitions().get(entry.getKey());
                if (prevPartition != null && Replicas.contains(prevPartition.replicas, replicaId)) {
                    deletes.add(new TopicPartition(name(), entry.getKey()));
                }
            } else if (entry.getValue().leader == replicaId) {
                PartitionRegistration prevPartition = image.partitions().get(entry.getKey());
                if (prevPartition == null || prevPartition.partitionEpoch != entry.getValue().partitionEpoch) {
                    leaders.put(
                        new TopicPartition(name(), entry.getKey()),
                        new PartitionInfo(id(), entry.getValue())
                    );
                }
            } else if (
                entry.getValue().leader != replicaId &&
                Replicas.contains(entry.getValue().replicas, replicaId)
            ) {
                PartitionRegistration prevPartition = image.partitions().get(entry.getKey());
                if (prevPartition == null || prevPartition.partitionEpoch != entry.getValue().partitionEpoch) {
                    followers.put(
                        new TopicPartition(name(), entry.getKey()),
                        new PartitionInfo(id(), entry.getValue())
                    );
                }
            }
        }

        return new LocalReplicaChanges(deletes, leaders, followers);
    }

    // TODO: move this class out of here.
    public static final class PartitionInfo {
        private final Uuid topicId;
        private final PartitionRegistration partition;

        public PartitionInfo(Uuid topicId, PartitionRegistration partition) {
            this.topicId = topicId;
            this.partition = partition;
        }

        @Override
        public String toString() {
            return String.format("PartitionInfo(topicId = %s, partition = %s)", topicId, partition);
        }

        public Uuid topicId() {
            return topicId;
        }

        public PartitionRegistration partition() {
            return partition;
        }
    }

    // TODO: should we use TopicIdPartition?
    // TODO: move this class out of here.
    public static final class LocalReplicaChanges {
        private final Set<TopicPartition> deletes;
        private final Map<TopicPartition, PartitionInfo> leaders;
        private final Map<TopicPartition, PartitionInfo> followers;

        LocalReplicaChanges(
            Set<TopicPartition> deletes,
            Map<TopicPartition, PartitionInfo> leaders,
            Map<TopicPartition, PartitionInfo> followers
        ) {
            this.deletes = deletes;
            this.leaders = leaders;
            this.followers = followers;
        }

        public Set<TopicPartition> deletes() {
            return deletes;
        }

        public Map<TopicPartition, PartitionInfo> leaders() {
            return leaders;
        }

        public Map<TopicPartition, PartitionInfo> followers() {
            return followers;
        }

        @Override
        public String toString() {
            return String.format(
                "LocalReplicaChanges(deletes = %s, leaders = %s, followers = %s)",
                deletes,
                leaders,
                followers
            );
        }
    }
}
