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
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.Replicas;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


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
     * Find the partitions that we are now leading, that we were not leading before.
     *
     * @param brokerId  The broker id.
     * @return          A list of (partition ID, partition registration) entries.
     */
    public List<Entry<Integer, PartitionRegistration>> newLocalLeaders(int brokerId) {
        List<Entry<Integer, PartitionRegistration>> results = new ArrayList<>();
        for (Entry<Integer, PartitionRegistration> entry : partitionChanges.entrySet()) {
            if (entry.getValue().leader == brokerId) {
                PartitionRegistration prevPartition = image.partitions().get(entry.getKey());
                if (prevPartition == null || prevPartition.leader != brokerId) {
                    results.add(entry);
                }
            }
        }
        return results;
    }

    /**
     * Find the partitions that we are now following, that we were not following before.
     *
     * @param brokerId  The broker id.
     * @return          A list of (partition ID, partition registration) entries.
     */
    public List<Entry<Integer, PartitionRegistration>> newLocalFollowers(int brokerId) {
        List<Entry<Integer, PartitionRegistration>> results = new ArrayList<>();
        for (Entry<Integer, PartitionRegistration> entry : partitionChanges.entrySet()) {
            if (entry.getValue().leader != brokerId &&
                    Replicas.contains(entry.getValue().replicas, brokerId)) {
                PartitionRegistration prevPartition = image.partitions().get(entry.getKey());
                if (prevPartition == null || prevPartition.leader == brokerId) {
                    results.add(entry);
                }
            }
        }
        return results;
    }
}
