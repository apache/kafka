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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.MirrorTopicStateChangeRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.Replicas;
import org.apache.kafka.server.mirror.MirrorPartitionState;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents changes to a topic in the metadata image.
 */
public final class TopicDelta {
    private final TopicImage image;
    private final Map<Integer, PartitionRegistration> partitionChanges = new HashMap<>();
    private final Map<Integer, Integer> partitionToUncleanLeaderElectionCount = new HashMap<>();
    private final Map<Integer, Integer> partitionToElrElectionCount = new HashMap<>();

    private boolean mirrorRecordReplayed;
    private String mirrorName;
    private byte desiredMirrorState;

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

    public Map<Integer, Integer> partitionToElrElectionCount() {
        return partitionToElrElectionCount;
    }
    public Map<Integer, Integer> partitionToUncleanLeaderElectionCount() {
        return partitionToUncleanLeaderElectionCount;
    }

    public void replay(PartitionRecord record) {
        int partitionId = record.partitionId();
        PartitionRegistration prevPartition = partitionChanges.get(partitionId);
        if (prevPartition == null) {
            prevPartition = image.partitions().get(partitionId);
        }
        if (prevPartition != null) {
            updateElectionStats(partitionId, prevPartition, record.leader(), record.leaderRecoveryState());
        }
        partitionChanges.put(record.partitionId(), new PartitionRegistration(record));
    }

    public void replay(PartitionChangeRecord record) {
        int partitionId = record.partitionId();
        PartitionRegistration prevPartition = partitionChanges.get(partitionId);
        if (prevPartition == null) {
            prevPartition = image.partitions().get(partitionId);
            if (prevPartition == null) {
                throw new RuntimeException("Unable to find partition " +
                    record.topicId() + ":" + partitionId);
            }
        }
        updateElectionStats(partitionId, prevPartition, record.leader(), record.leaderRecoveryState());
        partitionChanges.put(record.partitionId(), prevPartition.merge(record));
    }

    public void replay(MirrorTopicStateChangeRecord record) {
        if (record.mirrorName() == null && record.desiredState() != MirrorPartitionState.UNKNOWN.value()) {
            throw new IllegalArgumentException("desiredState must be UNKNOWN when mirrorName is null, got " + MirrorPartitionState.nameOf(record.desiredState()));
        }
        mirrorRecordReplayed = true;
        mirrorName = record.mirrorName();
        desiredMirrorState = record.desiredState();
    }

    /**
     * Returns the mirror name after this delta is applied.
     */
    public String mirrorName() {
        return mirrorRecordReplayed ? mirrorName : image.mirrorName();
    }

    /**
     * Returns the desired mirror state after this delta is applied.
     */
    public byte desiredMirrorState() {
        return mirrorRecordReplayed ? desiredMirrorState : image.desiredMirrorState();
    }

    /**
     * Returns the mirror topic state after this delta is applied if a mirror topic state change record was replayed,
     * or if the state differs from the state in the given image. Returns null otherwise. A null image is treated as a
     * topic that is not associated with a mirror.
     * <p>
     * A replayed record is reported even if it doesn't change the state, because the controller re-writes the current
     * state to make brokers re-process the mirror partitions (e.g. when recovering failed partitions).
     */
    LocalReplicaChanges.MirrorTopicState mirrorTopicStateChange(TopicImage prevImage) {
        String prevMirrorName = prevImage == null ? null : prevImage.mirrorName();
        byte prevDesiredMirrorState = prevImage == null ? MirrorPartitionState.UNKNOWN.value() : prevImage.desiredMirrorState();
        if (!mirrorRecordReplayed && Objects.equals(prevMirrorName, mirrorName()) && prevDesiredMirrorState == desiredMirrorState()) {
            return null;
        }
        return new LocalReplicaChanges.MirrorTopicState(mirrorName(), desiredMirrorState());
    }

    private void updateElectionStats(int partitionId, PartitionRegistration prevPartition, int newLeader, byte newLeaderRecoveryState) {
        if (PartitionRegistration.electionWasUnclean(newLeaderRecoveryState)) {
            partitionToUncleanLeaderElectionCount.put(partitionId, partitionToUncleanLeaderElectionCount.getOrDefault(partitionId, 0) + 1);
        }
        if (Replicas.contains(prevPartition.elr, newLeader)) {
            partitionToElrElectionCount.put(partitionId, partitionToElrElectionCount.getOrDefault(partitionId, 0) + 1);
        }
    }

    public void replay() {
        // Some partitions are not added to the image yet, let's check the partitionChanges first.
        partitionChanges.forEach(this::maybeClearElr);

        image.partitions().forEach((partitionId, partition) -> {
            if (!partitionChanges.containsKey(partitionId)) {
                maybeClearElr(partitionId, partition);
            }
        });
    }

    void maybeClearElr(int partitionId, PartitionRegistration partition) {
        if (partition.elr.length != 0 || partition.lastKnownElr.length != 0) {
            partitionChanges.put(partitionId, partition.merge(
                new PartitionChangeRecord().
                    setPartitionId(partitionId).
                    setTopicId(image.id()).
                    setEligibleLeaderReplicas(List.of()).
                    setLastKnownElr(List.of())
            ));
        }
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

        return new TopicImage(image.name(), image.id(), mirrorName(), desiredMirrorState(), newPartitions);
    }

    /**
     * Find the partitions that have change based on the replica given.
     * <p>
     * The changes identified are:
     * <ul>
     *   <li>deletes: partitions for which the broker is not a replica anymore</li>
     *   <li>electedLeaders: partitions for which the broker is now a leader (leader epoch bump on the leader)</li>
     *   <li>leaders: partitions for which the isr or replicas change if the broker is a leader (partition epoch bump on the leader)</li>
     *   <li>followers: partitions for which the broker is now a follower or follower with isr or replica updates (partition epoch bump on follower)</li>
     *   <li>topicIds: a map of topic names to topic IDs in leaders and followers changes</li>
     *   <li>directoryIds: partitions for which directory id changes or newly added to the broker</li>
     *   <li>mirrorTopicStates: topics for which a mirror topic state change record was replayed, or the mirror name or desired mirror state changed</li>
     * </ul>
     * <p>
     * Leader epoch bumps are a strict subset of all partition epoch bumps, so all partitions in electedLeaders will be in leaders.
     *
     * @param brokerId the broker id
     * @return the LocalReplicaChanges that cover changes in the broker
     */
    @SuppressWarnings("checkstyle:cyclomaticComplexity")
    public LocalReplicaChanges localChanges(int brokerId) {
        Set<TopicPartition> deletes = new HashSet<>();
        Map<TopicPartition, LocalReplicaChanges.PartitionInfo> electedLeaders = new HashMap<>();
        Map<TopicPartition, LocalReplicaChanges.PartitionInfo> leaders = new HashMap<>();
        Map<TopicPartition, LocalReplicaChanges.PartitionInfo> followers = new HashMap<>();
        Map<String, Uuid> topicIds = new HashMap<>();
        Map<TopicIdPartition, Uuid> directoryIds = new HashMap<>();
        Map<Uuid, LocalReplicaChanges.MirrorTopicState> mirrorTopicChanges = new HashMap<>();

        LocalReplicaChanges.MirrorTopicState mirrorTopicState = mirrorTopicStateChange(image);
        if (mirrorTopicState != null) {
            mirrorTopicChanges.put(id(), mirrorTopicState);
        }

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
            } else {
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
                if (prevPartition == null || prevPartition.directory(brokerId) != entry.getValue().directory(brokerId)) {
                    directoryIds.put(
                        new TopicIdPartition(id(), new TopicPartition(name(), entry.getKey())),
                        entry.getValue().directory(brokerId)
                    );
                }
            } catch (IllegalArgumentException e) {
                // Do nothing if broker isn't part of the replica set.
            }
        }

        return new LocalReplicaChanges(deletes, electedLeaders, leaders, followers, topicIds, directoryIds, mirrorTopicChanges);
    }

    @Override
    public String toString() {
        return "TopicDelta(" +
                "partitionChanges=" + partitionChanges +
                (mirrorRecordReplayed ? ", mirrorName=" + mirrorName + ", desiredMirrorState=" + desiredMirrorState : "") +
                ')';
    }
}
