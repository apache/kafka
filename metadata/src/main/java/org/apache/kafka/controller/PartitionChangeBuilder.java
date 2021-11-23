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
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.Replicas;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.kafka.common.metadata.MetadataRecordType.PARTITION_CHANGE_RECORD;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER_CHANGE;

/**
 * PartitionChangeBuilder handles changing partition registrations.
 */
public class PartitionChangeBuilder {
    private static final Logger log = LoggerFactory.getLogger(PartitionChangeBuilder.class);

    public static boolean changeRecordIsNoOp(PartitionChangeRecord record) {
        if (record.isr() != null) return false;
        if (record.leader() != NO_LEADER_CHANGE) return false;
        if (record.replicas() != null) return false;
        if (record.removingReplicas() != null) return false;
        if (record.addingReplicas() != null) return false;
        return true;
    }

    private final PartitionRegistration partition;
    private final Uuid topicId;
    private final int partitionId;
    private final Function<Integer, Boolean> isAcceptableLeader;
    private final Supplier<Boolean> uncleanElectionOk;
    private List<Integer> targetIsr;
    private List<Integer> targetReplicas;
    private List<Integer> targetRemoving;
    private List<Integer> targetAdding;
    private boolean alwaysElectPreferredIfPossible;

    public PartitionChangeBuilder(PartitionRegistration partition,
                                  Uuid topicId,
                                  int partitionId,
                                  Function<Integer, Boolean> isAcceptableLeader,
                                  Supplier<Boolean> uncleanElectionOk) {
        this.partition = partition;
        this.topicId = topicId;
        this.partitionId = partitionId;
        this.isAcceptableLeader = isAcceptableLeader;
        this.uncleanElectionOk = uncleanElectionOk;
        this.targetIsr = Replicas.toList(partition.isr);
        this.targetReplicas = Replicas.toList(partition.replicas);
        this.targetRemoving = Replicas.toList(partition.removingReplicas);
        this.targetAdding = Replicas.toList(partition.addingReplicas);
        this.alwaysElectPreferredIfPossible = false;
    }

    public PartitionChangeBuilder setTargetIsr(List<Integer> targetIsr) {
        this.targetIsr = targetIsr;
        return this;
    }

    public PartitionChangeBuilder setTargetReplicas(List<Integer> targetReplicas) {
        this.targetReplicas = targetReplicas;
        return this;
    }

    public PartitionChangeBuilder setAlwaysElectPreferredIfPossible(boolean alwaysElectPreferredIfPossible) {
        this.alwaysElectPreferredIfPossible = alwaysElectPreferredIfPossible;
        return this;
    }

    public PartitionChangeBuilder setTargetRemoving(List<Integer> targetRemoving) {
        this.targetRemoving = targetRemoving;
        return this;
    }

    public PartitionChangeBuilder setTargetAdding(List<Integer> targetAdding) {
        this.targetAdding = targetAdding;
        return this;
    }

    boolean shouldTryElection() {
        // If the new isr doesn't have the current leader, we need to try to elect a new
        // one. Note: this also handles the case where the current leader is NO_LEADER,
        // since that value cannot appear in targetIsr.
        if (!targetIsr.contains(partition.leader)) return true;

        // Check if we want to try to get away from a non-preferred leader.
        if (alwaysElectPreferredIfPossible && !partition.hasPreferredLeader()) return true;

        return false;
    }

    class BestLeader {
        final int node;
        final boolean unclean;

        BestLeader() {
            for (int replica : targetReplicas) {
                if (targetIsr.contains(replica) && isAcceptableLeader.apply(replica)) {
                    this.node = replica;
                    this.unclean = false;
                    return;
                }
            }
            if (uncleanElectionOk.get()) {
                for (int replica : targetReplicas) {
                    if (isAcceptableLeader.apply(replica)) {
                        this.node = replica;
                        this.unclean = true;
                        return;
                    }
                }
            }
            this.node = NO_LEADER;
            this.unclean = false;
        }
    }

    private void tryElection(PartitionChangeRecord record) {
        BestLeader bestLeader = new BestLeader();
        if (bestLeader.node != partition.leader) {
            log.debug("Setting new leader for topicId {}, partition {} to {}", topicId, partitionId, bestLeader.node);
            record.setLeader(bestLeader.node);
            if (bestLeader.unclean) {
                // If the election was unclean, we have to forcibly set the ISR to just the
                // new leader. This can result in data loss!
                record.setIsr(Collections.singletonList(bestLeader.node));
            }
        } else {
            log.debug("Failed to find a new leader with current state: {}", this);
        }
    }

    /**
     * Trigger a leader epoch bump if one is needed.
     *
     * We need to bump the leader epoch if:
     * 1. The leader changed, or
     * 2. The new ISR does not contain all the nodes that the old ISR did, or
     * 3. The new replia list does not contain all the nodes that the old replia list did.
     *
     * Changes that do NOT fall in any of these categories will increase the partition epoch, but
     * not the leader epoch. Note that if the leader epoch increases, the partition epoch will
     * always increase as well; there is no case where the partition epoch increases more slowly
     * than the leader epoch.
     *
     * If the PartitionChangeRecord sets the leader field to something other than
     * NO_LEADER_CHANGE, a leader epoch bump will automatically occur. That takes care of
     * case 1. In this function, we check for cases 2 and 3, and handle them by manually
     * setting record.leader to the current leader.
     */
    void triggerLeaderEpochBumpIfNeeded(PartitionChangeRecord record) {
        if (record.leader() == NO_LEADER_CHANGE) {
            if (!Replicas.contains(targetIsr, partition.isr) ||
                    !Replicas.contains(targetReplicas, partition.replicas)) {
                record.setLeader(partition.leader);
            }
        }
    }

    private void completeReassignmentIfNeeded() {
        // Check if there is a reassignment to complete.
        if (targetRemoving.isEmpty() && targetAdding.isEmpty()) return;

        List<Integer> newTargetIsr = targetIsr;
        List<Integer> newTargetReplicas = targetReplicas;
        if (!targetRemoving.isEmpty()) {
            newTargetIsr = new ArrayList<>(targetIsr.size());
            for (int replica : targetIsr) {
                if (!targetRemoving.contains(replica)) {
                    newTargetIsr.add(replica);
                }
            }
            if (newTargetIsr.isEmpty()) return;
            newTargetReplicas = new ArrayList<>(targetReplicas.size());
            for (int replica : targetReplicas) {
                if (!targetRemoving.contains(replica)) {
                    newTargetReplicas.add(replica);
                }
            }
            if (newTargetReplicas.isEmpty()) return;
        }
        for (int replica : targetAdding) {
            if (!newTargetIsr.contains(replica)) return;
        }
        targetIsr = newTargetIsr;
        targetReplicas = newTargetReplicas;
        targetRemoving = Collections.emptyList();
        targetAdding = Collections.emptyList();
    }

    public Optional<ApiMessageAndVersion> build() {
        PartitionChangeRecord record = new PartitionChangeRecord().
            setTopicId(topicId).
            setPartitionId(partitionId);

        completeReassignmentIfNeeded();

        if (shouldTryElection()) {
            tryElection(record);
        }

        triggerLeaderEpochBumpIfNeeded(record);

        if (!targetIsr.isEmpty() && !targetIsr.equals(Replicas.toList(partition.isr))) {
            record.setIsr(targetIsr);
        }
        if (!targetReplicas.isEmpty() && !targetReplicas.equals(Replicas.toList(partition.replicas))) {
            record.setReplicas(targetReplicas);
        }
        if (!targetRemoving.equals(Replicas.toList(partition.removingReplicas))) {
            record.setRemovingReplicas(targetRemoving);
        }
        if (!targetAdding.equals(Replicas.toList(partition.addingReplicas))) {
            record.setAddingReplicas(targetAdding);
        }
        if (changeRecordIsNoOp(record)) {
            return Optional.empty();
        } else {
            return Optional.of(new ApiMessageAndVersion(record,
                PARTITION_CHANGE_RECORD.highestSupportedVersion()));
        }
    }

    @Override
    public String toString() {
        return "PartitionChangeBuilder(" +
            "partition=" + partition +
            ", topicId=" + topicId +
            ", partitionId=" + partitionId +
            ", isAcceptableLeader=" + isAcceptableLeader +
            ", uncleanElectionOk=" + uncleanElectionOk +
            ", targetIsr=" + targetIsr +
            ", targetReplicas=" + targetReplicas +
            ", targetRemoving=" + targetRemoving +
            ", targetAdding=" + targetAdding +
            ", alwaysElectPreferredIfPossible=" + alwaysElectPreferredIfPossible +
            ')';
    }
}
