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
import org.apache.kafka.metadata.LeaderRecoveryState;
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

    /**
     * Election types.
     */
    public enum Election {
        /**
         * Perform leader election to keep the partition online. Elect the preferred replica if it is in the ISR.
         */
        PREFERRED,
        /**
         * Perform leader election from the ISR to keep the partition online.
         */
        ONLINE,
        /**
         * Prefer replicas in the ISR but keep the partition online even if it requires picking a leader that is not in the ISR.
         */
        UNCLEAN
    }

    private final PartitionRegistration partition;
    private final Uuid topicId;
    private final int partitionId;
    private final Function<Integer, Boolean> isAcceptableLeader;
    private final boolean isLeaderRecoverySupported;
    private List<Integer> targetIsr;
    private List<Integer> targetReplicas;
    private List<Integer> targetRemoving;
    private List<Integer> targetAdding;
    private Election election = Election.ONLINE;
    private LeaderRecoveryState targetLeaderRecoveryState;

    public PartitionChangeBuilder(PartitionRegistration partition,
                                  Uuid topicId,
                                  int partitionId,
                                  Function<Integer, Boolean> isAcceptableLeader,
                                  boolean isLeaderRecoverySupported) {
        this.partition = partition;
        this.topicId = topicId;
        this.partitionId = partitionId;
        this.isAcceptableLeader = isAcceptableLeader;
        this.isLeaderRecoverySupported = isLeaderRecoverySupported;
        this.targetIsr = Replicas.toList(partition.isr);
        this.targetReplicas = Replicas.toList(partition.replicas);
        this.targetRemoving = Replicas.toList(partition.removingReplicas);
        this.targetAdding = Replicas.toList(partition.addingReplicas);
        this.targetLeaderRecoveryState = partition.leaderRecoveryState;
    }

    public PartitionChangeBuilder setTargetIsr(List<Integer> targetIsr) {
        this.targetIsr = targetIsr;
        return this;
    }

    public PartitionChangeBuilder setTargetReplicas(List<Integer> targetReplicas) {
        this.targetReplicas = targetReplicas;
        return this;
    }

    public PartitionChangeBuilder setElection(Election election) {
        this.election = election;
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

    public PartitionChangeBuilder setTargetLeaderRecoveryState(LeaderRecoveryState targetLeaderRecoveryState) {
        this.targetLeaderRecoveryState = targetLeaderRecoveryState;
        return this;
    }

    // VisibleForTesting
    static class ElectionResult {
        final int node;
        final boolean unclean;

        private ElectionResult(int node, boolean unclean) {
            this.node = node;
            this.unclean = unclean;
        }
    }

    // VisibleForTesting
    /**
     * Perform leader election based on the partition state and leader election type.
     *
     * See documentation for the Election type to see more details on the election types supported.
     */
    ElectionResult electLeader() {
        if (election == Election.PREFERRED) {
            return electPreferredLeader();
        }

        return electAnyLeader();
    }

    /**
     * Assumes that the election type is Election.PREFERRED
     */
    private ElectionResult electPreferredLeader() {
        int preferredReplica = targetReplicas.get(0);
        if (isValidNewLeader(preferredReplica)) {
            return new ElectionResult(preferredReplica, false);
        }

        if (isValidNewLeader(partition.leader)) {
            // Don't consider a new leader since the current leader meets all the constraints
            return new ElectionResult(partition.leader, false);
        }

        Optional<Integer> onlineLeader = targetReplicas.stream()
            .skip(1)
            .filter(this::isValidNewLeader)
            .findFirst();
        if (onlineLeader.isPresent()) {
            return new ElectionResult(onlineLeader.get(), false);
        }

        return new ElectionResult(NO_LEADER, false);
    }

    /**
     * Assumes that the election type is either Election.ONLINE or Election.UNCLEAN
     */
    private ElectionResult electAnyLeader() {
        if (isValidNewLeader(partition.leader)) {
            // Don't consider a new leader since the current leader meets all the constraints
            return new ElectionResult(partition.leader, false);
        }

        Optional<Integer> onlineLeader = targetReplicas.stream()
            .filter(this::isValidNewLeader)
            .findFirst();
        if (onlineLeader.isPresent()) {
            return new ElectionResult(onlineLeader.get(), false);
        }

        if (election == Election.UNCLEAN) {
            // Attempt unclean leader election
            Optional<Integer> uncleanLeader = targetReplicas.stream()
                .filter(replica -> isAcceptableLeader.apply(replica))
                .findFirst();
            if (uncleanLeader.isPresent()) {
                return new ElectionResult(uncleanLeader.get(), true);
            }
        }

        return new ElectionResult(NO_LEADER, false);
    }

    private boolean isValidNewLeader(int replica) {
        return targetIsr.contains(replica) && isAcceptableLeader.apply(replica);
    }

    private void tryElection(PartitionChangeRecord record) {
        ElectionResult electionResult = electLeader();
        if (electionResult.node != partition.leader) {
            log.debug(
                "Setting new leader for topicId {}, partition {} to {} using {} election",
                topicId,
                partitionId,
                electionResult.node,
                electionResult.unclean ? "an unclean" : "a clean"
            );
            record.setLeader(electionResult.node);
            if (electionResult.unclean) {
                // If the election was unclean, we have to forcibly set the ISR to just the
                // new leader. This can result in data loss!
                record.setIsr(Collections.singletonList(electionResult.node));
                if (partition.leaderRecoveryState != LeaderRecoveryState.RECOVERING &&
                    isLeaderRecoverySupported) {
                    // And mark the leader recovery state as RECOVERING
                    record.setLeaderRecoveryState(LeaderRecoveryState.RECOVERING.value());
                }
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

        tryElection(record);

        triggerLeaderEpochBumpIfNeeded(record);

        if (record.isr() == null && !targetIsr.isEmpty() && !targetIsr.equals(Replicas.toList(partition.isr))) {
            // Set the new ISR if it is different from the current ISR and unclean leader election didn't already set it.
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
        if (targetLeaderRecoveryState != partition.leaderRecoveryState) {
            record.setLeaderRecoveryState(targetLeaderRecoveryState.value());
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
            ", targetIsr=" + targetIsr +
            ", targetReplicas=" + targetReplicas +
            ", targetRemoving=" + targetRemoving +
            ", targetAdding=" + targetAdding +
            ", election=" + election +
            ", targetLeaderRecoveryState=" + targetLeaderRecoveryState +
            ')';
    }
}
