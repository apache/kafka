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

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.AlterPartitionRequestData.BrokerState;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.Replicas;
import org.apache.kafka.metadata.placement.DefaultDirProvider;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER_CHANGE;

/**
 * PartitionChangeBuilder handles changing partition registrations.
 */
public class PartitionChangeBuilder {
    private static final Logger log = LoggerFactory.getLogger(PartitionChangeBuilder.class);

    public static boolean changeRecordIsNoOp(PartitionChangeRecord record) {
        if (record.isr() != null) return false;
        if (record.eligibleLeaderReplicas() != null) return false;
        if (record.lastKnownElr() != null) return false;
        if (record.leader() != NO_LEADER_CHANGE) return false;
        if (record.replicas() != null) return false;
        if (record.removingReplicas() != null) return false;
        if (record.addingReplicas() != null) return false;
        if (record.leaderRecoveryState() != LeaderRecoveryState.NO_CHANGE) return false;
        return record.directories() == null;
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
    private final IntPredicate isAcceptableLeader;
    private final MetadataVersion metadataVersion;
    private final int minISR;
    private final Map<Integer, Uuid> targetDirectories;
    private List<Integer> targetIsr;
    private List<Integer> targetReplicas;
    private List<Integer> targetRemoving;
    private List<Integer> targetAdding;
    private List<Integer> targetElr;
    private List<Integer> targetLastKnownElr;
    private List<Integer> uncleanShutdownReplicas;
    private Election election = Election.ONLINE;
    private LeaderRecoveryState targetLeaderRecoveryState;
    private boolean eligibleLeaderReplicasEnabled;
    private DefaultDirProvider defaultDirProvider;

    // Whether allow electing last known leader in a Balanced recovery. Note, the last known leader will be stored in the
    // lastKnownElr field if enabled.
    private boolean useLastKnownLeaderInBalancedRecovery = true;

    public PartitionChangeBuilder(
        PartitionRegistration partition,
        Uuid topicId,
        int partitionId,
        IntPredicate isAcceptableLeader,
        MetadataVersion metadataVersion,
        int minISR
    ) {
        this.partition = partition;
        this.topicId = topicId;
        this.partitionId = partitionId;
        this.isAcceptableLeader = isAcceptableLeader;
        this.metadataVersion = metadataVersion;
        this.eligibleLeaderReplicasEnabled = false;
        this.minISR = minISR;

        this.targetIsr = Replicas.toList(partition.isr);
        this.targetReplicas = Replicas.toList(partition.replicas);
        this.targetRemoving = Replicas.toList(partition.removingReplicas);
        this.targetAdding = Replicas.toList(partition.addingReplicas);
        this.targetElr = Replicas.toList(partition.elr);
        this.targetLastKnownElr = Replicas.toList(partition.lastKnownElr);
        this.targetLeaderRecoveryState = partition.leaderRecoveryState;
        this.targetDirectories = DirectoryId.createAssignmentMap(partition.replicas, partition.directories);
        this.defaultDirProvider = uuid -> {
            throw new IllegalStateException("DefaultDirProvider is not set");
        };
    }

    public PartitionChangeBuilder setTargetIsr(List<Integer> targetIsr) {
        this.targetIsr = targetIsr;
        return this;
    }

    public PartitionChangeBuilder setTargetIsrWithBrokerStates(List<BrokerState> targetIsrWithEpoch) {
        return setTargetIsr(
            targetIsrWithEpoch
              .stream()
              .map(BrokerState::brokerId)
              .collect(Collectors.toList())
        );
    }

    public PartitionChangeBuilder setTargetReplicas(List<Integer> targetReplicas) {
        this.targetReplicas = targetReplicas;
        return this;
    }

    public PartitionChangeBuilder setUncleanShutdownReplicas(List<Integer> uncleanShutdownReplicas) {
        this.uncleanShutdownReplicas = uncleanShutdownReplicas;
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

    public PartitionChangeBuilder setEligibleLeaderReplicasEnabled(boolean eligibleLeaderReplicasEnabled) {
        this.eligibleLeaderReplicasEnabled = eligibleLeaderReplicasEnabled;
        return this;
    }

    public PartitionChangeBuilder setUseLastKnownLeaderInBalancedRecovery(boolean useLastKnownLeaderInBalancedRecovery) {
        this.useLastKnownLeaderInBalancedRecovery = useLastKnownLeaderInBalancedRecovery;
        return this;
    }

    public PartitionChangeBuilder setDirectory(int brokerId, Uuid dir) {
        this.targetDirectories.put(brokerId, dir);
        return this;
    }

    public PartitionChangeBuilder setDefaultDirProvider(DefaultDirProvider defaultDirProvider) {
        this.defaultDirProvider = defaultDirProvider;
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

    public List<Integer> targetIsr() {
        return targetIsr;
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

        if (canElectLastKnownLeader()) {
            return new ElectionResult(partition.lastKnownElr[0], true);
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

        if (canElectLastKnownLeader()) {
            return new ElectionResult(partition.lastKnownElr[0], true);
        }

        if (election == Election.UNCLEAN) {
            // Attempt unclean leader election
            Optional<Integer> uncleanLeader = targetReplicas.stream()
                .filter(isAcceptableLeader::test)
                .findFirst();
            if (uncleanLeader.isPresent()) {
                return new ElectionResult(uncleanLeader.get(), true);
            }
        }

        return new ElectionResult(NO_LEADER, false);
    }

    private boolean canElectLastKnownLeader() {
        if (!eligibleLeaderReplicasEnabled || !useLastKnownLeaderInBalancedRecovery) {
            log.trace("Try to elect last known leader for {}-{} but elrEnabled={}, useLastKnownLeaderInBalancedRecovery={}",
                    topicId, partitionId, eligibleLeaderReplicasEnabled, useLastKnownLeaderInBalancedRecovery);
            return false;
        }
        if (!targetElr.isEmpty() || !targetIsr.isEmpty()) {
            log.trace("Try to elect last known leader for {}-{} but ELR/ISR is not empty. ISR={}, ELR={}",
                    topicId, partitionId, targetIsr, targetElr);
            return false;
        }

        // When the last known leader is enabled:
        // 1. The targetLastKnownElr will only be used to store the last known leader, and it is updated after the
        //    leader election. So we can only refer to the lastKnownElr in the existing partition registration.
        // 2. When useLastKnownLeaderInBalancedRecovery=false, it intends to use other type of unclean leader election
        //    and the lastKnownElr is populated with the real last known ELR members. Then it may have multiple members
        //    in the field even if useLastKnownLeaderInBalancedRecovery is set to true again. In this case, we can't
        //    refer to the lastKnownElr.
        if (partition.lastKnownElr.length != 1) {
            log.trace("Try to elect last known leader for {}-{} but lastKnownElr does not only have 1 member. lastKnownElr={}",
                    topicId, partitionId, Arrays.toString(partition.lastKnownElr));
            return false;
        }
        if (isAcceptableLeader.test(partition.lastKnownElr[0])) {
            log.trace("Try to elect last known leader for {}-{} but last known leader is not alive. last known leader={}",
                    topicId, partitionId, partition.lastKnownElr[0]);
        }
        return true;
    }

    private boolean isValidNewLeader(int replica) {
        // The valid new leader should be in either ISR or in ELR when ISR is empty.
        return (targetIsr.contains(replica) || (targetIsr.isEmpty() && targetElr.contains(replica))) &&
            isAcceptableLeader.test(replica);
    }

    private void tryElection(PartitionChangeRecord record) {
        ElectionResult electionResult = electLeader();
        if (electionResult.node != partition.leader) {
            // generating log messages for partition elections can get expensive on large clusters,
            // so only log clean elections at TRACE level; log unclean elections at INFO level
            // to ensure the message is emitted since an unclean election can lead to data loss;
            if (targetElr.contains(electionResult.node)) {
                targetIsr = Collections.singletonList(electionResult.node);
                targetElr = targetElr.stream().filter(replica -> replica != electionResult.node)
                    .collect(Collectors.toList());
                log.trace("Setting new leader for topicId {}, partition {} to {} using ELR",
                        topicId, partitionId, electionResult.node);
            } else if (electionResult.unclean) {
                log.info("Setting new leader for topicId {}, partition {} to {} using an unclean election",
                    topicId, partitionId, electionResult.node);
            } else {
                log.trace("Setting new leader for topicId {}, partition {} to {} using a clean election",
                    topicId, partitionId, electionResult.node);
            }
            record.setLeader(electionResult.node);
            if (electionResult.unclean) {
                // If the election was unclean, we have to forcibly set the ISR to just the
                // new leader. This can result in data loss!
                record.setIsr(Collections.singletonList(electionResult.node));
                if (partition.leaderRecoveryState != LeaderRecoveryState.RECOVERING &&
                    metadataVersion.isLeaderRecoverySupported()) {
                    // And mark the leader recovery state as RECOVERING
                    record.setLeaderRecoveryState(LeaderRecoveryState.RECOVERING.value());
                }
            }
        } else {
            log.trace("Failed to find a new leader with current state: {}", this);
        }
    }

    /**
     * Trigger a leader epoch bump if one is needed because of replica reassignment.
     *
     * Note that if the leader epoch increases, the partition epoch will always increase as well; there is no
     * case where the partition epoch increases more slowly than the leader epoch.
     */
    void triggerLeaderEpochBumpForReplicaReassignmentIfNeeded(PartitionChangeRecord record) {
        if (record.leader() != NO_LEADER_CHANGE) {
            // The leader is already changing, so there will already be a leader epoch bump.
            return;
        }
        if (!Replicas.contains(targetReplicas, partition.replicas)) {
            // If the new replica list does not contain all the brokers that the old one did,
            // ensure that there will be a leader epoch bump by setting the leader field.
            record.setLeader(partition.leader);
        }
    }

    /**
     * Trigger a leader epoch bump if one is needed because of an ISR shrink.
     *
     * Note that it's important to call this function only after we have set the ISR field in
     * the PartitionChangeRecord.
     */
    void triggerLeaderEpochBumpForIsrShrinkIfNeeded(PartitionChangeRecord record) {
        if (!metadataVersion.isLeaderEpochBumpRequiredOnIsrShrink()) {
            // We only need to bump the leader epoch on an ISR shrink in older metadata versions
            // before 3.6, where there was a bug (KAFKA-15021) in the broker replica manager that
            // required that the leader epoch be bumped whenever the ISR shrank. (This was never
            // necessary for EXPANSIONS, only SHRINKS.)
            return;
        }
        if (record.leader() != NO_LEADER_CHANGE) {
            // The leader is already changing, so there will already be a leader epoch bump.
            return;
        }
        if (record.isr() == null) {
            // The ISR is not changing.
            return;
        }
        if (!Replicas.contains(record.isr(), partition.isr)) {
            // If the new ISR list does not contain all the brokers that the old one did,
            // ensure that there will be a leader epoch bump by setting the leader field.
            record.setLeader(partition.leader);
        }
    }

    private void completeReassignmentIfNeeded() {
        PartitionReassignmentReplicas reassignmentReplicas =
            new PartitionReassignmentReplicas(
                targetRemoving,
                targetAdding,
                targetReplicas);

        Optional<PartitionReassignmentReplicas.CompletedReassignment> completedReassignmentOpt =
            reassignmentReplicas.maybeCompleteReassignment(targetIsr);
        if (completedReassignmentOpt.isEmpty()) {
            return;
        }

        PartitionReassignmentReplicas.CompletedReassignment completedReassignment = completedReassignmentOpt.get();

        targetIsr = completedReassignment.isr;
        targetReplicas = completedReassignment.replicas;
        targetRemoving = Collections.emptyList();
        targetAdding = Collections.emptyList();
    }

    public Optional<ApiMessageAndVersion> build() {
        PartitionChangeRecord record = new PartitionChangeRecord().
            setTopicId(topicId).
            setPartitionId(partitionId);

        completeReassignmentIfNeeded();

        maybePopulateTargetElr();

        tryElection(record);

        triggerLeaderEpochBumpForReplicaReassignmentIfNeeded(record);

        maybeUpdateRecordElr(record);

        // If ELR is enabled, the ISR is allowed to be empty.
        if (record.isr() == null && (!targetIsr.isEmpty() || eligibleLeaderReplicasEnabled) &&
            !targetIsr.equals(Replicas.toList(partition.isr))) {
            // Set the new ISR if it is different from the current ISR and unclean leader election didn't already set it.
            if (targetIsr.isEmpty()) {
                log.debug("A partition will have an empty ISR. {}", this);
            }
            record.setIsr(targetIsr);
        }

        triggerLeaderEpochBumpForIsrShrinkIfNeeded(record);

        maybeUpdateLastKnownLeader(record);

        setAssignmentChanges(record);

        if (targetLeaderRecoveryState != partition.leaderRecoveryState) {
            record.setLeaderRecoveryState(targetLeaderRecoveryState.value());
        }

        if (changeRecordIsNoOp(record)) {
            return Optional.empty();
        } else {
            return Optional.of(new ApiMessageAndVersion(record, metadataVersion.partitionChangeRecordVersion()));
        }
    }

    private void setAssignmentChanges(PartitionChangeRecord record) {
        if (!targetReplicas.isEmpty()) {
            if (metadataVersion.isDirectoryAssignmentSupported()) {
                List<Uuid> directories = new ArrayList<>(targetReplicas.size());
                for (int replica : targetReplicas) {
                    directories.add(this.targetDirectories.getOrDefault(replica, defaultDirProvider.defaultDir(replica)));
                }
                if (!directories.equals(Arrays.asList(partition.directories))) {
                    record.setDirectories(directories);
                }
            }
            if (!targetReplicas.equals(Replicas.toList(partition.replicas))) {
                record.setReplicas(targetReplicas);
            }
        }
        if (!targetRemoving.equals(Replicas.toList(partition.removingReplicas))) {
            record.setRemovingReplicas(targetRemoving);
        }
        if (!targetAdding.equals(Replicas.toList(partition.addingReplicas))) {
            record.setAddingReplicas(targetAdding);
        }
    }

    private void maybeUpdateLastKnownLeader(PartitionChangeRecord record) {
        if (!useLastKnownLeaderInBalancedRecovery || !eligibleLeaderReplicasEnabled) return;
        if (record.isr() != null && record.isr().isEmpty() && (partition.lastKnownElr.length != 1 ||
            partition.lastKnownElr[0] != partition.leader)) {
            // Only update the last known leader when the first time the partition becomes leaderless.
            record.setLastKnownElr(Collections.singletonList(partition.leader));
        } else if ((record.leader() >= 0 || (partition.leader != NO_LEADER && record.leader() != NO_LEADER))
            && partition.lastKnownElr.length > 0) {
            // Clear the LastKnownElr field if the partition will have or continues to have a valid leader.
            record.setLastKnownElr(Collections.emptyList());
        }
    }

    private void maybeUpdateRecordElr(PartitionChangeRecord record) {
        // During the leader election, it can set the record isr if an unclean leader election happens.
        boolean isCleanLeaderElection = record.isr() == null;

        // Clean the ELR related fields if it is an unclean election or ELR is disabled.
        if (!isCleanLeaderElection || !eligibleLeaderReplicasEnabled) {
            targetElr = Collections.emptyList();
            targetLastKnownElr = Collections.emptyList();
        }

        if (!targetElr.equals(Replicas.toList(partition.elr))) {
            record.setEligibleLeaderReplicas(targetElr);
        }

        if (useLastKnownLeaderInBalancedRecovery && partition.lastKnownElr.length == 1 &&
                (record.leader() == NO_LEADER || record.leader() == NO_LEADER_CHANGE && partition.leader == NO_LEADER)) {
            // If the last known leader is stored in the lastKnownElr, the last known elr should not be updated when
            // the partition does not have a leader.
            targetLastKnownElr = Replicas.toList(partition.lastKnownElr);
        }

        // If the last known ELR is expected to store the last known leader, the lastKnownElr field should be updated
        // later in maybeUpdateLastKnownLeader.
        if (useLastKnownLeaderInBalancedRecovery) return;

        if (!targetLastKnownElr.equals(Replicas.toList(partition.lastKnownElr))) {
            record.setLastKnownElr(targetLastKnownElr);
        }
    }

    private void maybePopulateTargetElr() {
        if (!eligibleLeaderReplicasEnabled) return;

        // If the ISR is larger or equal to the min ISR, clear the ELR and LastKnownElr.
        if (targetIsr.size() >= minISR) {
            targetElr = Collections.emptyList();
            targetLastKnownElr = Collections.emptyList();
            return;
        }

        Set<Integer> targetIsrSet = new HashSet<>(targetIsr);
        // Tracking the ELR. The new elr is expected to
        // 1. Include the current ISR
        // 2. Exclude the duplicate replicas between elr and target ISR.
        // 3. Exclude unclean shutdown replicas.
        // To do that, we first union the current ISR and current elr, then filter out the target ISR and unclean shutdown
        // Replicas.
        Set<Integer> candidateSet = new HashSet<>(targetElr);
        Arrays.stream(partition.isr).forEach(candidateSet::add);
        targetElr = candidateSet.stream()
            .filter(replica -> !targetIsrSet.contains(replica))
            .filter(replica -> uncleanShutdownReplicas == null || !uncleanShutdownReplicas.contains(replica))
            .collect(Collectors.toList());

        // Calculate the new last known ELR. Includes any ISR members since the ISR size drops below min ISR.
        // In order to reduce the metadata usage, the last known ELR excludes the members in ELR and current ISR.
        candidateSet.addAll(targetLastKnownElr);
        targetLastKnownElr = candidateSet.stream()
            .filter(replica -> !targetIsrSet.contains(replica))
            .filter(replica -> !targetElr.contains(replica))
            .collect(Collectors.toList());
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
            ", targetElr=" + targetElr +
            ", targetLastKnownElr=" + targetLastKnownElr +
            ", uncleanShutdownReplicas=" + uncleanShutdownReplicas +
            ", election=" + election +
            ", targetLeaderRecoveryState=" + targetLeaderRecoveryState +
            ')';
    }
}
