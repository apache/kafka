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
package org.apache.kafka.connect.runtime.distributed;

import java.util.Arrays;
import java.util.Map.Entry;

import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.ConnectorsAndTasks;
import org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.WorkerLoad;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.Assignment;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECT_PROTOCOL_V2;
import static org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.LeaderState;
import static org.apache.kafka.connect.util.ConnectUtils.combineCollections;
import static org.apache.kafka.connect.util.ConnectUtils.transformValues;

/**
 * An assignor that computes a distribution of connectors and tasks according to the incremental
 * cooperative strategy for rebalancing. Note that this class is NOT thread-safe.
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-415%3A+Incremental+Cooperative+Rebalancing+in+Kafka+Connect">
 * KIP-415 for a description of the assignment policy. </a>
 *
 */
public class IncrementalCooperativeAssignor implements ConnectAssignor {
    private final Logger log;
    private final Time time;
    private final int maxDelay;
    private ConnectorsAndTasks previousAssignment;
    private final ConnectorsAndTasks previousRevocation;
    private boolean revokedInPrevious;
    protected final Set<String> candidateWorkersForReassignment;
    protected long scheduledRebalance;
    protected int delay;
    protected int previousGenerationId;
    protected Set<String> previousMembers;

    private final ExponentialBackoff consecutiveRevokingRebalancesBackoff;

    private int numSuccessiveRevokingRebalances;

    public IncrementalCooperativeAssignor(LogContext logContext, Time time, int maxDelay) {
        this.log = logContext.logger(IncrementalCooperativeAssignor.class);
        this.time = time;
        this.maxDelay = maxDelay;
        this.previousAssignment = ConnectorsAndTasks.EMPTY;
        this.previousRevocation = new ConnectorsAndTasks.Builder().build();
        this.scheduledRebalance = 0;
        this.revokedInPrevious = false;
        this.candidateWorkersForReassignment = new LinkedHashSet<>();
        this.delay = 0;
        this.previousGenerationId = -1;
        this.previousMembers = Collections.emptySet();
        this.numSuccessiveRevokingRebalances = 0;
        // By default, initial interval is 1. The only corner case is when the user has set maxDelay to 0
        // in which case, the exponential backoff delay should be 0 which would return the backoff delay to be 0 always
        this.consecutiveRevokingRebalancesBackoff = new ExponentialBackoff(maxDelay == 0 ? 0 : 1, 40, maxDelay, 0);
    }

    @Override
    public Map<String, ByteBuffer> performAssignment(String leaderId, String protocol,
                                                     List<JoinGroupResponseMember> allMemberMetadata,
                                                     WorkerCoordinator coordinator) {
        log.debug("Performing task assignment");

        Map<String, ExtendedWorkerState> memberConfigs = new HashMap<>();
        for (JoinGroupResponseMember member : allMemberMetadata) {
            memberConfigs.put(
                    member.memberId(),
                    IncrementalCooperativeConnectProtocol.deserializeMetadata(ByteBuffer.wrap(member.metadata())));
        }
        log.debug("Member configs: {}", memberConfigs);

        // The new config offset is the maximum seen by any member. We always perform assignment using this offset,
        // even if some members have fallen behind. The config offset used to generate the assignment is included in
        // the response so members that have fallen behind will not use the assignment until they have caught up.
        long maxOffset = memberConfigs.values().stream().map(ExtendedWorkerState::offset).max(Long::compare).get();
        log.debug("Max config offset root: {}, local snapshot config offsets root: {}",
                  maxOffset, coordinator.configSnapshot().offset());

        short protocolVersion = ConnectProtocolCompatibility.fromProtocol(protocol).protocolVersion();

        Long leaderOffset = ensureLeaderConfig(maxOffset, coordinator);
        if (leaderOffset == null) {
            Map<String, ExtendedAssignment> assignments = fillAssignments(
                    memberConfigs.keySet(), Assignment.CONFIG_MISMATCH,
                    leaderId, memberConfigs.get(leaderId).url(), maxOffset,
                    ClusterAssignment.EMPTY, 0, protocolVersion);
            return serializeAssignments(assignments, protocolVersion);
        }
        return performTaskAssignment(leaderId, leaderOffset, memberConfigs, coordinator, protocolVersion);
    }

    private Long ensureLeaderConfig(long maxOffset, WorkerCoordinator coordinator) {
        // If this leader is behind some other members, we can't do assignment
        if (coordinator.configSnapshot().offset() < maxOffset) {
            // We might be able to take a new snapshot to catch up immediately and avoid another round of syncing here.
            // Alternatively, if this node has already passed the maximum reported by any other member of the group, it
            // is also safe to use this newer state.
            ClusterConfigState updatedSnapshot = coordinator.configFreshSnapshot();
            if (updatedSnapshot.offset() < maxOffset) {
                log.info("Was selected to perform assignments, but do not have latest config found in sync request. "
                         + "Returning an empty configuration to trigger re-sync.");
                return null;
            } else {
                coordinator.configSnapshot(updatedSnapshot);
                return updatedSnapshot.offset();
            }
        }
        return maxOffset;
    }

    /**
     * Performs task assignment based on the incremental cooperative connect protocol.
     * Read more on the design and implementation in:
     * <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-415%3A+Incremental+Cooperative+Rebalancing+in+Kafka+Connect">
     * KIP-415</a>
     *
     * @param leaderId the ID of the group leader
     * @param maxOffset the latest known offset of the configuration topic
     * @param memberConfigs the metadata of all the members of the group as gather in the current
     * round of rebalancing
     * @param coordinator the worker coordinator instance that provide the configuration snapshot
     * and get assigned the leader state during this assignment
     * @param protocolVersion the Connect subprotocol version
     * @return the serialized assignment of tasks to the whole group, including assigned or
     * revoked tasks
     */
    protected Map<String, ByteBuffer> performTaskAssignment(String leaderId, long maxOffset,
                                                            Map<String, ExtendedWorkerState> memberConfigs,
                                                            WorkerCoordinator coordinator, short protocolVersion) {
        log.debug("Performing task assignment during generation: {} with memberId: {}",
                coordinator.generationId(), coordinator.memberId());
        Map<String, ConnectorsAndTasks> memberAssignments = transformValues(
                memberConfigs,
                memberConfig -> new ConnectorsAndTasks.Builder()
                        .with(memberConfig.assignment().connectors(), memberConfig.assignment().tasks())
                        .build()
        );
        ClusterAssignment clusterAssignment = performTaskAssignment(
                coordinator.configSnapshot(),
                coordinator.lastCompletedGenerationId(),
                coordinator.generationId(),
                memberAssignments
        );

        coordinator.leaderState(new LeaderState(memberConfigs, clusterAssignment.allAssignedConnectors(), clusterAssignment.allAssignedTasks()));

        Map<String, ExtendedAssignment> assignments =
                fillAssignments(memberConfigs.keySet(), Assignment.NO_ERROR, leaderId,
                        memberConfigs.get(leaderId).url(), maxOffset,
                        clusterAssignment,
                        delay, protocolVersion);

        log.debug("Actual assignments: {}", assignments);
        return serializeAssignments(assignments, protocolVersion);
    }

    // Visible for testing
    ClusterAssignment performTaskAssignment(
            ClusterConfigState configSnapshot,
            int lastCompletedGenerationId,
            int currentGenerationId,
            Map<String, ConnectorsAndTasks> memberAssignments
    ) {
        // Base set: The previous assignment of connectors-and-tasks is a standalone snapshot that
        // can be used to calculate derived sets
        log.debug("Previous assignments: {}", previousAssignment);
        if (previousGenerationId != lastCompletedGenerationId) {
            log.debug("Clearing the view of previous assignments due to generation mismatch between "
                    + "previous generation ID {} and last completed generation ID {}. This can "
                    + "happen if the leader fails to sync the assignment within a rebalancing round. "
                    + "The following view of previous assignments might be outdated and will be "
                    + "ignored by the leader in the current computation of new assignments. "
                    + "Possibly outdated previous assignments: {}",
                    previousGenerationId, lastCompletedGenerationId, previousAssignment);
            this.previousAssignment = ConnectorsAndTasks.EMPTY;
        }

        Set<String> configuredConnectors = new TreeSet<>(configSnapshot.connectors());
        Set<ConnectorTaskId> configuredTasks = combineCollections(configuredConnectors, configSnapshot::tasks, Collectors.toSet());

        // Base set: The set of configured connectors-and-tasks is a standalone snapshot that can
        // be used to calculate derived sets
        ConnectorsAndTasks configured = new ConnectorsAndTasks.Builder()
                .with(configuredConnectors, configuredTasks).build();
        log.debug("Configured assignments: {}", configured);

        // Base set: The set of active connectors-and-tasks is a standalone snapshot that can be
        // used to calculate derived sets
        ConnectorsAndTasks activeAssignments = assignment(memberAssignments);
        log.debug("Active assignments: {}", activeAssignments);

        // This means that a previous revocation did not take effect. In this case, reset
        // appropriately and be ready to re-apply revocation of tasks
        if (!previousRevocation.isEmpty()) {
            if (previousRevocation.connectors().stream().anyMatch(c -> activeAssignments.connectors().contains(c))
                    || previousRevocation.tasks().stream().anyMatch(t -> activeAssignments.tasks().contains(t))) {
                previousAssignment = activeAssignments;
            }
            previousRevocation.connectors().clear();
            previousRevocation.tasks().clear();
        }

        // Derived set: The set of deleted connectors-and-tasks is a derived set from the set
        // difference of previous - configured
        ConnectorsAndTasks deleted = diff(previousAssignment, configured);
        log.debug("Deleted assignments: {}", deleted);

        // The connectors and tasks that are currently running on more than one worker each
        ConnectorsAndTasks duplicated = duplicatedAssignments(memberAssignments);
        log.trace("Duplicated assignments: {}", duplicated);

        // Derived set: The set of lost or unaccounted connectors-and-tasks is a derived set from
        // the set difference of previous - active - deleted
        ConnectorsAndTasks lostAssignments = diff(previousAssignment, activeAssignments, deleted);
        log.debug("Lost assignments: {}", lostAssignments);

        // Derived set: The set of new connectors-and-tasks is a derived set from the set
        // difference of configured - previous - active
        ConnectorsAndTasks created = diff(configured, previousAssignment, activeAssignments);
        log.debug("Created: {}", created);

        // A collection of the current assignment excluding the connectors-and-tasks to be deleted
        List<WorkerLoad> currentWorkerAssignment = workerAssignment(memberAssignments, deleted);

        Map<String, ConnectorsAndTasks.Builder> toRevoke = new HashMap<>();

        Map<String, ConnectorsAndTasks> deletedToRevoke = intersection(deleted, memberAssignments);
        log.debug("Deleted connectors and tasks to revoke from each worker: {}", deletedToRevoke);
        addAll(toRevoke, deletedToRevoke);

        // Revoking redundant connectors/tasks if the workers have duplicate assignments
        Map<String, ConnectorsAndTasks> duplicatedToRevoke = intersection(duplicated, memberAssignments);
        log.debug("Duplicated connectors and tasks to revoke from each worker: {}", duplicatedToRevoke);
        addAll(toRevoke, duplicatedToRevoke);

        // Compute the assignment that will be applied across the cluster after this round of rebalance
        // Later on, new submissions and lost-and-reassigned connectors and tasks will be added to these assignments,
        // and load-balancing revocations will be removed from them.
        List<WorkerLoad> nextWorkerAssignment = workerLoads(memberAssignments);
        removeAll(nextWorkerAssignment, deletedToRevoke);
        removeAll(nextWorkerAssignment, duplicatedToRevoke);

        // Collect the lost assignments that are ready to be reassigned because the workers that were
        // originally responsible for them appear to have left the cluster instead of rejoining within
        // the scheduled rebalance delay. These assignments will be re-allocated to the existing workers
        // in the cluster later on
        ConnectorsAndTasks.Builder lostAssignmentsToReassignBuilder = new ConnectorsAndTasks.Builder();
        handleLostAssignments(lostAssignments, lostAssignmentsToReassignBuilder, nextWorkerAssignment);
        ConnectorsAndTasks lostAssignmentsToReassign = lostAssignmentsToReassignBuilder.build();

        // Do not revoke resources for re-assignment while a delayed rebalance is active
        if (delay == 0) {
            Map<String, ConnectorsAndTasks> loadBalancingRevocations =
                    performLoadBalancingRevocations(configured, nextWorkerAssignment);

            // If this round and the previous round involved revocation, we will calculate a delay for
            // the next round when revoking rebalance would be allowed. Note that delay could be 0, in which
            // case we would always revoke.
            if (revokedInPrevious && !loadBalancingRevocations.isEmpty()) {
                numSuccessiveRevokingRebalances++; // Should we consider overflow for this?
                log.debug("Consecutive revoking rebalances observed. Computing delay and next scheduled rebalance.");
                delay = (int) consecutiveRevokingRebalancesBackoff.backoff(numSuccessiveRevokingRebalances);
                if (delay != 0) {
                    scheduledRebalance = time.milliseconds() + delay;
                    log.debug("Skipping revocations in the current round with a delay of {}ms. Next scheduled rebalance:{}",
                            delay, scheduledRebalance);
                } else {
                    log.debug("Revoking assignments immediately since scheduled.rebalance.max.delay.ms is set to 0");
                    addAll(toRevoke, loadBalancingRevocations);
                    // Remove all newly-revoked connectors and tasks from the next assignment, both to
                    // ensure that they are not included in the assignments during this round, and to produce
                    // an accurate allocation of all newly-created and lost-and-reassigned connectors and tasks
                    // that will have to be distributed across the cluster during this round
                    removeAll(nextWorkerAssignment, loadBalancingRevocations);
                }
            } else if (!loadBalancingRevocations.isEmpty()) {
                // We had a revocation in this round but not in the previous round. Let's store that state.
                log.debug("Performing allocation-balancing revocation immediately as no revocations took place during the previous rebalance");
                addAll(toRevoke, loadBalancingRevocations);
                removeAll(nextWorkerAssignment, loadBalancingRevocations);
                revokedInPrevious = true;
            } else if (revokedInPrevious) {
                // No revocations in this round but the previous round had one. Probably the workers
                // have converged to a balanced load. We can reset the rebalance clock
                log.debug("Previous round had revocations but this round didn't. Probably, the cluster has reached a " +
                        "balanced load. Resetting the exponential backoff clock");
                revokedInPrevious = false;
                numSuccessiveRevokingRebalances = 0;
            } else {
                // no-op
                log.debug("No revocations in previous and current round.");
            }
        } else {
            log.debug("Delayed rebalance is active. Delaying {}ms before revoking connectors and tasks: {}", delay, toRevoke);
            revokedInPrevious = false;
        }

        // The complete set of connectors and tasks that should be newly-assigned during this round
        ConnectorsAndTasks toAssign = new ConnectorsAndTasks.Builder()
                .addAll(created)
                .addAll(lostAssignmentsToReassign)
                .build();

        assignConnectors(nextWorkerAssignment, toAssign.connectors());
        assignTasks(nextWorkerAssignment, toAssign.tasks());

        Map<String, Collection<String>> nextConnectorAssignments = nextWorkerAssignment.stream()
                .collect(Collectors.toMap(
                        WorkerLoad::worker,
                        WorkerLoad::connectors
                ));
        Map<String, Collection<ConnectorTaskId>> nextTaskAssignments = nextWorkerAssignment.stream()
                .collect(Collectors.toMap(
                        WorkerLoad::worker,
                        WorkerLoad::tasks
                ));

        Map<String, Collection<String>> currentConnectorAssignments =
                currentWorkerAssignment.stream().collect(Collectors.toMap(WorkerLoad::worker, WorkerLoad::connectors));
        Map<String, Collection<ConnectorTaskId>> currentTaskAssignments =
                currentWorkerAssignment.stream().collect(Collectors.toMap(WorkerLoad::worker, WorkerLoad::tasks));
        Map<String, Collection<String>> incrementalConnectorAssignments =
                diff(nextConnectorAssignments, currentConnectorAssignments);
        Map<String, Collection<ConnectorTaskId>> incrementalTaskAssignments =
                diff(nextTaskAssignments, currentTaskAssignments);

        Map<String, ConnectorsAndTasks> revoked = buildAll(toRevoke);

        previousAssignment = computePreviousAssignment(revoked, nextConnectorAssignments, nextTaskAssignments, lostAssignments);
        previousGenerationId = currentGenerationId;
        previousMembers = memberAssignments.keySet();

        log.debug("Incremental connector assignments: {}", incrementalConnectorAssignments);
        log.debug("Incremental task assignments: {}", incrementalTaskAssignments);

        Map<String, Collection<String>> revokedConnectors = transformValues(revoked, ConnectorsAndTasks::connectors);
        Map<String, Collection<ConnectorTaskId>> revokedTasks = transformValues(revoked, ConnectorsAndTasks::tasks);

        return new ClusterAssignment(
                incrementalConnectorAssignments,
                incrementalTaskAssignments,
                revokedConnectors,
                revokedTasks,
                diff(nextConnectorAssignments, revokedConnectors),
                diff(nextTaskAssignments, revokedTasks)
        );
    }

    private ConnectorsAndTasks computePreviousAssignment(Map<String, ConnectorsAndTasks> toRevoke,
                                                         Map<String, Collection<String>> connectorAssignments,
                                                         Map<String, Collection<ConnectorTaskId>> taskAssignments,
                                                         ConnectorsAndTasks lostAssignments) {
        ConnectorsAndTasks previousAssignment = new ConnectorsAndTasks.Builder().with(
                ConnectUtils.combineCollections(connectorAssignments.values()),
                ConnectUtils.combineCollections(taskAssignments.values())
        ).build();

        for (ConnectorsAndTasks revoked : toRevoke.values()) {
            previousAssignment.connectors().removeAll(revoked.connectors());
            previousAssignment.tasks().removeAll(revoked.tasks());
            previousRevocation.connectors().addAll(revoked.connectors());
            previousRevocation.tasks().addAll(revoked.tasks());
        }

        // Depends on the previous assignment's collections being sets at the moment.
        // TODO: make it independent
        previousAssignment.connectors().addAll(lostAssignments.connectors());
        previousAssignment.tasks().addAll(lostAssignments.tasks());

        return previousAssignment;
    }

    private ConnectorsAndTasks duplicatedAssignments(Map<String, ConnectorsAndTasks> memberAssignments) {
        Map<String, Long> connectorInstanceCounts = combineCollections(
                memberAssignments.values(),
                ConnectorsAndTasks::connectors,
                Collectors.groupingBy(Function.identity(), Collectors.counting())
        );
        Set<String> duplicatedConnectors = connectorInstanceCounts
                .entrySet().stream()
                .filter(entry -> entry.getValue() > 1L)
                .map(Entry::getKey)
                .collect(Collectors.toSet());

        Map<ConnectorTaskId, Long> taskInstanceCounts = combineCollections(
                memberAssignments.values(),
                ConnectorsAndTasks::tasks,
                Collectors.groupingBy(Function.identity(), Collectors.counting())
        );
        Set<ConnectorTaskId> duplicatedTasks = taskInstanceCounts
                .entrySet().stream()
                .filter(entry -> entry.getValue() > 1L)
                .map(Entry::getKey)
                .collect(Collectors.toSet());

        return new ConnectorsAndTasks.Builder().with(duplicatedConnectors, duplicatedTasks).build();
    }

    // visible for testing
    protected void handleLostAssignments(ConnectorsAndTasks lostAssignments,
                                         ConnectorsAndTasks.Builder lostAssignmentsToReassign,
                                         List<WorkerLoad> completeWorkerAssignment) {
        // There are no lost assignments and there have been no successive revoking rebalances
        if (lostAssignments.isEmpty() && !revokedInPrevious) {
            resetDelay();
            return;
        }

        final long now = time.milliseconds();
        log.debug("Found the following connectors and tasks missing from previous assignments: "
                + lostAssignments);

        Set<String> activeMembers = completeWorkerAssignment.stream()
                .map(WorkerLoad::worker)
                .collect(Collectors.toSet());
        if (scheduledRebalance <= 0 && activeMembers.containsAll(previousMembers)) {
            log.debug("No worker seems to have departed the group during the rebalance. The "
                    + "missing assignments that the leader is detecting are probably due to some "
                    + "workers failing to receive the new assignments in the previous rebalance. "
                    + "Will reassign missing tasks as new tasks");
            lostAssignmentsToReassign.addAll(lostAssignments);
            return;
        } else if (maxDelay == 0) {
            log.debug("Scheduled rebalance delays are disabled ({} = 0); "
                    + "reassigning all lost connectors and tasks immediately",
                    SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG
            );
            lostAssignmentsToReassign.addAll(lostAssignments);
            return;
        }

        if (scheduledRebalance > 0 && now >= scheduledRebalance) {
            // delayed rebalance expired and it's time to assign resources
            log.debug("Delayed rebalance expired. Reassigning lost tasks");
            List<WorkerLoad> candidateWorkerLoad = Collections.emptyList();
            if (!candidateWorkersForReassignment.isEmpty()) {
                candidateWorkerLoad = pickCandidateWorkerForReassignment(completeWorkerAssignment);
            }

            if (!candidateWorkerLoad.isEmpty()) {
                log.debug("Assigning lost tasks to {} candidate workers: {}", 
                        candidateWorkerLoad.size(),
                        candidateWorkerLoad.stream().map(WorkerLoad::worker).collect(Collectors.joining(",")));
                Iterator<WorkerLoad> candidateWorkerIterator = candidateWorkerLoad.iterator();
                for (String connector : lostAssignments.connectors()) {
                    // Loop over the candidate workers as many times as it takes
                    if (!candidateWorkerIterator.hasNext()) {
                        candidateWorkerIterator = candidateWorkerLoad.iterator();
                    }
                    WorkerLoad worker = candidateWorkerIterator.next();
                    log.debug("Assigning connector id {} to member {}", connector, worker.worker());
                    worker.assign(connector);
                }
                candidateWorkerIterator = candidateWorkerLoad.iterator();
                for (ConnectorTaskId task : lostAssignments.tasks()) {
                    if (!candidateWorkerIterator.hasNext()) {
                        candidateWorkerIterator = candidateWorkerLoad.iterator();
                    }
                    WorkerLoad worker = candidateWorkerIterator.next();
                    log.debug("Assigning task id {} to member {}", task, worker.worker());
                    worker.assign(task);
                }
            } else {
                log.debug("No single candidate worker was found to assign lost tasks. Treating lost tasks as new tasks");
                lostAssignmentsToReassign.addAll(lostAssignments);
            }
            resetDelay();
            // Resetting the flag as now we can permit successive revoking rebalances.
            // since we have gone through the full rebalance delay
            revokedInPrevious = false;
        } else {
            candidateWorkersForReassignment
                    .addAll(candidateWorkersForReassignment(completeWorkerAssignment));
            if (now < scheduledRebalance) {
                // a delayed rebalance is in progress, but it's not yet time to reassign
                // unaccounted resources
                delay = calculateDelay(now);
                log.debug("Delayed rebalance in progress. Task reassignment is postponed. New computed rebalance delay: {}", delay);
            } else {
                // This means scheduledRebalance == 0
                // We could also extract the current minimum delay from the group, to make
                // independent of consecutive leader failures, but this optimization is skipped
                // at the moment
                delay = maxDelay;
                log.debug("Resetting rebalance delay to the max: {}. scheduledRebalance: {} now: {} diff scheduledRebalance - now: {}",
                        delay, scheduledRebalance, now, scheduledRebalance - now);
            }
            scheduledRebalance = now + delay;
        }
    }

    private void resetDelay() {
        candidateWorkersForReassignment.clear();
        scheduledRebalance = 0;
        if (delay != 0) {
            log.debug("Resetting delay from previous value: {} to 0", delay);
        }
        delay = 0;
    }

    private Set<String> candidateWorkersForReassignment(List<WorkerLoad> completeWorkerAssignment) {
        return completeWorkerAssignment.stream()
                .filter(WorkerLoad::isEmpty)
                .map(WorkerLoad::worker)
                .collect(Collectors.toSet());
    }

    private List<WorkerLoad> pickCandidateWorkerForReassignment(List<WorkerLoad> completeWorkerAssignment) {
        Map<String, WorkerLoad> activeWorkers = completeWorkerAssignment.stream()
                .collect(Collectors.toMap(WorkerLoad::worker, Function.identity()));
        return candidateWorkersForReassignment.stream()
                .map(activeWorkers::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private Map<String, ExtendedAssignment> fillAssignments(Collection<String> members, short error,
                                                            String leaderId, String leaderUrl, long maxOffset,
                                                            ClusterAssignment clusterAssignment,
                                                            int delay, short protocolVersion) {
        Map<String, ExtendedAssignment> groupAssignment = new HashMap<>();
        for (String member : members) {
            Collection<String> connectorsToStart = clusterAssignment.newlyAssignedConnectors(member);
            Collection<ConnectorTaskId> tasksToStart = clusterAssignment.newlyAssignedTasks(member);
            Collection<String> connectorsToStop = clusterAssignment.newlyRevokedConnectors(member);
            Collection<ConnectorTaskId> tasksToStop = clusterAssignment.newlyRevokedTasks(member);
            ExtendedAssignment assignment =
                    new ExtendedAssignment(protocolVersion, error, leaderId, leaderUrl, maxOffset,
                            connectorsToStart, tasksToStart, connectorsToStop, tasksToStop, delay);
            log.debug("Filling assignment: {} -> {}", member, assignment);
            groupAssignment.put(member, assignment);
        }
        log.debug("Finished assignment");
        return groupAssignment;
    }

    /**
     * From a map of workers to assignment object generate the equivalent map of workers to byte
     * buffers of serialized assignments.
     *
     * @param assignments the map of worker assignments
     * @return the serialized map of assignments to workers
     */
    protected Map<String, ByteBuffer> serializeAssignments(Map<String, ExtendedAssignment> assignments, short protocolVersion) {
        boolean sessioned = protocolVersion >= CONNECT_PROTOCOL_V2;
        return assignments.entrySet()
                .stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> IncrementalCooperativeConnectProtocol.serializeAssignment(e.getValue(), sessioned)));
    }

    private static ConnectorsAndTasks diff(ConnectorsAndTasks base,
                                           ConnectorsAndTasks... toSubtract) {
        Collection<String> connectors = new TreeSet<>(base.connectors());
        Collection<ConnectorTaskId> tasks = new TreeSet<>(base.tasks());
        for (ConnectorsAndTasks sub : toSubtract) {
            connectors.removeAll(sub.connectors());
            tasks.removeAll(sub.tasks());
        }
        return new ConnectorsAndTasks.Builder().with(connectors, tasks).build();
    }

    private static <T> Map<String, Collection<T>> diff(Map<String, Collection<T>> base,
                                                       Map<String, Collection<T>> toSubtract) {
        Map<String, Collection<T>> incremental = new HashMap<>();
        for (Map.Entry<String, Collection<T>> entry : base.entrySet()) {
            List<T> values = new ArrayList<>(entry.getValue());
            values.removeAll(toSubtract.getOrDefault(entry.getKey(), Collections.emptySet()));
            incremental.put(entry.getKey(), values);
        }
        return incremental;
    }

    private ConnectorsAndTasks assignment(Map<String, ConnectorsAndTasks> memberAssignments) {
        log.debug("Received assignments: {}", memberAssignments);
        return new ConnectorsAndTasks.Builder().with(
                ConnectUtils.combineCollections(memberAssignments.values(), ConnectorsAndTasks::connectors),
                ConnectUtils.combineCollections(memberAssignments.values(), ConnectorsAndTasks::tasks)
        ).build();
    }

    /**
     * Revoke connectors and tasks from each worker in the cluster until no worker is running more than it
     * would be with a perfectly-balanced assignment.
     * @param configured the set of configured connectors and tasks across the entire cluster
     * @param workers the workers in the cluster, whose assignments should not include any deleted or duplicated connectors or tasks
     *                that are already due to be revoked from the worker in this rebalance
     * @return which connectors and tasks should be revoked from which workers; never null, but may be empty
     * if no load-balancing revocations are necessary or possible
     */
    private Map<String, ConnectorsAndTasks> performLoadBalancingRevocations(
            ConnectorsAndTasks configured,
            Collection<WorkerLoad> workers
    ) {
        if (log.isTraceEnabled()) {
            workers.forEach(wl -> log.trace(
                    "Per worker current load size; worker: {} connectors: {} tasks: {}",
                    wl.worker(), wl.connectorsSize(), wl.tasksSize()));
        }

        if (workers.stream().allMatch(WorkerLoad::isEmpty)) {
            log.trace("No load-balancing revocations required; all workers are either new "
                    + "or will have all currently-assigned connectors and tasks revoked during this round"
            );
            return Collections.emptyMap();
        }
        if (configured.isEmpty()) {
            log.trace("No load-balancing revocations required; no connectors are currently configured on this cluster");
            return Collections.emptyMap();
        }

        Map<String, ConnectorsAndTasks.Builder> result = new HashMap<>();

        Map<String, Set<String>> connectorRevocations = loadBalancingRevocations(
                "connector",
                configured.connectors().size(),
                workers,
                WorkerLoad::connectors
        );
        Map<String, Set<ConnectorTaskId>> taskRevocations = loadBalancingRevocations(
                "task",
                configured.tasks().size(),
                workers,
                WorkerLoad::tasks
        );

        connectorRevocations.forEach((worker, revoked) ->
                result.computeIfAbsent(worker, w -> new ConnectorsAndTasks.Builder()).addConnectors(revoked)
        );
        taskRevocations.forEach((worker, revoked) ->
                result.computeIfAbsent(worker, w -> new ConnectorsAndTasks.Builder()).addTasks(revoked)
        );

        return buildAll(result);
    }

    private <E> Map<String, Set<E>> loadBalancingRevocations(
            String allocatedResourceName,
            int totalToAllocate,
            Collection<WorkerLoad> workers,
            Function<WorkerLoad, Collection<E>> workerAllocation
    ) {
        int totalWorkers = workers.size();
        // The minimum instances of this resource that should be assigned to each worker
        int minAllocatedPerWorker = totalToAllocate / totalWorkers;
        // How many workers are going to have to be allocated exactly one extra instance
        // (since the total number to allocate may not be a perfect multiple of the number of workers)
        int workersToAllocateExtra = totalToAllocate % totalWorkers;
        // Useful function to determine exactly how many instances of the resource a given worker is currently allocated
        Function<WorkerLoad, Integer> workerAllocationSize = workerAllocation.andThen(Collection::size);

        long workersAllocatedMinimum = workers.stream()
                .map(workerAllocationSize)
                .filter(n -> n == minAllocatedPerWorker)
                .count();
        long workersAllocatedSingleExtra = workers.stream()
                .map(workerAllocationSize)
                .filter(n -> n == minAllocatedPerWorker + 1)
                .count();
        if (workersAllocatedSingleExtra == workersToAllocateExtra
                && workersAllocatedMinimum + workersAllocatedSingleExtra == totalWorkers) {
            log.trace(
                    "No load-balancing {} revocations required; the current allocations, when combined with any newly-created {}s, should be balanced",
                    allocatedResourceName,
                    allocatedResourceName
            );
            return Collections.emptyMap();
        }

        Map<String, Set<E>> result = new HashMap<>();
        // How many workers we've allocated a single extra resource instance to
        int allocatedExtras = 0;
        // Calculate how many (and which) connectors/tasks to revoke from each worker here
        for (WorkerLoad worker : workers) {
            int currentAllocationSizeForWorker = workerAllocationSize.apply(worker);
            if (currentAllocationSizeForWorker <= minAllocatedPerWorker) {
                // This worker isn't allocated more than the minimum; no need to revoke anything
                continue;
            }
            int maxAllocationForWorker;
            if (allocatedExtras < workersToAllocateExtra) {
                // We'll allocate one of the extra resource instances to this worker
                allocatedExtras++;
                if (currentAllocationSizeForWorker == minAllocatedPerWorker + 1) {
                    // If the worker's running exactly one more than the minimum, and we're allowed to
                    // allocate an extra to it, there's no need to revoke anything
                    continue;
                }
                maxAllocationForWorker = minAllocatedPerWorker + 1;
            } else {
                maxAllocationForWorker = minAllocatedPerWorker;
            }

            Set<E> revokedFromWorker = new LinkedHashSet<>();
            result.put(worker.worker(), revokedFromWorker);

            Iterator<E> currentWorkerAllocation = workerAllocation.apply(worker).iterator();
            // Revoke resources from the worker until it isn't allocated any more than it should be
            for (int numRevoked = 0; currentAllocationSizeForWorker - numRevoked > maxAllocationForWorker; numRevoked++) {
                if (!currentWorkerAllocation.hasNext()) {
                    // Should never happen, but better to log a warning and move on than die and fail the whole rebalance if it does
                    log.warn(
                            "Unexpectedly ran out of {}s to revoke from worker {} while performing load-balancing revocations; " +
                                    "worker appears to still be allocated {} instances, which is more than the intended allocation of {}",
                            allocatedResourceName,
                            worker.worker(),
                            workerAllocationSize.apply(worker),
                            maxAllocationForWorker
                    );
                    break;
                }
                E revocation = currentWorkerAllocation.next();
                revokedFromWorker.add(revocation);
            }
        }
        return result;
    }

    private int calculateDelay(long now) {
        long diff = scheduledRebalance - now;
        return diff > 0 ? (int) Math.min(diff, maxDelay) : 0;
    }

    /**
     * Perform a round-robin assignment of connectors to workers with existing worker load. This
     * assignment tries to balance the load between workers, by assigning connectors to workers
     * that have equal load, starting with the least loaded workers.
     *
     * @param workerAssignment the current worker assignment; assigned connectors are added to this list
     * @param connectors the connectors to be assigned
     */
    protected void assignConnectors(List<WorkerLoad> workerAssignment, Collection<String> connectors) {
        workerAssignment.sort(WorkerLoad.connectorComparator());
        WorkerLoad first = workerAssignment.get(0);

        Iterator<String> load = connectors.iterator();
        while (load.hasNext()) {
            int firstLoad = first.connectorsSize();
            int upTo = IntStream.range(0, workerAssignment.size())
                    .filter(i -> workerAssignment.get(i).connectorsSize() > firstLoad)
                    .findFirst()
                    .orElse(workerAssignment.size());
            for (WorkerLoad worker : workerAssignment.subList(0, upTo)) {
                String connector = load.next();
                log.debug("Assigning connector {} to {}", connector, worker.worker());
                worker.assign(connector);
                if (!load.hasNext()) {
                    break;
                }
            }
        }
    }

    /**
     * Perform a round-robin assignment of tasks to workers with existing worker load. This
     * assignment tries to balance the load between workers, by assigning tasks to workers that
     * have equal load, starting with the least loaded workers.
     *
     * @param workerAssignment the current worker assignment; assigned tasks are added to this list
     * @param tasks the tasks to be assigned
     */
    protected void assignTasks(List<WorkerLoad> workerAssignment, Collection<ConnectorTaskId> tasks) {
        workerAssignment.sort(WorkerLoad.taskComparator());
        WorkerLoad first = workerAssignment.get(0);

        Iterator<ConnectorTaskId> load = tasks.iterator();
        while (load.hasNext()) {
            int firstLoad = first.tasksSize();
            int upTo = IntStream.range(0, workerAssignment.size())
                    .filter(i -> workerAssignment.get(i).tasksSize() > firstLoad)
                    .findFirst()
                    .orElse(workerAssignment.size());
            for (WorkerLoad worker : workerAssignment.subList(0, upTo)) {
                ConnectorTaskId task = load.next();
                log.debug("Assigning task {} to {}", task, worker.worker());
                worker.assign(task);
                if (!load.hasNext()) {
                    break;
                }
            }
        }
    }

    private static List<WorkerLoad> workerAssignment(Map<String, ConnectorsAndTasks> memberAssignments,
                                                     ConnectorsAndTasks toExclude) {
        ConnectorsAndTasks ignore = new ConnectorsAndTasks.Builder()
                .with(toExclude.connectors(), toExclude.tasks())
                .build();

        return memberAssignments.entrySet().stream()
                .map(e -> new WorkerLoad.Builder(e.getKey()).with(
                        e.getValue().connectors().stream()
                                .filter(v -> !ignore.connectors().contains(v))
                                .collect(Collectors.toList()),
                        e.getValue().tasks().stream()
                                .filter(v -> !ignore.tasks().contains(v))
                                .collect(Collectors.toList())
                        ).build()
                ).collect(Collectors.toList());
    }

    private static void addAll(Map<String, ConnectorsAndTasks.Builder> base, Map<String, ConnectorsAndTasks> toAdd) {
        toAdd.forEach((worker, assignment) -> base
                .computeIfAbsent(worker, w -> new ConnectorsAndTasks.Builder())
                .addAll(assignment)
        );
    }

    private static <K> Map<K, ConnectorsAndTasks> buildAll(Map<K, ConnectorsAndTasks.Builder> builders) {
        return transformValues(builders, ConnectorsAndTasks.Builder::build);
    }

    private static List<WorkerLoad> workerLoads(Map<String, ConnectorsAndTasks> memberAssignments) {
        return memberAssignments.entrySet().stream()
                .map(e -> new WorkerLoad.Builder(e.getKey()).with(e.getValue().connectors(), e.getValue().tasks()).build())
                .collect(Collectors.toList());
    }

    private static void removeAll(List<WorkerLoad> workerLoads, Map<String, ConnectorsAndTasks> toRemove) {
        workerLoads.forEach(workerLoad -> {
            String worker = workerLoad.worker();
            ConnectorsAndTasks toRemoveFromWorker = toRemove.getOrDefault(worker, ConnectorsAndTasks.EMPTY);
            workerLoad.connectors().removeAll(toRemoveFromWorker.connectors());
            workerLoad.tasks().removeAll(toRemoveFromWorker.tasks());
        });
    }

    private static Map<String, ConnectorsAndTasks> intersection(ConnectorsAndTasks connectorsAndTasks, Map<String, ConnectorsAndTasks> assignments) {
        return transformValues(assignments, assignment -> {
            Collection<String> connectors = new HashSet<>(assignment.connectors());
            connectors.retainAll(connectorsAndTasks.connectors());
            Collection<ConnectorTaskId> tasks = new HashSet<>(assignment.tasks());
            tasks.retainAll(connectorsAndTasks.tasks());
            return new ConnectorsAndTasks.Builder().with(connectors, tasks).build();
        });
    }

    static class ClusterAssignment {

        private final Map<String, Collection<String>> newlyAssignedConnectors;
        private final Map<String, Collection<ConnectorTaskId>> newlyAssignedTasks;
        private final Map<String, Collection<String>> newlyRevokedConnectors;
        private final Map<String, Collection<ConnectorTaskId>> newlyRevokedTasks;
        private final Map<String, Collection<String>> allAssignedConnectors;
        private final Map<String, Collection<ConnectorTaskId>> allAssignedTasks;
        private final Set<String> allWorkers;

        public static final ClusterAssignment EMPTY = new ClusterAssignment(
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap()
        );

        public ClusterAssignment(
                Map<String, Collection<String>> newlyAssignedConnectors,
                Map<String, Collection<ConnectorTaskId>> newlyAssignedTasks,
                Map<String, Collection<String>> newlyRevokedConnectors,
                Map<String, Collection<ConnectorTaskId>> newlyRevokedTasks,
                Map<String, Collection<String>> allAssignedConnectors,
                Map<String, Collection<ConnectorTaskId>> allAssignedTasks
        ) {
            this.newlyAssignedConnectors = newlyAssignedConnectors;
            this.newlyAssignedTasks = newlyAssignedTasks;
            this.newlyRevokedConnectors = newlyRevokedConnectors;
            this.newlyRevokedTasks = newlyRevokedTasks;
            this.allAssignedConnectors = allAssignedConnectors;
            this.allAssignedTasks = allAssignedTasks;
            this.allWorkers = combineCollections(
                    Arrays.asList(newlyAssignedConnectors, newlyAssignedTasks, newlyRevokedConnectors, newlyRevokedTasks, allAssignedConnectors, allAssignedTasks),
                    Map::keySet,
                    Collectors.toSet()
            );
        }

        public Map<String, Collection<String>> newlyAssignedConnectors() {
            return newlyAssignedConnectors;
        }

        public Collection<String> newlyAssignedConnectors(String worker) {
            return newlyAssignedConnectors.getOrDefault(worker, Collections.emptySet());
        }

        public Map<String, Collection<ConnectorTaskId>> newlyAssignedTasks() {
            return newlyAssignedTasks;
        }

        public Collection<ConnectorTaskId> newlyAssignedTasks(String worker) {
            return newlyAssignedTasks.getOrDefault(worker, Collections.emptySet());
        }

        public Map<String, Collection<String>> newlyRevokedConnectors() {
            return newlyRevokedConnectors;
        }

        public Collection<String> newlyRevokedConnectors(String worker) {
            return newlyRevokedConnectors.getOrDefault(worker, Collections.emptySet());
        }

        public Map<String, Collection<ConnectorTaskId>> newlyRevokedTasks() {
            return newlyRevokedTasks;
        }

        public Collection<ConnectorTaskId> newlyRevokedTasks(String worker) {
            return newlyRevokedTasks.getOrDefault(worker, Collections.emptySet());
        }

        public Map<String, Collection<String>> allAssignedConnectors() {
            return allAssignedConnectors;
        }

        public Map<String, Collection<ConnectorTaskId>> allAssignedTasks() {
            return allAssignedTasks;
        }

        public Set<String> allWorkers() {
            return allWorkers;
        }

        @Override
        public String toString() {
            return "ClusterAssignment{"
                    + "newlyAssignedConnectors=" + newlyAssignedConnectors
                    + ", newlyAssignedTasks=" + newlyAssignedTasks
                    + ", newlyRevokedConnectors=" + newlyRevokedConnectors
                    + ", newlyRevokedTasks=" + newlyRevokedTasks
                    + ", allAssignedConnectors=" + allAssignedConnectors
                    + ", allAssignedTasks=" + allAssignedTasks
                    + ", allWorkers=" + allWorkers
                    + '}';
        }
    }

}
