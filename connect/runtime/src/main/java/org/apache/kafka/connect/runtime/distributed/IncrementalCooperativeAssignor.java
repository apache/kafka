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

import java.util.Map.Entry;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.ConnectorsAndTasks;
import org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.WorkerLoad;
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
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECT_PROTOCOL_V1;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECT_PROTOCOL_V2;
import static org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.LeaderState;

/**
 * An assignor that computes a distribution of connectors and tasks according to the incremental
 * cooperative strategy for rebalancing. {@see
 * https://cwiki.apache.org/confluence/display/KAFKA/KIP-415%3A+Incremental+Cooperative
 * +Rebalancing+in+Kafka+Connect} for a description of the assignment policy.
 *
 * Note that this class is NOT thread-safe.
 */
public class IncrementalCooperativeAssignor implements ConnectAssignor {
    private final Logger log;
    private final Time time;
    private final int maxDelay;
    private ConnectorsAndTasks previousAssignment;
    private ConnectorsAndTasks previousRevocation;
    private boolean canRevoke;
    // visible for testing
    protected final Set<String> candidateWorkersForReassignment;
    protected long scheduledRebalance;
    protected int delay;
    protected int previousGenerationId;
    protected Set<String> previousMembers;

    public IncrementalCooperativeAssignor(LogContext logContext, Time time, int maxDelay) {
        this.log = logContext.logger(IncrementalCooperativeAssignor.class);
        this.time = time;
        this.maxDelay = maxDelay;
        this.previousAssignment = ConnectorsAndTasks.EMPTY;
        this.previousRevocation = new ConnectorsAndTasks.Builder().build();
        this.canRevoke = true;
        this.scheduledRebalance = 0;
        this.candidateWorkersForReassignment = new LinkedHashSet<>();
        this.delay = 0;
        this.previousGenerationId = -1;
        this.previousMembers = Collections.emptySet();
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

        short protocolVersion = memberConfigs.values().stream()
            .allMatch(state -> state.assignment().version() == CONNECT_PROTOCOL_V2)
                ? CONNECT_PROTOCOL_V2
                : CONNECT_PROTOCOL_V1;

        Long leaderOffset = ensureLeaderConfig(maxOffset, coordinator);
        if (leaderOffset == null) {
            Map<String, ExtendedAssignment> assignments = fillAssignments(
                    memberConfigs.keySet(), Assignment.CONFIG_MISMATCH,
                    leaderId, memberConfigs.get(leaderId).url(), maxOffset, Collections.emptyMap(),
                    Collections.emptyMap(), Collections.emptyMap(), 0, protocolVersion);
            return serializeAssignments(assignments);
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
     * {@see https://cwiki.apache.org/confluence/display/KAFKA/KIP-415%3A+Incremental+Cooperative+Rebalancing+in+Kafka+Connect}
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

        // Base set: The previous assignment of connectors-and-tasks is a standalone snapshot that
        // can be used to calculate derived sets
        log.debug("Previous assignments: {}", previousAssignment);
        int lastCompletedGenerationId = coordinator.lastCompletedGenerationId();
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

        ClusterConfigState snapshot = coordinator.configSnapshot();
        Set<String> configuredConnectors = new TreeSet<>(snapshot.connectors());
        Set<ConnectorTaskId> configuredTasks = configuredConnectors.stream()
                .flatMap(c -> snapshot.tasks(c).stream())
                .collect(Collectors.toSet());

        // Base set: The set of configured connectors-and-tasks is a standalone snapshot that can
        // be used to calculate derived sets
        ConnectorsAndTasks configured = new ConnectorsAndTasks.Builder()
                .with(configuredConnectors, configuredTasks).build();
        log.debug("Configured assignments: {}", configured);

        // Base set: The set of active connectors-and-tasks is a standalone snapshot that can be
        // used to calculate derived sets
        ConnectorsAndTasks activeAssignments = assignment(memberConfigs);
        log.debug("Active assignments: {}", activeAssignments);

        // This means that a previous revocation did not take effect. In this case, reset
        // appropriately and be ready to re-apply revocation of tasks
        if (!previousRevocation.isEmpty()) {
            if (previousRevocation.connectors().stream().anyMatch(c -> activeAssignments.connectors().contains(c))
                    || previousRevocation.tasks().stream().anyMatch(t -> activeAssignments.tasks().contains(t))) {
                previousAssignment = activeAssignments;
                canRevoke = true;
            }
            previousRevocation.connectors().clear();
            previousRevocation.tasks().clear();
        }

        // Derived set: The set of deleted connectors-and-tasks is a derived set from the set
        // difference of previous - configured
        ConnectorsAndTasks deleted = diff(previousAssignment, configured);
        log.debug("Deleted assignments: {}", deleted);

        // Derived set: The set of remaining active connectors-and-tasks is a derived set from the
        // set difference of active - deleted
        ConnectorsAndTasks remainingActive = diff(activeAssignments, deleted);
        log.debug("Remaining (excluding deleted) active assignments: {}", remainingActive);

        // Derived set: The set of lost or unaccounted connectors-and-tasks is a derived set from
        // the set difference of previous - active - deleted
        ConnectorsAndTasks lostAssignments = diff(previousAssignment, activeAssignments, deleted);
        log.debug("Lost assignments: {}", lostAssignments);

        // Derived set: The set of new connectors-and-tasks is a derived set from the set
        // difference of configured - previous - active
        ConnectorsAndTasks newSubmissions = diff(configured, previousAssignment, activeAssignments);
        log.debug("New assignments: {}", newSubmissions);

        // A collection of the complete assignment
        List<WorkerLoad> completeWorkerAssignment = workerAssignment(memberConfigs, ConnectorsAndTasks.EMPTY);
        log.debug("Complete (ignoring deletions) worker assignments: {}", completeWorkerAssignment);

        // Per worker connector assignments without removing deleted connectors yet
        Map<String, Collection<String>> connectorAssignments =
                completeWorkerAssignment.stream().collect(Collectors.toMap(WorkerLoad::worker, WorkerLoad::connectors));
        log.debug("Complete (ignoring deletions) connector assignments: {}", connectorAssignments);

        // Per worker task assignments without removing deleted connectors yet
        Map<String, Collection<ConnectorTaskId>> taskAssignments =
                completeWorkerAssignment.stream().collect(Collectors.toMap(WorkerLoad::worker, WorkerLoad::tasks));
        log.debug("Complete (ignoring deletions) task assignments: {}", taskAssignments);

        // A collection of the current assignment excluding the connectors-and-tasks to be deleted
        List<WorkerLoad> currentWorkerAssignment = workerAssignment(memberConfigs, deleted);

        Map<String, ConnectorsAndTasks> toRevoke = computeDeleted(deleted, connectorAssignments, taskAssignments);
        log.debug("Connector and task to delete assignments: {}", toRevoke);

        // Revoking redundant connectors/tasks if the workers have duplicate assignments
        toRevoke.putAll(computeDuplicatedAssignments(memberConfigs, connectorAssignments, taskAssignments));
        log.debug("Connector and task to revoke assignments (include duplicated assignments): {}", toRevoke);

        // Recompute the complete assignment excluding the deleted connectors-and-tasks
        completeWorkerAssignment = workerAssignment(memberConfigs, deleted);
        connectorAssignments =
                completeWorkerAssignment.stream().collect(Collectors.toMap(WorkerLoad::worker, WorkerLoad::connectors));
        taskAssignments =
                completeWorkerAssignment.stream().collect(Collectors.toMap(WorkerLoad::worker, WorkerLoad::tasks));

        handleLostAssignments(lostAssignments, newSubmissions, completeWorkerAssignment, memberConfigs);

        // Do not revoke resources for re-assignment while a delayed rebalance is active
        // Also we do not revoke in two consecutive rebalances by the same leader
        canRevoke = delay == 0 && canRevoke;

        // Compute the connectors-and-tasks to be revoked for load balancing without taking into
        // account the deleted ones.
        log.debug("Can leader revoke tasks in this assignment? {} (delay: {})", canRevoke, delay);
        if (canRevoke) {
            Map<String, ConnectorsAndTasks> toExplicitlyRevoke =
                    performTaskRevocation(activeAssignments, currentWorkerAssignment);

            log.debug("Connector and task to revoke assignments: {}", toRevoke);

            toExplicitlyRevoke.forEach(
                (worker, assignment) -> {
                    ConnectorsAndTasks existing = toRevoke.computeIfAbsent(
                        worker,
                        v -> new ConnectorsAndTasks.Builder().build());
                    existing.connectors().addAll(assignment.connectors());
                    existing.tasks().addAll(assignment.tasks());
                }
            );
            canRevoke = toExplicitlyRevoke.size() == 0;
        } else {
            canRevoke = delay == 0;
        }

        assignConnectors(completeWorkerAssignment, newSubmissions.connectors());
        assignTasks(completeWorkerAssignment, newSubmissions.tasks());
        log.debug("Current complete assignments: {}", currentWorkerAssignment);
        log.debug("New complete assignments: {}", completeWorkerAssignment);

        Map<String, Collection<String>> currentConnectorAssignments =
                currentWorkerAssignment.stream().collect(Collectors.toMap(WorkerLoad::worker, WorkerLoad::connectors));
        Map<String, Collection<ConnectorTaskId>> currentTaskAssignments =
                currentWorkerAssignment.stream().collect(Collectors.toMap(WorkerLoad::worker, WorkerLoad::tasks));
        Map<String, Collection<String>> incrementalConnectorAssignments =
                diff(connectorAssignments, currentConnectorAssignments);
        Map<String, Collection<ConnectorTaskId>> incrementalTaskAssignments =
                diff(taskAssignments, currentTaskAssignments);

        log.debug("Incremental connector assignments: {}", incrementalConnectorAssignments);
        log.debug("Incremental task assignments: {}", incrementalTaskAssignments);

        coordinator.leaderState(new LeaderState(memberConfigs, connectorAssignments, taskAssignments));

        Map<String, ExtendedAssignment> assignments =
                fillAssignments(memberConfigs.keySet(), Assignment.NO_ERROR, leaderId,
                                memberConfigs.get(leaderId).url(), maxOffset, incrementalConnectorAssignments,
                                incrementalTaskAssignments, toRevoke, delay, protocolVersion);
        previousAssignment = computePreviousAssignment(toRevoke, connectorAssignments, taskAssignments, lostAssignments);
        previousGenerationId = coordinator.generationId();
        previousMembers = memberConfigs.keySet();
        log.debug("Actual assignments: {}", assignments);
        return serializeAssignments(assignments);
    }

    private Map<String, ConnectorsAndTasks> computeDeleted(ConnectorsAndTasks deleted,
                                                           Map<String, Collection<String>> connectorAssignments,
                                                           Map<String, Collection<ConnectorTaskId>> taskAssignments) {
        // Connector to worker reverse lookup map
        Map<String, String> connectorOwners = WorkerCoordinator.invertAssignment(connectorAssignments);
        // Task to worker reverse lookup map
        Map<ConnectorTaskId, String> taskOwners = WorkerCoordinator.invertAssignment(taskAssignments);

        Map<String, ConnectorsAndTasks> toRevoke = new HashMap<>();
        // Add the connectors that have been deleted to the revoked set
        deleted.connectors().forEach(c ->
                toRevoke.computeIfAbsent(
                    connectorOwners.get(c),
                    v -> new ConnectorsAndTasks.Builder().build()
                ).connectors().add(c));
        // Add the tasks that have been deleted to the revoked set
        deleted.tasks().forEach(t ->
                toRevoke.computeIfAbsent(
                    taskOwners.get(t),
                    v -> new ConnectorsAndTasks.Builder().build()
                ).tasks().add(t));
        log.debug("Connectors and tasks to delete assignments: {}", toRevoke);
        return toRevoke;
    }

    private ConnectorsAndTasks computePreviousAssignment(Map<String, ConnectorsAndTasks> toRevoke,
                                                         Map<String, Collection<String>> connectorAssignments,
                                                         Map<String, Collection<ConnectorTaskId>> taskAssignments,
                                                         ConnectorsAndTasks lostAssignments) {
        ConnectorsAndTasks previousAssignment = new ConnectorsAndTasks.Builder().with(
                connectorAssignments.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()),
                taskAssignments.values() .stream() .flatMap(Collection::stream).collect(Collectors.toSet()))
                .build();

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

    private ConnectorsAndTasks duplicatedAssignments(Map<String, ExtendedWorkerState> memberConfigs) {
        Set<String> connectors = memberConfigs.entrySet().stream()
                .flatMap(memberConfig -> memberConfig.getValue().assignment().connectors().stream())
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .entrySet().stream()
                .filter(entry -> entry.getValue() > 1L)
                .map(Entry::getKey)
                .collect(Collectors.toSet());

        Set<ConnectorTaskId> tasks = memberConfigs.values().stream()
                .flatMap(state -> state.assignment().tasks().stream())
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .entrySet().stream()
                .filter(entry -> entry.getValue() > 1L)
                .map(Entry::getKey)
                .collect(Collectors.toSet());
        return new ConnectorsAndTasks.Builder().with(connectors, tasks).build();
    }

    private Map<String, ConnectorsAndTasks> computeDuplicatedAssignments(Map<String, ExtendedWorkerState> memberConfigs,
                                             Map<String, Collection<String>> connectorAssignments,
                                             Map<String, Collection<ConnectorTaskId>> taskAssignment) {
        ConnectorsAndTasks duplicatedAssignments = duplicatedAssignments(memberConfigs);
        log.debug("Duplicated assignments: {}", duplicatedAssignments);

        Map<String, ConnectorsAndTasks> toRevoke = new HashMap<>();
        if (!duplicatedAssignments.connectors().isEmpty()) {
            connectorAssignments.entrySet().stream()
                    .forEach(entry -> {
                        Set<String> duplicatedConnectors = new HashSet<>(duplicatedAssignments.connectors());
                        duplicatedConnectors.retainAll(entry.getValue());
                        if (!duplicatedConnectors.isEmpty()) {
                            toRevoke.computeIfAbsent(
                                entry.getKey(),
                                v -> new ConnectorsAndTasks.Builder().build()
                            ).connectors().addAll(duplicatedConnectors);
                        }
                    });
        }
        if (!duplicatedAssignments.tasks().isEmpty()) {
            taskAssignment.entrySet().stream()
                    .forEach(entry -> {
                        Set<ConnectorTaskId> duplicatedTasks = new HashSet<>(duplicatedAssignments.tasks());
                        duplicatedTasks.retainAll(entry.getValue());
                        if (!duplicatedTasks.isEmpty()) {
                            toRevoke.computeIfAbsent(
                                entry.getKey(),
                                v -> new ConnectorsAndTasks.Builder().build()
                            ).tasks().addAll(duplicatedTasks);
                        }
                    });
        }
        return toRevoke;
    }

    // visible for testing
    protected void handleLostAssignments(ConnectorsAndTasks lostAssignments,
                                         ConnectorsAndTasks newSubmissions,
                                         List<WorkerLoad> completeWorkerAssignment,
                                         Map<String, ExtendedWorkerState> memberConfigs) {
        if (lostAssignments.isEmpty()) {
            resetDelay();
            return;
        }

        final long now = time.milliseconds();
        log.debug("Found the following connectors and tasks missing from previous assignments: "
                + lostAssignments);

        if (scheduledRebalance <= 0 && memberConfigs.keySet().containsAll(previousMembers)) {
            log.debug("No worker seems to have departed the group during the rebalance. The "
                    + "missing assignments that the leader is detecting are probably due to some "
                    + "workers failing to receive the new assignments in the previous rebalance. "
                    + "Will reassign missing tasks as new tasks");
            newSubmissions.connectors().addAll(lostAssignments.connectors());
            newSubmissions.tasks().addAll(lostAssignments.tasks());
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
                newSubmissions.connectors().addAll(lostAssignments.connectors());
                newSubmissions.tasks().addAll(lostAssignments.tasks());
            }
            resetDelay();
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
                // We could also also extract the current minimum delay from the group, to make
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

    /**
     * Task revocation is based on an rough estimation of the lower average number of tasks before
     * and after new workers join the group. If no new workers join, no revocation takes place.
     * Based on this estimation, tasks are revoked until the new floor average is reached for
     * each existing worker. The revoked tasks, once assigned to the new workers will maintain
     * a balanced load among the group.
     *
     * @param activeAssignments
     * @param completeWorkerAssignment
     * @return
     */
    private Map<String, ConnectorsAndTasks> performTaskRevocation(ConnectorsAndTasks activeAssignments,
                                                                  Collection<WorkerLoad> completeWorkerAssignment) {
        int totalActiveConnectorsNum = activeAssignments.connectors().size();
        int totalActiveTasksNum = activeAssignments.tasks().size();
        Collection<WorkerLoad> existingWorkers = completeWorkerAssignment.stream()
                .filter(wl -> wl.size() > 0)
                .collect(Collectors.toList());
        int existingWorkersNum = existingWorkers.size();
        int totalWorkersNum = completeWorkerAssignment.size();
        int newWorkersNum = totalWorkersNum - existingWorkersNum;

        if (log.isDebugEnabled()) {
            completeWorkerAssignment.forEach(wl -> log.debug(
                    "Per worker current load size; worker: {} connectors: {} tasks: {}",
                    wl.worker(), wl.connectorsSize(), wl.tasksSize()));
        }

        Map<String, ConnectorsAndTasks> revoking = new HashMap<>();
        // If there are no new workers, or no existing workers to revoke tasks from return early
        // after logging the status
        if (!(newWorkersNum > 0 && existingWorkersNum > 0)) {
            log.debug("No task revocation required; workers with existing load: {} workers with "
                    + "no load {} total workers {}",
                    existingWorkersNum, newWorkersNum, totalWorkersNum);
            // This is intentionally empty but mutable, because the map is used to include deleted
            // connectors and tasks as well
            return revoking;
        }

        log.debug("Task revocation is required; workers with existing load: {} workers with "
                + "no load {} total workers {}",
                existingWorkersNum, newWorkersNum, totalWorkersNum);

        // We have at least one worker assignment (the leader itself) so totalWorkersNum can't be 0
        log.debug("Previous rounded down (floor) average number of connectors per worker {}", totalActiveConnectorsNum / existingWorkersNum);
        int floorConnectors = totalActiveConnectorsNum / totalWorkersNum;
        int ceilConnectors = floorConnectors + ((totalActiveConnectorsNum % totalWorkersNum == 0) ? 0 : 1);
        log.debug("New average number of connectors per worker rounded down (floor) {} and rounded up (ceil) {}", floorConnectors, ceilConnectors);


        log.debug("Previous rounded down (floor) average number of tasks per worker {}", totalActiveTasksNum / existingWorkersNum);
        int floorTasks = totalActiveTasksNum / totalWorkersNum;
        int ceilTasks = floorTasks + ((totalActiveTasksNum % totalWorkersNum == 0) ? 0 : 1);
        log.debug("New average number of tasks per worker rounded down (floor) {} and rounded up (ceil) {}", floorTasks, ceilTasks);
        int numToRevoke;

        for (WorkerLoad existing : existingWorkers) {
            Iterator<String> connectors = existing.connectors().iterator();
            numToRevoke = existing.connectorsSize() - ceilConnectors;
            for (int i = existing.connectorsSize(); i > floorConnectors && numToRevoke > 0; --i, --numToRevoke) {
                ConnectorsAndTasks resources = revoking.computeIfAbsent(
                    existing.worker(),
                    w -> new ConnectorsAndTasks.Builder().build());
                resources.connectors().add(connectors.next());
            }
        }

        for (WorkerLoad existing : existingWorkers) {
            Iterator<ConnectorTaskId> tasks = existing.tasks().iterator();
            numToRevoke = existing.tasksSize() - ceilTasks;
            log.debug("Tasks on worker {} is higher than ceiling, so revoking {} tasks", existing, numToRevoke);
            for (int i = existing.tasksSize(); i > floorTasks && numToRevoke > 0; --i, --numToRevoke) {
                ConnectorsAndTasks resources = revoking.computeIfAbsent(
                    existing.worker(),
                    w -> new ConnectorsAndTasks.Builder().build());
                resources.tasks().add(tasks.next());
            }
        }

        return revoking;
    }

    private Map<String, ExtendedAssignment> fillAssignments(Collection<String> members, short error,
                                                            String leaderId, String leaderUrl, long maxOffset,
                                                            Map<String, Collection<String>> connectorAssignments,
                                                            Map<String, Collection<ConnectorTaskId>> taskAssignments,
                                                            Map<String, ConnectorsAndTasks> revoked,
                                                            int delay, short protocolVersion) {
        Map<String, ExtendedAssignment> groupAssignment = new HashMap<>();
        for (String member : members) {
            Collection<String> connectorsToStart = connectorAssignments.getOrDefault(member, Collections.emptyList());
            Collection<ConnectorTaskId> tasksToStart = taskAssignments.getOrDefault(member, Collections.emptyList());
            Collection<String> connectorsToStop = revoked.getOrDefault(member, ConnectorsAndTasks.EMPTY).connectors();
            Collection<ConnectorTaskId> tasksToStop = revoked.getOrDefault(member, ConnectorsAndTasks.EMPTY).tasks();
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
    protected Map<String, ByteBuffer> serializeAssignments(Map<String, ExtendedAssignment> assignments) {
        return assignments.entrySet()
                .stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> IncrementalCooperativeConnectProtocol.serializeAssignment(e.getValue())));
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
            values.removeAll(toSubtract.get(entry.getKey()));
            incremental.put(entry.getKey(), values);
        }
        return incremental;
    }

    private ConnectorsAndTasks assignment(Map<String, ExtendedWorkerState> memberConfigs) {
        log.debug("Received assignments: {}", memberConfigs);
        Set<String> connectors = memberConfigs.values()
                .stream()
                .flatMap(state -> state.assignment().connectors().stream())
                .collect(Collectors.toSet());
        Set<ConnectorTaskId> tasks = memberConfigs.values()
                .stream()
                .flatMap(state -> state.assignment().tasks().stream())
                .collect(Collectors.toSet());
        return new ConnectorsAndTasks.Builder().with(connectors, tasks).build();
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

    private static List<WorkerLoad> workerAssignment(Map<String, ExtendedWorkerState> memberConfigs,
                                                     ConnectorsAndTasks toExclude) {
        ConnectorsAndTasks ignore = new ConnectorsAndTasks.Builder()
                .with(new HashSet<>(toExclude.connectors()), new HashSet<>(toExclude.tasks()))
                .build();

        return memberConfigs.entrySet().stream()
                .map(e -> new WorkerLoad.Builder(e.getKey()).with(
                        e.getValue().assignment().connectors().stream()
                                .filter(v -> !ignore.connectors().contains(v))
                                .collect(Collectors.toList()),
                        e.getValue().assignment().tasks().stream()
                                .filter(v -> !ignore.tasks().contains(v))
                                .collect(Collectors.toList())
                        ).build()
                ).collect(Collectors.toList());
    }

}
