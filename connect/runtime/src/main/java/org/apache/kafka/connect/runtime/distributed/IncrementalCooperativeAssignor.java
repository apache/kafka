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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.WorkerLoad;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.Assignment;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECT_PROTOCOL_V1;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECT_PROTOCOL_V2;
import static org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.LeaderState;
import static org.apache.kafka.connect.util.ConnectUtils.combineCollections;
import static org.apache.kafka.connect.util.ConnectUtils.duplicatedElements;
import static org.apache.kafka.connect.util.ConnectUtils.transformValues;

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
    final Set<String> candidateWorkersForReassignment;
    long scheduledRebalance;
    int delay;
    int previousGenerationId;
    Set<String> previousMembers;

    public IncrementalCooperativeAssignor(LogContext logContext, Time time, int maxDelay) {
        this.log = logContext.logger(IncrementalCooperativeAssignor.class);
        this.time = time;
        this.maxDelay = maxDelay;
        this.previousAssignment = ConnectorsAndTasks.EMPTY;
        this.previousRevocation = ConnectorsAndTasks.EMPTY;
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
        log.trace("Performing task assignment");

        Map<String, ExtendedWorkerState> memberConfigs = new HashMap<>();
        for (JoinGroupResponseMember member : allMemberMetadata) {
            memberConfigs.put(
                    member.memberId(),
                    IncrementalCooperativeConnectProtocol.deserializeMetadata(ByteBuffer.wrap(member.metadata())));
        }
        log.trace("Member configs: {}", memberConfigs);

        // The new config offset is the maximum seen by any member. We always perform assignment using this offset,
        // even if some members have fallen behind. The config offset used to generate the assignment is included in
        // the response so members that have fallen behind will not use the assignment until they have caught up.
        long maxOffset = memberConfigs.values().stream().map(ExtendedWorkerState::offset).max(Long::compare).get();
        log.trace("Max config offset root: {}, local snapshot config offsets root: {}",
                  maxOffset, coordinator.configSnapshot().offset());

        short protocolVersion = memberConfigs.values().stream()
            .allMatch(state -> state.assignment().version() == CONNECT_PROTOCOL_V2)
                ? CONNECT_PROTOCOL_V2
                : CONNECT_PROTOCOL_V1;

        Long leaderOffset = ensureLeaderConfig(maxOffset, coordinator);
        Map<String, ExtendedAssignment> assignments;
        if (leaderOffset == null) {
            assignments = fillAssignments(
                    memberConfigs.keySet(), Assignment.CONFIG_MISMATCH,
                    leaderId, memberConfigs.get(leaderId).url(), maxOffset,
                    ClusterAssignment.EMPTY, 0, protocolVersion);
        } else {
            assignments = performTaskAssignment(leaderId, leaderOffset, memberConfigs, coordinator, protocolVersion);
        }
        Map<String, ByteBuffer> result = serializeAssignments(assignments);
        log.debug("Finished assignment");
        return result;
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
     * @return the assignment of tasks to the whole group, including assigned or revoked tasks
     */
    private Map<String, ExtendedAssignment> performTaskAssignment(String leaderId, long maxOffset,
                                                            Map<String, ExtendedWorkerState> memberConfigs,
                                                            WorkerCoordinator coordinator, short protocolVersion) {
        log.trace("Performing task assignment during generation: {} with memberId: {}",
                coordinator.generationId(), coordinator.memberId());
        Map<String, ConnectorsAndTasks> memberAssignments = transformValues(
                memberConfigs,
                memberConfig -> ConnectorsAndTasks.of(memberConfig.assignment())
        );
        ClusterAssignment clusterAssignment = performTaskAssignment(
                coordinator.configSnapshot(),
                coordinator.lastCompletedGenerationId(),
                coordinator.generationId(),
                memberAssignments
        );

        coordinator.leaderState(new LeaderState(memberConfigs, clusterAssignment.allAssignedConnectors(), clusterAssignment.allAssignedTasks()));

        Map<String, ExtendedAssignment> result =
                fillAssignments(memberConfigs.keySet(), Assignment.NO_ERROR, leaderId,
                        memberConfigs.get(leaderId).url(), maxOffset,
                        clusterAssignment,
                        delay, protocolVersion);

        log.trace("Actual assignments: {}", result);
        return result;
    }

    // Visible for testing
    ClusterAssignment performTaskAssignment(
            final ClusterConfigState configSnapshot,
            final int lastCompletedGenerationId,
            final int currentGenerationId,
            final Map<String, ConnectorsAndTasks> memberAssignments
    ) {
        // Base set: The previous assignment of connectors-and-tasks is a standalone snapshot that
        // can be used to calculate derived sets
        log.trace("Previous assignments: {}", previousAssignment);
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

        final Set<String> configuredConnectors = new TreeSet<>(configSnapshot.connectors());
        final Set<ConnectorTaskId> configuredTasks = combineCollections(configuredConnectors, configSnapshot::tasks, Collectors.toSet());

        // The set of currently-configured connectors and tasks across the entire cluster
        final ConnectorsAndTasks configured = ConnectorsAndTasks.of(configuredConnectors, configuredTasks);
        log.trace("Configured assignments: {}", configured);

        // The set of active connectors and tasks currently running across the entire cluster
        final ConnectorsAndTasks activeAssignments = ConnectorsAndTasks.combine(memberAssignments.values());
        log.trace("Active assignments: {}", activeAssignments);

        // This means that a previous revocation did not take effect. In this case, reset
        // appropriately and be ready to re-apply revocation of tasks
        if (!previousRevocation.isEmpty()) {
            if (previousRevocation.connectors().stream().anyMatch(c -> activeAssignments.connectors().contains(c))
                    || previousRevocation.tasks().stream().anyMatch(t -> activeAssignments.tasks().contains(t))) {
                previousAssignment = activeAssignments;
                canRevoke = true;
            }
            previousRevocation = ConnectorsAndTasks.EMPTY;
        }

        // The connectors and tasks that have been deleted since the last rebalance
        final ConnectorsAndTasks deleted = ConnectorsAndTasks.diff(previousAssignment, configured);
        log.trace("Deleted assignments: {}", deleted);

        // The connectors and tasks that are currently running on more than one worker each
        final ConnectorsAndTasks duplicated = duplicated(memberAssignments);
        log.trace("Duplicated assignments: {}", duplicated);

        // The connectors and tasks that should already be running on the cluster, but which are not included
        // in the assignment reported by any workers in the cluster
        final ConnectorsAndTasks lostAssignments = ConnectorsAndTasks.diff(previousAssignment, activeAssignments, deleted);
        log.trace("Lost assignments: {}", lostAssignments);

        // The connectors and tasks that have been created since the last rebalance
        final ConnectorsAndTasks created = ConnectorsAndTasks.diff(configured, previousAssignment, activeAssignments);
        log.trace("New assignments: {}", created);

        final Map<String, ConnectorsAndTasks.Builder> toRevoke = new HashMap<>();

        final Map<String, ConnectorsAndTasks> deletedAndRevoked = intersection(deleted, memberAssignments);
        log.trace("Deleted connectors and tasks to revoke from each worker: {}", deletedAndRevoked);
        addAll(toRevoke, deletedAndRevoked);

        // Revoking redundant connectors/tasks if the workers have duplicate assignments
        final Map<String, ConnectorsAndTasks> duplicatedAndRevoked = intersection(duplicated, memberAssignments);
        log.trace("Duplicated connectors and tasks to revoke from each worker: {}", duplicatedAndRevoked);
        addAll(toRevoke, duplicatedAndRevoked);

        // Compute the assignment that will be applied across the cluster after this round of rebalance
        // Later on, new submissions and lost-and-reassigned connectors and tasks will be added to these assignments,
        // and load-balancing revocations will be removed from them.
        final List<WorkerLoad> nextWorkerAssignment = workerLoads(memberAssignments, ConnectorsAndTasks.combine(deleted, duplicated));

        final ConnectorsAndTasks.Builder lostAssignmentsToReassign = ConnectorsAndTasks.builder();
        handleLostAssignments(lostAssignments, lostAssignmentsToReassign, nextWorkerAssignment);

        // Do not revoke resources for re-assignment while a delayed rebalance is active
        // Also we do not revoke in two consecutive rebalances by the same leader, which
        // should be fine since workers immediately rejoin the group after a rebalance
        // if connectors and/or tasks were revoked from them
        canRevoke = canRevoke && delay == 0;

        // Compute the connectors-and-tasks to be revoked for load balancing without taking into
        // account the deleted ones.
        log.trace("Can leader revoke tasks in this assignment? {} (delay: {})", canRevoke, delay);
        if (canRevoke) {
            Map<String, ConnectorsAndTasks> loadBalancingRevocations =
                    performLoadBalancingRevocations(configured, nextWorkerAssignment);

            log.trace("Load-balancing revocations for each worker: {}", loadBalancingRevocations);
            addAll(toRevoke, loadBalancingRevocations);
            canRevoke = ConnectorsAndTasks.combine(loadBalancingRevocations.values()).isEmpty();
        } else {
            canRevoke = delay == 0;
        }

        final ConnectorsAndTasks toAssign = ConnectorsAndTasks.combine(created, lostAssignmentsToReassign.build());
        assignConnectors(nextWorkerAssignment, toAssign.connectors());
        assignTasks(nextWorkerAssignment, toAssign.tasks());

        // The complete set of connectors and tasks that will be running on each worker after this rebalance
        final Map<String, ConnectorsAndTasks> nextAssignments = assignments(nextWorkerAssignment);

        log.debug("Current complete assignments: {}", memberAssignments);
        log.debug("Next complete assignments: {}", nextAssignments);

        // The newly-assigned connectors and tasks for each worker during this round
        final Map<String, ConnectorsAndTasks> incrementalAssignments = diff(nextAssignments, memberAssignments);

        final Map<String, ConnectorsAndTasks> revoked = buildAll(toRevoke);

        previousAssignment = computePreviousAssignment(revoked, nextAssignments, lostAssignments);
        previousGenerationId = currentGenerationId;
        previousMembers = memberAssignments.keySet();

        log.trace("Incremental assignments: {}", incrementalAssignments);

        return new ClusterAssignment(
                incrementalAssignments,
                revoked,
                nextAssignments
        );
    }

    private ConnectorsAndTasks computePreviousAssignment(Map<String, ConnectorsAndTasks> toRevoke,
                                                         Map<String, ConnectorsAndTasks> nextAssignments,
                                                         ConnectorsAndTasks lostAssignments) {
        ConnectorsAndTasks.Builder previousAssignment = ConnectorsAndTasks.builder()
                .addConnectors(ConnectUtils.combineCollections(nextAssignments.values(), ConnectorsAndTasks::connectors))
                .addTasks(ConnectUtils.combineCollections(nextAssignments.values(), ConnectorsAndTasks::tasks));

        ConnectorsAndTasks.Builder previousRevocation = this.previousRevocation.toBuilder();

        for (ConnectorsAndTasks revoked : toRevoke.values()) {
            previousAssignment.removeAll(revoked);
            previousRevocation.addAll(revoked);
        }

        this.previousRevocation = previousRevocation.build();
        return previousAssignment.addAll(lostAssignments).build();
    }

    // visible for testing
    void handleLostAssignments(ConnectorsAndTasks lostAssignments,
                                         ConnectorsAndTasks.Builder toReassign,
                                         List<WorkerLoad> completeWorkerAssignment) {
        if (lostAssignments.isEmpty()) {
            resetDelay();
            return;
        }

        final long now = time.milliseconds();
        log.trace("Found the following connectors and tasks missing from previous assignments: {}", lostAssignments);

        Set<String> activeMembers = completeWorkerAssignment.stream()
                .map(WorkerLoad::worker)
                .collect(Collectors.toSet());
        if (scheduledRebalance <= 0 && activeMembers.containsAll(previousMembers)) {
            log.debug("No worker seems to have departed the group during the rebalance. The "
                    + "missing assignments that the leader is detecting are probably due to some "
                    + "workers failing to receive the new assignments in the previous rebalance. "
                    + "Will reassign missing tasks as new tasks");
            toReassign.addAll(lostAssignments);
            return;
        }

        if (scheduledRebalance > 0 && now >= scheduledRebalance) {
            // delayed rebalance expired and it's time to assign resources
            log.debug("Delayed rebalance expired. Reassigning lost tasks");
            List<WorkerLoad> candidateWorkerLoads = pickCandidateWorkerForReassignment(completeWorkerAssignment);

            if (!candidateWorkerLoads.isEmpty()) {
                log.debug("Assigning lost assignments to {} candidate workers: {}",
                        candidateWorkerLoads.size(),
                        candidateWorkerLoads.stream().map(WorkerLoad::worker).collect(Collectors.joining(",")));
                assignConnectors(candidateWorkerLoads, lostAssignments.connectors());
                assignTasks(candidateWorkerLoads, lostAssignments.tasks());
            } else {
                log.debug("No single candidate worker was found to assign lost tasks. Treating lost tasks as new tasks");
                toReassign.addAll(lostAssignments);
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
            log.trace("Resetting delay from previous value: {} to 0", delay);
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
     * Revoke connectors and tasks from each worker in the cluster until no worker is running more than it would be if:
     * <ul>
     *     <li>The allocation of connectors and tasks across the cluster were as balanced as possible (i.e., the difference in allocation size between any two workers is at most one)</li>
     *     <li>Any workers that left the group within the scheduled rebalance delay permanently left the group</li>
     *     <li>All currently-configured connectors and tasks were allocated (including instances that may be revoked in this round because they are duplicated across workers)</li>
     * </ul>
     * @param configured the set of configured connectors and tasks across the entire cluster
     * @param workers the workers in the cluster, whose assignments should not include any deleted or duplicated connectors or tasks
     *                that are already due to be revoked from the worker in this rebalance
     * @return which connectors and tasks should be revoked from which workers; never null, but may be empty
     * if no load-balancing revocations are necessary or possible
     */
    private Map<String, ConnectorsAndTasks> performLoadBalancingRevocations(
            final ConnectorsAndTasks configured,
            final Collection<WorkerLoad> workers
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

        final Map<String, ConnectorsAndTasks.Builder> result = new HashMap<>();

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
            result.computeIfAbsent(worker, w -> ConnectorsAndTasks.builder())
                    .addConnectors(revoked)
        );
        taskRevocations.forEach((worker, revoked) ->
            result.computeIfAbsent(worker, w -> ConnectorsAndTasks.builder())
                    .addTasks(revoked)
        );

        return buildAll(result);
    }

    private <E> Map<String, Set<E>> loadBalancingRevocations(
            final String allocatedResourceName,
            final int totalToAllocate,
            final Collection<WorkerLoad> workers,
            final Function<WorkerLoad, Collection<E>> workerAllocation
    ) {
        final int totalWorkers = workers.size();
        // The minimum instances of this resource that should be assigned to each worker
        final int minAllocatedPerWorker = totalToAllocate / totalWorkers;
        // How many workers are going to have to be allocated exactly one extra instance
        // (since the total number to allocate may not be a perfect multiple of the number of workers)
        final int extrasToAllocate = totalToAllocate % totalWorkers;
        // Useful function to determine exactly how many instances of the resource a given worker is currently allocated
        final Function<WorkerLoad, Integer> workerAllocationSize = workerAllocation.andThen(Collection::size);

        final long workersAllocatedMinimum = workers.stream()
                .map(workerAllocationSize)
                .filter(n -> n == minAllocatedPerWorker)
                .count();
        final long workersAllocatedSingleExtra = workers.stream()
                .map(workerAllocationSize)
                .filter(n -> n == minAllocatedPerWorker + 1)
                .count();
        if (workersAllocatedSingleExtra == extrasToAllocate
                && workersAllocatedMinimum + workersAllocatedSingleExtra == totalWorkers) {
            log.trace(
                    "No load-balancing {} revocations required; the current allocations, when combined with any newly-created {}, should be balanced",
                    allocatedResourceName,
                    allocatedResourceName
            );
            return Collections.emptyMap();
        }

        final Map<String, Set<E>> result = new HashMap<>();
        // How many workers we've allocated a single extra resource instance to
        int allocatedExtras = 0;
        for (WorkerLoad worker : workers) {
            int currentAllocationSizeForWorker = workerAllocationSize.apply(worker);
            if (currentAllocationSizeForWorker <= minAllocatedPerWorker) {
                // This worker isn't allocated more than the minimum; no need to revoke anything
                continue;
            }
            int maxAllocationForWorker;
            if (allocatedExtras < extrasToAllocate) {
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
            while (workerAllocationSize.apply(worker) > maxAllocationForWorker) {
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
                // Make sure to remove the resource from the worker load so that later operations
                // (such as assigning newly-created connectors and tasks) can take that into account
                currentWorkerAllocation.remove();
            }
        }
        return result;
    }

    private int calculateDelay(long now) {
        long diff = scheduledRebalance - now;
        return diff > 0 ? (int) Math.min(diff, maxDelay) : 0;
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
            log.trace("Filling assignment: {} -> {}", member, assignment);
            groupAssignment.put(member, assignment);
        }
        return groupAssignment;
    }

    /**
     * Perform a round-robin assignment of connectors to workers with existing worker load. This
     * assignment tries to balance the load between workers, by assigning connectors to workers
     * that have equal load, starting with the least loaded workers.
     *
     * @param workerAssignment the current worker assignment; assigned connectors are added to this list
     * @param connectors the connectors to be assigned
     */
    // Visible for testing
    void assignConnectors(List<WorkerLoad> workerAssignment, Collection<String> connectors) {
        assign(workerAssignment, connectors, WorkerLoad::connectors, WorkerLoad::assign);
    }

    /**
     * Perform a round-robin assignment of tasks to workers with existing worker load. This
     * assignment tries to balance the load between workers, by assigning tasks to workers that
     * have equal load, starting with the least loaded workers.
     *
     * @param workerAssignment the current worker assignment; assigned tasks are added to this list
     * @param tasks the tasks to be assigned
     */
    // Visible for testing
    void assignTasks(List<WorkerLoad> workerAssignment, Collection<ConnectorTaskId> tasks) {
        assign(workerAssignment, tasks, WorkerLoad::tasks, WorkerLoad::assign);
    }

    private <E> void assign(
            List<WorkerLoad> workers,
            Collection<E> toAssign,
            Function<WorkerLoad, Collection<E>> currentAllocation,
            BiConsumer<WorkerLoad, E> assignToWorker
    ) {
        Function<WorkerLoad, Integer> allocationSize = currentAllocation.andThen(Collection::size);
        workers.sort(Comparator.comparing(allocationSize));
        WorkerLoad first = workers.get(0);

        Iterator<E> load = toAssign.stream().sorted().iterator();
        while (load.hasNext()) {
            int firstLoad = allocationSize.apply(first);
            int upTo = IntStream.range(0, workers.size())
                    .filter(i -> allocationSize.apply(workers.get(i)) > firstLoad)
                    .findFirst()
                    .orElse(workers.size());
            for (WorkerLoad worker : workers.subList(0, upTo)) {
                E nextToAssign = load.next();
                log.trace("Assigning {} to {}", nextToAssign, worker.worker());
                assignToWorker.accept(worker, nextToAssign);
                if (!load.hasNext()) {
                    break;
                }
            }
        }
    }

    private static Map<String, ConnectorsAndTasks> assignments(Collection<WorkerLoad> workers) {
        return workers.stream()
                .collect(Collectors.toMap(
                        WorkerLoad::worker,
                        ConnectorsAndTasks::of
                ));
    }

    private static ConnectorsAndTasks duplicated(Map<String, ConnectorsAndTasks> memberAssignments) {
        return ConnectorsAndTasks.of(
                duplicatedElements(combineCollections(memberAssignments.values(), ConnectorsAndTasks::connectors)),
                duplicatedElements(combineCollections(memberAssignments.values(), ConnectorsAndTasks::tasks))
        );
    }

    private static Map<String, ConnectorsAndTasks> intersection(ConnectorsAndTasks connectorsAndTasks, Map<String, ConnectorsAndTasks> assignments) {
        return transformValues(assignments, assignment -> ConnectorsAndTasks.intersection(assignment, connectorsAndTasks));
    }

    /**
     * From a map of workers to assignment object generate the equivalent map of workers to byte
     * buffers of serialized assignments.
     *
     * @param assignments the map of worker assignments
     * @return the serialized map of assignments to workers
     */
    private static Map<String, ByteBuffer> serializeAssignments(Map<String, ExtendedAssignment> assignments) {
        return assignments.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> IncrementalCooperativeConnectProtocol.serializeAssignment(e.getValue())));
    }

    private static void addAll(Map<String, ConnectorsAndTasks.Builder> base, Map<String, ConnectorsAndTasks> toAdd) {
        toAdd.forEach((worker, assignment) -> base
                .computeIfAbsent(worker, w -> ConnectorsAndTasks.builder())
                .addAll(assignment)
        );
    }

    private static Map<String, ConnectorsAndTasks> diff(Map<String, ConnectorsAndTasks> base, Map<String, ConnectorsAndTasks> toSubtract) {
        return base.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> {
                            ConnectorsAndTasks subtraction = toSubtract.getOrDefault(e.getKey(), ConnectorsAndTasks.EMPTY);
                            return ConnectorsAndTasks.diff(e.getValue(), subtraction);
                        }
                ));
    }

    private static <K> Map<K, ConnectorsAndTasks> buildAll(Map<K, ConnectorsAndTasks.Builder> builders) {
        return transformValues(builders, ConnectorsAndTasks.Builder::build);
    }

    private static List<WorkerLoad> workerLoads(Map<String, ConnectorsAndTasks> memberAssignments, ConnectorsAndTasks toExclude) {
        return memberAssignments.entrySet().stream()
                .map(e -> new WorkerLoad.Builder(e.getKey())
                        .with(ConnectorsAndTasks.diff(e.getValue(), toExclude))
                        .build()
                ).collect(Collectors.toList());
    }

    static class ClusterAssignment {

        private final Map<String, ConnectorsAndTasks> newlyAssigned;
        private final Map<String, ConnectorsAndTasks> newlyRevoked;
        private final Map<String, ConnectorsAndTasks> allAssigned;
        private final Set<String> allWorkers;

        public static final ClusterAssignment EMPTY = new ClusterAssignment(
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap()
        );

        public ClusterAssignment(
                Map<String, ConnectorsAndTasks> newlyAssigned,
                Map<String, ConnectorsAndTasks> newlyRevoked,
                Map<String, ConnectorsAndTasks> allAssigned
        ) {
            this.newlyAssigned = newlyAssigned;
            this.newlyRevoked = newlyRevoked;
            this.allAssigned = allAssigned;
            this.allWorkers = combineCollections(
                    Arrays.asList(newlyRevoked, newlyAssigned, allAssigned),
                    Map::keySet,
                    Collectors.toSet()
            );
        }

        public Map<String, Set<String>> newlyAssignedConnectors() {
            return transformValues(newlyAssigned, ConnectorsAndTasks::connectors);
        }

        public Set<String> newlyAssignedConnectors(String worker) {
            return newlyAssignedConnectors().getOrDefault(worker, Collections.emptySet());
        }

        public Map<String, Set<ConnectorTaskId>> newlyAssignedTasks() {
            return transformValues(newlyAssigned, ConnectorsAndTasks::tasks);
        }

        public Set<ConnectorTaskId> newlyAssignedTasks(String worker) {
            return newlyAssignedTasks().getOrDefault(worker, Collections.emptySet());
        }

        public Map<String, Set<String>> newlyRevokedConnectors() {
            return transformValues(newlyRevoked, ConnectorsAndTasks::connectors);
        }

        public Set<String> newlyRevokedConnectors(String worker) {
            return newlyRevokedConnectors().getOrDefault(worker, Collections.emptySet());
        }

        public Map<String, Set<ConnectorTaskId>> newlyRevokedTasks() {
            return transformValues(newlyRevoked, ConnectorsAndTasks::tasks);
        }

        public Set<ConnectorTaskId> newlyRevokedTasks(String worker) {
            return newlyRevokedTasks().getOrDefault(worker, Collections.emptySet());
        }

        public Map<String, Set<String>> allAssignedConnectors() {
            return transformValues(allAssigned, ConnectorsAndTasks::connectors);
        }

        public Map<String, Set<ConnectorTaskId>> allAssignedTasks() {
            return transformValues(allAssigned, ConnectorsAndTasks::tasks);
        }

        public Set<String> allWorkers() {
            return allWorkers;
        }

    }

}
