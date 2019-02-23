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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.ExtendedAssignment;
import org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.ExtendedWorkerState;
import org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.ConnectorsAndTasks;
import org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.WorkerLoad;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.Assignment;
import static org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.LeaderState;

/**
 * An assignor that computes a distribution of connectors and tasks according to the incremental
 * cooperative strategy for rebalancing. {@see
 * https://cwiki.apache.org/confluence/display/KAFKA/KIP-415%3A+Incremental+Cooperative
 * +Rebalancing+in+Kafka+Connect} for a description of the assignment policy.
 */
public class IncrementalCooperativeAssignor implements ConnectAssignor {
    private final Logger log;
    private ConnectorsAndTasks previousAssignment = ConnectorsAndTasks.EMPTY;

    public IncrementalCooperativeAssignor(LogContext logContext) {
        this.log = logContext.logger(IncrementalCooperativeAssignor.class);
    }

    @Override
    public Map<String, ByteBuffer> performAssignment(String leaderId, String protocol,
                                                     Map<String, ByteBuffer> allMemberMetadata,
                                                     WorkerCoordinator coordinator) {
        log.debug("Performing task assignment");

        Map<String, ExtendedWorkerState> memberConfigs = new HashMap<>();
        for (Map.Entry<String, ByteBuffer> entry : allMemberMetadata.entrySet())
            memberConfigs.put(entry.getKey(), IncrementalCooperativeConnectProtocol.deserializeMetadata(entry.getValue()));

        // The new config offset is the maximum seen by any member. We always perform assignment using this offset,
        // even if some members have fallen behind. The config offset used to generate the assignment is included in
        // the response so members that have fallen behind will not use the assignment until they have caught up.
        long maxOffset = memberConfigs.values().stream().map(ExtendedWorkerState::offset).max(Long::compare).get();
        log.debug("Max config offset root: {}, local snapshot config offsets root: {}",
                  maxOffset, coordinator.configSnapshot().offset());

        Long leaderOffset = ensureLeaderConfig(maxOffset, coordinator);
        if (leaderOffset == null) {
            Map<String, ExtendedAssignment> assignments = fillAssignments(
                    memberConfigs.keySet(), Assignment.CONFIG_MISMATCH,
                    leaderId, memberConfigs.get(leaderId).url(), maxOffset, Collections.emptyMap(),
                    Collections.emptyMap(), Collections.emptyMap());
            return serializeAssignments(assignments);
        }
        return performTaskAssignment(leaderId, leaderOffset, memberConfigs, coordinator);
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
     * TODO: describe algorithm
     * @param leaderId
     * @param maxOffset
     * @param memberConfigs
     * @param coordinator
     * @return
     */
    private Map<String, ByteBuffer> performTaskAssignment(String leaderId, long maxOffset,
                                                          Map<String, ExtendedWorkerState> memberConfigs,
                                                          WorkerCoordinator coordinator) {
        ConnectorsAndTasks activeAssignments = assignment(memberConfigs);
        log.debug("Active assignments: {}", activeAssignments);
        ConnectorsAndTasks lostAssignments = diff(previousAssignment, activeAssignments);
        log.debug("Lost assignments: {}", lostAssignments);

        List<WorkerLoad> completeWorkerAssignment = workerAssignment(memberConfigs, true);

        Map<String, ConnectorsAndTasks> toRevoke = performTaskRevocation(activeAssignments, completeWorkerAssignment);
        log.debug("Connectors and tasks to revoke: {}", toRevoke);

        Set<String> configuredConnectors = new TreeSet<>(coordinator.configSnapshot().connectors());
        Set<ConnectorTaskId> configuredTasks = configuredConnectors.stream()
                .flatMap(c -> coordinator.configSnapshot().tasks(c).stream())
                .collect(Collectors.toSet());

        ConnectorsAndTasks configured = ConnectorsAndTasks.embed(configuredConnectors, configuredTasks);
        log.debug("Configured assignments: {}", configured);
        ConnectorsAndTasks newSubmissions = diff(configured, previousAssignment);
        log.debug("New assignments: {}", newSubmissions);

        List<WorkerLoad> currentWorkerAssignment = workerAssignment(memberConfigs, true);

        if (lostAssignments.isEmpty()) {
            assignConnectors(completeWorkerAssignment, newSubmissions.connectors());
            assignTasks(completeWorkerAssignment, newSubmissions.tasks());
        } else {
            log.debug("Found the following connectors and tasks missing from previous assignment: "
                      + lostAssignments);
        }

        log.debug("Complete assignments: {}", currentWorkerAssignment);
        log.debug("New complete assignments: {}", completeWorkerAssignment);

        Map<String, Collection<String>> connectorAssignments =
                completeWorkerAssignment.stream().collect(Collectors.toMap(WorkerLoad::worker, WorkerLoad::connectors));

        Map<String, Collection<ConnectorTaskId>> taskAssignments =
                completeWorkerAssignment.stream().collect(Collectors.toMap(WorkerLoad::worker, WorkerLoad::tasks));

        Map<String, Collection<String>> currentConnectorAssignments =
                currentWorkerAssignment.stream().collect(Collectors.toMap(WorkerLoad::worker, WorkerLoad::connectors));

        Map<String, Collection<ConnectorTaskId>> currentTaskAssignments =
                currentWorkerAssignment.stream().collect(Collectors.toMap(WorkerLoad::worker, WorkerLoad::tasks));

        Map<String, Collection<String>> incrementalConnectorAssignments =
                diff(connectorAssignments, currentConnectorAssignments);

        Map<String, Collection<ConnectorTaskId>> incrementalTaskAssignments =
                diff(taskAssignments, currentTaskAssignments);

        log.debug("Incremental Connector assignments: {}", incrementalConnectorAssignments);
        log.debug("Incremental Task assignments: {}", incrementalTaskAssignments);

        coordinator.leaderState(new LeaderState(memberConfigs, connectorAssignments, taskAssignments));

        Map<String, ExtendedAssignment> assignments =
                fillAssignments(memberConfigs.keySet(), Assignment.NO_ERROR, leaderId,
                                memberConfigs.get(leaderId).url(), maxOffset, incrementalConnectorAssignments,
                                incrementalTaskAssignments, toRevoke);

        previousAssignment = ConnectorsAndTasks.embed(
                connectorAssignments.values().stream().flatMap(Collection::stream).collect(Collectors.toList()),
                taskAssignments.values().stream().flatMap(Collection::stream).collect(Collectors.toList()));

        for (ConnectorsAndTasks revoked : toRevoke.values()) {
            previousAssignment.connectors().removeAll(revoked.connectors());
            previousAssignment.tasks().removeAll(revoked.tasks());
        }

        log.debug("Actual assignments: {}", assignments);
        return serializeAssignments(assignments);
    }

    /**
     * TODO: describe algorithm
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

        if (!(newWorkersNum > 0 && existingWorkersNum > 0)) {
            log.debug("Business as usual existing {} new {} total {}", existingWorkersNum,
                    newWorkersNum, totalWorkersNum
            );
            completeWorkerAssignment.stream()
                    .forEachOrdered(wl -> log.debug("Current plan worker {} connectors {} tasks {}",
                            wl.worker(),
                            wl.connectorsSize(),
                            wl.tasksSize()));
            return Collections.emptyMap();
        }

        log.debug("Yay!!! Fresh new workers {} with {} existing for a total of {}",
                newWorkersNum, existingWorkersNum, totalWorkersNum);

        int floorConnectors = totalActiveConnectorsNum / totalWorkersNum;
        int floorTasks = totalActiveTasksNum / totalWorkersNum;

        log.debug("Old connectors per worker {}", totalActiveConnectorsNum / existingWorkersNum);
        log.debug("New connectors per worker {}", totalActiveConnectorsNum / totalWorkersNum);
        log.debug("Old tasks per worker {}", totalActiveTasksNum / existingWorkersNum);
        log.debug("New tasks per worker {}", totalActiveTasksNum / totalWorkersNum);

        Map<String, ConnectorsAndTasks> revoking = new HashMap<>();
        int numToRevoke = floorConnectors;
        for (WorkerLoad existing : existingWorkers) {
            Iterator<String> connectors = existing.connectors().iterator();
            for (int i = existing.connectorsSize(); i > floorConnectors && numToRevoke > 0; --i, --numToRevoke) {
                ConnectorsAndTasks resources = revoking.computeIfAbsent(
                    existing.worker(),
                    w -> ConnectorsAndTasks.embed(new ArrayList<>(), new ArrayList<>()));
                resources.connectors().add(connectors.next());
            }
            if (numToRevoke == 0) {
                break;
            }
        }

        numToRevoke = floorTasks;
        for (WorkerLoad existing : existingWorkers) {
            Iterator<ConnectorTaskId> tasks = existing.tasks().iterator();
            for (int i = existing.tasksSize(); i > floorTasks && numToRevoke > 0; --i, --numToRevoke) {
                ConnectorsAndTasks resources = revoking.computeIfAbsent(
                    existing.worker(),
                    w -> ConnectorsAndTasks.embed(new ArrayList<>(), new ArrayList<>()));
                resources.tasks().add(tasks.next());
            }
            if (numToRevoke == 0) {
                break;
            }
        }

        completeWorkerAssignment.stream().forEachOrdered(wl -> log.debug("Old plan worker {} "
                + "connectors {} tasks {}", wl.worker(), wl.connectorsSize(), wl.tasksSize()));
        return revoking;
    }

    private Map<String, ExtendedAssignment> fillAssignments(Collection<String> members, short error,
                                                            String leaderId, String leaderUrl, long maxOffset,
                                                            Map<String, Collection<String>> connectorAssignments,
                                                            Map<String, Collection<ConnectorTaskId>> taskAssignments,
                                                            Map<String, ConnectorsAndTasks> revoked) {
        Map<String, ExtendedAssignment> groupAssignment = new HashMap<>();
        for (String member : members) {
            Collection<String> connectorsToStart = connectorAssignments.getOrDefault(member, Collections.emptyList());
            Collection<ConnectorTaskId> tasksToStart = taskAssignments.getOrDefault(member, Collections.emptyList());
            Collection<String> connectorsToStop = revoked.getOrDefault(member, ConnectorsAndTasks.EMPTY).connectors();
            Collection<ConnectorTaskId> tasksToStop = revoked.getOrDefault(member, ConnectorsAndTasks.EMPTY).tasks();
            ExtendedAssignment assignment =
                    new ExtendedAssignment(error, leaderId, leaderUrl, maxOffset, connectorsToStart,
                                           tasksToStart, connectorsToStop, tasksToStop);
            log.debug("Filling assignment: {} -> {}", member, assignment);
            groupAssignment.put(member, assignment);
        }
        log.debug("Finished assignment");
        return groupAssignment;
    }

    private Map<String, ByteBuffer> serializeAssignments(Map<String, ExtendedAssignment> assignments) {
        return assignments.entrySet()
                .stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> IncrementalCooperativeConnectProtocol.serializeAssignment(e.getValue())));
    }

    private static ConnectorsAndTasks diff(ConnectorsAndTasks base, ConnectorsAndTasks toSubtract) {
        Collection<String> connectors = new TreeSet<>(base.connectors());
        connectors.removeAll(toSubtract.connectors());
        Collection<ConnectorTaskId> tasks = new TreeSet<>(base.tasks());
        tasks.removeAll(toSubtract.tasks());
        return ConnectorsAndTasks.embed(connectors, tasks);
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
        return ConnectorsAndTasks.embed(connectors, tasks);
    }

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

    private static List<WorkerLoad> workerAssignment(Map<String, ExtendedWorkerState> memberConfigs, boolean complete) {
        return memberConfigs.entrySet()
                .stream()
                .map(e -> WorkerLoad.copy(
                        e.getKey(),
                        complete ? e.getValue().assignment().connectors() : new ArrayList<>(),
                        complete ? e.getValue().assignment().tasks() : new ArrayList<>()))
                .collect(Collectors.toList());
    }

}
