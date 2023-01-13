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

import org.apache.kafka.common.utils.CircularIterator;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.Assignment;
import static org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.LeaderState;


/**
 * An assignor that computes a unweighted round-robin distribution of connectors and tasks. The
 * connectors are assigned to the workers first, followed by the tasks. This is to avoid
 * load imbalance when several 1-task connectors are running, given that a connector is usually
 * more lightweight than a task.
 * <p>
 * Note that this class is NOT thread-safe.
 */
public class EagerAssignor implements ConnectAssignor {
    private final Logger log;

    public EagerAssignor(LogContext logContext) {
        this.log = logContext.logger(EagerAssignor.class);
    }

    @Override
    public Map<String, ByteBuffer> performAssignment(String leaderId, String protocol,
                                                     List<JoinGroupResponseMember> allMemberMetadata,
                                                     WorkerCoordinator coordinator) {
        log.debug("Performing task assignment");
        Map<String, ExtendedWorkerState> memberConfigs = new HashMap<>();
        for (JoinGroupResponseMember member : allMemberMetadata)
            memberConfigs.put(member.memberId(), IncrementalCooperativeConnectProtocol.deserializeMetadata(ByteBuffer.wrap(member.metadata())));

        long maxOffset = findMaxMemberConfigOffset(memberConfigs, coordinator);
        Long leaderOffset = ensureLeaderConfig(maxOffset, coordinator);
        if (leaderOffset == null)
            return fillAssignmentsAndSerialize(memberConfigs.keySet(), Assignment.CONFIG_MISMATCH,
                    leaderId, memberConfigs.get(leaderId).url(), maxOffset,
                    new HashMap<>(), new HashMap<>());
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
                log.info("Was selected to perform assignments, but do not have latest config found in sync request. " +
                        "Returning an empty configuration to trigger re-sync.");
                return null;
            } else {
                coordinator.configSnapshot(updatedSnapshot);
                return updatedSnapshot.offset();
            }
        }
        return maxOffset;
    }

    private Map<String, ByteBuffer> performTaskAssignment(String leaderId, long maxOffset,
                                                          Map<String, ExtendedWorkerState> memberConfigs,
                                                          WorkerCoordinator coordinator) {
        Map<String, Collection<String>> connectorAssignments = new HashMap<>();
        Map<String, Collection<ConnectorTaskId>> taskAssignments = new HashMap<>();

        // Perform round-robin task assignment. Assign all connectors and then all tasks because assigning both the
        // connector and its tasks can lead to very uneven distribution of work in some common cases (e.g. for connectors
        // that generate only 1 task each; in a cluster of 2 or an even # of nodes, only even nodes will be assigned
        // connectors and only odd nodes will be assigned tasks, but tasks are, on average, actually more resource
        // intensive than connectors).
        List<String> connectorsSorted = sorted(coordinator.configSnapshot().connectors());
        CircularIterator<String> memberIt = new CircularIterator<>(sorted(memberConfigs.keySet()));
        for (String connectorId : connectorsSorted) {
            String connectorAssignedTo = memberIt.next();
            log.trace("Assigning connector {} to {}", connectorId, connectorAssignedTo);
            Collection<String> memberConnectors = connectorAssignments.computeIfAbsent(connectorAssignedTo, k -> new ArrayList<>());
            memberConnectors.add(connectorId);
        }
        for (String connectorId : connectorsSorted) {
            for (ConnectorTaskId taskId : sorted(coordinator.configSnapshot().tasks(connectorId))) {
                String taskAssignedTo = memberIt.next();
                log.trace("Assigning task {} to {}", taskId, taskAssignedTo);
                Collection<ConnectorTaskId> memberTasks = taskAssignments.computeIfAbsent(taskAssignedTo, k -> new ArrayList<>());
                memberTasks.add(taskId);
            }
        }

        coordinator.leaderState(new LeaderState(memberConfigs, connectorAssignments, taskAssignments));

        return fillAssignmentsAndSerialize(memberConfigs.keySet(), Assignment.NO_ERROR,
                leaderId, memberConfigs.get(leaderId).url(), maxOffset, connectorAssignments, taskAssignments);
    }

    private Map<String, ByteBuffer> fillAssignmentsAndSerialize(Collection<String> members,
                                                                short error,
                                                                String leaderId,
                                                                String leaderUrl,
                                                                long maxOffset,
                                                                Map<String, Collection<String>> connectorAssignments,
                                                                Map<String, Collection<ConnectorTaskId>> taskAssignments) {

        Map<String, ByteBuffer> groupAssignment = new HashMap<>();
        for (String member : members) {
            Collection<String> connectors = connectorAssignments.get(member);
            if (connectors == null) {
                connectors = Collections.emptyList();
            }
            Collection<ConnectorTaskId> tasks = taskAssignments.get(member);
            if (tasks == null) {
                tasks = Collections.emptyList();
            }
            Assignment assignment = new Assignment(error, leaderId, leaderUrl, maxOffset, connectors, tasks);
            log.debug("Assignment: {} -> {}", member, assignment);
            groupAssignment.put(member, ConnectProtocol.serializeAssignment(assignment));
        }
        log.debug("Finished assignment");
        return groupAssignment;
    }

    private long findMaxMemberConfigOffset(Map<String, ExtendedWorkerState> memberConfigs,
                                           WorkerCoordinator coordinator) {
        // The new config offset is the maximum seen by any member. We always perform assignment using this offset,
        // even if some members have fallen behind. The config offset used to generate the assignment is included in
        // the response so members that have fallen behind will not use the assignment until they have caught up.
        Long maxOffset = null;
        for (Map.Entry<String, ExtendedWorkerState> stateEntry : memberConfigs.entrySet()) {
            long memberRootOffset = stateEntry.getValue().offset();
            if (maxOffset == null)
                maxOffset = memberRootOffset;
            else
                maxOffset = Math.max(maxOffset, memberRootOffset);
        }

        log.debug("Max config offset root: {}, local snapshot config offsets root: {}",
                  maxOffset, coordinator.configSnapshot().offset());
        return maxOffset;
    }

    private static <T extends Comparable<T>> List<T> sorted(Collection<T> members) {
        List<T> res = new ArrayList<>(members);
        Collections.sort(res);
        return res;
    }

}
