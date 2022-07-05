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
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.Assignment;

/**
 * An assignor that always revokes every connector and task from every worker in the cluster.
 */
public class RevokingAssignor implements ConnectAssignor {

    private final Logger log;

    public RevokingAssignor(LogContext logContext) {
        this.log = logContext.logger(RevokingAssignor.class);
    }

    @Override
    public Map<String, ByteBuffer> performAssignment(String leaderId, String protocol,
                                                     List<JoinGroupResponseMember> allMemberMetadata,
                                                     WorkerCoordinator coordinator) {
        Map<String, ExtendedWorkerState> memberConfigs = new HashMap<>();
        for (JoinGroupResponseMember member : allMemberMetadata) {
            memberConfigs.put(
                    member.memberId(),
                    IncrementalCooperativeConnectProtocol.deserializeMetadata(ByteBuffer.wrap(member.metadata())));
        }
        log.debug("Member configs: {}", memberConfigs);

        short protocolVersion = ConnectProtocolCompatibility.fromProtocol(protocol).protocolVersion();
        String leaderUrl = memberConfigs.get(leaderId).url();

        // The new config offset is the maximum seen by any member. We always perform assignment using this offset,
        // even if some members have fallen behind. The config offset used to generate the assignment is included in
        // the response so members that have fallen behind will not use the assignment until they have caught up.
        long maxOffset = memberConfigs.values().stream().map(ExtendedWorkerState::offset).max(Long::compare).get();
        log.debug("Max config offset root: {}, local snapshot config offsets root: {}",
                maxOffset, coordinator.configSnapshot().offset());
        Long leaderOffset = ensureLeaderConfig(maxOffset, coordinator);

        Map<String, ExtendedAssignment> assignments;
        if (leaderOffset == null) {
            assignments = fillFailedAssignments(maxOffset, leaderId, leaderUrl, memberConfigs.keySet(), protocolVersion, Assignment.CONFIG_MISMATCH);
        } else {
            assignments = fillRevokingAssignments(maxOffset, leaderId, leaderUrl, memberConfigs, protocolVersion);
        }
        return serializeAssignments(assignments);
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

    // Revoke all assignments
    private Map<String, ExtendedAssignment> fillRevokingAssignments(
            long maxOffset,
            String leaderId,
            String leaderUrl,
            Map<String, ExtendedWorkerState> memberConfigs,
            short protocolVersion
    ) {
        Map<String, ExtendedAssignment> groupAssignment = new HashMap<>();
        memberConfigs.forEach((member, memberState) -> {
            Collection<String> revokedConnectors = memberState.assignment().connectors();
            Collection<ConnectorTaskId> revokedTasks = memberState.assignment().tasks();
            ExtendedAssignment assignment = new ExtendedAssignment(
                    protocolVersion, Assignment.NO_ERROR, leaderId, leaderUrl, maxOffset,
                    Collections.emptySet(), Collections.emptySet(), revokedConnectors, revokedTasks,
                    0
            );
            log.debug("Filling assignment: {} -> {}", member, assignment);
            groupAssignment.put(member, assignment);
        });
        log.debug("Finished assignment");
        return groupAssignment;
    }

    // Fail all assignments
    private Map<String, ExtendedAssignment> fillFailedAssignments(
            long maxOffset,
            String leaderId,
            String leaderUrl,
            Collection<String> members,
            short protocolVersion,
            short error
    ) {
        Map<String, ExtendedAssignment> groupAssignment = new HashMap<>();
        members.forEach(member -> {
            ExtendedAssignment assignment = new ExtendedAssignment(
                    protocolVersion, error, leaderId, leaderUrl, maxOffset,
                    Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
                    0
            );
            log.debug("Filling assignment: {} -> {}", member, assignment);
            groupAssignment.put(member, assignment);
        });
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
        // sessioned could just be set to false as the new protocol is EAGER which doesn't support sessioning.
        return assignments.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Entry::getKey,
                        e -> IncrementalCooperativeConnectProtocol.serializeAssignment(e.getValue(), false)));
    }

}