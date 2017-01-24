/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.clients.consumer.internals.AbstractCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupRequest.ProtocolMetadata;
import org.apache.kafka.common.utils.CircularIterator;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class manages the coordination process with the Kafka group coordinator on the broker for managing assignments
 * to workers.
 */
public final class WorkerCoordinator extends AbstractCoordinator implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(WorkerCoordinator.class);

    // Currently doesn't support multiple task assignment strategies, so we just fill in a default value
    public static final String DEFAULT_SUBPROTOCOL = "default";

    private final String restUrl;
    private final ConfigBackingStore configStorage;
    private ConnectProtocol.Assignment assignmentSnapshot;
    private ClusterConfigState configSnapshot;
    private final WorkerRebalanceListener listener;
    private LeaderState leaderState;

    private boolean rejoinRequested;

    /**
     * Initialize the coordination manager.
     */
    public WorkerCoordinator(ConsumerNetworkClient client,
                             String groupId,
                             int rebalanceTimeoutMs,
                             int sessionTimeoutMs,
                             int heartbeatIntervalMs,
                             Metrics metrics,
                             String metricGrpPrefix,
                             Time time,
                             long retryBackoffMs,
                             String restUrl,
                             ConfigBackingStore configStorage,
                             WorkerRebalanceListener listener) {
        super(client,
                groupId,
                rebalanceTimeoutMs,
                sessionTimeoutMs,
                heartbeatIntervalMs,
                metrics,
                metricGrpPrefix,
                time,
                retryBackoffMs);
        this.restUrl = restUrl;
        this.configStorage = configStorage;
        this.assignmentSnapshot = null;
        new WorkerCoordinatorMetrics(metrics, metricGrpPrefix);
        this.listener = listener;
        this.rejoinRequested = false;
    }

    public void requestRejoin() {
        rejoinRequested = true;
    }

    @Override
    public String protocolType() {
        return "connect";
    }

    public void poll(long timeout) {
        // poll for io until the timeout expires
        final long start = time.milliseconds();
        long now = start;
        long remaining;

        do {
            if (coordinatorUnknown()) {
                ensureCoordinatorReady();
                now = time.milliseconds();
            }

            if (needRejoin()) {
                ensureActiveGroup();
                now = time.milliseconds();
            }

            pollHeartbeat(now);

            long elapsed = now - start;
            remaining = timeout - elapsed;

            // Note that because the network client is shared with the background heartbeat thread,
            // we do not want to block in poll longer than the time to the next heartbeat.
            client.poll(Math.min(Math.max(0, remaining), timeToNextHeartbeat(now)));

            now = time.milliseconds();
            elapsed = now - start;
            remaining = timeout - elapsed;
        } while (remaining > 0);
    }

    @Override
    public List<ProtocolMetadata> metadata() {
        configSnapshot = configStorage.snapshot();
        ConnectProtocol.WorkerState workerState = new ConnectProtocol.WorkerState(restUrl, configSnapshot.offset());
        ByteBuffer metadata = ConnectProtocol.serializeMetadata(workerState);
        return Collections.singletonList(new ProtocolMetadata(DEFAULT_SUBPROTOCOL, metadata));
    }

    @Override
    protected void onJoinComplete(int generation, String memberId, String protocol, ByteBuffer memberAssignment) {
        assignmentSnapshot = ConnectProtocol.deserializeAssignment(memberAssignment);
        // At this point we always consider ourselves to be a member of the cluster, even if there was an assignment
        // error (the leader couldn't make the assignment) or we are behind the config and cannot yet work on our assigned
        // tasks. It's the responsibility of the code driving this process to decide how to react (e.g. trying to get
        // up to date, try to rejoin again, leaving the group and backing off, etc.).
        rejoinRequested = false;
        listener.onAssigned(assignmentSnapshot, generation);
    }

    @Override
    protected Map<String, ByteBuffer> performAssignment(String leaderId, String protocol, Map<String, ByteBuffer> allMemberMetadata) {
        log.debug("Performing task assignment");

        Map<String, ConnectProtocol.WorkerState> memberConfigs = new HashMap<>();
        for (Map.Entry<String, ByteBuffer> entry : allMemberMetadata.entrySet())
            memberConfigs.put(entry.getKey(), ConnectProtocol.deserializeMetadata(entry.getValue()));

        long maxOffset = findMaxMemberConfigOffset(memberConfigs);
        Long leaderOffset = ensureLeaderConfig(maxOffset);
        if (leaderOffset == null)
            return fillAssignmentsAndSerialize(memberConfigs.keySet(), ConnectProtocol.Assignment.CONFIG_MISMATCH,
                    leaderId, memberConfigs.get(leaderId).url(), maxOffset,
                    new HashMap<String, List<String>>(), new HashMap<String, List<ConnectorTaskId>>());
        return performTaskAssignment(leaderId, leaderOffset, memberConfigs);
    }

    private long findMaxMemberConfigOffset(Map<String, ConnectProtocol.WorkerState> memberConfigs) {
        // The new config offset is the maximum seen by any member. We always perform assignment using this offset,
        // even if some members have fallen behind. The config offset used to generate the assignment is included in
        // the response so members that have fallen behind will not use the assignment until they have caught up.
        Long maxOffset = null;
        for (Map.Entry<String, ConnectProtocol.WorkerState> stateEntry : memberConfigs.entrySet()) {
            long memberRootOffset = stateEntry.getValue().offset();
            if (maxOffset == null)
                maxOffset = memberRootOffset;
            else
                maxOffset = Math.max(maxOffset, memberRootOffset);
        }

        log.debug("Max config offset root: {}, local snapshot config offsets root: {}",
                maxOffset, configSnapshot.offset());
        return maxOffset;
    }

    private Long ensureLeaderConfig(long maxOffset) {
        // If this leader is behind some other members, we can't do assignment
        if (configSnapshot.offset() < maxOffset) {
            // We might be able to take a new snapshot to catch up immediately and avoid another round of syncing here.
            // Alternatively, if this node has already passed the maximum reported by any other member of the group, it
            // is also safe to use this newer state.
            ClusterConfigState updatedSnapshot = configStorage.snapshot();
            if (updatedSnapshot.offset() < maxOffset) {
                log.info("Was selected to perform assignments, but do not have latest config found in sync request. " +
                        "Returning an empty configuration to trigger re-sync.");
                return null;
            } else {
                configSnapshot = updatedSnapshot;
                return configSnapshot.offset();
            }
        }

        return maxOffset;
    }

    private Map<String, ByteBuffer> performTaskAssignment(String leaderId, long maxOffset, Map<String, ConnectProtocol.WorkerState> memberConfigs) {
        Map<String, List<String>> connectorAssignments = new HashMap<>();
        Map<String, List<ConnectorTaskId>> taskAssignments = new HashMap<>();

        // Perform round-robin task assignment. Assign all connectors and then all tasks because assigning both the
        // connector and its tasks can lead to very uneven distribution of work in some common cases (e.g. for connectors
        // that generate only 1 task each; in a cluster of 2 or an even # of nodes, only even nodes will be assigned
        // connectors and only odd nodes will be assigned tasks, but tasks are, on average, actually more resource
        // intensive than connectors).
        List<String> connectorsSorted = sorted(configSnapshot.connectors());
        CircularIterator<String> memberIt = new CircularIterator<>(sorted(memberConfigs.keySet()));
        for (String connectorId : connectorsSorted) {
            String connectorAssignedTo = memberIt.next();
            log.trace("Assigning connector {} to {}", connectorId, connectorAssignedTo);
            List<String> memberConnectors = connectorAssignments.get(connectorAssignedTo);
            if (memberConnectors == null) {
                memberConnectors = new ArrayList<>();
                connectorAssignments.put(connectorAssignedTo, memberConnectors);
            }
            memberConnectors.add(connectorId);
        }
        for (String connectorId : connectorsSorted) {
            for (ConnectorTaskId taskId : sorted(configSnapshot.tasks(connectorId))) {
                String taskAssignedTo = memberIt.next();
                log.trace("Assigning task {} to {}", taskId, taskAssignedTo);
                List<ConnectorTaskId> memberTasks = taskAssignments.get(taskAssignedTo);
                if (memberTasks == null) {
                    memberTasks = new ArrayList<>();
                    taskAssignments.put(taskAssignedTo, memberTasks);
                }
                memberTasks.add(taskId);
            }
        }

        this.leaderState = new LeaderState(memberConfigs, connectorAssignments, taskAssignments);

        return fillAssignmentsAndSerialize(memberConfigs.keySet(), ConnectProtocol.Assignment.NO_ERROR,
                leaderId, memberConfigs.get(leaderId).url(), maxOffset, connectorAssignments, taskAssignments);
    }

    private Map<String, ByteBuffer> fillAssignmentsAndSerialize(Collection<String> members,
                                                                short error,
                                                                String leaderId,
                                                                String leaderUrl,
                                                                long maxOffset,
                                                                Map<String, List<String>> connectorAssignments,
                                                                Map<String, List<ConnectorTaskId>> taskAssignments) {

        Map<String, ByteBuffer> groupAssignment = new HashMap<>();
        for (String member : members) {
            List<String> connectors = connectorAssignments.get(member);
            if (connectors == null)
                connectors = Collections.emptyList();
            List<ConnectorTaskId> tasks = taskAssignments.get(member);
            if (tasks == null)
                tasks = Collections.emptyList();
            ConnectProtocol.Assignment assignment = new ConnectProtocol.Assignment(error, leaderId, leaderUrl, maxOffset, connectors, tasks);
            log.debug("Assignment: {} -> {}", member, assignment);
            groupAssignment.put(member, ConnectProtocol.serializeAssignment(assignment));
        }
        log.debug("Finished assignment");
        return groupAssignment;
    }

    @Override
    protected void onJoinPrepare(int generation, String memberId) {
        this.leaderState = null;
        log.debug("Revoking previous assignment {}", assignmentSnapshot);
        if (assignmentSnapshot != null && !assignmentSnapshot.failed())
            listener.onRevoked(assignmentSnapshot.leader(), assignmentSnapshot.connectors(), assignmentSnapshot.tasks());
    }

    @Override
    protected boolean needRejoin() {
        return super.needRejoin() || (assignmentSnapshot == null || assignmentSnapshot.failed()) || rejoinRequested;
    }

    public String memberId() {
        Generation generation = generation();
        if (generation != null)
            return generation.memberId;
        return JoinGroupRequest.UNKNOWN_MEMBER_ID;
    }

    @Override
    public void close() {
        super.close();
    }

    private boolean isLeader() {
        return assignmentSnapshot != null && memberId().equals(assignmentSnapshot.leader());
    }

    public String ownerUrl(String connector) {
        if (needRejoin() || !isLeader())
            return null;
        return leaderState.ownerUrl(connector);
    }

    public String ownerUrl(ConnectorTaskId task) {
        if (needRejoin() || !isLeader())
            return null;
        return leaderState.ownerUrl(task);
    }

    private class WorkerCoordinatorMetrics {
        public final String metricGrpName;

        public WorkerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            Measurable numConnectors = new Measurable() {
                public double measure(MetricConfig config, long now) {
                    return assignmentSnapshot.connectors().size();
                }
            };

            Measurable numTasks = new Measurable() {
                public double measure(MetricConfig config, long now) {
                    return assignmentSnapshot.tasks().size();
                }
            };

            metrics.addMetric(metrics.metricName("assigned-connectors",
                              this.metricGrpName,
                              "The number of connector instances currently assigned to this consumer"), numConnectors);
            metrics.addMetric(metrics.metricName("assigned-tasks",
                              this.metricGrpName,
                              "The number of tasks currently assigned to this consumer"), numTasks);
        }
    }

    private static <T extends Comparable<T>> List<T> sorted(Collection<T> members) {
        List<T> res = new ArrayList<>(members);
        Collections.sort(res);
        return res;
    }

    private static <K, V> Map<V, K> invertAssignment(Map<K, List<V>> assignment) {
        Map<V, K> inverted = new HashMap<>();
        for (Map.Entry<K, List<V>> assignmentEntry : assignment.entrySet()) {
            K key = assignmentEntry.getKey();
            for (V value : assignmentEntry.getValue())
                inverted.put(value, key);
        }
        return inverted;
    }

    private static class LeaderState {
        private final Map<String, ConnectProtocol.WorkerState> allMembers;
        private final Map<String, String> connectorOwners;
        private final Map<ConnectorTaskId, String> taskOwners;

        public LeaderState(Map<String, ConnectProtocol.WorkerState> allMembers,
                           Map<String, List<String>> connectorAssignment,
                           Map<String, List<ConnectorTaskId>> taskAssignment) {
            this.allMembers = allMembers;
            this.connectorOwners = invertAssignment(connectorAssignment);
            this.taskOwners = invertAssignment(taskAssignment);
        }

        private String ownerUrl(ConnectorTaskId id) {
            String ownerId = taskOwners.get(id);
            if (ownerId == null)
                return null;
            return allMembers.get(ownerId).url();
        }

        private String ownerUrl(String connector) {
            String ownerId = connectorOwners.get(connector);
            if (ownerId == null)
                return null;
            return allMembers.get(ownerId).url();
        }

    }

}
