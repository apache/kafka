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

import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.connect.internals.generated.ExtendedConnectAssignment;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONNECTOR_TASK;

/**
 * The extended assignment of connectors and tasks that includes revoked connectors and tasks
 * as well as a scheduled rebalancing delay.
 */
public class ExtendedAssignment extends Assignment {

    public static ByteBuffer toByteBuffer(ExtendedAssignment assignment) {
        if (assignment == null || ExtendedAssignment.empty().equals(assignment)) return null;
        return MessageUtil.toVersionPrefixedByteBuffer(ExtendedConnectAssignment.HIGHEST_SUPPORTED_VERSION, new ExtendedConnectAssignment()
                .setError(assignment.error())
                .setLeader(assignment.leader())
                .setLeaderUrl(assignment.leaderUrl())
                .setConfigOffset(assignment.offset())
                .setConnectorTasks(to(assignment.asMap()))
                .setRevokedTasks(to(assignment.revokedAsMap()))
                .setDelayMs(assignment.delay()));
    }

    private static List<ExtendedConnectAssignment.ConnectorTask> to(Map<String, List<Integer>> map) {
        return map == null ? null : map.entrySet().stream().map(entry -> new ExtendedConnectAssignment.ConnectorTask()
                .setConnector(entry.getKey())
                .setTaskIds(entry.getValue()))
                .collect(Collectors.toList());
    }

    /**
     * Given a byte buffer that contains an assignment as defined by this protocol, return the
     * deserialized form of the assignment.
     *
     * @param buffer the buffer containing a serialized assignment
     * @return the deserialized assignment
     * @throws SchemaException on incompatible Connect protocol version
     */
    public static ExtendedAssignment of(ByteBuffer buffer) {
        if (buffer == null) return null;
        short version = buffer.getShort();
        if (version >= ExtendedConnectAssignment.LOWEST_SUPPORTED_VERSION && version <= ExtendedConnectAssignment.HIGHEST_SUPPORTED_VERSION) {
            ExtendedConnectAssignment assignment = new ExtendedConnectAssignment(new ByteBufferAccessor(buffer), version);
            AbstractMap.SimpleEntry<Collection<String>, Collection<ConnectorTaskId>> newConnectors =
                    extract(assignment.connectorTasks());
            AbstractMap.SimpleEntry<Collection<String>, Collection<ConnectorTaskId>> revokedConnectors =
                    extract(assignment.revokedTasks());
            return new ExtendedAssignment(
                    version,
                    assignment.error(),
                    assignment.leader(),
                    assignment.leaderUrl(),
                    assignment.configOffset(),
                    newConnectors.getKey(),
                    newConnectors.getValue(),
                    revokedConnectors.getKey(),
                    revokedConnectors.getValue(),
                    assignment.delayMs());
        } else throw new SchemaException("Unsupported subscription version: " + version);
    }

    static AbstractMap.SimpleEntry<Collection<String>, Collection<ConnectorTaskId>> extract(
            Collection<ExtendedConnectAssignment.ConnectorTask> tasks) {
        if (tasks == null) return new AbstractMap.SimpleEntry<>(Collections.emptyList(), Collections.emptyList());
        List<String> connectorIds = new ArrayList<>();
        List<ConnectorTaskId> taskIds = new ArrayList<>();
        tasks.forEach(connectorTask -> connectorTask.taskIds().forEach(id -> {
            if (id == CONNECTOR_TASK) connectorIds.add(connectorTask.connector());
            else taskIds.add(new ConnectorTaskId(connectorTask.connector(), id));
        }));
        return new AbstractMap.SimpleEntry<>(connectorIds, taskIds);
    }

    private final short version;
    private final Collection<String> revokedConnectorIds;
    private final Collection<ConnectorTaskId> revokedTaskIds;
    private final int delay;

    private static final ExtendedAssignment EMPTY = new ExtendedAssignment(
            ConnectProtocolCompatibility.COMPATIBLE.protocolVersion(), Assignment.NO_ERROR, null, null, -1,
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), 0);

    /**
     * Create an assignment indicating responsibility for the given connector instances and task Ids.
     *
     * @param version Connect protocol version
     * @param error error code for this assignment; {@code ConnectProtocol.Assignment.NO_ERROR}
     *              indicates no error during assignment
     * @param leader Connect group's leader Id; may be null only on the empty assignment
     * @param leaderUrl Connect group's leader URL; may be null only on the empty assignment
     * @param configOffset the offset in the config topic that this assignment is corresponding to
     * @param connectorIds list of connectors that the worker should instantiate and run; may not be null
     * @param taskIds list of task IDs that the worker should instantiate and run; may not be null
     * @param revokedConnectorIds list of connectors that the worker should stop running; may not be null
     * @param revokedTaskIds list of task IDs that the worker should stop running; may not be null
     * @param delay the scheduled delay after which the worker should rejoin the group
     */
    public ExtendedAssignment(short version, short error, String leader, String leaderUrl, long configOffset,
                             Collection<String> connectorIds, Collection<ConnectorTaskId> taskIds,
                             Collection<String> revokedConnectorIds, Collection<ConnectorTaskId> revokedTaskIds,
                             int delay) {
        super(error, leader, leaderUrl, configOffset, connectorIds, taskIds);
        this.version = version;
        this.revokedConnectorIds = Objects.requireNonNull(revokedConnectorIds,
                "Revoked connector IDs may be empty but not null");
        this.revokedTaskIds = Objects.requireNonNull(revokedTaskIds,
                "Revoked task IDs may be empty but not null");
        this.delay = delay;
    }

    public static ExtendedAssignment duplicate(ExtendedAssignment assignment) {
        return new ExtendedAssignment(
                assignment.version(),
                assignment.error(),
                assignment.leader(),
                assignment.leaderUrl(),
                assignment.offset(),
                new LinkedHashSet<>(assignment.connectors()),
                new LinkedHashSet<>(assignment.tasks()),
                new LinkedHashSet<>(assignment.revokedConnectors()),
                new LinkedHashSet<>(assignment.revokedTasks()),
                assignment.delay());
    }

    /**
     * Return the version of the connect protocol that this assignment belongs to.
     *
     * @return the connect protocol version of this assignment
     */
    public short version() {
        return version;
    }

    /**
     * Return the IDs of the connectors that are revoked by this assignment.
     *
     * @return the revoked connector IDs; empty if there are no revoked connectors
     */
    public Collection<String> revokedConnectors() {
        return revokedConnectorIds;
    }

    /**
     * Return the IDs of the tasks that are revoked by this assignment.
     *
     * @return the revoked task IDs; empty if there are no revoked tasks
     */
    public Collection<ConnectorTaskId> revokedTasks() {
        return revokedTaskIds;
    }

    /**
     * Return the delay for the rebalance that is scheduled by this assignment.
     *
     * @return the scheduled delay
     */
    public int delay() {
        return delay;
    }

    /**
     * Return an empty assignment.
     *
     * @return an empty assignment
     */
    public static ExtendedAssignment empty() {
        return EMPTY;
    }

    @Override
    public String toString() {
        return "Assignment{" +
                "error=" + error() +
                ", leader='" + leader() + '\'' +
                ", leaderUrl='" + leaderUrl() + '\'' +
                ", offset=" + offset() +
                ", connectorIds=" + connectors() +
                ", taskIds=" + tasks() +
                ", revokedConnectorIds=" + revokedConnectorIds +
                ", revokedTaskIds=" + revokedTaskIds +
                ", delay=" + delay +
                '}';
    }

    private Map<String, List<Integer>> revokedAsMap() {
        if (revokedConnectorIds == null && revokedTaskIds == null) {
            return null;
        }
        // Using LinkedHashMap preserves the ordering, which is helpful for tests and debugging
        Map<String, List<Integer>> taskMap = new LinkedHashMap<>();
        Optional.ofNullable(revokedConnectorIds)
                .orElseGet(Collections::emptyList)
                .stream()
                .distinct()
                .forEachOrdered(connectorId -> {
                    Collection<Integer> connectorTasks =
                            taskMap.computeIfAbsent(connectorId, v -> new ArrayList<>());
                    connectorTasks.add(CONNECTOR_TASK);
                });

        Optional.ofNullable(revokedTaskIds)
                .orElseGet(Collections::emptyList)
                .forEach(taskId -> {
                    String connectorId = taskId.connector();
                    Collection<Integer> connectorTasks =
                            taskMap.computeIfAbsent(connectorId, v -> new ArrayList<>());
                    connectorTasks.add(taskId.task());
                });
        return taskMap;
    }
}
