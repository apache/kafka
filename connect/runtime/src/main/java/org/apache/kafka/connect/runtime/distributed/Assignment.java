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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The basic assignment of connectors and tasks introduced with V0 version of the Connect protocol.
 */
public class Assignment {

    public static ByteBuffer toByteBuffer(Assignment assignment) {
        return MessageUtil.toVersionPrefixedByteBuffer(ExtendedConnectAssignment.LOWEST_SUPPORTED_VERSION, new ExtendedConnectAssignment()
                .setError(assignment.error())
                .setLeader(assignment.leader())
                .setLeaderUrl(assignment.leaderUrl())
                .setConfigOffset(assignment.offset())
                .setConnectorTasks(assignment.asMap().entrySet().stream().map(entry -> new ExtendedConnectAssignment.ConnectorTask()
                        .setConnector(entry.getKey())
                        .setTaskIds(entry.getValue()))
                        .collect(Collectors.toList())));
    }

    /**
     * Given a byte buffer that contains an assignment as defined by this protocol, return the
     * deserialized form of the assignment.
     *
     * @param buffer the buffer containing a serialized assignment
     * @return the deserialized assignment
     * @throws SchemaException on incompatible Connect protocol version
     */
    public static Assignment of(ByteBuffer buffer) {
        short version = buffer.getShort();
        if (version >= ExtendedConnectAssignment.LOWEST_SUPPORTED_VERSION && version <= ExtendedConnectAssignment.HIGHEST_SUPPORTED_VERSION) {
            ExtendedConnectAssignment assignment = new ExtendedConnectAssignment(new ByteBufferAccessor(buffer), version);
            AbstractMap.SimpleEntry<Collection<String>, Collection<ConnectorTaskId>> newConnectors =
                    ExtendedAssignment.extract(assignment.connectorTasks());
            return new Assignment(
                    assignment.error(),
                    assignment.leader(),
                    assignment.leaderUrl(),
                    assignment.configOffset(),
                    newConnectors.getKey(),
                    newConnectors.getValue()
            );
        } else throw new SchemaException("Unsupported subscription version: " + version);
    }

    public static final short NO_ERROR = 0;
    // Configuration offsets mismatched in a way that the leader could not resolve. Workers should read to the end
    // of the config log and try to re-join
    public static final short CONFIG_MISMATCH = 1;

    private final short error;
    private final String leader;
    private final String leaderUrl;
    private final long offset;
    private final Collection<String> connectorIds;
    private final Collection<ConnectorTaskId> taskIds;

    /**
     * Create an assignment indicating responsibility for the given connector instances and task Ids.
     *
     * @param error        error code for this assignment; {@code ConnectProtocol.Assignment.NO_ERROR}
     *                     indicates no error during assignment
     * @param leader       Connect group's leader Id; may be null only on the empty assignment
     * @param leaderUrl    Connect group's leader URL; may be null only on the empty assignment
     * @param configOffset the most up-to-date configuration offset according to this assignment
     * @param connectorIds list of connectors that the worker should instantiate and run; may not be null
     * @param taskIds      list of task IDs that the worker should instantiate and run; may not be null
     */
    public Assignment(short error, String leader, String leaderUrl, long configOffset,
                      Collection<String> connectorIds, Collection<ConnectorTaskId> taskIds) {
        this.error = error;
        this.leader = leader;
        this.leaderUrl = leaderUrl;
        this.offset = configOffset;
        this.connectorIds = Objects.requireNonNull(connectorIds,
                "Assigned connector IDs may be empty but not null");
        this.taskIds = Objects.requireNonNull(taskIds,
                "Assigned task IDs may be empty but not null");
    }

    /**
     * Return the error code of this assignment; 0 signals successful assignment ({@code ConnectProtocol.Assignment.NO_ERROR}).
     *
     * @return the error code of the assignment
     */
    public short error() {
        return error;
    }

    /**
     * Return the ID of the leader Connect worker in this assignment.
     *
     * @return the ID of the leader
     */
    public String leader() {
        return leader;
    }

    /**
     * Return the URL to which the leader accepts requests from other members of the group.
     *
     * @return the leader URL
     */
    public String leaderUrl() {
        return leaderUrl;
    }

    /**
     * Check if this assignment failed.
     *
     * @return true if this assignment failed; false otherwise
     */
    public boolean failed() {
        return error != NO_ERROR;
    }

    /**
     * Return the most up-to-date offset in the configuration topic according to this assignment
     *
     * @return the configuration topic
     */
    public long offset() {
        return offset;
    }

    /**
     * The connectors included in this assignment.
     *
     * @return the connectors
     */
    public Collection<String> connectors() {
        return connectorIds;
    }

    /**
     * The tasks included in this assignment.
     *
     * @return the tasks
     */
    public Collection<ConnectorTaskId> tasks() {
        return taskIds;
    }

    @Override
    public String toString() {
        return "Assignment{" +
                "error=" + error +
                ", leader='" + leader + '\'' +
                ", leaderUrl='" + leaderUrl + '\'' +
                ", offset=" + offset +
                ", connectorIds=" + connectorIds +
                ", taskIds=" + taskIds +
                '}';
    }

    protected Map<String, List<Integer>> asMap() {
        // Using LinkedHashMap preserves the ordering, which is helpful for tests and debugging
        Map<String, List<Integer>> taskMap = new LinkedHashMap<>();
        for (String connectorId : new HashSet<>(connectorIds)) {
            taskMap.computeIfAbsent(connectorId, key -> new ArrayList<>()).add(ConnectProtocol.CONNECTOR_TASK);
        }
        for (ConnectorTaskId taskId : taskIds) {
            String connectorId = taskId.connector();
            taskMap.computeIfAbsent(connectorId, key -> new ArrayList<>()).add(taskId.task());
        }
        return taskMap;
    }
}
