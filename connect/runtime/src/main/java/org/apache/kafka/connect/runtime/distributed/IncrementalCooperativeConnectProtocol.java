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

import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.kafka.common.protocol.types.Type.NULLABLE_BYTES;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.ASSIGNMENT_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONFIG_OFFSET_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONFIG_STATE_V0;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONNECTOR_ASSIGNMENT_V0;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONNECTOR_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONNECTOR_TASK;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONNECT_PROTOCOL_HEADER_SCHEMA;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONNECT_PROTOCOL_V0;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.ERROR_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.LEADER_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.LEADER_URL_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.TASKS_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.URL_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.VERSION_KEY_NAME;

/**
 * This class implements the protocol for Kafka Connect workers in a group. It includes the format of worker state used when
 * joining the group and distributing assignments, and the format of assignments of connectors and tasks to workers.
 */
public class IncrementalCooperativeConnectProtocol {
    public static final String ALLOCATION_KEY_NAME = "allocation";
    public static final String REVOKED_KEY_NAME = "revoked";
    public static final short CONNECT_PROTOCOL_V1 = 1;

    /**
     * Connect Protocol Header V1:
     *   Version            => Int16
     */
    private static final Struct CONNECT_PROTOCOL_HEADER_V1 = new Struct(CONNECT_PROTOCOL_HEADER_SCHEMA)
            .set(VERSION_KEY_NAME, CONNECT_PROTOCOL_V1);

    /**
     * Config State V1:
     *   Url                => [String]
     *   ConfigOffset       => Int64
     */
    public static final Schema CONFIG_STATE_V1 = CONFIG_STATE_V0;

    /**
     * Subscription V1
     *   Url                => [String]
     *   ConfigOffset       => Int64
     */
    public static final Schema ALLOCATION_V1 = new Schema(
            new Field(ALLOCATION_KEY_NAME, NULLABLE_BYTES));

    /**
     * Connector Assignment V0:
     *   Connector          => [String]
     *   Tasks              => [Int32]
     */
    // Assignments for each worker are a set of connectors and tasks. These are categorized by connector ID. A sentinel
    // task ID (CONNECTOR_TASK) is used to indicate the connector itself (i.e. that the assignment includes
    // responsibility for running the Connector instance in addition to any tasks it generates).
    public static final Schema CONNECTOR_ASSIGNMENT_V1 = CONNECTOR_ASSIGNMENT_V0;

    /**
     * Assignment V1:
     *   Error              => Int16
     *   Leader             => [String]
     *   LeaderUrl          => [String]
     *   ConfigOffset       => Int64
     *   Assignment         => [Connector Assignment]
     *   Revoked            => [Connector Assignment]
     */
    public static final Schema ASSIGNMENT_V1 = new Schema(
            new Field(ERROR_KEY_NAME, Type.INT16),
            new Field(LEADER_KEY_NAME, Type.STRING),
            new Field(LEADER_URL_KEY_NAME, Type.STRING),
            new Field(CONFIG_OFFSET_KEY_NAME, Type.INT64),
            new Field(ASSIGNMENT_KEY_NAME, ArrayOf.nullable(CONNECTOR_ASSIGNMENT_V1)),
            new Field(REVOKED_KEY_NAME, ArrayOf.nullable(CONNECTOR_ASSIGNMENT_V1)));

    /**
     * The fields are serialized in sequence as follows:
     * Subscription V1:
     *   Version            => Int16
     *   Url                => [String]
     *   ConfigOffset       => Int64
     *   Assignment         => [Byte]
     */
    public static ByteBuffer serializeMetadata(ExtendedWorkerState workerState) {
        Struct configState = new Struct(CONFIG_STATE_V1)
                .set(URL_KEY_NAME, workerState.url())
                .set(CONFIG_OFFSET_KEY_NAME, workerState.offset());
        // Not a big issue if we embed the protocol version with the assignment in the metadata
        Struct allocation = new Struct(ALLOCATION_V1)
                .set(ALLOCATION_KEY_NAME, serializeAssignment(workerState.assignment));
        ByteBuffer buffer = ByteBuffer.allocate(CONNECT_PROTOCOL_HEADER_V1.sizeOf()
                                                + CONFIG_STATE_V1.sizeOf(configState)
                                                + ALLOCATION_V1.sizeOf(allocation));
        CONNECT_PROTOCOL_HEADER_V1.writeTo(buffer);
        CONFIG_STATE_V1.write(buffer, configState);
        ALLOCATION_V1.write(buffer, allocation);
        buffer.flip();
        return buffer;
    }

    public static ExtendedWorkerState deserializeMetadata(ByteBuffer buffer) {
        Struct header = CONNECT_PROTOCOL_HEADER_SCHEMA.read(buffer);
        Short version = header.getShort(VERSION_KEY_NAME);
        checkVersionCompatibility(version);
        Struct configState = CONFIG_STATE_V1.read(buffer);
        long configOffset = configState.getLong(CONFIG_OFFSET_KEY_NAME);
        String url = configState.getString(URL_KEY_NAME);
        Struct allocation = ALLOCATION_V1.read(buffer);
        // Protocol version is embeded with the assignment in the metadata
        ExtendedAssignment assignment = deserializeAssignment(allocation.getBytes(ALLOCATION_KEY_NAME));
        return new ExtendedWorkerState(url, configOffset, assignment);
    }

    public static ByteBuffer serializeAssignment(ExtendedAssignment assignment) {
        if (assignment == null) {
            return null;
        }
        Struct struct = assignment.toStruct();
        ByteBuffer buffer = ByteBuffer.allocate(CONNECT_PROTOCOL_HEADER_V1.sizeOf()
                                                + ASSIGNMENT_V1.sizeOf(struct));
        CONNECT_PROTOCOL_HEADER_V1.writeTo(buffer);
        ASSIGNMENT_V1.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    public static ExtendedAssignment deserializeAssignment(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        Struct header = CONNECT_PROTOCOL_HEADER_SCHEMA.read(buffer);
        Short version = header.getShort(VERSION_KEY_NAME);
        checkVersionCompatibility(version);
        Struct struct = ASSIGNMENT_V1.read(buffer);
        return ExtendedAssignment.fromStruct(struct);
    }

    private static List<Struct> taskAssignments(Map<String, List<Integer>> assignments) {
        return assignments == null
               ? null
               : assignments.entrySet().stream()
                       .map(connectorEntry -> {
                           Struct taskAssignment = new Struct(CONNECTOR_ASSIGNMENT_V1);
                           taskAssignment.set(CONNECTOR_KEY_NAME, connectorEntry.getKey());
                           taskAssignment.set(TASKS_KEY_NAME, connectorEntry.getValue().toArray());
                           return taskAssignment;
                       }).collect(Collectors.toList());
    }

    private static List<String> extractConnectors(Struct struct, String key) {
        Object[] connectors = struct.getArray(key);
        if (connectors == null) {
            return null;
        }
        List<String> connectorIds = new ArrayList<>();
        for (Object structObj : struct.getArray(key)) {
            Struct assignment = (Struct) structObj;
            String connector = assignment.getString(CONNECTOR_KEY_NAME);
            for (Object taskIdObj : assignment.getArray(TASKS_KEY_NAME)) {
                Integer taskId = (Integer) taskIdObj;
                if (taskId == CONNECTOR_TASK) {
                    connectorIds.add(connector);
                }
            }
        }
        return connectorIds;
    }

    private static List<ConnectorTaskId> extractTasks(Struct struct, String key) {
        Object[] tasks = struct.getArray(key);
        if (tasks == null) {
            return null;
        }
        List<ConnectorTaskId> tasksIds = new ArrayList<>();
        for (Object structObj : struct.getArray(key)) {
            Struct assignment = (Struct) structObj;
            String connector = assignment.getString(CONNECTOR_KEY_NAME);
            for (Object taskIdObj : assignment.getArray(TASKS_KEY_NAME)) {
                Integer taskId = (Integer) taskIdObj;
                if (taskId != CONNECTOR_TASK) {
                    tasksIds.add(new ConnectorTaskId(connector, taskId));
                }
            }
        }
        return tasksIds;
    }

    public static class ExtendedWorkerState extends ConnectProtocol.WorkerState {
        private final ExtendedAssignment assignment;

        public ExtendedWorkerState(String url, long offset, ExtendedAssignment assignment) {
            super(url, offset);
            this.assignment = assignment;
        }

        public ExtendedAssignment assignment() {
            return assignment;
        }

        @Override
        public String toString() {
            return "WorkerState{" +
                    "url='" + url() + '\'' +
                    ", offset=" + offset() +
                    ", " + assignment +
                    '}';
        }
    }

    public static class ExtendedAssignment extends ConnectProtocol.Assignment {
        private final List<String> revokedConnectorIds;
        private final List<ConnectorTaskId> revokedTaskIds;

        private static final ExtendedAssignment EMPTY = new ExtendedAssignment(
                ConnectProtocol.Assignment.NO_ERROR, null, null, -1, Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

        /**
         * Create an assignment indicating responsibility for the given connector instances and task Ids.
         * @param revokedConnectorIds list of connectors that the worker should instantiate and run
         * @param revokedTaskIds list of task IDs that the worker should instantiate and run
         */
        public ExtendedAssignment(short error, String leader, String leaderUrl, long configOffset,
                                  List<String> connectorIds, List<ConnectorTaskId> taskIds,
                                  List<String> revokedConnectorIds, List<ConnectorTaskId> revokedTaskIds) {
            super(error, leader, leaderUrl, configOffset, connectorIds, taskIds);
            this.revokedConnectorIds = revokedConnectorIds;
            this.revokedTaskIds = revokedTaskIds;
        }

        public ExtendedAssignment(
                ConnectProtocol.Assignment assignmentV0,
                List<String> revokedConnectorIds,
                List<ConnectorTaskId> revokedTaskIds
        ) {
            super(assignmentV0.error(), assignmentV0.leader(), assignmentV0.leaderUrl(),
                  assignmentV0.offset(), assignmentV0.connectors(), assignmentV0.tasks());
            this.revokedConnectorIds = revokedConnectorIds;
            this.revokedTaskIds = revokedTaskIds;
        }

        public List<String> revokedConnectors() {
            return revokedConnectorIds;
        }

        public List<ConnectorTaskId> revokedTasks() {
            return revokedTaskIds;
        }

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
                        List<Integer> connectorTasks = taskMap.get(connectorId);
                        taskMap.computeIfAbsent(connectorId, v -> new ArrayList<>());
                        taskMap.put(connectorId, connectorTasks);
                        connectorTasks.add(CONNECTOR_TASK);
                    });

            Optional.ofNullable(revokedTaskIds)
                    .orElseGet(Collections::emptyList)
                    .stream()
                    .forEachOrdered(taskId -> {
                        String connectorId = taskId.connector();
                        List<Integer> connectorTasks = taskMap.get(connectorId);
                        taskMap.computeIfAbsent(connectorId, v -> new ArrayList<>());
                        taskMap.put(connectorId, connectorTasks);
                        connectorTasks.add(taskId.task());
                    });
            return taskMap;
        }

        public Struct toStruct() {
            List<Struct> assigned = taskAssignments(asMap());
            List<Struct> revoked = taskAssignments(revokedAsMap());
            return new Struct(ASSIGNMENT_V1)
                    .set(ERROR_KEY_NAME, error())
                    .set(LEADER_KEY_NAME, leader())
                    .set(LEADER_URL_KEY_NAME, leaderUrl())
                    .set(CONFIG_OFFSET_KEY_NAME, offset())
                    .set(ASSIGNMENT_KEY_NAME, assigned != null ? assigned.toArray() : null)
                    .set(REVOKED_KEY_NAME, revoked != null ? revoked.toArray() : null);
        }

        public static ExtendedAssignment fromStruct(Struct struct) {
            return struct == null
                   ? null
                   : new ExtendedAssignment(
                           struct.getShort(ERROR_KEY_NAME),
                           struct.getString(LEADER_KEY_NAME),
                           struct.getString(LEADER_URL_KEY_NAME),
                           struct.getLong(CONFIG_OFFSET_KEY_NAME),
                           extractConnectors(struct, ASSIGNMENT_KEY_NAME),
                           extractTasks(struct, ASSIGNMENT_KEY_NAME),
                           extractConnectors(struct, REVOKED_KEY_NAME),
                           extractTasks(struct, REVOKED_KEY_NAME));
        }
    }

    private static void checkVersionCompatibility(short version) {
        // check for invalid versions
        if (version < CONNECT_PROTOCOL_V0)
            throw new SchemaException("Unsupported subscription version: " + version);

        // otherwise, assume versions can be parsed
    }

}
