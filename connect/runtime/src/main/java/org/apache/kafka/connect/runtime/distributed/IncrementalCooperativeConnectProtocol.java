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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class implements the protocol for Kafka Connect workers in a group. It includes the format of worker state used when
 * joining the group and distributing assignments, and the format of assignments of connectors and tasks to workers.
 */
public class IncrementalCooperativeConnectProtocol extends ConnectProtocol {
    public static final String REVOKED_KEY_NAME = "revoked";
    public static final short CONNECT_PROTOCOL_V1 = 1;
    private static final Struct CONNECT_PROTOCOL_HEADER_V1 = new Struct(CONNECT_PROTOCOL_HEADER_SCHEMA)
            .set(VERSION_KEY_NAME, CONNECT_PROTOCOL_V1);
    public static final Schema CONFIG_STATE_V1 = CONFIG_STATE_V0;

    // Assignments for each worker are a set of connectors and tasks. These are categorized by connector ID. A sentinel
    // task ID (CONNECTOR_TASK) is used to indicate the connector itself (i.e. that the assignment includes
    // responsibility for running the Connector instance in addition to any tasks it generates).
    public static final Schema CONNECTOR_ASSIGNMENT_V1 = CONNECTOR_ASSIGNMENT_V0;
    public static final Schema ASSIGNMENT_V1 = new Schema(
            new Field(ERROR_KEY_NAME, Type.INT16),
            new Field(LEADER_KEY_NAME, Type.STRING),
            new Field(LEADER_URL_KEY_NAME, Type.STRING),
            new Field(CONFIG_OFFSET_KEY_NAME, Type.INT64),
            new Field(ASSIGNMENT_KEY_NAME, new ArrayOf(CONNECTOR_ASSIGNMENT_V1)),
            new Field(REVOKED_KEY_NAME, new ArrayOf(CONNECTOR_ASSIGNMENT_V1)));


    public static ByteBuffer serializeMetadata(WorkerState workerState) {
        Struct struct = new Struct(CONFIG_STATE_V1);
        struct.set(URL_KEY_NAME, workerState.url());
        struct.set(CONFIG_OFFSET_KEY_NAME, workerState.offset());
        ByteBuffer buffer = ByteBuffer.allocate(CONNECT_PROTOCOL_HEADER_V1.sizeOf() + CONFIG_STATE_V1.sizeOf(struct));
        CONNECT_PROTOCOL_HEADER_V1.writeTo(buffer);
        CONFIG_STATE_V1.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer serializeAssignment(ExtendedAssignment assignment) {
        Struct struct = new Struct(ASSIGNMENT_V1);
        struct.set(ERROR_KEY_NAME, assignment.error());
        struct.set(LEADER_KEY_NAME, assignment.leader());
        struct.set(LEADER_URL_KEY_NAME, assignment.leaderUrl());
        struct.set(CONFIG_OFFSET_KEY_NAME, assignment.offset());

        struct.set(ASSIGNMENT_KEY_NAME, taskAssignments(assignment.asMap()).toArray());
        struct.set(REVOKED_KEY_NAME, taskAssignments(assignment.revokedAsMap()).toArray());

        ByteBuffer buffer = ByteBuffer.allocate(CONNECT_PROTOCOL_HEADER_V1.sizeOf() + ASSIGNMENT_V1.sizeOf(struct));
        CONNECT_PROTOCOL_HEADER_V1.writeTo(buffer);
        ASSIGNMENT_V1.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    private static List<Struct> taskAssignments(Map<String, List<Integer>> assignments) {
        List<Struct> taskAssignments = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> connectorEntry : assignments.entrySet()) {
            Struct taskAssignment = new Struct(CONNECTOR_ASSIGNMENT_V1);
            taskAssignment.set(CONNECTOR_KEY_NAME, connectorEntry.getKey());
            List<Integer> tasks = connectorEntry.getValue();
            taskAssignment.set(TASKS_KEY_NAME, tasks.toArray());
            taskAssignments.add(taskAssignment);
        }
        return taskAssignments;
    }

    public static Assignment deserializeAssignment(ByteBuffer buffer) {
        Struct header = CONNECT_PROTOCOL_HEADER_SCHEMA.read(buffer);
        Short version = header.getShort(VERSION_KEY_NAME);
        checkVersionCompatibility(version);
        Struct struct = ASSIGNMENT_V1.read(buffer);
        short error = struct.getShort(ERROR_KEY_NAME);
        String leader = struct.getString(LEADER_KEY_NAME);
        String leaderUrl = struct.getString(LEADER_URL_KEY_NAME);
        long offset = struct.getLong(CONFIG_OFFSET_KEY_NAME);
        List<String> connectorIds = extractConnectors(struct, ASSIGNMENT_KEY_NAME);
        List<ConnectorTaskId> taskIds = extractTasks(struct, ASSIGNMENT_KEY_NAME);
        List<String> revokedConnectorIds = extractConnectors(struct, REVOKED_KEY_NAME);
        List<ConnectorTaskId> revokedTaskIds = extractTasks(struct, ASSIGNMENT_KEY_NAME);
        return new ExtendedAssignment(error, leader, leaderUrl, offset, connectorIds, taskIds,
                                      revokedConnectorIds, revokedTaskIds);
    }

    private static List<String> extractConnectors(Struct struct, String key) {
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

    public static class ExtendedAssignment extends Assignment {
        private final List<String> revokedConnectorIds;
        private final List<ConnectorTaskId> revokedTaskIds;

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

        public List<String> revokedConnectors() {
            return revokedConnectorIds;
        }

        public List<ConnectorTaskId> revokedTasks() {
            return revokedTaskIds;
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
            // Using LinkedHashMap preserves the ordering, which is helpful for tests and debugging
            Map<String, List<Integer>> taskMap = new LinkedHashMap<>();
            revokedConnectorIds.stream().distinct().forEachOrdered(connectorId -> {
                List<Integer> connectorTasks = taskMap.get(connectorId);
                taskMap.computeIfAbsent(connectorId, v -> new ArrayList<>());
                taskMap.put(connectorId, connectorTasks);
                connectorTasks.add(CONNECTOR_TASK);
            });

            revokedTaskIds.stream().forEachOrdered(taskId -> {
                String connectorId = taskId.connector();
                List<Integer> connectorTasks = taskMap.get(connectorId);
                taskMap.computeIfAbsent(connectorId, v -> new ArrayList<>());
                taskMap.put(connectorId, connectorTasks);
                connectorTasks.add(taskId.task());
            });
            return taskMap;
        }
    }

    private static void checkVersionCompatibility(short version) {
        // check for invalid versions
        if (version < CONNECT_PROTOCOL_V1)
            throw new SchemaException("Unsupported subscription version: " + version);

        // otherwise, assume versions can be parsed as V0
    }

}
