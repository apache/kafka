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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol;
import static org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility.EAGER;

/**
 * This class implements the protocol for Kafka Connect workers in a group. It includes the format of worker state used when
 * joining the group and distributing assignments, and the format of assignments of connectors and tasks to workers.
 */
public class ConnectProtocol {
    public static final String VERSION_KEY_NAME = "version";
    public static final String URL_KEY_NAME = "url";
    public static final String CONFIG_OFFSET_KEY_NAME = "config-offset";
    public static final String CONNECTOR_KEY_NAME = "connector";
    public static final String LEADER_KEY_NAME = "leader";
    public static final String LEADER_URL_KEY_NAME = "leader-url";
    public static final String ERROR_KEY_NAME = "error";
    public static final String TASKS_KEY_NAME = "tasks";
    public static final String ASSIGNMENT_KEY_NAME = "assignment";
    public static final int CONNECTOR_TASK = -1;

    public static final short CONNECT_PROTOCOL_V0 = 0;
    public static final Schema CONNECT_PROTOCOL_HEADER_SCHEMA = new Schema(
            new Field(VERSION_KEY_NAME, Type.INT16));

    /**
     * Connect Protocol Header V0:
     * <pre>
     *   Version            => Int16
     * </pre>
     */
    private static final Struct CONNECT_PROTOCOL_HEADER_V0 = new Struct(CONNECT_PROTOCOL_HEADER_SCHEMA)
            .set(VERSION_KEY_NAME, CONNECT_PROTOCOL_V0);

    /**
     * Config State V0:
     * <pre>
     *   Url                => [String]
     *   ConfigOffset       => Int64
     * </pre>
     */
    public static final Schema CONFIG_STATE_V0 = new Schema(
            new Field(URL_KEY_NAME, Type.STRING),
            new Field(CONFIG_OFFSET_KEY_NAME, Type.INT64));

    /**
     * Connector Assignment V0:
     * <pre>
     *   Connector          => [String]
     *   Tasks              => [Int32]
     * </pre>
     *
     * <p>Assignments for each worker are a set of connectors and tasks. These are categorized by
     * connector ID. A sentinel task ID (CONNECTOR_TASK) is used to indicate the connector itself
     * (i.e. that the assignment includes responsibility for running the Connector instance in
     * addition to any tasks it generates).</p>
     */
    public static final Schema CONNECTOR_ASSIGNMENT_V0 = new Schema(
            new Field(CONNECTOR_KEY_NAME, Type.STRING),
            new Field(TASKS_KEY_NAME, new ArrayOf(Type.INT32)));

    /**
     * Assignment V0:
     * <pre>
     *   Error              => Int16
     *   Leader             => [String]
     *   LeaderUrl          => [String]
     *   ConfigOffset       => Int64
     *   Assignment         => [Connector Assignment]
     * </pre>
     */
    public static final Schema ASSIGNMENT_V0 = new Schema(
            new Field(ERROR_KEY_NAME, Type.INT16),
            new Field(LEADER_KEY_NAME, Type.STRING),
            new Field(LEADER_URL_KEY_NAME, Type.STRING),
            new Field(CONFIG_OFFSET_KEY_NAME, Type.INT64),
            new Field(ASSIGNMENT_KEY_NAME, new ArrayOf(CONNECTOR_ASSIGNMENT_V0)));

    /**
     * The fields are serialized in sequence as follows:
     * Subscription V0:
     * <pre>
     *   Version            => Int16
     *   Url                => [String]
     *   ConfigOffset       => Int64
     * </pre>
     *
     * @param workerState the current state of the worker metadata
     * @return the serialized state of the worker metadata
     */
    public static ByteBuffer serializeMetadata(WorkerState workerState) {
        Struct struct = new Struct(CONFIG_STATE_V0);
        struct.set(URL_KEY_NAME, workerState.url());
        struct.set(CONFIG_OFFSET_KEY_NAME, workerState.offset());
        ByteBuffer buffer = ByteBuffer.allocate(CONNECT_PROTOCOL_HEADER_V0.sizeOf() + CONFIG_STATE_V0.sizeOf(struct));
        CONNECT_PROTOCOL_HEADER_V0.writeTo(buffer);
        CONFIG_STATE_V0.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    /**
     * Returns the collection of Connect protocols that are supported by this version along
     * with their serialized metadata. The protocols are ordered by preference.
     *
     * @param workerState the current state of the worker metadata
     * @return the collection of Connect protocol metadata
     */
    public static JoinGroupRequestProtocolCollection metadataRequest(WorkerState workerState) {
        return new JoinGroupRequestProtocolCollection(Collections.singleton(
                new JoinGroupRequestProtocol()
                        .setName(EAGER.protocol())
                        .setMetadata(ConnectProtocol.serializeMetadata(workerState).array()))
                .iterator());
    }

    /**
     * Given a byte buffer that contains protocol metadata return the deserialized form of the
     * metadata.
     *
     * @param buffer A buffer containing the protocols metadata
     * @return the deserialized metadata
     * @throws SchemaException on incompatible Connect protocol version
     */
    public static WorkerState deserializeMetadata(ByteBuffer buffer) {
        Struct header = CONNECT_PROTOCOL_HEADER_SCHEMA.read(buffer);
        Short version = header.getShort(VERSION_KEY_NAME);
        checkVersionCompatibility(version);
        Struct struct = CONFIG_STATE_V0.read(buffer);
        long configOffset = struct.getLong(CONFIG_OFFSET_KEY_NAME);
        String url = struct.getString(URL_KEY_NAME);
        return new WorkerState(url, configOffset);
    }

    /**
     * The fields are serialized in sequence as follows:
     * Complete Assignment V0:
     * <pre>
     *   Version            => Int16
     *   Error              => Int16
     *   Leader             => [String]
     *   LeaderUrl          => [String]
     *   ConfigOffset       => Int64
     *   Assignment         => [Connector Assignment]
     * </pre>
     */
    public static ByteBuffer serializeAssignment(Assignment assignment) {
        Struct struct = new Struct(ASSIGNMENT_V0);
        struct.set(ERROR_KEY_NAME, assignment.error());
        struct.set(LEADER_KEY_NAME, assignment.leader());
        struct.set(LEADER_URL_KEY_NAME, assignment.leaderUrl());
        struct.set(CONFIG_OFFSET_KEY_NAME, assignment.offset());
        List<Struct> taskAssignments = new ArrayList<>();
        for (Map.Entry<String, Collection<Integer>> connectorEntry : assignment.asMap().entrySet()) {
            Struct taskAssignment = new Struct(CONNECTOR_ASSIGNMENT_V0);
            taskAssignment.set(CONNECTOR_KEY_NAME, connectorEntry.getKey());
            Collection<Integer> tasks = connectorEntry.getValue();
            taskAssignment.set(TASKS_KEY_NAME, tasks.toArray());
            taskAssignments.add(taskAssignment);
        }
        struct.set(ASSIGNMENT_KEY_NAME, taskAssignments.toArray());

        ByteBuffer buffer = ByteBuffer.allocate(CONNECT_PROTOCOL_HEADER_V0.sizeOf() + ASSIGNMENT_V0.sizeOf(struct));
        CONNECT_PROTOCOL_HEADER_V0.writeTo(buffer);
        ASSIGNMENT_V0.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    /**
     * Given a byte buffer that contains an assignment as defined by this protocol, return the
     * deserialized form of the assignment.
     *
     * @param buffer the buffer containing a serialized assignment
     * @return the deserialized assignment
     * @throws SchemaException on incompatible Connect protocol version
     */
    public static Assignment deserializeAssignment(ByteBuffer buffer) {
        Struct header = CONNECT_PROTOCOL_HEADER_SCHEMA.read(buffer);
        Short version = header.getShort(VERSION_KEY_NAME);
        checkVersionCompatibility(version);
        Struct struct = ASSIGNMENT_V0.read(buffer);
        short error = struct.getShort(ERROR_KEY_NAME);
        String leader = struct.getString(LEADER_KEY_NAME);
        String leaderUrl = struct.getString(LEADER_URL_KEY_NAME);
        long offset = struct.getLong(CONFIG_OFFSET_KEY_NAME);
        List<String> connectorIds = new ArrayList<>();
        List<ConnectorTaskId> taskIds = new ArrayList<>();
        for (Object structObj : struct.getArray(ASSIGNMENT_KEY_NAME)) {
            Struct assignment = (Struct) structObj;
            String connector = assignment.getString(CONNECTOR_KEY_NAME);
            for (Object taskIdObj : assignment.getArray(TASKS_KEY_NAME)) {
                Integer taskId = (Integer) taskIdObj;
                if (taskId == CONNECTOR_TASK)
                    connectorIds.add(connector);
                else
                    taskIds.add(new ConnectorTaskId(connector, taskId));
            }
        }
        return new Assignment(error, leader, leaderUrl, offset, connectorIds, taskIds);
    }

    /**
     * A class that captures the deserialized form of a worker's metadata.
     */
    public static class WorkerState {
        private final String url;
        private final long offset;

        public WorkerState(String url, long offset) {
            this.url = url;
            this.offset = offset;
        }

        public String url() {
            return url;
        }

        /**
         * The most up-to-date (maximum) configuration offset according known to this worker.
         *
         * @return the configuration offset
         */
        public long offset() {
            return offset;
        }

        @Override
        public String toString() {
            return "WorkerState{" +
                    "url='" + url + '\'' +
                    ", offset=" + offset +
                    '}';
        }
    }

    /**
     * The basic assignment of connectors and tasks introduced with V0 version of the Connect protocol.
     */
    public static class Assignment {
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
         * @param error error code for this assignment; {@code ConnectProtocol.Assignment.NO_ERROR}
         *              indicates no error during assignment
         * @param leader Connect group's leader Id; may be null only on the empty assignment
         * @param leaderUrl Connect group's leader URL; may be null only on the empty assignment
         * @param configOffset the most up-to-date configuration offset according to this assignment
         * @param connectorIds list of connectors that the worker should instantiate and run; may not be null
         * @param taskIds list of task IDs that the worker should instantiate and run; may not be null
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

        protected Map<String, Collection<Integer>> asMap() {
            // Using LinkedHashMap preserves the ordering, which is helpful for tests and debugging
            Map<String, Collection<Integer>> taskMap = new LinkedHashMap<>();
            for (String connectorId : new HashSet<>(connectorIds)) {
                Collection<Integer> connectorTasks = taskMap.get(connectorId);
                if (connectorTasks == null) {
                    connectorTasks = new ArrayList<>();
                    taskMap.put(connectorId, connectorTasks);
                }
                connectorTasks.add(CONNECTOR_TASK);
            }
            for (ConnectorTaskId taskId : taskIds) {
                String connectorId = taskId.connector();
                Collection<Integer> connectorTasks = taskMap.get(connectorId);
                if (connectorTasks == null) {
                    connectorTasks = new ArrayList<>();
                    taskMap.put(connectorId, connectorTasks);
                }
                connectorTasks.add(taskId.task());
            }
            return taskMap;
        }
    }

    private static void checkVersionCompatibility(short version) {
        // check for invalid versions
        if (version < CONNECT_PROTOCOL_V0)
            throw new SchemaException("Unsupported subscription version: " + version);

        // otherwise, assume versions can be parsed as V0
    }
}
