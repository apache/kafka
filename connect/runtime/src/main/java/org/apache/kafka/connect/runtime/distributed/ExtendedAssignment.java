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

import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.connect.util.ConnectorTaskId;

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

import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.ASSIGNMENT_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONFIG_OFFSET_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONNECTOR_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONNECTOR_TASK;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.ERROR_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.LEADER_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.LEADER_URL_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.TASKS_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.ASSIGNMENT_V1;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECTOR_ASSIGNMENT_V1;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECT_PROTOCOL_V1;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.REVOKED_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.SCHEDULED_DELAY_KEY_NAME;

/**
 * The extended assignment of connectors and tasks that includes revoked connectors and tasks
 * as well as a scheduled rebalancing delay.
 */
public class ExtendedAssignment extends ConnectProtocol.Assignment {
    private final short version;
    private final Collection<String> revokedConnectorIds;
    private final Collection<ConnectorTaskId> revokedTaskIds;
    private final int delay;

    private static final ExtendedAssignment EMPTY = new ExtendedAssignment(
            CONNECT_PROTOCOL_V1, ConnectProtocol.Assignment.NO_ERROR, null, null, -1,
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

    private Map<String, Collection<Integer>> revokedAsMap() {
        if (revokedConnectorIds == null && revokedTaskIds == null) {
            return null;
        }
        // Using LinkedHashMap preserves the ordering, which is helpful for tests and debugging
        Map<String, Collection<Integer>> taskMap = new LinkedHashMap<>();
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

    /**
     * Return the {@code Struct} that corresponds to this assignment.
     *
     * @return the assignment struct
     */
    public Struct toStruct() {
        Collection<Struct> assigned = taskAssignments(asMap());
        Collection<Struct> revoked = taskAssignments(revokedAsMap());
        return new Struct(ASSIGNMENT_V1)
                .set(ERROR_KEY_NAME, error())
                .set(LEADER_KEY_NAME, leader())
                .set(LEADER_URL_KEY_NAME, leaderUrl())
                .set(CONFIG_OFFSET_KEY_NAME, offset())
                .set(ASSIGNMENT_KEY_NAME, assigned != null ? assigned.toArray() : null)
                .set(REVOKED_KEY_NAME, revoked != null ? revoked.toArray() : null)
                .set(SCHEDULED_DELAY_KEY_NAME, delay);
    }

    /**
     * Given a {@code Struct} that encodes an assignment return the assignment object.
     *
     * @param struct a struct representing an assignment
     * @return the assignment
     */
    public static ExtendedAssignment fromStruct(short version, Struct struct) {
        return struct == null
               ? null
               : new ExtendedAssignment(
                       version,
                       struct.getShort(ERROR_KEY_NAME),
                       struct.getString(LEADER_KEY_NAME),
                       struct.getString(LEADER_URL_KEY_NAME),
                       struct.getLong(CONFIG_OFFSET_KEY_NAME),
                       extractConnectors(struct, ASSIGNMENT_KEY_NAME),
                       extractTasks(struct, ASSIGNMENT_KEY_NAME),
                       extractConnectors(struct, REVOKED_KEY_NAME),
                       extractTasks(struct, REVOKED_KEY_NAME),
                       struct.getInt(SCHEDULED_DELAY_KEY_NAME));
    }

    private static Collection<Struct> taskAssignments(Map<String, Collection<Integer>> assignments) {
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

    private static Collection<String> extractConnectors(Struct struct, String key) {
        assert REVOKED_KEY_NAME.equals(key) || ASSIGNMENT_KEY_NAME.equals(key);

        Object[] connectors = struct.getArray(key);
        if (connectors == null) {
            return Collections.emptyList();
        }
        List<String> connectorIds = new ArrayList<>();
        for (Object structObj : connectors) {
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

    private static Collection<ConnectorTaskId> extractTasks(Struct struct, String key) {
        assert REVOKED_KEY_NAME.equals(key) || ASSIGNMENT_KEY_NAME.equals(key);

        Object[] tasks = struct.getArray(key);
        if (tasks == null) {
            return Collections.emptyList();
        }
        List<ConnectorTaskId> tasksIds = new ArrayList<>();
        for (Object structObj : tasks) {
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

}
