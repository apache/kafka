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
package org.apache.kafka.streams.processor.assignment;

import java.util.Map;
import java.util.SortedSet;
import org.apache.kafka.streams.processor.TaskId;

/**
 * A set of utilities to help implement task assignment via the {@link TaskAssignor}
 */
public final class TaskAssignmentUtils {
    /**
     * Assign standby tasks to KafkaStreams clients according to the default logic.
     * <p>
     * If rack-aware client tags are configured, the rack-aware standby task assignor will be used
     *
     * @param applicationState        the metadata and other info describing the current application state
     * @param kafkaStreamsAssignments the current assignment of tasks to KafkaStreams clients
     *
     * @return a new map containing the mappings from KafkaStreamsAssignments updated with the default
     *         standby assignment
     */
    public static Map<ProcessId, KafkaStreamsAssignment> defaultStandbyTaskAssignment(
        final ApplicationState applicationState,
        final Map<ProcessId, KafkaStreamsAssignment> kafkaStreamsAssignments
    ) {
        throw new UnsupportedOperationException("Not Implemented.");
    }

    /**
     * Optimize the active task assignment for rack-awareness
     *
     * @param applicationState        the metadata and other info describing the current application state
     * @param kafkaStreamsAssignments the current assignment of tasks to KafkaStreams clients
     * @param tasks                   the set of tasks to reassign if possible. Must already be assigned
     *                                to a KafkaStreams client
     *
     * @return a new map containing the mappings from KafkaStreamsAssignments updated with the default
     *         rack-aware assignment for active tasks
     */
    public static Map<ProcessId, KafkaStreamsAssignment> optimizeRackAwareActiveTasks(
        final ApplicationState applicationState,
        final Map<ProcessId, KafkaStreamsAssignment> kafkaStreamsAssignments,
        final SortedSet<TaskId> tasks
    ) {
        throw new UnsupportedOperationException("Not Implemented.");
    }

    /**
     * Optimize the standby task assignment for rack-awareness
     *
     * @param kafkaStreamsAssignments the current assignment of tasks to KafkaStreams clients
     * @param applicationState        the metadata and other info describing the current application state
     *
     * @return a new map containing the mappings from KafkaStreamsAssignments updated with the default
     *         rack-aware assignment for standby tasks
     */
    public static Map<ProcessId, KafkaStreamsAssignment> optimizeRackAwareStandbyTasks(
        final ApplicationState applicationState,
        final Map<ProcessId, KafkaStreamsAssignment> kafkaStreamsAssignments
    ) {
        throw new UnsupportedOperationException("Not Implemented.");
    }

    /**
     * Return a "no-op" assignment that just copies the previous assignment of tasks to KafkaStreams clients
     *
     * @param applicationState the metadata and other info describing the current application state
     *
     * @return a new map containing an assignment that replicates exactly the previous assignment reported
     *         in the applicationState
     */
    public static Map<ProcessId, KafkaStreamsAssignment> identityAssignment(
        final ApplicationState applicationState
    ) {
        throw new UnsupportedOperationException("Not Implemented.");
    }
}