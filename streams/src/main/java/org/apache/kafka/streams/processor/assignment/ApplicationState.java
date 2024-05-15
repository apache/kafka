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
import java.util.Set;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.errors.TaskAssignmentException;

/**
 * A read-only metadata class representing the state of the application and the current rebalance.
 * This class wraps all the input parameters to the task assignment, including the current state
 * of each KafkaStreams client with at least one StreamThread participating in this rebalance, the
 * assignment-related configs, and the tasks to be assigned.
 */
public interface ApplicationState {
    /**
     * @param computeTaskLags whether to include task lag information in the returned metadata. Note that passing
     * in "true" will result in a remote call to fetch changelog topic end offsets, and you should pass in "false" unless
     * you specifically need the task lag information.
     *
     * @return a map from the {@code processId} to {@link KafkaStreamsState} for all KafkaStreams clients in this app
     *
     * @throws TaskAssignmentException if a retriable error occurs while computing KafkaStreamsState metadata. Re-throw
     *                                 this exception to have Kafka Streams retry the rebalance by returning the same
     *                                 assignment and scheduling an immediate followup rebalance
     */
    Map<ProcessId, KafkaStreamsState> kafkaStreamsStates(boolean computeTaskLags);

    /**
     * @return a simple container class with the Streams configs relevant to assignment
     */
    AssignmentConfigs assignmentConfigs();

    /**
     * @return the set of all tasks in this topology which must be assigned
     */
    Set<TaskId> allTasks();

    /**
     *
     * @return the set of stateful and changelogged tasks in this topology
     */
    Set<TaskId> statefulTasks();

    /**
     *
     * @return the set of stateless or changelog-less tasks in this topology
     */
    Set<TaskId> statelessTasks();
}