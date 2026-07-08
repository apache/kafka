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
package org.apache.kafka.coordinator.group.api.assignor.streams;

import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Map;
import java.util.Set;

/**
 * Interface representing the current assignment state for a streams group member.
 *
 * <p>This is the assignment the member currently owns and is used by the {@link TaskAssignor}
 * to compute a new, sticky target assignment. Note that the assignor does not assign warm-up
 * tasks itself (they are decided during reconciliation), but the currently-assigned warm-up
 * tasks are exposed here so that the assignor can take them into account.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface MemberAssignmentState {

    /**
     * @return The current active tasks keyed by subtopology Id.
     */
    Map<String, Set<Integer>> activeTasks();

    /**
     * @return The current standby tasks keyed by subtopology Id.
     */
    Map<String, Set<Integer>> standbyTasks();

    /**
     * @return The current warm-up tasks keyed by subtopology Id.
     */
    Map<String, Set<Integer>> warmupTasks();

    /**
     * @return The last received cumulative task offsets of assigned tasks or dormant tasks.
     */
    Map<TaskId, Long> taskOffsets();

    /**
     * @return The last received cumulative task end offsets of assigned tasks or dormant tasks.
     */
    Map<TaskId, Long> taskEndOffsets();

}
