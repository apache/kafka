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
 * Interface representing the current assignment state for a streams group member, used by the
 * {@link TaskAssignor} to compute a new, sticky target assignment.
 *
 * <p>The active and standby tasks are the member's current <em>target</em> assignment (the last
 * assignment committed for the member). Using the committed target as the stickiness baseline keeps
 * in-flight task moves converging rather than reverting them. Warm-up tasks are not assigned by the
 * assignor (they are decided during reconciliation); the tasks the member is <em>currently</em>
 * warming up are exposed here so that the assignor can take them into account. As a result, a task
 * that is being moved to this member can appear both as an active or standby task (from the target)
 * and as a warm-up task (currently in progress).
 *
 * <p>All accessors are keyed by subtopology ID. The task-set accessors ({@link #activeTasks()},
 * {@link #standbyTasks()}, {@link #warmupTasks()}) map each subtopology ID to its set of
 * partitions, while {@link #taskOffsets()} and {@link #taskEndOffsets()} map each subtopology ID
 * to a map from partition to offset.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface MemberAssignmentState {

    /**
     * @return The member's current target active tasks keyed by subtopology Id.
     */
    Map<String, Set<Integer>> activeTasks();

    /**
     * @return The member's current target standby tasks keyed by subtopology Id.
     */
    Map<String, Set<Integer>> standbyTasks();

    /**
     * @return The tasks the member is currently warming up, keyed by subtopology Id.
     */
    Map<String, Set<Integer>> warmupTasks();

    /**
     * @return The last received cumulative task offsets of assigned tasks or dormant tasks.
     *         The outer map is keyed by subtopology ID and the inner map by partition.
     */
    Map<String, Map<Integer, Long>> taskOffsets();

    /**
     * @return The last received cumulative task end offsets of assigned tasks or dormant tasks.
     *         The outer map is keyed by subtopology ID and the inner map by partition.
     */
    Map<String, Map<Integer, Long>> taskEndOffsets();

}
