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
package org.apache.kafka.coordinator.group.api.streams.assignor;

import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Map;
import java.util.Set;

/**
 * The active, standby, and warm-up tasks that a streams group member currently has,
 * used by the {@link TaskAssignor} to compute a new target assignment.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface MemberAssignmentState {

    /**
     * @return The member's current target active tasks keyed by subtopology Id.
     */
    Map<String, Set<Integer>> activeTasks();

    /**
     * @return The standby tasks currently assigned to the member, keyed by subtopology Id.
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
