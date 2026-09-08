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
import java.util.Objects;
import java.util.Set;

/**
 * The task assignment for a streams group member.
 *
 * <p>Only active and standby tasks are assigned by the {@link TaskAssignor}. Warm-up tasks are
 * not assigned by the assignor; they are decided during reconciliation.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MemberAssignment {

    private final Map<String, Set<Integer>> activeTasks;
    private final Map<String, Set<Integer>> standbyTasks;

    public MemberAssignment(
        Map<String, Set<Integer>> activeTasks,
        Map<String, Set<Integer>> standbyTasks
    ) {
        this.activeTasks = Objects.requireNonNull(activeTasks);
        this.standbyTasks = Objects.requireNonNull(standbyTasks);
    }

    /**
     * @return The active tasks assigned to this member keyed by subtopology Id.
     */
    public Map<String, Set<Integer>> activeTasks() {
        return activeTasks;
    }

    /**
     * @return The standby tasks assigned to this member keyed by subtopology Id.
     */
    public Map<String, Set<Integer>> standbyTasks() {
        return standbyTasks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemberAssignment that = (MemberAssignment) o;
        return activeTasks.equals(that.activeTasks) && standbyTasks.equals(that.standbyTasks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(activeTasks, standbyTasks);
    }

    @Override
    public String toString() {
        return "MemberAssignment(activeTasks=" + activeTasks + ", standbyTasks=" + standbyTasks + ')';
    }
}
