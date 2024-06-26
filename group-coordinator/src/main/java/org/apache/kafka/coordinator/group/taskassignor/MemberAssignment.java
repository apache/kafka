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
package org.apache.kafka.coordinator.group.taskassignor;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The task assignment for a Streams group member.
 */
public class MemberAssignment {

    public static MemberAssignment empty() {
        return new MemberAssignment(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    }

    /**
     * The target tasks assigned to this member keyed by subtopologyId.
     */
    private final Map<String, Set<Integer>> activeTasks;
    private final Map<String, Set<Integer>> standbyTasks;
    private final Map<String, Set<Integer>> warmupTasks;

    public MemberAssignment(final Map<String, Set<Integer>> activeTasks,
                            final Map<String, Set<Integer>> standbyTasks,
                            final Map<String, Set<Integer>> warmupTasks) {
        this.activeTasks = activeTasks;
        this.standbyTasks = standbyTasks;
        this.warmupTasks = warmupTasks;
    }

    public Map<String, Set<Integer>> activeTasks() {
        return activeTasks;
    }

    public Map<String, Set<Integer>> standbyTasks() {
        return standbyTasks;
    }

    public Map<String, Set<Integer>> warmupTasks() {
        return warmupTasks;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final MemberAssignment that = (MemberAssignment) o;
        return Objects.equals(activeTasks, that.activeTasks)
            && Objects.equals(standbyTasks, that.standbyTasks)
            && Objects.equals(warmupTasks, that.warmupTasks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            activeTasks,
            standbyTasks,
            warmupTasks
        );
    }

    @Override
    public String toString() {
        return "MemberAssignment{" +
            "activeTasks=" + activeTasks +
            ", standbyTasks=" + standbyTasks +
            ", warmupTasks=" + warmupTasks +
            '}';
    }
}
