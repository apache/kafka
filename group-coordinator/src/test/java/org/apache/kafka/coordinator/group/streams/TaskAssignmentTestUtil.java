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
package org.apache.kafka.coordinator.group.streams;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TaskAssignmentTestUtil {

    public static Assignment mkAssignment(final Map<String, Set<Integer>> activeTasks,
                                          final Map<String, Set<Integer>> standbyTasks,
                                          final Map<String, Set<Integer>> warmupTasks) {
        return new Assignment(
            Collections.unmodifiableMap(Objects.requireNonNull(activeTasks)),
            Collections.unmodifiableMap(Objects.requireNonNull(standbyTasks)),
            Collections.unmodifiableMap(Objects.requireNonNull(warmupTasks))
        );
    }

    public static Map.Entry<String, Set<Integer>> mkTasks(String subtopologyId,
                                                          Integer... tasks) {
        return new AbstractMap.SimpleEntry<>(
            subtopologyId,
            new HashSet<>(List.of(tasks))
        );
    }

    @SafeVarargs
    public static Map<String, Set<Integer>> mkTasksPerSubtopology(Map.Entry<String,
                                                                  Set<Integer>>... entries) {
        Map<String, Set<Integer>> assignment = new HashMap<>();
        for (Map.Entry<String, Set<Integer>> entry : entries) {
            assignment.put(entry.getKey(), entry.getValue());
        }
        return assignment;
    }
}
