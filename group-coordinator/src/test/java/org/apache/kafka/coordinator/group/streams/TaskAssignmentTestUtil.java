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

import org.apache.kafka.coordinator.group.taskassignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.taskassignor.GroupAssignment;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TaskAssignmentTestUtil {

    public static Map.Entry<String, Set<Integer>> mkTaskAssignment(
        String subtopologyId,
        Integer... tasks
    ) {
        return new AbstractMap.SimpleEntry<>(
            subtopologyId,
            new HashSet<>(Arrays.asList(tasks))
        );
    }

    public static Map.Entry<String, Set<Integer>> mkSortedTaskAssignment(
        String subtopologyId,
        Integer... tasks
    ) {
        return new AbstractMap.SimpleEntry<>(
            subtopologyId,
            new TreeSet<>(Arrays.asList(tasks))
        );
    }

    @SafeVarargs
    public static Map<String, Set<Integer>> mkAssignment(Map.Entry<String, Set<Integer>>... entries) {
        Map<String, Set<Integer>> assignment = new HashMap<>();
        for (Map.Entry<String, Set<Integer>> entry : entries) {
            assignment.put(entry.getKey(), entry.getValue());
        }
        return assignment;
    }

    @SafeVarargs
    public static Map<String, Set<Integer>> mkSortedAssignment(Map.Entry<String, Set<Integer>>... entries) {
        Map<String, Set<Integer>> assignment = new LinkedHashMap<>();
        for (Map.Entry<String, Set<Integer>> entry : entries) {
            assignment.put(entry.getKey(), entry.getValue());
        }
        return assignment;
    }

    @SafeVarargs
    public static Map<String, Set<Integer>> mkStreamsAssignment(Map.Entry<String, Set<Integer>>... entries) {
        Map<String, Set<Integer>> assignment = new HashMap<>();
        for (Map.Entry<String, Set<Integer>> entry : entries) {
            assignment.put(entry.getKey(), entry.getValue());
        }
        return assignment;
    }

    /**
     * Verifies that the expected assignment is equal to the computed assignment for every member in the group.
     */
    public static void assertAssignment(
        Map<String, Map<String, Set<Integer>>> expectedAssignment,
        GroupAssignment computedGroupAssignment
    ) {
        assertEquals(expectedAssignment.size(), computedGroupAssignment.members().size());
        computedGroupAssignment.members().forEach((memberId, memberAssignment) -> {
            Map<String, Set<Integer>> computedAssignmentForMember = memberAssignment.activeTasks();
            assertEquals(expectedAssignment.get(memberId), computedAssignmentForMember);
        });
    }

    /**
     * Generate a reverse look up map of partition to member target assignments from the given member spec.
     *
     * @param memberSpec A map where the key is the member Id and the value is an AssignmentMemberSpec object containing the member's
     *                   partition assignments.
     * @return Map of topic partition to member assignments.
     */
    public static Map<String, Map<Integer, String>> invertedTargetAssignment(
        Map<String, AssignmentMemberSpec> memberSpec
    ) {
        Map<String, Map<Integer, String>> invertedTargetAssignment = new HashMap<>();
        for (Map.Entry<String, AssignmentMemberSpec> memberEntry : memberSpec.entrySet()) {
            String memberId = memberEntry.getKey();
            Map<String, Set<Integer>> topicsAndTasks = memberEntry.getValue().activeTasks();

            for (Map.Entry<String, Set<Integer>> topicEntry : topicsAndTasks.entrySet()) {
                String subtopologyId = topicEntry.getKey();
                Set<Integer> tasks = topicEntry.getValue();

                Map<Integer, String> partitionMap = invertedTargetAssignment.computeIfAbsent(subtopologyId, k -> new HashMap<>());

                for (Integer partitionId : tasks) {
                    partitionMap.put(partitionId, memberId);
                }
            }
        }
        return invertedTargetAssignment;
    }
}
