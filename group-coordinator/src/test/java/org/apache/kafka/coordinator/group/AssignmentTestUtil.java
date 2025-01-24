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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AssignmentTestUtil {
    public static Map.Entry<Uuid, Set<Integer>> mkTopicAssignment(
        Uuid topicId,
        Integer... partitions
    ) {
        return new AbstractMap.SimpleEntry<>(
            topicId,
            new HashSet<>(Arrays.asList(partitions))
        );
    }

    public static Map.Entry<Uuid, Set<Integer>> mkOrderedTopicAssignment(
        Uuid topicId,
        Integer... partitions
    ) {
        return new AbstractMap.SimpleEntry<>(
            topicId,
            new LinkedHashSet<>(Arrays.asList(partitions))
        );
    }

    @SafeVarargs
    public static Map<Uuid, Set<Integer>> mkAssignment(Map.Entry<Uuid, Set<Integer>>... entries) {
        Map<Uuid, Set<Integer>> assignment = new HashMap<>();
        for (Map.Entry<Uuid, Set<Integer>> entry : entries) {
            assignment.put(entry.getKey(), Collections.unmodifiableSet(entry.getValue()));
        }
        return Collections.unmodifiableMap(assignment);
    }

    @SafeVarargs
    public static Map<Uuid, Set<Integer>> mkOrderedAssignment(Map.Entry<Uuid, Set<Integer>>... entries) {
        Map<Uuid, Set<Integer>> assignment = new LinkedHashMap<>();
        for (Map.Entry<Uuid, Set<Integer>> entry : entries) {
            assignment.put(entry.getKey(), Collections.unmodifiableSet(entry.getValue()));
        }
        return Collections.unmodifiableMap(assignment);
    }

    /**
     * Verifies that the expected assignment is equal to the computed assignment for every member in the group.
     */
    public static void assertAssignment(
        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment,
        GroupAssignment computedGroupAssignment
    ) {
        assertEquals(expectedAssignment.size(), computedGroupAssignment.members().size());
        computedGroupAssignment.members().forEach((memberId, memberAssignment) -> {
            Map<Uuid, Set<Integer>> computedAssignmentForMember = memberAssignment.partitions();
            assertEquals(expectedAssignment.get(memberId), computedAssignmentForMember);
        });
    }

    /**
     * Generate a reverse look up map of partition to member target assignments from the given metadata.
     *
     * @param members       The member subscription specs.
     * @return Map of topic partition to member assignments.
     */
    public static Map<Uuid, Map<Integer, String>> invertedTargetAssignment(
        Map<String, MemberSubscriptionAndAssignmentImpl> members
    ) {
        Map<Uuid, Map<Integer, String>> invertedTargetAssignment = new HashMap<>();
        for (Map.Entry<String, MemberSubscriptionAndAssignmentImpl> memberEntry : members.entrySet()) {
            String memberId = memberEntry.getKey();
            Map<Uuid, Set<Integer>> memberAssignment = memberEntry.getValue().partitions();

            for (Map.Entry<Uuid, Set<Integer>> topicEntry : memberAssignment.entrySet()) {
                Uuid topicId = topicEntry.getKey();
                Set<Integer> partitions = topicEntry.getValue();

                Map<Integer, String> partitionMap = invertedTargetAssignment.computeIfAbsent(topicId, k -> new HashMap<>());

                for (Integer partitionId : partitions) {
                    partitionMap.put(partitionId, memberId);
                }
            }
        }
        return invertedTargetAssignment;
    }
}
