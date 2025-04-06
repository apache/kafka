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
package org.apache.kafka.coordinator.group.streams.assignor;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Mock implementation of {@link TaskAssignor} that assigns tasks to members in a round-robin fashion, with a bit of stickiness.
 */
public class MockAssignor implements TaskAssignor {

    public static final String MOCK_ASSIGNOR_NAME = "mock";

    @Override
    public String name() {
        return MOCK_ASSIGNOR_NAME;
    }

    @Override
    public GroupAssignment assign(
        final GroupSpec groupSpec,
        final TopologyDescriber topologyDescriber
    ) throws TaskAssignorException {

        Map<String, MemberAssignment> newTargetAssignment = new HashMap<>();
        Map<String, String[]> subtopologyToActiveMember = new HashMap<>();

        for (String subtopology : topologyDescriber.subtopologies()) {
            int numberOfPartitions = topologyDescriber.maxNumInputPartitions(subtopology);
            subtopologyToActiveMember.put(subtopology, new String[numberOfPartitions]);
        }

        // Copy existing assignment and fill temporary data structures
        for (Map.Entry<String, AssignmentMemberSpec> memberEntry : groupSpec.members().entrySet()) {
            final String memberId = memberEntry.getKey();
            final AssignmentMemberSpec memberSpec = memberEntry.getValue();

            Map<String, Set<Integer>> activeTasks = new HashMap<>(memberSpec.activeTasks());

            newTargetAssignment.put(memberId, new MemberAssignment(activeTasks, new HashMap<>(), new HashMap<>()));
            for (Map.Entry<String, Set<Integer>> entry : activeTasks.entrySet()) {
                final String subtopologyId = entry.getKey();
                final Set<Integer> taskIds = entry.getValue();
                final String[] activeMembers = subtopologyToActiveMember.get(subtopologyId);
                for (int taskId : taskIds) {
                    if (activeMembers[taskId] != null) {
                        throw new TaskAssignorException(
                            "Task " + taskId + " of subtopology " + subtopologyId + " is assigned to multiple members");
                    }
                    activeMembers[taskId] = memberId;
                }
            }
        }

        // Define priority queue to sort members by task count
        PriorityQueue<MemberAndTaskCount> memberAndTaskCount = new PriorityQueue<>(Comparator.comparingInt(m -> m.taskCount));
        memberAndTaskCount.addAll(
            newTargetAssignment.keySet().stream()
                .map(memberId -> new MemberAndTaskCount(memberId,
                    newTargetAssignment.get(memberId).activeTasks().values().stream().mapToInt(Set::size).sum()))
                .collect(Collectors.toSet())
        );

        // Assign unassigned tasks to members with the fewest tasks
        for (Map.Entry<String, String[]> entry : subtopologyToActiveMember.entrySet()) {
            final String subtopologyId = entry.getKey();
            final String[] activeMembers = entry.getValue();
            for (int i = 0; i < activeMembers.length; i++) {
                if (activeMembers[i] == null) {
                    final MemberAndTaskCount m = memberAndTaskCount.poll();
                    if (m == null) {
                        throw new TaskAssignorException("No member available to assign task " + i + " of subtopology " + subtopologyId);
                    }
                    newTargetAssignment.get(m.memberId).activeTasks().computeIfAbsent(subtopologyId, k -> new HashSet<>()).add(i);
                    activeMembers[i] = m.memberId;
                    memberAndTaskCount.add(new MemberAndTaskCount(m.memberId, m.taskCount + 1));
                }
            }
        }

        return new GroupAssignment(newTargetAssignment);
    }

    private record MemberAndTaskCount(String memberId, int taskCount) {
    }
}
