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

import org.apache.kafka.coordinator.group.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.streams.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignor;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignorException;
import org.apache.kafka.coordinator.group.streams.assignor.TopologyDescriber;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class MockTaskAssignor implements TaskAssignor {

    private final String name;
    private GroupAssignment preparedGroupAssignment = null;
    private Map<String, String> assignmentConfigs = Map.of();

    public MockTaskAssignor(String name) {
        this.name = name;
    }

    public void prepareGroupAssignment(GroupAssignment prepareGroupAssignment) {
        this.preparedGroupAssignment = prepareGroupAssignment;
    }

    public void prepareGroupAssignment(Map<String, TasksTuple> memberAssignments) {
        this.preparedGroupAssignment =
            new GroupAssignment(memberAssignments.entrySet().stream().collect(
                Collectors.toMap(
                    Entry::getKey,
                    entry -> {
                        TasksTuple tasksTuple = entry.getValue();
                        return new MemberAssignment(
                            tasksTuple.activeTasks(), tasksTuple.standbyTasks(), tasksTuple.warmupTasks());
                    })));
    }

    public Map<String, String> lastPassedAssignmentConfigs() {
        return assignmentConfigs;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return name();
    }

    @Override
    public GroupAssignment assign(final GroupSpec groupSpec, final TopologyDescriber topologyDescriber)
        throws TaskAssignorException {
        assignmentConfigs = groupSpec.assignmentConfigs();
        return preparedGroupAssignment;
    }
}
