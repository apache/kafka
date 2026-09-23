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

import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;

import java.util.Map;
import java.util.SortedMap;

/**
 * An {@link AssignmentRefiner} that hands out a prepared intermediate assignment and records what it was called with,
 * so that a test can drive the group towards an assignment with warm-up tasks and check that the group context reaches
 * the refiner. Returns the target assignment unchanged until an assignment is prepared.
 */
public class MockAssignmentRefiner implements AssignmentRefiner {

    private Map<String, TasksTuple> preparedRefinedAssignment = null;
    private int numRefinements = 0;
    private Map<String, StreamsGroupMember> lastPassedMembers = Map.of();
    private Map<String, TasksTuple> lastPassedTargetAssignment = Map.of();
    private Map<String, MemberTaskOffsets> lastPassedTaskOffsets = Map.of();
    private SortedMap<String, ConfiguredSubtopology> lastPassedSubtopologies = null;
    private int lastPassedNumWarmupReplicas = -1;
    private long lastPassedAcceptableRecoveryLag = -1L;

    public void prepareRefinedAssignment(Map<String, TasksTuple> refinedAssignment) {
        this.preparedRefinedAssignment = refinedAssignment;
    }

    public int numRefinements() {
        return numRefinements;
    }

    public Map<String, StreamsGroupMember> lastPassedMembers() {
        return lastPassedMembers;
    }

    public Map<String, TasksTuple> lastPassedTargetAssignment() {
        return lastPassedTargetAssignment;
    }

    public Map<String, MemberTaskOffsets> lastPassedTaskOffsets() {
        return lastPassedTaskOffsets;
    }

    public SortedMap<String, ConfiguredSubtopology> lastPassedSubtopologies() {
        return lastPassedSubtopologies;
    }

    public int lastPassedNumWarmupReplicas() {
        return lastPassedNumWarmupReplicas;
    }

    public long lastPassedAcceptableRecoveryLag() {
        return lastPassedAcceptableRecoveryLag;
    }

    @Override
    public Map<String, TasksTuple> refine(
        Map<String, StreamsGroupMember> members,
        Map<String, TasksTuple> targetAssignment,
        Map<String, MemberTaskOffsets> taskOffsets,
        SortedMap<String, ConfiguredSubtopology> subtopologies,
        int numWarmupReplicas,
        long acceptableRecoveryLag
    ) {
        numRefinements++;
        lastPassedMembers = members;
        lastPassedTargetAssignment = targetAssignment;
        lastPassedTaskOffsets = taskOffsets;
        lastPassedSubtopologies = subtopologies;
        lastPassedNumWarmupReplicas = numWarmupReplicas;
        lastPassedAcceptableRecoveryLag = acceptableRecoveryLag;
        return preparedRefinedAssignment == null ? targetAssignment : preparedRefinedAssignment;
    }
}
