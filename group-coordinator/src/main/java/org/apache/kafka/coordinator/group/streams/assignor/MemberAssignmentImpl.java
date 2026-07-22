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

import org.apache.kafka.coordinator.group.api.streams.assignor.MemberAssignment;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The task assignment for a streams group member.
 *
 * @param activeTasks  The active tasks assigned to this member keyed by subtopology Id.
 * @param standbyTasks The standby tasks assigned to this member keyed by subtopology Id.
 */
public record MemberAssignmentImpl(
    Map<String, Set<Integer>> activeTasks,
    Map<String, Set<Integer>> standbyTasks
) implements MemberAssignment {

    public MemberAssignmentImpl {
        Objects.requireNonNull(activeTasks);
        Objects.requireNonNull(standbyTasks);
    }

    public static MemberAssignmentImpl empty() {
        return new MemberAssignmentImpl(new HashMap<>(), new HashMap<>());
    }

}
