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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The task assignment for a Streams group member.
 *
 * @param activeTasks The target tasks assigned to this member keyed by subtopologyId.
 */
public record MemberAssignment(Map<String, Set<Integer>> activeTasks,
                               Map<String, Set<Integer>> standbyTasks,
                               Map<String, Set<Integer>> warmupTasks) {

    public MemberAssignment {
        Objects.requireNonNull(activeTasks);
        Objects.requireNonNull(standbyTasks);
        Objects.requireNonNull(warmupTasks);
    }

    public static MemberAssignment empty() {
        return new MemberAssignment(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    }
}
