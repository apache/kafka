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
package org.apache.kafka.coordinator.group.modern;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The partition assignment for a modern group member.
 *
 * @param partitions The partitions assigned to this member keyed by topicId.
 *                   The map will not be made immutable, since the server-side assignors rely on
 *                   being able to mutate the map while building new assignments.
 */
public record MemberAssignmentImpl(Map<Uuid, Set<Integer>> partitions) implements MemberAssignment {
    public MemberAssignmentImpl {
        Objects.requireNonNull(partitions);
    }

    /**
     * @return The assigned partitions keyed by topic Ids.
     */
    @Override
    public Map<Uuid, Set<Integer>> partitions() {
        return this.partitions;
    }

    @Override
    public String toString() {
        return "MemberAssignment(partitions=" + partitions + ')';
    }
}
