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
package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.common.Uuid;

import java.util.Map;
import java.util.Objects;
import java.util.Set;


/**
 * The partition assignment for a consumer group member.
 */
public class MemberAssignment {

    private final Map<Uuid, Set<Integer>> assignmentPerTopic;


    public MemberAssignment(Map<Uuid, Set<Integer>> assignmentPerTopic) {
        Objects.requireNonNull(assignmentPerTopic);
        this.assignmentPerTopic = assignmentPerTopic;
    }

    public Map<Uuid, Set<Integer>> getAssignmentPerTopic() {
        return this.assignmentPerTopic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MemberAssignment that = (MemberAssignment) o;

        return assignmentPerTopic.equals(that.assignmentPerTopic);
    }

    @Override
    public int hashCode() {
        return assignmentPerTopic.hashCode();
    }

    @Override
    public String toString() {
        return "MemberAssignment(targetPartitions=" + assignmentPerTopic + ')';
    }
}
