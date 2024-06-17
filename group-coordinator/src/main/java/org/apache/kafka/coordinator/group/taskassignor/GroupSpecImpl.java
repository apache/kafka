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
package org.apache.kafka.coordinator.group.taskassignor;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The assignment specification for a Streams group.
 */
public class GroupSpecImpl implements GroupSpec {

    /**
     * The member metadata keyed by member Id.
     */
    private final Map<String, AssignmentMemberSpec> members;

    /**
     * The subtopologies.
     */
    private final List<String> subtopologies;

    public GroupSpecImpl(
        Map<String, AssignmentMemberSpec> members,
        List<String> subtopologies
    ) {
        Objects.requireNonNull(members);
        Objects.requireNonNull(subtopologies);
        this.members = members;
        this.subtopologies = subtopologies;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, AssignmentMemberSpec> members() {
        return members;
    }

    @Override
    public List<String> subtopologies() {
        return subtopologies;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final GroupSpecImpl groupSpec = (GroupSpecImpl) o;
        return Objects.equals(members, groupSpec.members)
            && Objects.equals(subtopologies, groupSpec.subtopologies);
    }

    @Override
    public int hashCode() {
        return Objects.hash(members, subtopologies);
    }

    @Override
    public String toString() {
        return "GroupSpecImpl{" +
            "members=" + members +
            ", partitionsPerSubtopology=" + subtopologies +
            '}';
    }
}
