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

/**
 * The assignment specification for a consumer group.
 */
public class AssignmentSpec {
    /**
     * The members keyed by member id.
     */
    final Map<String, AssignmentMemberSpec> members;

    /**
     * The topics' metadata keyed by topic id
     */
    final Map<Uuid, AssignmentTopicMetadata> topics;

    public AssignmentSpec(
        Map<String, AssignmentMemberSpec> members,
        Map<Uuid, AssignmentTopicMetadata> topics
    ) {
        Objects.requireNonNull(members);
        Objects.requireNonNull(topics);
        this.members = members;
        this.topics = topics;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AssignmentSpec that = (AssignmentSpec) o;

        if (!members.equals(that.members)) return false;
        return topics.equals(that.topics);
    }

    @Override
    public int hashCode() {
        int result = members.hashCode();
        result = 31 * result + topics.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "AssignmentSpec(members=" + members +
            ", topics=" + topics +
            ')';
    }
}
