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

import java.util.Map;
import java.util.Objects;

/**
 * The partition assignment for a consumer group.
 */
public class GroupAssignment {
    /**
     * The member assignments keyed by member id.
     */
    private final Map<String, MemberAssignment> members;

    public GroupAssignment(
        Map<String, MemberAssignment> members
    ) {
        Objects.requireNonNull(members);
        this.members = members;
    }

    /**
     * @return Member assignments keyed by member Ids.
     */
    public Map<String, MemberAssignment> members() {
        return members;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GroupAssignment that = (GroupAssignment) o;
        return members.equals(that.members);
    }

    @Override
    public int hashCode() {
        return members.hashCode();
    }

    @Override
    public String toString() {
        return "GroupAssignment(members=" + members + ')';
    }
}
