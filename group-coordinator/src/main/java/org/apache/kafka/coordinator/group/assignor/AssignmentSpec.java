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
 * The assignment specification for a consumer group.
 */
public class AssignmentSpec {
    /**
     * The member metadata keyed by member Id.
     */
    private final Map<String, AssignmentMemberSpec> members;

    /**
     * The subscription type followed by the group.
     */
    private final SubscriptionType subscriptionType;

    public AssignmentSpec(
        Map<String, AssignmentMemberSpec> members,
        SubscriptionType subscriptionType
    ) {
        Objects.requireNonNull(members);
        this.members = members;
        this.subscriptionType = subscriptionType;
    }

    /**
     * @return Member metadata keyed by member Id.
     */
    public Map<String, AssignmentMemberSpec> members() {
        return members;
    }

    /**
     * @return The group's subscription type.
     */
    public SubscriptionType subscriptionType() {
        return subscriptionType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AssignmentSpec that = (AssignmentSpec) o;
        return subscriptionType == that.subscriptionType &&
            members.equals(that.members);
    }

    @Override
    public int hashCode() {
        return Objects.hash(members, subscriptionType);
    }

    public String toString() {
        return "AssignmentSpec(members=" + members + ", subscriptionType=" + subscriptionType.toString() + ')';
    }
}
