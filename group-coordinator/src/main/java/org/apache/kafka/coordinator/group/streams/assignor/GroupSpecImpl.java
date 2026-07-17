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

import org.apache.kafka.coordinator.group.api.assignor.streams.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.streams.MemberAssignmentMetadata;
import org.apache.kafka.coordinator.group.api.assignor.streams.MemberAssignmentState;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * The assignment specification for a streams group.
 *
 * @param members The member metadata keyed by member Id. Each value provides both the
 *                {@link MemberAssignmentMetadata} and the {@link MemberAssignmentState} for the member.
 * @param configs Any configurations passed to the assignor.
 */
public record GroupSpecImpl(
    Map<String, MemberMetadataAndAssignmentImpl> members,
    Map<String, String> configs
) implements GroupSpec {

    public GroupSpecImpl {
        Objects.requireNonNull(members);
        Objects.requireNonNull(configs);
    }

    @Override
    public Collection<String> memberIds() {
        return members.keySet();
    }

    @Override
    public MemberAssignmentMetadata memberMetadata(String memberId) {
        return requireMember(memberId);
    }

    @Override
    public MemberAssignmentState memberAssignmentState(String memberId) {
        return requireMember(memberId);
    }

    private MemberMetadataAndAssignmentImpl requireMember(String memberId) {
        MemberMetadataAndAssignmentImpl member = members.get(memberId);
        if (member == null) {
            throw new IllegalArgumentException("Member Id " + memberId + " not found.");
        }
        return member;
    }

}
