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
package org.apache.kafka.coordinator.group.util;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * A view of a group's members, static members, and target assignment after unwritten membership
 * changes have been applied.
 *
 * @param <M> The member type.
 * @param <A> The member's target assignment type.
 */
public class UpdatedMembersAndTargetAssignmentView<M, A> {

    /**
     * The group members.
     */
    private final OverlayMap<String, M> members;

    /**
     * The static group members.
     */
    private final OverlayMap<String, String> staticMembers;

    /**
     * The target assignment per member id.
     */
    private final OverlayMap<String, A> targetAssignment;

    /**
     * Gets a member's instance id, or {@code null} if the member is not static.
     */
    private final Function<M, String> getInstanceId;

    /**
     * @param members          The group members. Must not be modified during the lifetime of the view.
     * @param staticMembers    The static group members. Must not be modified during the lifetime of the view.
     * @param targetAssignment The target assignment per member id. Must not be modified during the lifetime of the view.
     * @param getInstanceId    Gets a member's instance id, or {@code null} if the member is not static.
     */
    public UpdatedMembersAndTargetAssignmentView(
        Map<String, M> members,
        Map<String, String> staticMembers,
        Map<String, A> targetAssignment,
        Function<M, String> getInstanceId
    ) {
        this.members = new OverlayMap<>(Objects.requireNonNull(members));
        this.staticMembers = new OverlayMap<>(Objects.requireNonNull(staticMembers));
        this.targetAssignment = new OverlayMap<>(Objects.requireNonNull(targetAssignment));
        this.getInstanceId = Objects.requireNonNull(getInstanceId);
    }

    /**
     * @return The group members after updates.
     */
    public Map<String, M> members() {
        return Collections.unmodifiableMap(members);
    }

    /**
     * @return The static group members after updates.
     */
    public Map<String, String> staticMembers() {
        return Collections.unmodifiableMap(staticMembers);
    }

    /**
     * @return The target assignment per member id after updates.
     */
    public Map<String, A> targetAssignment() {
        return Collections.unmodifiableMap(targetAssignment);
    }

    /**
     * Adds or updates a member. If the member is static and there is a different existing static
     * member for the same instance id, the previous static member's target assignment is moved to
     * the new member and the previous static member is removed from the view.
     *
     * @param memberId The member id.
     * @param member   The member to add or update.
     */
    public void addOrUpdateMember(String memberId, M member) {
        M previousMember = members.put(memberId, member);
        String previousInstanceId = previousMember != null ? getInstanceId.apply(previousMember) : null;
        String instanceId = getInstanceId.apply(member);

        // Remove the old static member mapping when the instance id has changed.
        // We don't remove the mapping when the instance id has not changed, otherwise we won't
        // detect static member replacement correctly below.
        if (previousInstanceId != null && !previousInstanceId.equals(instanceId)) {
            staticMembers.remove(previousInstanceId);
        }

        if (instanceId != null) {
            String previousMemberId = staticMembers.put(instanceId, memberId);
            if (previousMemberId != null && !memberId.equals(previousMemberId)) {
                // A static member is being replaced. Move the assignment to the new member.
                A memberAssignment = targetAssignment.get(previousMemberId);
                if (memberAssignment != null) {
                    targetAssignment.put(memberId, memberAssignment);
                }

                // Remove the previous member.
                members.remove(previousMemberId);
                targetAssignment.remove(previousMemberId);
            }
        }
    }

    /**
     * Removes a member.
     *
     * @param memberId The member id.
     */
    public void removeMember(String memberId) {
        M member = members.remove(memberId);
        String instanceId = member != null ? getInstanceId.apply(member) : null;
        if (instanceId != null && memberId.equals(staticMembers.get(instanceId))) {
            staticMembers.remove(instanceId);
        }
        targetAssignment.remove(memberId);
    }
}
