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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.requests.JoinGroupRequest;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Options for {@link AdminClient#removeMembersFromConsumerGroup(String, RemoveMembersFromConsumerGroupOptions)}.
 * It carries the members to be removed from the consumer group.
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class RemoveMembersFromConsumerGroupOptions extends AbstractOptions<RemoveMembersFromConsumerGroupOptions> {

    /**
     * A struct containing member's info.
     */
    public static class RemovingMemberInfo {
        public String memberId;
        public String groupInstanceId;

        public RemovingMemberInfo(String memberId, String groupInstanceId) {
            this.memberId = memberId;
            this.groupInstanceId = groupInstanceId;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof RemovingMemberInfo) {
                RemovingMemberInfo otherMember = (RemovingMemberInfo) o;
                return this.memberId.equals(otherMember.memberId) &&
                           this.groupInstanceId.equals(otherMember.groupInstanceId);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(groupInstanceId, memberId);
        }
    }

    private Set<RemovingMemberInfo> members;

    public RemoveMembersFromConsumerGroupOptions(Collection<String> groupInstanceIds) {
        members = groupInstanceIds.stream().map(
            instanceId -> new RemovingMemberInfo(JoinGroupRequest.UNKNOWN_MEMBER_ID, instanceId)
        ).collect(Collectors.toSet());
    }

    public Set<RemovingMemberInfo> members() {
        return members;
    }

    static MemberIdentity convertToMemberIdentity(RemovingMemberInfo memberInfo) {
        return new MemberIdentity()
                   .setGroupInstanceId(memberInfo.groupInstanceId)
                   .setMemberId(memberInfo.memberId);
    }
}

