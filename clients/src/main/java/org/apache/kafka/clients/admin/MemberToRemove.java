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

import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.requests.JoinGroupRequest;

import java.util.Objects;

/**
 * A struct containing information about the member to be removed.
 */
public class MemberToRemove {
    private String groupInstanceId;
    private String memberId;

    public MemberToRemove() {
        this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        this.groupInstanceId = null;
    }

    /**
     * @deprecated use {@link #MemberToRemove()} instead
     */
    @Deprecated
    public MemberToRemove(String groupInstanceId) {
        this.groupInstanceId = groupInstanceId;
        this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MemberToRemove) {
            MemberToRemove otherMember = (MemberToRemove) o;
            Boolean groupInstanceIdEquality;
            if (this.groupInstanceId == null) {
                groupInstanceIdEquality = otherMember.groupInstanceId == null;
            } else {
                groupInstanceIdEquality = this.groupInstanceId.equals(otherMember.groupInstanceId);
            }
            return groupInstanceIdEquality && this.memberId.equals(otherMember.memberId);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupInstanceId, memberId);
    }

    MemberIdentity toMemberIdentity() {
        return new MemberIdentity()
                   .setGroupInstanceId(groupInstanceId)
                   .setMemberId(memberId);
    }

    public MemberToRemove withGroupInstanceId(String groupInstanceId) {
        this.groupInstanceId = groupInstanceId;
        return this;
    }

    public MemberToRemove withMemberId(String memberId) {
        this.memberId = memberId;
        return this;
    }

    @Override
    public String toString() {
        return "(memberId=" + memberId +
                ", groupInstanceId=" + groupInstanceId;
    }
}
