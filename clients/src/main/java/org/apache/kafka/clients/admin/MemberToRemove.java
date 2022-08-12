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
    private final String groupInstanceId;

    public MemberToRemove(String groupInstanceId) {
        this.groupInstanceId = groupInstanceId;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MemberToRemove) {
            MemberToRemove otherMember = (MemberToRemove) o;
            return this.groupInstanceId.equals(otherMember.groupInstanceId);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupInstanceId);
    }

    MemberIdentity toMemberIdentity() {
        return new MemberIdentity()
            .setGroupInstanceId(groupInstanceId)
            .setMemberId(JoinGroupRequest.UNKNOWN_MEMBER_ID);
    }

    public String groupInstanceId() {
        return groupInstanceId;
    }
}
