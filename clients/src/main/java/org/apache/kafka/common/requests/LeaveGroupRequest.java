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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class LeaveGroupRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<LeaveGroupRequest> {
        private final String groupId;
        private final List<MemberIdentity> members;

        public Builder(String groupId, List<MemberIdentity> members) {
            this(groupId, members, ApiKeys.LEAVE_GROUP.oldestVersion(), ApiKeys.LEAVE_GROUP.latestVersion());
        }

        Builder(String groupId, List<MemberIdentity> members, short oldestVersion, short latestVersion) {
            super(ApiKeys.LEAVE_GROUP, oldestVersion, latestVersion);
            this.groupId = groupId;
            this.members = members;
            if (members.isEmpty()) {
                throw new IllegalArgumentException("leaving members should not be empty");
            }
        }

        /**
         * Based on the request version to choose fields.
         */
        @Override
        public LeaveGroupRequest build(short version) {
            final LeaveGroupRequestData data;
            // Starting from version 3, all the leave group request will be in batch.
            if (version >= 3) {
                data = new LeaveGroupRequestData()
                           .setGroupId(groupId)
                           .setMembers(members);
            } else {
                if (members.size() != 1) {
                    throw new UnsupportedVersionException("Version " + version + " leave group request only " +
                                                              "supports single member instance than " + members.size() + " members");
                }

                data = new LeaveGroupRequestData()
                           .setGroupId(groupId)
                           .setMemberId(members.get(0).memberId());
            }
            return new LeaveGroupRequest(data, version);
        }

        @Override
        public String toString() {
            return "(type=LeaveGroupRequest" +
                       ", groupId=" + groupId +
                       ", members=" + MessageUtil.deepToString(members.iterator()) +
                       ")";
        }
    }
    private final LeaveGroupRequestData data;

    private LeaveGroupRequest(LeaveGroupRequestData data, short version) {
        super(ApiKeys.LEAVE_GROUP, version);
        this.data = data;
    }

    @Override
    public LeaveGroupRequestData data() {
        return data;
    }

    public List<MemberIdentity> members() {
        // Before version 3, leave group request is still in single mode
        return version() <= 2 ? Collections.singletonList(
            new MemberIdentity()
                .setMemberId(data.memberId())) : data.members();
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        LeaveGroupResponseData responseData = new LeaveGroupResponseData()
                                                  .setErrorCode(Errors.forException(e).code());

        if (version() >= 1) {
            responseData.setThrottleTimeMs(throttleTimeMs);
        }
        return new LeaveGroupResponse(responseData);
    }

    public static LeaveGroupRequest parse(ByteBuffer buffer, short version) {
        return new LeaveGroupRequest(new LeaveGroupRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
