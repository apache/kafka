/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;

public class LeaveGroupRequest extends AbstractRequest {
    private static final String GROUP_ID_KEY_NAME = "group_id";
    private static final String MEMBER_ID_KEY_NAME = "member_id";

    public static class Builder extends AbstractRequest.Builder<LeaveGroupRequest> {
        private final String groupId;
        private final String memberId;

        public Builder(String groupId, String memberId) {
            super(ApiKeys.LEAVE_GROUP);
            this.groupId = groupId;
            this.memberId = memberId;
        }

        @Override
        public LeaveGroupRequest build() {
            return new LeaveGroupRequest(groupId, memberId, version());
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=LeaveGroupRequest").
                append(", groupId=").append(groupId).
                append(", memberId=").append(memberId).
                append(")");
            return bld.toString();
        }
    }

    private final String groupId;
    private final String memberId;

    private LeaveGroupRequest(String groupId, String memberId, short version) {
        super(new Struct(ProtoUtils.requestSchema(ApiKeys.LEAVE_GROUP.id, version)),
                version);
        struct.set(GROUP_ID_KEY_NAME, groupId);
        struct.set(MEMBER_ID_KEY_NAME, memberId);
        this.groupId = groupId;
        this.memberId = memberId;
    }

    public LeaveGroupRequest(Struct struct, short version) {
        super(struct, version);
        groupId = struct.getString(GROUP_ID_KEY_NAME);
        memberId = struct.getString(MEMBER_ID_KEY_NAME);
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
                return new LeaveGroupResponse(Errors.forException(e).code());
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.LEAVE_GROUP.id)));
        }
    }

    public String groupId() {
        return groupId;
    }

    public String memberId() {
        return memberId;
    }

    public static LeaveGroupRequest parse(ByteBuffer buffer, int versionId) {
        return new LeaveGroupRequest(ProtoUtils.parseRequest(ApiKeys.LEAVE_GROUP.id, versionId, buffer),
                (short) versionId);
    }

    public static LeaveGroupRequest parse(ByteBuffer buffer) {
        return parse(buffer, ProtoUtils.latestVersion(ApiKeys.LEAVE_GROUP.id));
    }
}
