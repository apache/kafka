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

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

import static org.apache.kafka.common.protocol.CommonFields.GROUP_ID;
import static org.apache.kafka.common.protocol.CommonFields.MEMBER_ID;

public class LeaveGroupRequest extends AbstractRequest {
    private static final Schema LEAVE_GROUP_REQUEST_V0 = new Schema(
            GROUP_ID,
            MEMBER_ID);

    /* v1 request is the same as v0. Throttle time has been added to response */
    private static final Schema LEAVE_GROUP_REQUEST_V1 = LEAVE_GROUP_REQUEST_V0;

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema LEAVE_GROUP_REQUEST_V2 = LEAVE_GROUP_REQUEST_V1;

    public static Schema[] schemaVersions() {
        return new Schema[] {LEAVE_GROUP_REQUEST_V0, LEAVE_GROUP_REQUEST_V1,
            LEAVE_GROUP_REQUEST_V2};
    }

    public static class Builder extends AbstractRequest.Builder<LeaveGroupRequest> {
        private final String groupId;
        private final String memberId;

        public Builder(String groupId, String memberId) {
            super(ApiKeys.LEAVE_GROUP);
            this.groupId = groupId;
            this.memberId = memberId;
        }

        @Override
        public LeaveGroupRequest build(short version) {
            return new LeaveGroupRequest(groupId, memberId, version);
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
        super(version);
        this.groupId = groupId;
        this.memberId = memberId;
    }

    public LeaveGroupRequest(Struct struct, short version) {
        super(version);
        groupId = struct.get(GROUP_ID);
        memberId = struct.get(MEMBER_ID);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
                return new LeaveGroupResponse(Errors.forException(e));
            case 1:
            case 2:
                return new LeaveGroupResponse(throttleTimeMs, Errors.forException(e));
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.LEAVE_GROUP.latestVersion()));
        }
    }

    public String groupId() {
        return groupId;
    }

    public String memberId() {
        return memberId;
    }

    public static LeaveGroupRequest parse(ByteBuffer buffer, short version) {
        return new LeaveGroupRequest(ApiKeys.LEAVE_GROUP.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.LEAVE_GROUP.requestSchema(version()));
        struct.set(GROUP_ID, groupId);
        struct.set(MEMBER_ID, memberId);
        return struct;
    }
}
