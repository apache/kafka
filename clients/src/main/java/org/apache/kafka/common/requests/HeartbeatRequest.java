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
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.STRING;

public class HeartbeatRequest extends AbstractRequest {
    private static final String GROUP_ID_KEY_NAME = "group_id";
    private static final String GROUP_GENERATION_ID_KEY_NAME = "group_generation_id";
    private static final String MEMBER_ID_KEY_NAME = "member_id";

    private static final Schema HEARTBEAT_REQUEST_V0 = new Schema(
            new Field(GROUP_ID_KEY_NAME, STRING, "The group id."),
            new Field(GROUP_GENERATION_ID_KEY_NAME, INT32, "The generation of the group."),
            new Field(MEMBER_ID_KEY_NAME, STRING, "The member id assigned by the group coordinator."));

    /* v1 request is the same as v0. Throttle time has been added to response */
    private static final Schema HEARTBEAT_REQUEST_V1 = HEARTBEAT_REQUEST_V0;

    public static Schema[] schemaVersions() {
        return new Schema[] {HEARTBEAT_REQUEST_V0, HEARTBEAT_REQUEST_V1};
    }

    public static class Builder extends AbstractRequest.Builder<HeartbeatRequest> {
        private final String groupId;
        private final int groupGenerationId;
        private final String memberId;

        public Builder(String groupId, int groupGenerationId, String memberId) {
            super(ApiKeys.HEARTBEAT);
            this.groupId = groupId;
            this.groupGenerationId = groupGenerationId;
            this.memberId = memberId;
        }

        @Override
        public HeartbeatRequest build(short version) {
            return new HeartbeatRequest(groupId, groupGenerationId, memberId, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=HeartbeatRequest").
                append(", groupId=").append(groupId).
                append(", groupGenerationId=").append(groupGenerationId).
                append(", memberId=").append(memberId).
                append(")");
            return bld.toString();
        }
    }

    private final String groupId;
    private final int groupGenerationId;
    private final String memberId;

    private HeartbeatRequest(String groupId, int groupGenerationId, String memberId, short version) {
        super(version);
        this.groupId = groupId;
        this.groupGenerationId = groupGenerationId;
        this.memberId = memberId;
    }

    public HeartbeatRequest(Struct struct, short version) {
        super(version);
        groupId = struct.getString(GROUP_ID_KEY_NAME);
        groupGenerationId = struct.getInt(GROUP_GENERATION_ID_KEY_NAME);
        memberId = struct.getString(MEMBER_ID_KEY_NAME);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
                return new HeartbeatResponse(Errors.forException(e));
            case 1:
                return new HeartbeatResponse(throttleTimeMs, Errors.forException(e));
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.HEARTBEAT.latestVersion()));
        }
    }

    public String groupId() {
        return groupId;
    }

    public int groupGenerationId() {
        return groupGenerationId;
    }

    public String memberId() {
        return memberId;
    }

    public static HeartbeatRequest parse(ByteBuffer buffer, short version) {
        return new HeartbeatRequest(ApiKeys.HEARTBEAT.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.HEARTBEAT.requestSchema(version()));
        struct.set(GROUP_ID_KEY_NAME, groupId);
        struct.set(GROUP_GENERATION_ID_KEY_NAME, groupGenerationId);
        struct.set(MEMBER_ID_KEY_NAME, memberId);
        return struct;
    }
}
