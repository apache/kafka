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
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.types.Type.BYTES;
import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.STRING;

public class SyncGroupRequest extends AbstractRequest {
    private static final String GROUP_ID_KEY_NAME = "group_id";
    private static final String GENERATION_ID_KEY_NAME = "generation_id";
    private static final String MEMBER_ID_KEY_NAME = "member_id";
    private static final String MEMBER_ASSIGNMENT_KEY_NAME = "member_assignment";
    private static final String GROUP_ASSIGNMENT_KEY_NAME = "group_assignment";

    private static final Schema SYNC_GROUP_REQUEST_MEMBER_V0 = new Schema(
            new Field(MEMBER_ID_KEY_NAME, STRING),
            new Field(MEMBER_ASSIGNMENT_KEY_NAME, BYTES));
    private static final Schema SYNC_GROUP_REQUEST_V0 = new Schema(
            new Field(GROUP_ID_KEY_NAME, STRING),
            new Field(GENERATION_ID_KEY_NAME, INT32),
            new Field(MEMBER_ID_KEY_NAME, STRING),
            new Field(GROUP_ASSIGNMENT_KEY_NAME, new ArrayOf(SYNC_GROUP_REQUEST_MEMBER_V0)));

    /* v1 request is the same as v0. Throttle time has been added to response */
    private static final Schema SYNC_GROUP_REQUEST_V1 = SYNC_GROUP_REQUEST_V0;

    public static Schema[] schemaVersions() {
        return new Schema[] {SYNC_GROUP_REQUEST_V0, SYNC_GROUP_REQUEST_V1};
    }

    public static class Builder extends AbstractRequest.Builder<SyncGroupRequest> {
        private final String groupId;
        private final int generationId;
        private final String memberId;
        private final Map<String, ByteBuffer> groupAssignment;

        public Builder(String groupId, int generationId, String memberId,
                       Map<String, ByteBuffer> groupAssignment) {
            super(ApiKeys.SYNC_GROUP);
            this.groupId = groupId;
            this.generationId = generationId;
            this.memberId = memberId;
            this.groupAssignment = groupAssignment;
        }

        @Override
        public SyncGroupRequest build(short version) {
            return new SyncGroupRequest(groupId, generationId, memberId, groupAssignment, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=SyncGroupRequest").
                    append(", groupId=").append(groupId).
                    append(", generationId=").append(generationId).
                    append(", memberId=").append(memberId).
                    append(", groupAssignment=").
                    append(Utils.join(groupAssignment.keySet(), ",")).
                    append(")");
            return bld.toString();
        }
    }
    private final String groupId;
    private final int generationId;
    private final String memberId;
    private final Map<String, ByteBuffer> groupAssignment;

    private SyncGroupRequest(String groupId, int generationId, String memberId,
                             Map<String, ByteBuffer> groupAssignment, short version) {
        super(version);
        this.groupId = groupId;
        this.generationId = generationId;
        this.memberId = memberId;
        this.groupAssignment = groupAssignment;
    }

    public SyncGroupRequest(Struct struct, short version) {
        super(version);
        this.groupId = struct.getString(GROUP_ID_KEY_NAME);
        this.generationId = struct.getInt(GENERATION_ID_KEY_NAME);
        this.memberId = struct.getString(MEMBER_ID_KEY_NAME);

        groupAssignment = new HashMap<>();

        for (Object memberDataObj : struct.getArray(GROUP_ASSIGNMENT_KEY_NAME)) {
            Struct memberData = (Struct) memberDataObj;
            String memberId = memberData.getString(MEMBER_ID_KEY_NAME);
            ByteBuffer memberMetadata = memberData.getBytes(MEMBER_ASSIGNMENT_KEY_NAME);
            groupAssignment.put(memberId, memberMetadata);
        }
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
                return new SyncGroupResponse(
                        Errors.forException(e),
                        ByteBuffer.wrap(new byte[]{}));
            case 1:
                return new SyncGroupResponse(
                        throttleTimeMs,
                        Errors.forException(e),
                        ByteBuffer.wrap(new byte[]{}));
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.SYNC_GROUP.latestVersion()));
        }
    }

    public String groupId() {
        return groupId;
    }

    public int generationId() {
        return generationId;
    }

    public Map<String, ByteBuffer> groupAssignment() {
        return groupAssignment;
    }

    public String memberId() {
        return memberId;
    }

    public static SyncGroupRequest parse(ByteBuffer buffer, short version) {
        return new SyncGroupRequest(ApiKeys.SYNC_GROUP.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.SYNC_GROUP.requestSchema(version()));
        struct.set(GROUP_ID_KEY_NAME, groupId);
        struct.set(GENERATION_ID_KEY_NAME, generationId);
        struct.set(MEMBER_ID_KEY_NAME, memberId);

        List<Struct> memberArray = new ArrayList<>();
        for (Map.Entry<String, ByteBuffer> entries: groupAssignment.entrySet()) {
            Struct memberData = struct.instance(GROUP_ASSIGNMENT_KEY_NAME);
            memberData.set(MEMBER_ID_KEY_NAME, entries.getKey());
            memberData.set(MEMBER_ASSIGNMENT_KEY_NAME, entries.getValue());
            memberArray.add(memberData);
        }
        struct.set(GROUP_ASSIGNMENT_KEY_NAME, memberArray.toArray());
        return struct;
    }
}
