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

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class JoinGroupRequest extends AbstractRequest {
    
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.JOIN_GROUP.id);
    private static final String GROUP_PROTOCOL_KEY_NAME = "group_protocol";
    private static final String GROUP_ID_KEY_NAME = "group_id";
    private static final String SUB_PROTOCOLS_KEY_NAME = "sub_protocols";
    private static final String SESSION_TIMEOUT_KEY_NAME = "session_timeout";
    private static final String MEMBER_ID_KEY_NAME = "member_id";
    private static final String MEMBER_METADATA_KEY_NAME = "member_metadata";

    public static final String UNKNOWN_MEMBER_ID = "";

    private final String groupProtocol;
    private final String groupId;
    private final Collection<String> subProtocols;
    private final int sessionTimeout;
    private final String memberId;
    private final ByteBuffer memberMetadata;

    public JoinGroupRequest(String groupProtocol,
                            String groupId,
                            Collection<String> subProtocols,
                            int sessionTimeout,
                            String memberId,
                            ByteBuffer memberMetadata) {
        super(new Struct(CURRENT_SCHEMA));
        struct.set(GROUP_PROTOCOL_KEY_NAME, groupProtocol);
        struct.set(GROUP_ID_KEY_NAME, groupId);
        struct.set(SUB_PROTOCOLS_KEY_NAME, subProtocols.toArray());
        struct.set(SESSION_TIMEOUT_KEY_NAME, sessionTimeout);
        struct.set(MEMBER_ID_KEY_NAME, memberId);
        struct.set(MEMBER_METADATA_KEY_NAME, memberMetadata);
        this.groupProtocol = groupProtocol;
        this.groupId = groupId;
        this.subProtocols = subProtocols;
        this.sessionTimeout = sessionTimeout;
        this.memberId = memberId;
        this.memberMetadata = memberMetadata;
    }

    public JoinGroupRequest(Struct struct) {
        super(struct);
        groupProtocol = struct.getString(GROUP_PROTOCOL_KEY_NAME);
        groupId = struct.getString(GROUP_ID_KEY_NAME);
        subProtocols = new ArrayList<>();
        for (Object subProtocolObj : struct.getArray(SUB_PROTOCOLS_KEY_NAME))
            subProtocols.add((String) subProtocolObj);
        sessionTimeout = struct.getInt(SESSION_TIMEOUT_KEY_NAME);
        memberId = struct.getString(MEMBER_ID_KEY_NAME);
        memberMetadata = struct.getBytes(MEMBER_METADATA_KEY_NAME);
    }

    @Override
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        switch (versionId) {
            case 0:
                return new JoinGroupResponse(
                        Errors.forException(e).code(),
                        JoinGroupResponse.UNKNOWN_GENERATION_ID,
                        JoinGroupResponse.UNKNOWN_SUB_PROTOCOL,
                        JoinGroupResponse.UNKNOWN_MEMBER_ID,
                        JoinGroupResponse.UNKNOWN_MEMBER_ID,
                        Collections.<String, ByteBuffer>emptyMap());
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.JOIN_GROUP.id)));
        }
    }

    public String groupProtocol() {
        return groupProtocol;
    }

    public String groupId() {
        return groupId;
    }

    public Collection<String> subProtocols() {
        return subProtocols;
    }

    public int sessionTimeout() {
        return sessionTimeout;
    }

    public String memberId() {
        return memberId;
    }

    public ByteBuffer memberMetadata() {
        return memberMetadata;
    }

    public static JoinGroupRequest parse(ByteBuffer buffer, int versionId) {
        return new JoinGroupRequest(ProtoUtils.parseRequest(ApiKeys.JOIN_GROUP.id, versionId, buffer));
    }

    public static JoinGroupRequest parse(ByteBuffer buffer) {
        return new JoinGroupRequest((Struct) CURRENT_SCHEMA.read(buffer));
    }
}
