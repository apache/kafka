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
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JoinGroupRequest extends AbstractRequest {
    private static final String GROUP_ID_KEY_NAME = "group_id";
    private static final String SESSION_TIMEOUT_KEY_NAME = "session_timeout";
    private static final String REBALANCE_TIMEOUT_KEY_NAME = "rebalance_timeout";
    private static final String MEMBER_ID_KEY_NAME = "member_id";
    private static final String PROTOCOL_TYPE_KEY_NAME = "protocol_type";
    private static final String GROUP_PROTOCOLS_KEY_NAME = "group_protocols";
    private static final String PROTOCOL_NAME_KEY_NAME = "protocol_name";
    private static final String PROTOCOL_METADATA_KEY_NAME = "protocol_metadata";

    public static final String UNKNOWN_MEMBER_ID = "";

    private final String groupId;
    private final int sessionTimeout;
    private final int rebalanceTimeout;
    private final String memberId;
    private final String protocolType;
    private final List<ProtocolMetadata> groupProtocols;

    public static class ProtocolMetadata {
        private final String name;
        private final ByteBuffer metadata;

        public ProtocolMetadata(String name, ByteBuffer metadata) {
            this.name = name;
            this.metadata = metadata;
        }

        public String name() {
            return name;
        }

        public ByteBuffer metadata() {
            return metadata;
        }
    }

    public static class Builder extends AbstractRequest.Builder<JoinGroupRequest> {
        private final String groupId;
        private final int sessionTimeout;
        private final String memberId;
        private final String protocolType;
        private final List<ProtocolMetadata> groupProtocols;
        private int rebalanceTimeout = 0;

        public Builder(String groupId, int sessionTimeout, String memberId,
                       String protocolType, List<ProtocolMetadata> groupProtocols) {
            super(ApiKeys.JOIN_GROUP);
            this.groupId = groupId;
            this.sessionTimeout = sessionTimeout;
            this.rebalanceTimeout = sessionTimeout;
            this.memberId = memberId;
            this.protocolType = protocolType;
            this.groupProtocols = groupProtocols;
        }

        public Builder setRebalanceTimeout(int rebalanceTimeout) {
            this.rebalanceTimeout = rebalanceTimeout;
            return this;
        }

        @Override
        public JoinGroupRequest build() {
            short version = version();
            if (version < 1) {
                rebalanceTimeout = -1;
            }
            return new JoinGroupRequest(version, groupId, sessionTimeout,
                    rebalanceTimeout, memberId, protocolType, groupProtocols);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type: JoinGroupRequest").
                append(", groupId=").append(groupId).
                append(", sessionTimeout=").append(sessionTimeout).
                append(", rebalanceTimeout=").append(rebalanceTimeout).
                append(", memberId=").append(memberId).
                append(", protocolType=").append(protocolType).
                append(", groupProtocols=").append(Utils.join(groupProtocols, ", ")).
                append(")");
            return bld.toString();
        }
    }

    private JoinGroupRequest(short version, String groupId, int sessionTimeout,
            int rebalanceTimeout, String memberId, String protocolType,
            List<ProtocolMetadata> groupProtocols) {
        super(new Struct(ProtoUtils.
                requestSchema(ApiKeys.JOIN_GROUP.id, version)), version);
        struct.set(GROUP_ID_KEY_NAME, groupId);
        struct.set(SESSION_TIMEOUT_KEY_NAME, sessionTimeout);
        if (version >= 1) {
            struct.set(REBALANCE_TIMEOUT_KEY_NAME, rebalanceTimeout);
        }
        struct.set(MEMBER_ID_KEY_NAME, memberId);
        struct.set(PROTOCOL_TYPE_KEY_NAME, protocolType);
        List<Struct> groupProtocolsList = new ArrayList<>(groupProtocols.size());
        for (ProtocolMetadata protocol : groupProtocols) {
            Struct protocolStruct = struct.instance(GROUP_PROTOCOLS_KEY_NAME);
            protocolStruct.set(PROTOCOL_NAME_KEY_NAME, protocol.name);
            protocolStruct.set(PROTOCOL_METADATA_KEY_NAME, protocol.metadata);
            groupProtocolsList.add(protocolStruct);
        }
        struct.set(GROUP_PROTOCOLS_KEY_NAME, groupProtocolsList.toArray());
        this.groupId = groupId;
        this.sessionTimeout = sessionTimeout;
        this.rebalanceTimeout = rebalanceTimeout;
        this.memberId = memberId;
        this.protocolType = protocolType;
        this.groupProtocols = groupProtocols;
    }

    public JoinGroupRequest(Struct struct, short versionId) {
        super(struct, versionId);

        groupId = struct.getString(GROUP_ID_KEY_NAME);
        sessionTimeout = struct.getInt(SESSION_TIMEOUT_KEY_NAME);

        if (struct.hasField(REBALANCE_TIMEOUT_KEY_NAME))
            // rebalance timeout is added in v1
            rebalanceTimeout = struct.getInt(REBALANCE_TIMEOUT_KEY_NAME);
        else
            // v0 had no rebalance timeout but used session timeout implicitly
            rebalanceTimeout = sessionTimeout;

        memberId = struct.getString(MEMBER_ID_KEY_NAME);
        protocolType = struct.getString(PROTOCOL_TYPE_KEY_NAME);

        groupProtocols = new ArrayList<>();
        for (Object groupProtocolObj : struct.getArray(GROUP_PROTOCOLS_KEY_NAME)) {
            Struct groupProtocolStruct = (Struct) groupProtocolObj;
            String name = groupProtocolStruct.getString(PROTOCOL_NAME_KEY_NAME);
            ByteBuffer metadata = groupProtocolStruct.getBytes(PROTOCOL_METADATA_KEY_NAME);
            groupProtocols.add(new ProtocolMetadata(name, metadata));
        }
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
                return new JoinGroupResponse(
                        versionId,
                        Errors.forException(e).code(),
                        JoinGroupResponse.UNKNOWN_GENERATION_ID,
                        JoinGroupResponse.UNKNOWN_PROTOCOL,
                        JoinGroupResponse.UNKNOWN_MEMBER_ID, // memberId
                        JoinGroupResponse.UNKNOWN_MEMBER_ID, // leaderId
                        Collections.<String, ByteBuffer>emptyMap());

            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.JOIN_GROUP.id)));
        }
    }

    public String groupId() {
        return groupId;
    }

    public int sessionTimeout() {
        return sessionTimeout;
    }

    public int rebalanceTimeout() {
        return rebalanceTimeout;
    }

    public String memberId() {
        return memberId;
    }

    public List<ProtocolMetadata> groupProtocols() {
        return groupProtocols;
    }

    public String protocolType() {
        return protocolType;
    }

    public static JoinGroupRequest parse(ByteBuffer buffer, int versionId) {
        return new JoinGroupRequest(ProtoUtils.parseRequest(ApiKeys.JOIN_GROUP.id, versionId, buffer),
                (short) versionId);
    }

    public static JoinGroupRequest parse(ByteBuffer buffer) {
        return parse(buffer, ProtoUtils.latestVersion(ApiKeys.JOIN_GROUP.id));
    }
}
