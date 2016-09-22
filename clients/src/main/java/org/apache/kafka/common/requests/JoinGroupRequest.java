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
import java.util.Collections;
import java.util.List;

public class JoinGroupRequest extends AbstractRequest {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.JOIN_GROUP.id);
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

    // v0 constructor
    @Deprecated
    public JoinGroupRequest(String groupId,
                            int sessionTimeout,
                            String memberId,
                            String protocolType,
                            List<ProtocolMetadata> groupProtocols) {
        this(0, groupId, sessionTimeout, sessionTimeout, memberId, protocolType, groupProtocols);
    }

    public JoinGroupRequest(String groupId,
                            int sessionTimeout,
                            int rebalanceTimeout,
                            String memberId,
                            String protocolType,
                            List<ProtocolMetadata> groupProtocols) {
        this(1, groupId, sessionTimeout, rebalanceTimeout, memberId, protocolType, groupProtocols);
    }

    private JoinGroupRequest(int version,
                             String groupId,
                             int sessionTimeout,
                             int rebalanceTimeout,
                             String memberId,
                             String protocolType,
                             List<ProtocolMetadata> groupProtocols) {
        super(new Struct(ProtoUtils.requestSchema(ApiKeys.JOIN_GROUP.id, version)));

        struct.set(GROUP_ID_KEY_NAME, groupId);
        struct.set(SESSION_TIMEOUT_KEY_NAME, sessionTimeout);

        if (version >= 1)
            struct.set(REBALANCE_TIMEOUT_KEY_NAME, rebalanceTimeout);

        struct.set(MEMBER_ID_KEY_NAME, memberId);
        struct.set(PROTOCOL_TYPE_KEY_NAME, protocolType);

        List<Struct> groupProtocolsList = new ArrayList<>();
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

    public JoinGroupRequest(Struct struct) {
        super(struct);

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
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
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
        return new JoinGroupRequest(ProtoUtils.parseRequest(ApiKeys.JOIN_GROUP.id, versionId, buffer));
    }

    public static JoinGroupRequest parse(ByteBuffer buffer) {
        return new JoinGroupRequest(CURRENT_SCHEMA.read(buffer));
    }
}
