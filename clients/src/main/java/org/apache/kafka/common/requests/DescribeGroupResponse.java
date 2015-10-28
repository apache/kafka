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

public class DescribeGroupResponse extends AbstractRequestResponse {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.DESCRIBE_GROUP.id);

    private static final String ERROR_CODE_KEY_NAME = "error_code";
    private static final String GROUP_STATE_KEY_NAME = "state";
    private static final String PROTOCOL_TYPE_KEY_NAME = "protocol_type";
    private static final String PROTOCOL_KEY_NAME = "protocol";

    private static final String MEMBERS_KEY_NAME = "members";
    private static final String MEMBER_ID_KEY_NAME = "member_id";
    private static final String CLIENT_ID_KEY_NAME = "client_id";
    private static final String CLIENT_HOST_KEY_NAME = "client_host";
    private static final String MEMBER_METADATA_KEY_NAME = "member_metadata";
    private static final String MEMBER_ASSIGNMENT_KEY_NAME = "member_assignment";

    public static final String UNKNOWN_STATE = "";
    public static final String UNKNOWN_PROTOCOL_TYPE = "";
    public static final String UNKNOWN_PROTOCOL = "";

    /**
     * Possible error codes:
     *
     * GROUP_COORDINATOR_NOT_AVAILABLE (15)
     * NOT_COORDINATOR_FOR_GROUP (16)
     * AUTHORIZATION_FAILED (29)
     */

    private final short errorCode;
    private final String state;
    private final String protocolType;
    private final String protocol;
    private final List<GroupMember> members;

    public DescribeGroupResponse(short errorCode,
                                 String state,
                                 String protocolType,
                                 String protocol,
                                 List<GroupMember> members) {
        super(new Struct(CURRENT_SCHEMA));
        struct.set(ERROR_CODE_KEY_NAME, errorCode);
        struct.set(GROUP_STATE_KEY_NAME, state);
        struct.set(PROTOCOL_TYPE_KEY_NAME, protocolType);
        struct.set(PROTOCOL_KEY_NAME, protocol);
        List<Struct> membersList = new ArrayList<>();
        for (GroupMember member : members) {
            Struct memberStruct = struct.instance(MEMBERS_KEY_NAME);
            memberStruct.set(MEMBER_ID_KEY_NAME, member.memberId);
            memberStruct.set(CLIENT_ID_KEY_NAME, member.clientId);
            memberStruct.set(CLIENT_HOST_KEY_NAME, member.clientHost);
            memberStruct.set(MEMBER_METADATA_KEY_NAME, member.memberMetadata);
            memberStruct.set(MEMBER_ASSIGNMENT_KEY_NAME, member.memberAssignment);
            membersList.add(memberStruct);
        }
        struct.set(MEMBERS_KEY_NAME, membersList.toArray());

        this.errorCode = errorCode;
        this.state = state;
        this.protocolType = protocolType;
        this.protocol = protocol;
        this.members = members;
    }

    public DescribeGroupResponse(Struct struct) {
        super(struct);
        this.errorCode = struct.getShort(ERROR_CODE_KEY_NAME);
        this.state = struct.getString(GROUP_STATE_KEY_NAME);
        this.protocolType = struct.getString(PROTOCOL_TYPE_KEY_NAME);
        this.protocol = struct.getString(PROTOCOL_KEY_NAME);

        this.members = new ArrayList<>();
        for (Object memberObj : struct.getArray(MEMBERS_KEY_NAME)) {
            Struct memberStruct = (Struct) memberObj;
            String memberId = memberStruct.getString(MEMBER_ID_KEY_NAME);
            String clientId = memberStruct.getString(CLIENT_ID_KEY_NAME);
            String clientHost = memberStruct.getString(CLIENT_HOST_KEY_NAME);
            ByteBuffer memberMetadata = memberStruct.getBytes(MEMBER_METADATA_KEY_NAME);
            ByteBuffer memberAssignment = memberStruct.getBytes(MEMBER_ASSIGNMENT_KEY_NAME);
            this.members.add(new GroupMember(memberId, clientId, clientHost,
                    memberMetadata, memberAssignment));
        }
    }

    public short errorCode() {
        return errorCode;
    }

    public String state() {
        return state;
    }

    public String protocolType() {
        return protocolType;
    }

    public String protocol() {
        return protocol;
    }

    public List<GroupMember> members() {
        return members;
    }

    public static class GroupMember {
        private final String memberId;
        private final String clientId;
        private final String clientHost;
        private final ByteBuffer memberMetadata;
        private final ByteBuffer memberAssignment;

        public GroupMember(String memberId,
                           String clientId,
                           String clientHost,
                           ByteBuffer memberMetadata,
                           ByteBuffer memberAssignment) {
            this.memberId = memberId;
            this.clientId = clientId;
            this.clientHost = clientHost;
            this.memberMetadata = memberMetadata;
            this.memberAssignment = memberAssignment;
        }

        public String memberId() {
            return memberId;
        }

        public String clientId() {
            return clientId;
        }

        public String clientHost() {
            return clientHost;
        }

        public ByteBuffer memberMetadata() {
            return memberMetadata;
        }

        public ByteBuffer memberAssignment() {
            return memberAssignment;
        }
    }

    public static DescribeGroupResponse parse(ByteBuffer buffer) {
        return new DescribeGroupResponse((Struct) CURRENT_SCHEMA.read(buffer));
    }

    public static DescribeGroupResponse fromError(Errors error) {
        return new DescribeGroupResponse(error.code(), UNKNOWN_STATE, UNKNOWN_PROTOCOL_TYPE, UNKNOWN_PROTOCOL,
                Collections.<GroupMember>emptyList());
    }

}
