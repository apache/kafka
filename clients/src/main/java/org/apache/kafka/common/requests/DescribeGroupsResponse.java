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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DescribeGroupsResponse extends AbstractRequestResponse {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.DESCRIBE_GROUPS.id);

    private static final String GROUPS_KEY_NAME = "groups";

    private static final String ERROR_CODE_KEY_NAME = "error_code";
    private static final String GROUP_ID_KEY_NAME = "group_id";
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
     * Possible per-group error codes:
     *
     * GROUP_LOAD_IN_PROGRESS (14)
     * GROUP_COORDINATOR_NOT_AVAILABLE (15)
     * NOT_COORDINATOR_FOR_GROUP (16)
     * AUTHORIZATION_FAILED (29)
     */

    private final Map<String, GroupMetadata> groups;

    public DescribeGroupsResponse(Map<String, GroupMetadata> groups) {
        super(new Struct(CURRENT_SCHEMA));

        List<Struct> groupStructs = new ArrayList<>();
        for (Map.Entry<String, GroupMetadata> groupEntry : groups.entrySet()) {
            Struct groupStruct = struct.instance(GROUPS_KEY_NAME);
            GroupMetadata group = groupEntry.getValue();
            groupStruct.set(GROUP_ID_KEY_NAME, groupEntry.getKey());
            groupStruct.set(ERROR_CODE_KEY_NAME, group.errorCode);
            groupStruct.set(GROUP_STATE_KEY_NAME, group.state);
            groupStruct.set(PROTOCOL_TYPE_KEY_NAME, group.protocolType);
            groupStruct.set(PROTOCOL_KEY_NAME, group.protocol);
            List<Struct> membersList = new ArrayList<>();
            for (GroupMember member : group.members) {
                Struct memberStruct = groupStruct.instance(MEMBERS_KEY_NAME);
                memberStruct.set(MEMBER_ID_KEY_NAME, member.memberId);
                memberStruct.set(CLIENT_ID_KEY_NAME, member.clientId);
                memberStruct.set(CLIENT_HOST_KEY_NAME, member.clientHost);
                memberStruct.set(MEMBER_METADATA_KEY_NAME, member.memberMetadata);
                memberStruct.set(MEMBER_ASSIGNMENT_KEY_NAME, member.memberAssignment);
                membersList.add(memberStruct);
            }
            groupStruct.set(MEMBERS_KEY_NAME, membersList.toArray());
            groupStructs.add(groupStruct);
        }
        struct.set(GROUPS_KEY_NAME, groupStructs.toArray());
        this.groups = groups;
    }

    public DescribeGroupsResponse(Struct struct) {
        super(struct);
        this.groups = new HashMap<>();
        for (Object groupObj : struct.getArray(GROUPS_KEY_NAME)) {
            Struct groupStruct = (Struct) groupObj;

            String groupId = groupStruct.getString(GROUP_ID_KEY_NAME);
            short errorCode = groupStruct.getShort(ERROR_CODE_KEY_NAME);
            String state = groupStruct.getString(GROUP_STATE_KEY_NAME);
            String protocolType = groupStruct.getString(PROTOCOL_TYPE_KEY_NAME);
            String protocol = groupStruct.getString(PROTOCOL_KEY_NAME);

            List<GroupMember> members = new ArrayList<>();
            for (Object memberObj : groupStruct.getArray(MEMBERS_KEY_NAME)) {
                Struct memberStruct = (Struct) memberObj;
                String memberId = memberStruct.getString(MEMBER_ID_KEY_NAME);
                String clientId = memberStruct.getString(CLIENT_ID_KEY_NAME);
                String clientHost = memberStruct.getString(CLIENT_HOST_KEY_NAME);
                ByteBuffer memberMetadata = memberStruct.getBytes(MEMBER_METADATA_KEY_NAME);
                ByteBuffer memberAssignment = memberStruct.getBytes(MEMBER_ASSIGNMENT_KEY_NAME);
                members.add(new GroupMember(memberId, clientId, clientHost,
                        memberMetadata, memberAssignment));
            }
            this.groups.put(groupId, new GroupMetadata(errorCode, state, protocolType, protocol, members));
        }
    }

    public Map<String, GroupMetadata> groups() {
        return groups;
    }


    public static class GroupMetadata {
        private final short errorCode;
        private final String state;
        private final String protocolType;
        private final String protocol;
        private final List<GroupMember> members;

        public GroupMetadata(short errorCode,
                             String state,
                             String protocolType,
                             String protocol,
                             List<GroupMember> members) {
            this.errorCode = errorCode;
            this.state = state;
            this.protocolType = protocolType;
            this.protocol = protocol;
            this.members = members;
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

        public static GroupMetadata forError(Errors error) {
            return new DescribeGroupsResponse.GroupMetadata(
                    error.code(),
                    DescribeGroupsResponse.UNKNOWN_STATE,
                    DescribeGroupsResponse.UNKNOWN_PROTOCOL_TYPE,
                    DescribeGroupsResponse.UNKNOWN_PROTOCOL,
                    Collections.<DescribeGroupsResponse.GroupMember>emptyList());
        }
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

    public static DescribeGroupsResponse parse(ByteBuffer buffer) {
        return new DescribeGroupsResponse(CURRENT_SCHEMA.read(buffer));
    }

    public static DescribeGroupsResponse fromError(Errors error, List<String> groupIds) {
        GroupMetadata errorMetadata = GroupMetadata.forError(error);
        Map<String, GroupMetadata> groups = new HashMap<>();
        for (String groupId : groupIds)
            groups.put(groupId, errorMetadata);
        return new DescribeGroupsResponse(groups);
    }

}
