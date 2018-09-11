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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.GROUP_ID;
import static org.apache.kafka.common.protocol.CommonFields.MEMBER_ID;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.types.Type.BYTES;
import static org.apache.kafka.common.protocol.types.Type.STRING;

public class DescribeGroupsResponse extends AbstractResponse {

    private static final String GROUPS_KEY_NAME = "groups";

    private static final String GROUP_STATE_KEY_NAME = "state";
    private static final String PROTOCOL_TYPE_KEY_NAME = "protocol_type";
    private static final String PROTOCOL_KEY_NAME = "protocol";

    private static final String MEMBERS_KEY_NAME = "members";
    private static final String CLIENT_ID_KEY_NAME = "client_id";
    private static final String CLIENT_HOST_KEY_NAME = "client_host";
    private static final String MEMBER_METADATA_KEY_NAME = "member_metadata";
    private static final String MEMBER_ASSIGNMENT_KEY_NAME = "member_assignment";

    private static final Schema DESCRIBE_GROUPS_RESPONSE_MEMBER_V0 = new Schema(
            MEMBER_ID,
            new Field(CLIENT_ID_KEY_NAME, STRING, "The client id used in the member's latest join group request"),
            new Field(CLIENT_HOST_KEY_NAME, STRING, "The client host used in the request session corresponding to the " +
                    "member's join group."),
            new Field(MEMBER_METADATA_KEY_NAME, BYTES, "The metadata corresponding to the current group protocol in " +
                    "use (will only be present if the group is stable)."),
            new Field(MEMBER_ASSIGNMENT_KEY_NAME, BYTES, "The current assignment provided by the group leader " +
                    "(will only be present if the group is stable)."));

    private static final Schema DESCRIBE_GROUPS_RESPONSE_GROUP_METADATA_V0 = new Schema(
            ERROR_CODE,
            GROUP_ID,
            new Field(GROUP_STATE_KEY_NAME, STRING, "The current state of the group (one of: Dead, Stable, CompletingRebalance, " +
                    "PreparingRebalance, or empty if there is no active group)"),
            new Field(PROTOCOL_TYPE_KEY_NAME, STRING, "The current group protocol type (will be empty if there is no active group)"),
            new Field(PROTOCOL_KEY_NAME, STRING, "The current group protocol (only provided if the group is Stable)"),
            new Field(MEMBERS_KEY_NAME, new ArrayOf(DESCRIBE_GROUPS_RESPONSE_MEMBER_V0), "Current group members " +
                    "(only provided if the group is not Dead)"));

    private static final Schema DESCRIBE_GROUPS_RESPONSE_V0 = new Schema(
            new Field(GROUPS_KEY_NAME, new ArrayOf(DESCRIBE_GROUPS_RESPONSE_GROUP_METADATA_V0)));
    private static final Schema DESCRIBE_GROUPS_RESPONSE_V1 = new Schema(
            THROTTLE_TIME_MS,
            new Field(GROUPS_KEY_NAME, new ArrayOf(DESCRIBE_GROUPS_RESPONSE_GROUP_METADATA_V0)));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema DESCRIBE_GROUPS_RESPONSE_V2 = DESCRIBE_GROUPS_RESPONSE_V1;

    public static Schema[] schemaVersions() {
        return new Schema[] {DESCRIBE_GROUPS_RESPONSE_V0, DESCRIBE_GROUPS_RESPONSE_V1, DESCRIBE_GROUPS_RESPONSE_V2};
    }

    public static final String UNKNOWN_STATE = "";
    public static final String UNKNOWN_PROTOCOL_TYPE = "";
    public static final String UNKNOWN_PROTOCOL = "";

    /**
     * Possible per-group error codes:
     *
     * COORDINATOR_LOAD_IN_PROGRESS (14)
     * COORDINATOR_NOT_AVAILABLE (15)
     * NOT_COORDINATOR (16)
     * AUTHORIZATION_FAILED (29)
     */

    private final Map<String, GroupMetadata> groups;
    private final int throttleTimeMs;

    public DescribeGroupsResponse(Map<String, GroupMetadata> groups) {
        this(DEFAULT_THROTTLE_TIME, groups);
    }

    public DescribeGroupsResponse(int throttleTimeMs, Map<String, GroupMetadata> groups) {
        this.throttleTimeMs = throttleTimeMs;
        this.groups = groups;
    }

    public DescribeGroupsResponse(Struct struct) {
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
        this.groups = new HashMap<>();
        for (Object groupObj : struct.getArray(GROUPS_KEY_NAME)) {
            Struct groupStruct = (Struct) groupObj;

            String groupId = groupStruct.get(GROUP_ID);
            Errors error = Errors.forCode(groupStruct.get(ERROR_CODE));
            String state = groupStruct.getString(GROUP_STATE_KEY_NAME);
            String protocolType = groupStruct.getString(PROTOCOL_TYPE_KEY_NAME);
            String protocol = groupStruct.getString(PROTOCOL_KEY_NAME);

            List<GroupMember> members = new ArrayList<>();
            for (Object memberObj : groupStruct.getArray(MEMBERS_KEY_NAME)) {
                Struct memberStruct = (Struct) memberObj;
                String memberId = memberStruct.get(MEMBER_ID);
                String clientId = memberStruct.getString(CLIENT_ID_KEY_NAME);
                String clientHost = memberStruct.getString(CLIENT_HOST_KEY_NAME);
                ByteBuffer memberMetadata = memberStruct.getBytes(MEMBER_METADATA_KEY_NAME);
                ByteBuffer memberAssignment = memberStruct.getBytes(MEMBER_ASSIGNMENT_KEY_NAME);
                members.add(new GroupMember(memberId, clientId, clientHost,
                        memberMetadata, memberAssignment));
            }
            this.groups.put(groupId, new GroupMetadata(error, state, protocolType, protocol, members));
        }
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Map<String, GroupMetadata> groups() {
        return groups;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (GroupMetadata response : groups.values())
            updateErrorCounts(errorCounts, response.error);
        return errorCounts;
    }

    public static class GroupMetadata {
        private final Errors error;
        private final String state;
        private final String protocolType;
        private final String protocol;
        private final List<GroupMember> members;

        public GroupMetadata(Errors error,
                             String state,
                             String protocolType,
                             String protocol,
                             List<GroupMember> members) {
            this.error = error;
            this.state = state;
            this.protocolType = protocolType;
            this.protocol = protocol;
            this.members = members;
        }

        public Errors error() {
            return error;
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
                error,
                DescribeGroupsResponse.UNKNOWN_STATE,
                DescribeGroupsResponse.UNKNOWN_PROTOCOL_TYPE,
                DescribeGroupsResponse.UNKNOWN_PROTOCOL,
                Collections.emptyList());
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

    public static DescribeGroupsResponse fromError(Errors error, List<String> groupIds) {
        return fromError(DEFAULT_THROTTLE_TIME, error, groupIds);
    }

    public static DescribeGroupsResponse fromError(int throttleTimeMs, Errors error, List<String> groupIds) {
        GroupMetadata errorMetadata = GroupMetadata.forError(error);
        Map<String, GroupMetadata> groups = new HashMap<>();
        for (String groupId : groupIds)
            groups.put(groupId, errorMetadata);
        return new DescribeGroupsResponse(throttleTimeMs, groups);
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.DESCRIBE_GROUPS.responseSchema(version));
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);

        List<Struct> groupStructs = new ArrayList<>();
        for (Map.Entry<String, GroupMetadata> groupEntry : groups.entrySet()) {
            Struct groupStruct = struct.instance(GROUPS_KEY_NAME);
            GroupMetadata group = groupEntry.getValue();
            groupStruct.set(GROUP_ID, groupEntry.getKey());
            groupStruct.set(ERROR_CODE, group.error.code());
            groupStruct.set(GROUP_STATE_KEY_NAME, group.state);
            groupStruct.set(PROTOCOL_TYPE_KEY_NAME, group.protocolType);
            groupStruct.set(PROTOCOL_KEY_NAME, group.protocol);
            List<Struct> membersList = new ArrayList<>();
            for (GroupMember member : group.members) {
                Struct memberStruct = groupStruct.instance(MEMBERS_KEY_NAME);
                memberStruct.set(MEMBER_ID, member.memberId);
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

        return struct;
    }

    public static DescribeGroupsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeGroupsResponse(ApiKeys.DESCRIBE_GROUPS.parseResponse(version, buffer));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 2;
    }
}
