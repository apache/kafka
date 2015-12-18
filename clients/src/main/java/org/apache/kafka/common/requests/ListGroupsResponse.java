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

public class ListGroupsResponse extends AbstractRequestResponse {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.LIST_GROUPS.id);

    public static final String ERROR_CODE_KEY_NAME = "error_code";
    public static final String GROUPS_KEY_NAME = "groups";
    public static final String GROUP_ID_KEY_NAME = "group_id";
    public static final String PROTOCOL_TYPE_KEY_NAME = "protocol_type";

    /**
     * Possible error codes:
     *
     * GROUP_COORDINATOR_NOT_AVAILABLE (15)
     * AUTHORIZATION_FAILED (29)
     */

    private final short errorCode;
    private final List<Group> groups;

    public ListGroupsResponse(short errorCode, List<Group> groups) {
        super(new Struct(CURRENT_SCHEMA));
        struct.set(ERROR_CODE_KEY_NAME, errorCode);
        List<Struct> groupList = new ArrayList<>();
        for (Group group : groups) {
            Struct groupStruct = struct.instance(GROUPS_KEY_NAME);
            groupStruct.set(GROUP_ID_KEY_NAME, group.groupId);
            groupStruct.set(PROTOCOL_TYPE_KEY_NAME, group.protocolType);
            groupList.add(groupStruct);
        }
        struct.set(GROUPS_KEY_NAME, groupList.toArray());
        this.errorCode = errorCode;
        this.groups = groups;
    }

    public ListGroupsResponse(Struct struct) {
        super(struct);
        this.errorCode = struct.getShort(ERROR_CODE_KEY_NAME);
        this.groups = new ArrayList<>();
        for (Object groupObj : struct.getArray(GROUPS_KEY_NAME)) {
            Struct groupStruct = (Struct) groupObj;
            String groupId = groupStruct.getString(GROUP_ID_KEY_NAME);
            String protocolType = groupStruct.getString(PROTOCOL_TYPE_KEY_NAME);
            this.groups.add(new Group(groupId, protocolType));
        }
    }

    public List<Group> groups() {
        return groups;
    }

    public short errorCode() {
        return errorCode;
    }

    public static class Group {
        private final String groupId;
        private final String protocolType;

        public Group(String groupId, String protocolType) {
            this.groupId = groupId;
            this.protocolType = protocolType;
        }

        public String groupId() {
            return groupId;
        }

        public String protocolType() {
            return protocolType;
        }

    }

    public static ListGroupsResponse parse(ByteBuffer buffer) {
        return new ListGroupsResponse(CURRENT_SCHEMA.read(buffer));
    }

    public static ListGroupsResponse fromError(Errors error) {
        return new ListGroupsResponse(error.code(), Collections.<Group>emptyList());
    }

}
