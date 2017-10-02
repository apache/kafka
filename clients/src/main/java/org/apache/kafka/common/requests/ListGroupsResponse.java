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
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.types.Type.STRING;

public class ListGroupsResponse extends AbstractResponse {

    private static final String GROUPS_KEY_NAME = "groups";
    private static final String GROUP_ID_KEY_NAME = "group_id";
    private static final String PROTOCOL_TYPE_KEY_NAME = "protocol_type";

    private static final Schema LIST_GROUPS_RESPONSE_GROUP_V0 = new Schema(
            new Field(GROUP_ID_KEY_NAME, STRING),
            new Field(PROTOCOL_TYPE_KEY_NAME, STRING));
    private static final Schema LIST_GROUPS_RESPONSE_V0 = new Schema(
            ERROR_CODE,
            new Field(GROUPS_KEY_NAME, new ArrayOf(LIST_GROUPS_RESPONSE_GROUP_V0)));
    private static final Schema LIST_GROUPS_RESPONSE_V1 = new Schema(
            THROTTLE_TIME_MS,
            ERROR_CODE,
            new Field(GROUPS_KEY_NAME, new ArrayOf(LIST_GROUPS_RESPONSE_GROUP_V0)));

    public static Schema[] schemaVersions() {
        return new Schema[] {LIST_GROUPS_RESPONSE_V0, LIST_GROUPS_RESPONSE_V1};
    }

    /**
     * Possible error codes:
     *
     * COORDINATOR_NOT_AVAILABLE (15)
     * AUTHORIZATION_FAILED (29)
     */

    private final Errors error;
    private final int throttleTimeMs;
    private final List<Group> groups;

    public ListGroupsResponse(Errors error, List<Group> groups) {
        this(DEFAULT_THROTTLE_TIME, error, groups);
    }

    public ListGroupsResponse(int throttleTimeMs, Errors error, List<Group> groups) {
        this.throttleTimeMs = throttleTimeMs;
        this.error = error;
        this.groups = groups;
    }

    public ListGroupsResponse(Struct struct) {
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
        this.error = Errors.forCode(struct.get(ERROR_CODE));
        this.groups = new ArrayList<>();
        for (Object groupObj : struct.getArray(GROUPS_KEY_NAME)) {
            Struct groupStruct = (Struct) groupObj;
            String groupId = groupStruct.getString(GROUP_ID_KEY_NAME);
            String protocolType = groupStruct.getString(PROTOCOL_TYPE_KEY_NAME);
            this.groups.add(new Group(groupId, protocolType));
        }
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public List<Group> groups() {
        return groups;
    }

    public Errors error() {
        return error;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
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

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.LIST_GROUPS.responseSchema(version));
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);
        struct.set(ERROR_CODE, error.code());
        List<Struct> groupList = new ArrayList<>();
        for (Group group : groups) {
            Struct groupStruct = struct.instance(GROUPS_KEY_NAME);
            groupStruct.set(GROUP_ID_KEY_NAME, group.groupId);
            groupStruct.set(PROTOCOL_TYPE_KEY_NAME, group.protocolType);
            groupList.add(groupStruct);
        }
        struct.set(GROUPS_KEY_NAME, groupList.toArray());
        return struct;
    }

    public static ListGroupsResponse parse(ByteBuffer buffer, short version) {
        return new ListGroupsResponse(ApiKeys.LIST_GROUPS.parseResponse(version, buffer));
    }

}
