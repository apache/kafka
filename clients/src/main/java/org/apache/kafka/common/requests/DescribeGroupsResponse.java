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

import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroup;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroupMember;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DescribeGroupsResponse extends AbstractResponse {

    /**
     * Possible per-group error codes:
     *
     * COORDINATOR_LOAD_IN_PROGRESS (14)
     * COORDINATOR_NOT_AVAILABLE (15)
     * NOT_COORDINATOR (16)
     * AUTHORIZATION_FAILED (29)
     */

    private DescribeGroupsResponseData data;

    public DescribeGroupsResponse(DescribeGroupsResponseData data) {
        this.data = data;
    }

    public DescribeGroupsResponse(Struct struct, short version) {
        this.data = new DescribeGroupsResponseData(struct, version);
    }

    public static DescribedGroupMember groupMember(
        final String memberId,
        final String clientId,
        final String clientHost,
        final byte[] assignment,
        final byte[] metadata) {
        return new DescribedGroupMember()
            .setMemberId(memberId)
            .setClientId(clientId)
            .setClientHost(clientHost)
            .setMemberAssignment(assignment)
            .setMemberMetadata(metadata);
    }

    public static DescribedGroup groupMetadata(
        final String groupId,
        final Errors error,
        final String state,
        final String protocolType,
        final String protocol,
        final List<DescribedGroupMember> members,
        final Set<Byte> authorizedOperations) {
        DescribedGroup groupMetada = new DescribedGroup();
        groupMetada.setGroupId(groupId)
            .setErrorCode(error.code())
            .setGroupState(state)
            .setProtocolType(protocolType)
            .setProtocolData(protocol)
            .setMembers(members)
            .setAuthorizedOperations(Utils.to32BitField(authorizedOperations));
        return  groupMetada;
    }

    public static DescribedGroup groupMetadata(
        final String groupId,
        final Errors error,
        final String state,
        final String protocolType,
        final String protocol,
        final List<DescribedGroupMember> members,
        final int authorizedOperations) {
        DescribedGroup groupMetada = new DescribedGroup();
        groupMetada.setGroupId(groupId)
            .setErrorCode(error.code())
            .setGroupState(state)
            .setProtocolType(protocolType)
            .setProtocolData(protocol)
            .setMembers(members)
            .setAuthorizedOperations(authorizedOperations);
        return  groupMetada;
    }

    public DescribeGroupsResponseData data() {
        return data;
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public static final String UNKNOWN_STATE = "";
    public static final String UNKNOWN_PROTOCOL_TYPE = "";
    public static final String UNKNOWN_PROTOCOL = "";

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        data.groups().forEach(describedGroup -> {
            updateErrorCounts(errorCounts, Errors.forCode(describedGroup.errorCode()));
        });
        return errorCounts;
    }

    public static DescribedGroup forError(String groupId, Errors error) {
        return groupMetadata(groupId, error, DescribeGroupsResponse.UNKNOWN_STATE, DescribeGroupsResponse.UNKNOWN_PROTOCOL_TYPE,
                DescribeGroupsResponse.UNKNOWN_PROTOCOL, Collections.emptyList(), Collections.emptySet());
    }

    public static DescribeGroupsResponse fromError(int throttleTimeMs, Errors error, List<String> groupIds) {
        DescribeGroupsResponseData describeGroupsResponseData = new DescribeGroupsResponseData();
        describeGroupsResponseData.setThrottleTimeMs(throttleTimeMs);
        for (String groupId : groupIds)
            describeGroupsResponseData.groups().add(DescribeGroupsResponse.forError(groupId, error));
        return new DescribeGroupsResponse(describeGroupsResponseData);
    }

    public static DescribeGroupsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeGroupsResponse(
                ApiKeys.DESCRIBE_GROUPS.responseSchema(version).read(buffer), version);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 2;
    }
}
