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

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class LeaveGroupRequestTest {

    private final String groupId = "group_id";
    private final String memberIdOne = "member_1";
    private final String instanceIdOne = "instance_1";
    private final String memberIdTwo = "member_2";
    private final String instanceIdTwo = "instance_2";

    private final int throttleTimeMs = 10;

    private LeaveGroupRequest.Builder builder;
    private List<MemberIdentity> members;

    @BeforeEach
    public void setUp() {
        members = Arrays.asList(new MemberIdentity()
                                         .setMemberId(memberIdOne)
                                         .setGroupInstanceId(instanceIdOne),
                                new MemberIdentity()
                                         .setMemberId(memberIdTwo)
                                         .setGroupInstanceId(instanceIdTwo));
        builder = new LeaveGroupRequest.Builder(
            groupId,
            members
        );
    }

    @Test
    public void testMultiLeaveConstructor() {
        final LeaveGroupRequestData expectedData = new LeaveGroupRequestData()
                                                       .setGroupId(groupId)
                                                       .setMembers(members);

        for (short version : ApiKeys.LEAVE_GROUP.allVersions()) {
            try {
                LeaveGroupRequest request = builder.build(version);
                if (version <= 2) {
                    fail("Older version " + version +
                             " request data should not be created due to non-single members");
                }
                assertEquals(expectedData, request.data());
                assertEquals(members, request.members());

                LeaveGroupResponse expectedResponse = new LeaveGroupResponse(
                    Collections.emptyList(),
                    Errors.COORDINATOR_LOAD_IN_PROGRESS,
                    throttleTimeMs,
                    version
                );

                assertEquals(expectedResponse, request.getErrorResponse(throttleTimeMs,
                                                                        Errors.COORDINATOR_LOAD_IN_PROGRESS.exception()));
            } catch (UnsupportedVersionException e) {
                assertTrue(e.getMessage().contains("leave group request only supports single member instance"));
            }
        }

    }

    @Test
    public void testSingleLeaveConstructor() {
        final LeaveGroupRequestData expectedData = new LeaveGroupRequestData()
                                                       .setGroupId(groupId)
                                                       .setMemberId(memberIdOne);
        List<MemberIdentity> singleMember = Collections.singletonList(
            new MemberIdentity()
                .setMemberId(memberIdOne));

        builder = new LeaveGroupRequest.Builder(groupId, singleMember);

        for (short version = 0; version <= 2; version++) {
            LeaveGroupRequest request = builder.build(version);
            assertEquals(expectedData, request.data());
            assertEquals(singleMember, request.members());

            int expectedThrottleTime = version >= 1 ? throttleTimeMs
                                           : AbstractResponse.DEFAULT_THROTTLE_TIME;
            LeaveGroupResponse expectedResponse = new LeaveGroupResponse(
                new LeaveGroupResponseData()
                    .setErrorCode(Errors.NOT_CONTROLLER.code())
                    .setThrottleTimeMs(expectedThrottleTime)
            );

            assertEquals(expectedResponse, request.getErrorResponse(throttleTimeMs,
                                                                    Errors.NOT_CONTROLLER.exception()));
        }
    }

    @Test
    public void testBuildEmptyMembers() {
        assertThrows(IllegalArgumentException.class,
            () -> new LeaveGroupRequest.Builder(groupId, Collections.emptyList()));
    }
}
