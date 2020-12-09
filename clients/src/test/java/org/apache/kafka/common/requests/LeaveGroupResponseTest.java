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

import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.requests.AbstractResponse.DEFAULT_THROTTLE_TIME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LeaveGroupResponseTest {

    private final String memberIdOne = "member_1";
    private final String instanceIdOne = "instance_1";
    private final String memberIdTwo = "member_2";
    private final String instanceIdTwo = "instance_2";

    private final int throttleTimeMs = 10;

    private List<MemberResponse> memberResponses;

    @Before
    public void setUp() {
        memberResponses = Arrays.asList(new MemberResponse()
                                            .setMemberId(memberIdOne)
                                            .setGroupInstanceId(instanceIdOne)
                                            .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code()),
                                        new MemberResponse()
                                            .setMemberId(memberIdTwo)
                                            .setGroupInstanceId(instanceIdTwo)
                                            .setErrorCode(Errors.FENCED_INSTANCE_ID.code())
        );
    }

    @Test
    public void testConstructorWithMemberResponses() {
        Map<Errors, Integer> expectedErrorCounts = new HashMap<>();
        expectedErrorCounts.put(Errors.NONE, 1); // top level
        expectedErrorCounts.put(Errors.UNKNOWN_MEMBER_ID, 1);
        expectedErrorCounts.put(Errors.FENCED_INSTANCE_ID, 1);

        for (short version = 0; version <= ApiKeys.LEAVE_GROUP.latestVersion(); version++) {
            LeaveGroupResponse leaveGroupResponse = new LeaveGroupResponse(memberResponses,
                                                                           Errors.NONE,
                                                                           throttleTimeMs,
                                                                           version);

            if (version >= 3) {
                assertEquals(expectedErrorCounts, leaveGroupResponse.errorCounts());
                assertEquals(memberResponses, leaveGroupResponse.memberResponses());
            } else {
                assertEquals(Collections.singletonMap(Errors.UNKNOWN_MEMBER_ID, 1),
                             leaveGroupResponse.errorCounts());
                assertEquals(Collections.emptyList(), leaveGroupResponse.memberResponses());
            }

            if (version >= 1) {
                assertEquals(throttleTimeMs, leaveGroupResponse.throttleTimeMs());
            } else {
                assertEquals(DEFAULT_THROTTLE_TIME, leaveGroupResponse.throttleTimeMs());
            }

            assertEquals(Errors.UNKNOWN_MEMBER_ID, leaveGroupResponse.error());
        }
    }

    @Test
    public void testShouldThrottle() {
        LeaveGroupResponse response = new LeaveGroupResponse(new LeaveGroupResponseData());
        for (short version = 0; version <= ApiKeys.LEAVE_GROUP.latestVersion(); version++) {
            if (version >= 2) {
                assertTrue(response.shouldClientThrottle(version));
            } else {
                assertFalse(response.shouldClientThrottle(version));
            }
        }
    }

    @Test
    public void testEqualityWithSerialization() {
        LeaveGroupResponseData responseData = new LeaveGroupResponseData()
                .setErrorCode(Errors.NONE.code())
                .setThrottleTimeMs(throttleTimeMs);
        for (short version = 0; version <= ApiKeys.LEAVE_GROUP.latestVersion(); version++) {
            LeaveGroupResponse primaryResponse = LeaveGroupResponse.parse(
                MessageUtil.toByteBuffer(responseData, version), version);
            LeaveGroupResponse secondaryResponse = LeaveGroupResponse.parse(
                MessageUtil.toByteBuffer(responseData, version), version);

            assertEquals(primaryResponse, primaryResponse);
            assertEquals(primaryResponse, secondaryResponse);
            assertEquals(primaryResponse.hashCode(), secondaryResponse.hashCode());
        }
    }

    @Test
    public void testParse() {
        Map<Errors, Integer> expectedErrorCounts = Collections.singletonMap(Errors.NOT_COORDINATOR, 1);

        LeaveGroupResponseData data = new LeaveGroupResponseData()
            .setErrorCode(Errors.NOT_COORDINATOR.code())
            .setThrottleTimeMs(throttleTimeMs);

        for (short version = 0; version <= ApiKeys.LEAVE_GROUP.latestVersion(); version++) {
            ByteBuffer buffer = MessageUtil.toByteBuffer(data, version);
            LeaveGroupResponse leaveGroupResponse = LeaveGroupResponse.parse(buffer, version);
            assertEquals(expectedErrorCounts, leaveGroupResponse.errorCounts());

            if (version >= 1) {
                assertEquals(throttleTimeMs, leaveGroupResponse.throttleTimeMs());
            } else {
                assertEquals(DEFAULT_THROTTLE_TIME, leaveGroupResponse.throttleTimeMs());
            }

            assertEquals(Errors.NOT_COORDINATOR, leaveGroupResponse.error());
        }
    }

    @Test
    public void testEqualityWithMemberResponses() {
        for (short version = 0; version <= ApiKeys.LEAVE_GROUP.latestVersion(); version++) {
            List<MemberResponse> localResponses = version > 2 ? memberResponses : memberResponses.subList(0, 1);
            LeaveGroupResponse primaryResponse = new LeaveGroupResponse(localResponses,
                                                                        Errors.NONE,
                                                                        throttleTimeMs,
                                                                        version);

            // The order of members should not alter result data.
            Collections.reverse(localResponses);
            LeaveGroupResponse reversedResponse = new LeaveGroupResponse(localResponses,
                                                                         Errors.NONE,
                                                                         throttleTimeMs,
                                                                         version);

            assertEquals(primaryResponse, primaryResponse);
            assertEquals(primaryResponse, reversedResponse);
            assertEquals(primaryResponse.hashCode(), reversedResponse.hashCode());
        }
    }
}
