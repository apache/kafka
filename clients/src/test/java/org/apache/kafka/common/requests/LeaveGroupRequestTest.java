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

import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LeaveGroupRequestTest {

    @Test
    public void testLeaveConstructor() {
        final String groupId = "group_id";
        final String memberId = "member_id";
        final int throttleTimeMs = 10;

        final LeaveGroupRequestData expectedData = new LeaveGroupRequestData()
                                                       .setGroupId(groupId)
                                                       .setMemberId(memberId);

        final LeaveGroupRequest.Builder builder =
            new LeaveGroupRequest.Builder(new LeaveGroupRequestData()
                                              .setGroupId(groupId)
                                              .setMemberId(memberId));

        for (short version = 0; version <= ApiKeys.LEAVE_GROUP.latestVersion(); version++) {
            LeaveGroupRequest request = builder.build(version);
            assertEquals(expectedData, request.data());

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
}
