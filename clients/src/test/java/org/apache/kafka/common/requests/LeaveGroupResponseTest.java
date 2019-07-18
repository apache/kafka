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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.common.requests.AbstractResponse.DEFAULT_THROTTLE_TIME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LeaveGroupResponseTest {
    private final int throttleTimeMs = 10;

    @Test
    public void testConstructor() {
        Map<Errors, Integer> expectedErrorCounts = Collections.singletonMap(Errors.NOT_COORDINATOR, 1);

        LeaveGroupResponseData responseData = new LeaveGroupResponseData()
                                                  .setErrorCode(Errors.NOT_COORDINATOR.code())
                                                  .setThrottleTimeMs(throttleTimeMs);
        for (short version = 0; version <= ApiKeys.LEAVE_GROUP.latestVersion(); version++) {
            LeaveGroupResponse leaveGroupResponse = new LeaveGroupResponse(responseData.toStruct(version), version);

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
    public void testShouldThrottle() {
        // A dummy setup is ok.
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
    public void testEquality() {
        LeaveGroupResponseData responseData = new LeaveGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setThrottleTimeMs(throttleTimeMs);
        for (short version = 0; version <= ApiKeys.LEAVE_GROUP.latestVersion(); version++) {
            LeaveGroupResponse primaryResponse = new LeaveGroupResponse(responseData.toStruct(version), version);

            LeaveGroupResponse secondaryResponse = new LeaveGroupResponse(responseData.toStruct(version), version);

            assertEquals(primaryResponse, primaryResponse);
            assertEquals(primaryResponse, secondaryResponse);
            assertEquals(primaryResponse.hashCode(), secondaryResponse.hashCode());
        }
    }
}
