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

import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;

import org.junit.jupiter.params.ParameterizedTest;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StreamsGroupHeartbeatResponseTest {

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.STREAMS_GROUP_HEARTBEAT)
    public void testAcceptableRecoveryLagFieldVersionCompatibility(short version) {
        // Set the appropriate field based on version
        StreamsGroupHeartbeatResponseData data = new StreamsGroupHeartbeatResponseData()
            .setMemberId("test-member")
            .setMemberEpoch(1)
            .setHeartbeatIntervalMs(5000)
            .setTaskOffsetIntervalMs(60000);

        if (version == 0) {
            data.setAcceptableRecoveryLagLegacy(10000);  // int32 - version 0 only
        } else {
            data.setAcceptableRecoveryLag(20000L);       // int64 - version 1+
        }

        // Create response with specific version
        StreamsGroupHeartbeatResponse response = new StreamsGroupHeartbeatResponse(data);

        // Serialize and deserialize to simulate wire protocol
        StreamsGroupHeartbeatResponse parsedResponse =
            StreamsGroupHeartbeatResponse.parse(response.serialize(version), version);

        if (version == 0) {
            // Version 0: should have legacy field, new field should be default (-1)
            assertEquals(10000, parsedResponse.data().acceptableRecoveryLagLegacy(),
                "Version 0 response should include acceptableRecoveryLagLegacy");
            assertEquals(-1L, parsedResponse.data().acceptableRecoveryLag(),
                "Version 0 response should NOT include acceptableRecoveryLag (should be default -1)");
        } else {
            // Version 1+: should have new field, legacy field should be default (0)
            assertEquals(0, parsedResponse.data().acceptableRecoveryLagLegacy(),
                "Version 1+ response should NOT include acceptableRecoveryLagLegacy (should be default 0)");
            assertEquals(20000L, parsedResponse.data().acceptableRecoveryLag(),
                "Version 1+ response should include acceptableRecoveryLag");
        }

        // Common fields should be preserved across versions
        assertEquals("test-member", parsedResponse.data().memberId());
        assertEquals(1, parsedResponse.data().memberEpoch());
        assertEquals(5000, parsedResponse.data().heartbeatIntervalMs());
        assertEquals(60000, parsedResponse.data().taskOffsetIntervalMs());
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.STREAMS_GROUP_HEARTBEAT)
    public void testAcceptableRecoveryLagWithZeroValue(short version) {
        // Test that a config value of 0 is handled correctly
        StreamsGroupHeartbeatResponseData data = new StreamsGroupHeartbeatResponseData()
            .setMemberId("test-member")
            .setMemberEpoch(1)
            .setHeartbeatIntervalMs(5000)
            .setTaskOffsetIntervalMs(60000);

        if (version == 0) {
            data.setAcceptableRecoveryLagLegacy(0);  // Valid config value for v0
        } else {
            data.setAcceptableRecoveryLag(0L);       // Valid config value for v1+
        }

        StreamsGroupHeartbeatResponse response = new StreamsGroupHeartbeatResponse(data);
        StreamsGroupHeartbeatResponse parsedResponse =
            StreamsGroupHeartbeatResponse.parse(response.serialize(version), version);

        if (version == 0) {
            assertEquals(0, parsedResponse.data().acceptableRecoveryLagLegacy(),
                "Version 0 response should preserve acceptableRecoveryLagLegacy value of 0");
        } else {
            assertEquals(0L, parsedResponse.data().acceptableRecoveryLag(),
                "Version 1+ response should preserve acceptableRecoveryLag value of 0");
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.STREAMS_GROUP_HEARTBEAT)
    public void testAcceptableRecoveryLagWithMaxValue(short version) {
        // Test that large values work correctly, especially for version 1+ int64 field
        long maxValue = 1_000_000_000_000L; // 1 trillion - exceeds int32 range
        int cappedValue = Integer.MAX_VALUE; // Capped for int32 field

        StreamsGroupHeartbeatResponseData data = new StreamsGroupHeartbeatResponseData()
            .setMemberId("test-member")
            .setMemberEpoch(1)
            .setHeartbeatIntervalMs(5000)
            .setTaskOffsetIntervalMs(60000);

        if (version == 0) {
            data.setAcceptableRecoveryLagLegacy(cappedValue);
        } else {
            data.setAcceptableRecoveryLag(maxValue);
        }

        StreamsGroupHeartbeatResponse response = new StreamsGroupHeartbeatResponse(data);
        StreamsGroupHeartbeatResponse parsedResponse =
            StreamsGroupHeartbeatResponse.parse(response.serialize(version), version);

        if (version == 0) {
            assertEquals(cappedValue, parsedResponse.data().acceptableRecoveryLagLegacy(),
                "Version 0 response should preserve acceptableRecoveryLagLegacy at Integer.MAX_VALUE");
        } else {
            assertEquals(maxValue, parsedResponse.data().acceptableRecoveryLag(),
                "Version 1+ response should preserve full int64 value beyond Integer.MAX_VALUE");
        }
    }
}
