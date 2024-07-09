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

import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.protocol.Errors;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumerGroupHeartbeatRequestTest {
    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {
        "UNSUPPORTED_VERSION",
        "GROUP_AUTHORIZATION_FAILED",
        "NOT_COORDINATOR",
        "COORDINATOR_NOT_AVAILABLE",
        "COORDINATOR_LOAD_IN_PROGRESS",
        "INVALID_REQUEST",
        "UNKNOWN_MEMBER_ID",
        "FENCED_MEMBER_EPOCH",
        "UNSUPPORTED_ASSIGNOR",
        "UNRELEASED_INSTANCE_ID",
        "GROUP_MAX_SIZE_REACHED"})
    void testGetErrorConsumerGroupHeartbeatResponse(Errors e) {
        ConsumerGroupHeartbeatRequestData data = new ConsumerGroupHeartbeatRequestData();
        ConsumerGroupHeartbeatRequest request = new ConsumerGroupHeartbeatRequest.Builder(data).build();
        int throttleTimeMs = 1000;
        ConsumerGroupHeartbeatResponse response = request.getErrorResponse(throttleTimeMs, e.exception());
        assertEquals(response.throttleTimeMs(), throttleTimeMs);
        assertEquals(response.data().errorCode(), e.code());
        assertEquals(response.data().errorMessage(), e.message());
    }
}
