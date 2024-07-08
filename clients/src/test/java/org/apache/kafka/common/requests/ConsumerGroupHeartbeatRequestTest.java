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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumerGroupHeartbeatRequestTest {
    private void testGetError(Errors e) {
        ConsumerGroupHeartbeatRequestData data = new ConsumerGroupHeartbeatRequestData();
        ConsumerGroupHeartbeatRequest request = new ConsumerGroupHeartbeatRequest.Builder(data).build();
        int throttleTimeMs = 1000;
        ConsumerGroupHeartbeatResponse response = request.getErrorResponse(throttleTimeMs, e.exception());
        assertEquals(response.throttleTimeMs(), throttleTimeMs);
        assertEquals(response.data().errorCode(), e.code());
        assertEquals(response.data().errorMessage(), e.message());
    }

    @Test
    void testGetErrorConsumerGroupHeartbeatResponse() {
        testGetError(Errors.UNSUPPORTED_VERSION);
        testGetError(Errors.GROUP_AUTHORIZATION_FAILED);
        testGetError(Errors.NOT_COORDINATOR);
        testGetError(Errors.COORDINATOR_NOT_AVAILABLE);
        testGetError(Errors.COORDINATOR_LOAD_IN_PROGRESS);
        testGetError(Errors.INVALID_REQUEST);
        testGetError(Errors.UNKNOWN_MEMBER_ID);
        testGetError(Errors.FENCED_MEMBER_EPOCH);
        testGetError(Errors.UNSUPPORTED_ASSIGNOR);
        testGetError(Errors.UNRELEASED_INSTANCE_ID);
        testGetError(Errors.GROUP_MAX_SIZE_REACHED);
    }
}
