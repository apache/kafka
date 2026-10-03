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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsRequestData;
import org.apache.kafka.common.protocol.Errors;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GetTelemetrySubscriptionsRequestTest {

    @Test
    public void testGetErrorResponse() {
        GetTelemetrySubscriptionsRequest req = new GetTelemetrySubscriptionsRequest(new GetTelemetrySubscriptionsRequestData(), (short) 0);
        GetTelemetrySubscriptionsResponse response = req.getErrorResponse(0, Errors.CLUSTER_AUTHORIZATION_FAILED.exception());
        assertEquals(Collections.singletonMap(Errors.CLUSTER_AUTHORIZATION_FAILED, 1), response.errorCounts());
    }

    @Test
    public void testBuildV1ClearsClientInstanceIdInBody() {
        Uuid clientInstanceId = Uuid.randomUuid();
        GetTelemetrySubscriptionsRequest.Builder builder = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData().setClientInstanceId(clientInstanceId), true);

        assertEquals(clientInstanceId, builder.build((short) 0).data().clientInstanceId());

        // In v1 the ID travels in the request header.
        GetTelemetrySubscriptionsRequest v1 = builder.build((short) 1);
        assertEquals(Uuid.ZERO_UUID, v1.data().clientInstanceId());
        // Building v1 must not mutate the data the builder was given.
        assertEquals(clientInstanceId, builder.build((short) 0).data().clientInstanceId());
    }
}
