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
package kafka.metrics;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientMetricsInstanceTest {

    private ClientMetricsInstance clientInstance;

    @BeforeEach
    public void setUp() throws UnknownHostException {
        Uuid uuid = Uuid.randomUuid();
        ClientMetricsInstanceMetadata instanceMetadata = new ClientMetricsInstanceMetadata(uuid,
            ClientMetricsTestUtils.requestContext());
        clientInstance = new ClientMetricsInstance(Uuid.randomUuid(), instanceMetadata, 0, 0,
            null, ClientMetricsConfigs.DEFAULT_INTERVAL_MS);
    }

    @Test
    public void testCanAcceptRequestValid() {
        // First request should be accepted.
        assertTrue(clientInstance.canAcceptRequest());
    }

    @Test
    public void testCanAcceptRequestAfterElapsedTimeValid() {
        assertTrue(clientInstance.canAcceptRequest());
        clientInstance.lastRequestEpoch(System.currentTimeMillis() - ClientMetricsConfigs.DEFAULT_INTERVAL_MS);
        // Second request should be accepted as time since last request is greater than the retry interval.
        assertTrue(clientInstance.canAcceptRequest());
    }

    @Test
    public void testCanAcceptRequestWithImmediateRetryFail() {
        assertTrue(clientInstance.canAcceptRequest());
        clientInstance.lastRequestEpoch(System.currentTimeMillis());
        // Second request should be rejected as time since last request is less than the retry interval.
        assertFalse(clientInstance.canAcceptRequest());
    }
}
