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
package org.apache.kafka.server.metrics;

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
        clientInstance = new ClientMetricsInstance(uuid, instanceMetadata, 0, 0, null, ClientMetricsConfigs.INTERVAL_MS_DEFAULT);
    }

    @Test
    public void testMaybeUpdateRequestTimestampValid() {
        // First request should be accepted.
        assertTrue(clientInstance.maybeUpdateGetRequestTimestamp(System.currentTimeMillis()));
        assertTrue(clientInstance.maybeUpdatePushRequestTimestamp(System.currentTimeMillis()));
    }

    @Test
    public void testMaybeUpdateGetRequestAfterElapsedTimeValid() {
        assertTrue(clientInstance.maybeUpdateGetRequestTimestamp(System.currentTimeMillis() - ClientMetricsConfigs.INTERVAL_MS_DEFAULT));
        // Second request should be accepted as time since last request is greater than the push interval.
        assertTrue(clientInstance.maybeUpdateGetRequestTimestamp(System.currentTimeMillis()));
    }

    @Test
    public void testMaybeUpdateGetRequestWithImmediateRetryFail() {
        assertTrue(clientInstance.maybeUpdateGetRequestTimestamp(System.currentTimeMillis()));
        // Second request should be rejected as time since last request is less than the push interval.
        assertFalse(clientInstance.maybeUpdateGetRequestTimestamp(System.currentTimeMillis()));
    }

    @Test
    public void testMaybeUpdatePushRequestAfterElapsedTimeValid() {
        assertTrue(clientInstance.maybeUpdatePushRequestTimestamp(System.currentTimeMillis() - ClientMetricsConfigs.INTERVAL_MS_DEFAULT));
        // Second request should be accepted as time since last request is greater than the push interval.
        assertTrue(clientInstance.maybeUpdatePushRequestTimestamp(System.currentTimeMillis()));
    }

    @Test
    public void testMaybeUpdateGetRequestWithImmediateRetryAfterPushFail() {
        assertTrue(clientInstance.maybeUpdatePushRequestTimestamp(System.currentTimeMillis()));
        // Next request after push should be rejected as time since last request is less than the push interval.
        assertFalse(clientInstance.maybeUpdateGetRequestTimestamp(System.currentTimeMillis() + 1));
    }

    @Test
    public void testMaybeUpdatePushRequestWithImmediateRetryFail() {
        assertTrue(clientInstance.maybeUpdatePushRequestTimestamp(System.currentTimeMillis()));
        // Second request should be rejected as time since last request is less than the push interval.
        assertFalse(clientInstance.maybeUpdatePushRequestTimestamp(System.currentTimeMillis()));
    }

    @Test
    public void testMaybeUpdatePushRequestWithImmediateRetryAfterGetValid() {
        assertTrue(clientInstance.maybeUpdatePushRequestTimestamp(System.currentTimeMillis() - ClientMetricsConfigs.INTERVAL_MS_DEFAULT));
        assertTrue(clientInstance.maybeUpdateGetRequestTimestamp(System.currentTimeMillis()));
        // Next request after get should be accepted.
        assertTrue(clientInstance.maybeUpdatePushRequestTimestamp(System.currentTimeMillis() + 1));
    }
}
