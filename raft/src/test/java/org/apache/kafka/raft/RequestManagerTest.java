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
package org.apache.kafka.raft;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequestManagerTest {
    private final MockTime time = new MockTime();
    private final int requestTimeoutMs = 30000;
    private final int retryBackoffMs = 100;
    private final Random random = new Random(1);

    @Test
    public void testResetAllConnections() {
        RequestManager cache = new RequestManager(
            Utils.mkSet(1, 2, 3),
            retryBackoffMs,
            requestTimeoutMs,
            random);

        // One host has an inflight request
        RequestManager.ConnectionState connectionState1 = cache.getOrCreate(1);
        connectionState1.onRequestSent(1, time.milliseconds());
        assertFalse(connectionState1.isReady(time.milliseconds()));

        // Another is backing off
        RequestManager.ConnectionState connectionState2 = cache.getOrCreate(2);
        connectionState2.onRequestSent(2, time.milliseconds());
        connectionState2.onResponseError(2, time.milliseconds());
        assertFalse(connectionState2.isReady(time.milliseconds()));

        cache.resetAll();

        // Now both should be ready
        assertTrue(connectionState1.isReady(time.milliseconds()));
        assertTrue(connectionState2.isReady(time.milliseconds()));
    }

    @Test
    public void testBackoffAfterFailure() {
        RequestManager cache = new RequestManager(
            Utils.mkSet(1, 2, 3),
            retryBackoffMs,
            requestTimeoutMs,
            random);

        RequestManager.ConnectionState connectionState = cache.getOrCreate(1);
        assertTrue(connectionState.isReady(time.milliseconds()));

        long correlationId = 1;
        connectionState.onRequestSent(correlationId, time.milliseconds());
        assertFalse(connectionState.isReady(time.milliseconds()));

        connectionState.onResponseError(correlationId, time.milliseconds());
        assertFalse(connectionState.isReady(time.milliseconds()));

        time.sleep(retryBackoffMs);
        assertTrue(connectionState.isReady(time.milliseconds()));
    }

    @Test
    public void testSuccessfulResponse() {
        RequestManager cache = new RequestManager(
            Utils.mkSet(1, 2, 3),
            retryBackoffMs,
            requestTimeoutMs,
            random);

        RequestManager.ConnectionState connectionState = cache.getOrCreate(1);

        long correlationId = 1;
        connectionState.onRequestSent(correlationId, time.milliseconds());
        assertFalse(connectionState.isReady(time.milliseconds()));
        connectionState.onResponseReceived(correlationId, time.milliseconds());
        assertTrue(connectionState.isReady(time.milliseconds()));
    }

    @Test
    public void testIgnoreUnexpectedResponse() {
        RequestManager cache = new RequestManager(
            Utils.mkSet(1, 2, 3),
            retryBackoffMs,
            requestTimeoutMs,
            random);

        RequestManager.ConnectionState connectionState = cache.getOrCreate(1);

        long correlationId = 1;
        connectionState.onRequestSent(correlationId, time.milliseconds());
        assertFalse(connectionState.isReady(time.milliseconds()));
        connectionState.onResponseReceived(correlationId + 1, time.milliseconds());
        assertFalse(connectionState.isReady(time.milliseconds()));
    }

    @Test
    public void testRequestTimeout() {
        RequestManager cache = new RequestManager(
            Utils.mkSet(1, 2, 3),
            retryBackoffMs,
            requestTimeoutMs,
            random);


        RequestManager.ConnectionState connectionState = cache.getOrCreate(1);

        long correlationId = 1;
        connectionState.onRequestSent(correlationId, time.milliseconds());
        assertFalse(connectionState.isReady(time.milliseconds()));

        time.sleep(requestTimeoutMs - 1);
        assertFalse(connectionState.isReady(time.milliseconds()));

        time.sleep(1);
        assertTrue(connectionState.isReady(time.milliseconds()));
    }

}
