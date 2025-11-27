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

package org.apache.kafka.common.utils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExponentialBackoffManagerTest {

    private static final ArrayList<Long> BACKOFF_LIST = new ArrayList<>(List.of(100L, 200L, 400L, 800L, 1600L));

    @Test
    public void testInitialState() {
        ExponentialBackoffManager manager = new ExponentialBackoffManager(
            5, 100, 2, 1000, 0.0);
        assertEquals(0, manager.attempts());
        assertTrue(manager.canAttempt());
    }

    @Test
    public void testIncrementAttempt() {
        ExponentialBackoffManager manager = new ExponentialBackoffManager(
                5, 100, 2, 1000, 0.0);
        assertEquals(0, manager.attempts());
        manager.incrementAttempt();
        assertEquals(1, manager.attempts());
    }

    @Test
    public void testResetAttempts() {
        ExponentialBackoffManager manager = new ExponentialBackoffManager(
                5, 100, 2, 1000, 0.0);
        manager.incrementAttempt();
        manager.incrementAttempt();
        manager.incrementAttempt();
        assertEquals(3, manager.attempts());
        
        manager.resetAttempts();
        assertEquals(0, manager.attempts());
        assertTrue(manager.canAttempt());
    }

    @Test
    public void testCanAttempt() {
        ExponentialBackoffManager manager = new ExponentialBackoffManager(
                3, 100, 2, 1000, 0.0);
        // Initially can attempt
        assertTrue(manager.canAttempt());
        assertEquals(0, manager.attempts());

        manager.incrementAttempt();
        manager.incrementAttempt();
        manager.incrementAttempt();
        // After all retry attempts are exhausted
        assertFalse(manager.canAttempt());
        assertEquals(3, manager.attempts());
    }

    @Test
    public void testBackOffWithoutJitter() {
        ExponentialBackoffManager manager = new ExponentialBackoffManager(
                5, 100, 2, 1000, 0.0);
        for (int i = 0; i < 5; i++) {
            long backoff = manager.backOff();
            // without jitter, the backoff values should be exact multiples.
            assertEquals(Math.min(1000L, BACKOFF_LIST.get(i)), backoff);
            manager.incrementAttempt();
        }
    }

    @Test
    public void testBackOffWithJitter() {
        ExponentialBackoffManager manager = new ExponentialBackoffManager(
                5, 100, 2, 1000, 0.2);
        for (int i = 0; i < 5; i++) {
            long backoff = manager.backOff();
            // with jitter, the backoff values should be within 20% of the expected value.
            assertTrue(backoff >= 0.8 * Math.min(1000L, BACKOFF_LIST.get(i)));
            assertTrue(backoff <= 1.2 * Math.min(1000L, BACKOFF_LIST.get(i)));
            manager.incrementAttempt();
        }
    }
}