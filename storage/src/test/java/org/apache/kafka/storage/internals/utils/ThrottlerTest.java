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
package org.apache.kafka.storage.internals.utils;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.MockTime;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


class ThrottlerTest {

    @Test
    public void testThrottleDesiredRate() {
        long throttleCheckIntervalMs = 100L;
        double desiredCountPerSec = 1000.0;
        double desiredCountPerInterval = desiredCountPerSec * throttleCheckIntervalMs / 1000.0;

        Time mockTime = new MockTime();
        Throttler throttler = new Throttler(desiredCountPerSec,
                                            throttleCheckIntervalMs,
                                            "throttler",
                                            "entries",
                                            mockTime);

        // Observe desiredCountPerInterval at t1
        long t1 = mockTime.milliseconds();
        throttler.maybeThrottle(desiredCountPerInterval);
        assertEquals(t1, mockTime.milliseconds());

        // Observe desiredCountPerInterval at t1 + throttleCheckIntervalMs + 1,
        mockTime.sleep(throttleCheckIntervalMs + 1);
        throttler.maybeThrottle(desiredCountPerInterval);
        long t2 = mockTime.milliseconds();
        assertTrue(t2 >= t1 + 2 * throttleCheckIntervalMs);

        // Observe desiredCountPerInterval at t2
        throttler.maybeThrottle(desiredCountPerInterval);
        assertEquals(t2, mockTime.milliseconds());

        // Observe desiredCountPerInterval at t2 + throttleCheckIntervalMs + 1
        mockTime.sleep(throttleCheckIntervalMs + 1);
        throttler.maybeThrottle(desiredCountPerInterval);
        long t3 = mockTime.milliseconds();
        assertTrue(t3 >= t2 + 2 * throttleCheckIntervalMs);

        long elapsedTimeMs = t3 - t1;
        double actualCountPerSec = 4 * desiredCountPerInterval * 1000 / elapsedTimeMs;
        assertTrue(actualCountPerSec <= desiredCountPerSec);
    }

    @Test
    public void testUpdateThrottleDesiredRate() {
        long throttleCheckIntervalMs = 100L;
        double desiredCountPerSec = 1000.0;
        double desiredCountPerInterval = desiredCountPerSec * throttleCheckIntervalMs / 1000.0;
        double updatedDesiredCountPerSec = 1500.0;
        double updatedDesiredCountPerInterval = updatedDesiredCountPerSec * throttleCheckIntervalMs / 1000.0;

        Time mockTime = new MockTime();
        Throttler throttler = new Throttler(desiredCountPerSec,
                                            throttleCheckIntervalMs,
                                            "throttler",
                                            "entries",
                                            mockTime);

        // Observe desiredCountPerInterval at t1
        long t1 = mockTime.milliseconds();
        throttler.maybeThrottle(desiredCountPerInterval);
        assertEquals(t1, mockTime.milliseconds());

        // Observe desiredCountPerInterval at t1 + throttleCheckIntervalMs + 1,
        mockTime.sleep(throttleCheckIntervalMs + 1);
        throttler.maybeThrottle(desiredCountPerInterval);
        long t2 = mockTime.milliseconds();
        assertTrue(t2 >= t1 + 2 * throttleCheckIntervalMs);

        long elapsedTimeMs = t2 - t1;
        double actualCountPerSec = 2 * desiredCountPerInterval * 1000 / elapsedTimeMs;
        assertTrue(actualCountPerSec <= desiredCountPerSec);

        // Update ThrottleDesiredRate
        throttler.updateDesiredRatePerSec(updatedDesiredCountPerSec);

        // Observe updatedDesiredCountPerInterval at t2
        throttler.maybeThrottle(updatedDesiredCountPerInterval);
        assertEquals(t2, mockTime.milliseconds());

        // Observe updatedDesiredCountPerInterval at t2 + throttleCheckIntervalMs + 1
        mockTime.sleep(throttleCheckIntervalMs + 1);
        throttler.maybeThrottle(updatedDesiredCountPerInterval);
        long t3 = mockTime.milliseconds();
        assertTrue(t3 >= t2 + 2 * throttleCheckIntervalMs);

        long updatedElapsedTimeMs = t3 - t2;
        double updatedActualCountPerSec = 2 * updatedDesiredCountPerInterval * 1000 / updatedElapsedTimeMs;
        assertTrue(updatedActualCountPerSec <= updatedDesiredCountPerSec);
    }
}
