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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExponentialBackoffTest {
    @Test
    public void testExponentialBackoff() {
        long scaleFactor = 100;
        int ratio = 2;
        long backoffMax = 2000;
        double jitter = 0.2;
        ExponentialBackoff exponentialBackoff = new ExponentialBackoff(
                scaleFactor, ratio, backoffMax, jitter
        );

        for (int i = 0; i <= 100; i++) {
            for (int attempts = 0; attempts <= 10; attempts++) {
                if (attempts <= 4) {
                    assertEquals(scaleFactor * Math.pow(ratio, attempts),
                            exponentialBackoff.backoff(attempts),
                            scaleFactor * Math.pow(ratio, attempts) * jitter);
                } else {
                    assertTrue(exponentialBackoff.backoff(attempts) <= backoffMax * (1 + jitter));
                }
            }
        }
    }

    @Test
    public void testExponentialBackoffWithoutJitter() {
        ExponentialBackoff exponentialBackoff = new ExponentialBackoff(100, 2, 400, 0.0);
        assertEquals(100, exponentialBackoff.backoff(0));
        assertEquals(200, exponentialBackoff.backoff(1));
        assertEquals(400, exponentialBackoff.backoff(2));
        assertEquals(400, exponentialBackoff.backoff(3));
    }
}
