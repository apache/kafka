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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExponentialBackoffTest {
    @Test
    public void testGeometricProgression() {
        long scaleFactor = 100;
        int ratio = 2;
        long termMax = 2000;
        double jitter = 0.2;
        ExponentialBackoff exponentialBackoff = new ExponentialBackoff(
                scaleFactor, ratio, termMax, jitter
        );

        for (int i = 0; i <= 100; i++) {
            for (int n = 0; n <= 4; n++) {
                assertEquals(scaleFactor * Math.pow(ratio, n), exponentialBackoff.backoff(n),
                        scaleFactor * Math.pow(ratio, n) * jitter);
            }
            System.out.println(exponentialBackoff.backoff(5));
            assertTrue(exponentialBackoff.backoff(1000) <= termMax * (1 + jitter));
        }
    }
}
