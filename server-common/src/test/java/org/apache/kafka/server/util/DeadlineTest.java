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

package org.apache.kafka.server.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


@Timeout(value = 120)
public class DeadlineTest {
    private static final Logger log = LoggerFactory.getLogger(FutureUtilsTest.class);

    @Test
    public void testOneMillisecondDeadline() {
        assertEquals(TimeUnit.MILLISECONDS.toNanos(1),
                Deadline.fromDelay(0, 1, TimeUnit.MILLISECONDS).nanoseconds());
    }

    @Test
    public void testOneMillisecondDeadlineWithBase() {
        final long nowNs = 123456789L;
        assertEquals(nowNs + TimeUnit.MILLISECONDS.toNanos(1),
                Deadline.fromDelay(nowNs, 1, TimeUnit.MILLISECONDS).nanoseconds());
    }

    @Test
    public void testNegativeDelayFails() {
        assertEquals("Negative delays are not allowed.",
            assertThrows(RuntimeException.class,
                () -> Deadline.fromDelay(123456789L, -1L, TimeUnit.MILLISECONDS)).
                    getMessage());
    }

    @Test
    public void testMaximumDelay() {
        assertEquals(Long.MAX_VALUE,
                Deadline.fromDelay(123L, Long.MAX_VALUE, TimeUnit.HOURS).nanoseconds());
        assertEquals(Long.MAX_VALUE,
                Deadline.fromDelay(0, Long.MAX_VALUE / 2, TimeUnit.MILLISECONDS).nanoseconds());
        assertEquals(Long.MAX_VALUE,
                Deadline.fromDelay(Long.MAX_VALUE, Long.MAX_VALUE, TimeUnit.NANOSECONDS).nanoseconds());
    }
}
