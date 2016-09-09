/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class ExponentialReconnectAttemptPolicyTest {


    public static final long BASE_DELAY_MS = 2000L;

    @Test
    public void testSimpleExponentialReconnectionPolicy() {

        ExponentialReconnectAttemptPolicy policy = new ExponentialReconnectAttemptPolicy(BASE_DELAY_MS, BASE_DELAY_MS * 60 * 5);
        ReconnectAttemptPolicy.ReconnectAttemptScheduler scheduler = policy.newScheduler();
        assertEquals(2000, scheduler.nextReconnectBackoffMs());
        assertEquals(4000, scheduler.nextReconnectBackoffMs());
        assertEquals(8000, scheduler.nextReconnectBackoffMs());
        assertEquals(16000, scheduler.nextReconnectBackoffMs());
        assertEquals(32000, scheduler.nextReconnectBackoffMs());
        for (int i = 0; i < 64; i++) { // force overflow
            scheduler.nextReconnectBackoffMs();
        }
        assertEquals(policy.getMaxDelayMs(), scheduler.nextReconnectBackoffMs());
    }

    @Test
    public void testIllegalArgumentsExponentialReconnectionPolicy() {
        try {
            new ExponentialReconnectAttemptPolicy(-1, -1);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof IllegalArgumentException);
        }

        try {
            new ExponentialReconnectAttemptPolicy(100, 10);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof IllegalArgumentException);
        }
    }
}