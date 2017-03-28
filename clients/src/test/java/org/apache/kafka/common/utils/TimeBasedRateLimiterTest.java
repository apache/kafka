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

import org.apache.kafka.common.utils.TimeBasedRateLimiter.Performer;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TimeBasedRateLimiterTest {
    @Test
    public void testPerform() {
        TimeBasedRateLimiter limiter = new TimeBasedRateLimiter(100, TimeUnit.MILLISECONDS);
        expectExpired(limiter, 1000);
        expectNotExpired(limiter, 1001);
        expectNotExpired(limiter, 1099);
        expectExpired(limiter, 1100);
        expectNotExpired(limiter, 0);
        expectNotExpired(limiter, 100);
        expectNotExpired(limiter, 1198);
        expectExpired(limiter, 1200);
    }

    private void expectNotExpired(TimeBasedRateLimiter limiter, long currentMs) {
        final AtomicBoolean succeeded = new AtomicBoolean(false);
        limiter.perform(currentMs, new Performer() {
            @Override
            public void timerExpired() {
                fail("Should have gotten timer not expired");
            }

            @Override
            public void timerNotExpired() {
                succeeded.compareAndSet(false, true);
            }
        });
        assertTrue(succeeded.get());
    }

    private void expectExpired(TimeBasedRateLimiter limiter, long currentMs) {
        final AtomicBoolean succeeded = new AtomicBoolean(false);
        limiter.perform(currentMs, new Performer() {
            @Override
            public void timerExpired() {
                succeeded.compareAndSet(false, true);
            }

            @Override
            public void timerNotExpired() {
                fail("Should have gotten timer expired");
            }
        });
        assertTrue(succeeded.get());
    }
}
