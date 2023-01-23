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

import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


@Timeout(value = 120)
public class FutureUtilsTest {
    private static final Logger log = LoggerFactory.getLogger(FutureUtilsTest.class);

    @Test
    public void testOneMillisecondDeadline() {
        assertEquals(TimeUnit.MILLISECONDS.toNanos(1), FutureUtils.getDeadlineNsFromDelayMs(0, 1));
    }

    @Test
    public void testOneMillisecondDeadlineWithBase() {
        final long nowNs = 123456789L;
        assertEquals(nowNs + TimeUnit.MILLISECONDS.toNanos(1), FutureUtils.getDeadlineNsFromDelayMs(nowNs, 1));
    }

    @Test
    public void testNegativeDelayFails() {
        assertEquals("Negative delays are not allowed.",
            assertThrows(RuntimeException.class,
                () -> FutureUtils.getDeadlineNsFromDelayMs(123456789L, -1L)).
                    getMessage());
    }

    @Test
    public void testMaximumDelay() {
        assertEquals(Long.MAX_VALUE, FutureUtils.getDeadlineNsFromDelayMs(123L, Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, FutureUtils.getDeadlineNsFromDelayMs(Long.MAX_VALUE / 2, Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, FutureUtils.getDeadlineNsFromDelayMs(Long.MAX_VALUE, Long.MAX_VALUE));
    }

    @Test
    public void testWaitWithLogging() throws Throwable {
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
        CompletableFuture<Integer> future = new CompletableFuture<>();
        executorService.schedule(() -> future.complete(123), 1000, TimeUnit.NANOSECONDS);
        assertEquals(123, FutureUtils.waitWithLogging(log,
            "the future to be completed",
            future,
            FutureUtils.getDeadlineNsFromDelayMs(Time.SYSTEM.nanoseconds(), 15000),
            Time.SYSTEM));
        executorService.shutdownNow();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testWaitWithLoggingTimeout(boolean immediateTimeout) throws Throwable {
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
        CompletableFuture<Integer> future = new CompletableFuture<>();
        executorService.schedule(() -> future.complete(456), 10000, TimeUnit.MILLISECONDS);
        assertThrows(TimeoutException.class, () -> {
            FutureUtils.waitWithLogging(log,
                "the future to be completed",
                future,
                immediateTimeout ?
                    Time.SYSTEM.nanoseconds() - 10000 :
                    Time.SYSTEM.nanoseconds() + 10000,
                Time.SYSTEM);
        });
        executorService.shutdownNow();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    @Test
    public void testWaitWithLoggingError() throws Throwable {
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
        CompletableFuture<Integer> future = new CompletableFuture<>();
        executorService.schedule(() -> {
            future.completeExceptionally(new IllegalArgumentException("uh oh"));
        }, 1, TimeUnit.NANOSECONDS);
        assertEquals("Received a fatal error while waiting for the future to be completed",
            assertThrows(RuntimeException.class, () -> {
                FutureUtils.waitWithLogging(log,
                        "the future to be completed",
                        future,
                        FutureUtils.getDeadlineNsFromDelayMs(Time.SYSTEM.nanoseconds(), 5000),
                        Time.SYSTEM);
            }).getMessage());
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }
}
