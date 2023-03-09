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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;


@Timeout(value = 120)
public class FutureUtilsTest {
    private static final Logger log = LoggerFactory.getLogger(FutureUtilsTest.class);

    @Test
    public void testWaitWithLogging() throws Throwable {
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
        CompletableFuture<Integer> future = new CompletableFuture<>();
        executorService.schedule(() -> future.complete(123), 1000, TimeUnit.NANOSECONDS);
        assertEquals(123, FutureUtils.waitWithLogging(log,
            "[FutureUtilsTest] ",
            "the future to be completed",
            future,
            Deadline.fromDelay(Time.SYSTEM, 30, TimeUnit.SECONDS),
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
                "[FutureUtilsTest] ",
                "the future to be completed",
                future,
                immediateTimeout ?
                    Deadline.fromDelay(Time.SYSTEM, 0, TimeUnit.SECONDS) :
                    Deadline.fromDelay(Time.SYSTEM, 1, TimeUnit.MILLISECONDS),
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
                    "[FutureUtilsTest] ",
                    "the future to be completed",
                    future,
                    Deadline.fromDelay(Time.SYSTEM, 30, TimeUnit.SECONDS),
                    Time.SYSTEM);
            }).getMessage());
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    @Test
    public void testChainFuture() throws Throwable {
        CompletableFuture<Integer> sourceFuture = new CompletableFuture<>();
        CompletableFuture<Number> destinationFuture = new CompletableFuture<>();
        FutureUtils.chainFuture(sourceFuture, destinationFuture);
        assertFalse(sourceFuture.isDone());
        assertFalse(destinationFuture.isDone());
        assertFalse(sourceFuture.isCancelled());
        assertFalse(destinationFuture.isCancelled());
        assertFalse(sourceFuture.isCompletedExceptionally());
        assertFalse(destinationFuture.isCompletedExceptionally());
        sourceFuture.complete(123);
        assertEquals(Integer.valueOf(123), destinationFuture.get());
    }

    @Test
    public void testChainFutureExceptionally() throws Throwable {
        CompletableFuture<Integer> sourceFuture = new CompletableFuture<>();
        CompletableFuture<Number> destinationFuture = new CompletableFuture<>();
        FutureUtils.chainFuture(sourceFuture, destinationFuture);
        sourceFuture.completeExceptionally(new RuntimeException("source failed"));
        Throwable cause = assertThrows(ExecutionException.class,
                () -> destinationFuture.get()).getCause();
        assertEquals(RuntimeException.class, cause.getClass());
        assertEquals("source failed", cause.getMessage());
    }
}
