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
package org.apache.kafka.server.purgatory;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DelayedFutureTest {

    @Test
    void testDelayedFuture() throws Exception {
        String purgatoryName = "testDelayedFuture";
        DelayedFuturePurgatory purgatory = new DelayedFuturePurgatory(purgatoryName, 0);
        try {
            AtomicInteger result = new AtomicInteger();

            Supplier<Boolean> hasExecutorThread = () -> Thread.getAllStackTraces().keySet().stream()
                .map(Thread::getName)
                .anyMatch(name -> name.contains("DelayedExecutor-" + purgatoryName));

            Consumer<List<CompletableFuture<Integer>>> updateResult = futures ->
                    result.set(futures.stream()
                            .filter(Predicate.not(CompletableFuture::isCompletedExceptionally))
                            .mapToInt(future -> assertDoesNotThrow(() -> future.get()))
                            .sum());

            assertFalse(hasExecutorThread.get(), "Unnecessary thread created");

            // Two completed futures: callback should be executed immediately on the same thread
            List<CompletableFuture<Integer>> futures1 = List.of(
                CompletableFuture.completedFuture(10),
                CompletableFuture.completedFuture(11)
            );
            DelayedFuture<Integer> r1 = purgatory.tryCompleteElseWatch(100000L, futures1, () -> updateResult.accept(futures1));
            assertTrue(r1.isCompleted(), "r1 not completed");
            assertEquals(21, result.get());
            assertFalse(hasExecutorThread.get(), "Unnecessary thread created");

            // Two delayed futures: callback should wait for both to complete
            result.set(-1);
            List<CompletableFuture<Integer>> futures2 = List.of(new CompletableFuture<>(), new CompletableFuture<>());
            DelayedFuture<Integer> r2 = purgatory.tryCompleteElseWatch(100000L, futures2, () -> updateResult.accept(futures2));
            assertFalse(r2.isCompleted(), "r2 should be incomplete");
            futures2.get(0).complete(20);
            assertFalse(r2.isCompleted());
            assertEquals(-1, result.get());
            futures2.get(1).complete(21);
            TestUtils.waitForCondition(r2::isCompleted, "r2 not completed");
            TestUtils.waitForCondition(() -> result.get() == 41, "callback not invoked");
            assertTrue(hasExecutorThread.get(), "Thread not created for executing delayed task");

            // One immediate and one delayed future: callback should wait for delayed task to complete
            result.set(-1);
            List<CompletableFuture<Integer>> futures3 = List.of(
                new CompletableFuture<>(),
                CompletableFuture.completedFuture(31)
            );
            DelayedFuture<Integer> r3 = purgatory.tryCompleteElseWatch(100000L, futures3, () -> updateResult.accept(futures3));
            assertFalse(r3.isCompleted(), "r3 should be incomplete");
            assertEquals(-1, result.get());
            futures3.get(0).complete(30);
            TestUtils.waitForCondition(r3::isCompleted, "r3 not completed");
            TestUtils.waitForCondition(() -> result.get() == 61, "callback not invoked");

            // One future doesn't complete within timeout. Should expire and invoke callback after timeout.
            result.set(-1);
            long start = Time.SYSTEM.hiResClockMs();
            long expirationMs = 2000L;
            List<CompletableFuture<Integer>> futures4 = List.of(new CompletableFuture<>(), new CompletableFuture<>());
            DelayedFuture<Integer> r4 = purgatory.tryCompleteElseWatch(expirationMs, futures4, () -> updateResult.accept(futures4));
            futures4.get(0).complete(40);
            TestUtils.waitForCondition(() -> futures4.get(1).isDone(), "r4 futures not expired");
            assertTrue(r4.isCompleted(), "r4 not completed after timeout");
            long elapsed = Time.SYSTEM.hiResClockMs() - start;
            assertTrue(elapsed >= expirationMs, "Time for expiration " + elapsed + " should at least " + expirationMs);
            assertEquals(40, futures4.get(0).get());
            Exception exception = assertThrows(ExecutionException.class, () -> futures4.get(1).get());
            assertEquals(TimeoutException.class, exception.getCause().getClass());
            TestUtils.waitForCondition(() -> result.get() == 40, "callback not invoked");
        } finally {
            purgatory.shutdown();
        }
    }
}
