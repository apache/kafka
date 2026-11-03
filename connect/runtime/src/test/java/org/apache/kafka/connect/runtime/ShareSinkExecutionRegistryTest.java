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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.util.ConnectorTaskId;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(30)
class ShareSinkExecutionRegistryTest {

    @Test
    void retainsCancelledExecutionUntilActualCleanupAndFencesLateCallbacks() throws Exception {
        ShareSinkExecutionRegistry registry = new ShareSinkExecutionRegistry();
        ConnectorTaskId id = new ConnectorTaskId("orders", 0);
        ConnectorTaskId rotatedId = new ConnectorTaskId("replacement-orders", 0);
        ShareSinkExecutionRegistry.Reservation original = registry.reserve(id);
        assertEquals(id, original.id());
        assertTrue(original.incarnation() > 0);
        assertThrows(ConnectException.class, () -> registry.reserve(id));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch ignoredInterrupt = new CountDownLatch(1);
        CountDownLatch permitExit = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        AtomicInteger releaseCallbacks = new AtomicInteger();
        AtomicInteger retirements = new AtomicInteger();
        original.onRetirement(() -> {
            assertFalse(Thread.holdsLock(registry));
            assertTrue(registry.contains(id));
            assertThrows(ConnectException.class, () -> registry.reserve(id));
            assertFalse(original.released().toCompletableFuture().isDone());
            retirements.incrementAndGet();
        });
        CompletableFuture<Void> released = original.released().thenRun(() -> {
            assertFalse(Thread.holdsLock(registry));
            releaseCallbacks.incrementAndGet();
        }).toCompletableFuture();
        ExecutorService plugin = Executors.newSingleThreadExecutor();
        Future<?> execution = plugin.submit(() -> {
            entered.countDown();
            try {
                boolean permitted = false;
                while (!permitted) {
                    try {
                        permitExit.await();
                        permitted = true;
                    } catch (InterruptedException ignored) {
                        ignoredInterrupt.countDown();
                    }
                }
            } finally {
                original.pluginExited(true);
                exited.countDown();
            }
        });

        try {
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            assertTrue(execution.cancel(true));
            assertTrue(ignoredInterrupt.await(10, TimeUnit.SECONDS));
            assertTrue(execution.isDone());
            original.markOrphan();
            original.markOrphan();
            original.controllerExited(true);

            assertTrue(registry.hasOrphans());
            assertTrue(registry.contains(id));
            assertEquals(1, registry.size());
            assertFalse(released.isDone());
            assertThrows(ConnectException.class, () -> registry.reserve(id));
            assertThrows(ConnectException.class, () -> registry.reserve(rotatedId));

            permitExit.countDown();
            assertTrue(exited.await(10, TimeUnit.SECONDS));
            released.get(10, TimeUnit.SECONDS);
            assertEquals(1, releaseCallbacks.get());
            assertEquals(1, retirements.get());
            assertFalse(registry.hasOrphans());
            assertFalse(registry.contains(id));
            assertEquals(0, registry.size());

            ShareSinkExecutionRegistry.Reservation replacement = registry.reserve(id);
            assertTrue(replacement.incarnation() > original.incarnation());
            original.markOrphan();
            original.pluginExited(false);
            original.controllerExited(false);
            assertFalse(registry.hasOrphans());
            assertTrue(registry.contains(id));
            assertEquals(1, releaseCallbacks.get());

            ShareSinkExecutionRegistry.Reservation failedRetirement = registry.reserve(rotatedId);
            ConnectException retirementFailure = new ConnectException("Old task metrics could not be removed");
            failedRetirement.onRetirement(() -> {
                throw retirementFailure;
            });
            failedRetirement.pluginExited(true);
            assertSame(retirementFailure, assertThrows(ConnectException.class, () -> failedRetirement.controllerExited(true)));
            assertTrue(registry.hasOrphans());
            assertTrue(registry.contains(rotatedId));
            assertFalse(failedRetirement.released().toCompletableFuture().isDone());

            replacement.pluginExited(false);
            replacement.controllerExited(true);
            replacement.pluginExited(true);
            assertTrue(registry.hasOrphans());
            assertEquals(2, registry.size());
            assertFalse(replacement.released().toCompletableFuture().isDone());
            assertThrows(ConnectException.class, () -> registry.reserve(id));
            assertThrows(ConnectException.class, () -> registry.reserve(rotatedId));
        } finally {
            permitExit.countDown();
            plugin.shutdownNow();
            assertTrue(plugin.awaitTermination(10, TimeUnit.SECONDS));
        }
    }
}
