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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class WakeupTriggerTest {
    private final static long DEFAULT_TIMEOUT_MS = 1000;
    private WakeupTrigger wakeupTrigger;

    @BeforeEach
    public void setup() {
        this.wakeupTrigger = new WakeupTrigger();
    }
    
    @Test
    public void testEnsureActiveFutureCanBeWakeUp() {
        CompletableFuture<Void> task = new CompletableFuture<>();
        wakeupTrigger.setActiveTask(task);
        wakeupTrigger.wakeup();
        assertWakeupExceptionIsThrown(task);
        assertNull(wakeupTrigger.getPendingTask());
    }

    @Test
    public void testSettingActiveFutureAfterWakeupShouldThrow() {
        wakeupTrigger.wakeup();
        CompletableFuture<Void> task = new CompletableFuture<>();
        wakeupTrigger.setActiveTask(task);
        assertWakeupExceptionIsThrown(task);
        assertNull(wakeupTrigger.getPendingTask());
    }

    @Test
    public void testUnsetActiveFuture() {
        CompletableFuture<Void> task = new CompletableFuture<>();
        wakeupTrigger.setActiveTask(task);
        wakeupTrigger.clearTask();
        assertNull(wakeupTrigger.getPendingTask());
    }

    @Test
    public void testSettingFetchAction() {
        try (final FetchBuffer fetchBuffer = mock(FetchBuffer.class)) {
            wakeupTrigger.setFetchAction(fetchBuffer);

            final WakeupTrigger.Wakeupable wakeupable = wakeupTrigger.getPendingTask();
            assertInstanceOf(WakeupTrigger.FetchAction.class, wakeupable);
            assertEquals(fetchBuffer, ((WakeupTrigger.FetchAction) wakeupable).fetchBuffer());
        }
    }

    @Test
    public void testUnsetFetchAction() {
        try (final FetchBuffer fetchBuffer = mock(FetchBuffer.class)) {
            wakeupTrigger.setFetchAction(fetchBuffer);

            wakeupTrigger.clearTask();

            assertNull(wakeupTrigger.getPendingTask());
        }
    }

    @Test
    public void testWakeupFromFetchAction() {
        try (final FetchBuffer fetchBuffer = mock(FetchBuffer.class)) {
            wakeupTrigger.setFetchAction(fetchBuffer);

            wakeupTrigger.wakeup();

            verify(fetchBuffer).wakeup();
            final WakeupTrigger.Wakeupable wakeupable = wakeupTrigger.getPendingTask();
            assertInstanceOf(WakeupTrigger.WakeupFuture.class, wakeupable);
        }
    }

    @Test
    public void testManualTriggerWhenWakeupCalled() {
        wakeupTrigger.wakeup();
        assertThrows(WakeupException.class, () -> wakeupTrigger.maybeTriggerWakeup());
    }

    @Test
    public void testManualTriggerWhenWakeupNotCalled() {
        assertDoesNotThrow(() -> wakeupTrigger.maybeTriggerWakeup());
    }

    @Test
    public void testManualTriggerWhenWakeupCalledAndActiveTaskSet() {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        wakeupTrigger.setActiveTask(future);
        assertDoesNotThrow(() -> wakeupTrigger.maybeTriggerWakeup());
    }

    @Test
    public void testManualTriggerWhenWakeupCalledAndFetchActionSet() {
        try (final FetchBuffer fetchBuffer = mock(FetchBuffer.class)) {
            wakeupTrigger.setFetchAction(fetchBuffer);
            assertDoesNotThrow(() -> wakeupTrigger.maybeTriggerWakeup());
        }
    }

    @Test
    public void testDisableWakeupWithoutPendingTask() {
        wakeupTrigger.disableWakeups();
        wakeupTrigger.wakeup();
        assertDoesNotThrow(() -> wakeupTrigger.maybeTriggerWakeup());
    }

    @Test
    public void testDisableWakeupWithPendingTask() {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        wakeupTrigger.disableWakeups();
        wakeupTrigger.setActiveTask(future);
        wakeupTrigger.wakeup();
        assertFalse(future.isCompletedExceptionally());
        assertDoesNotThrow(() -> wakeupTrigger.maybeTriggerWakeup());
    }

    @Test
    public void testDisableWakeupWithFetchAction() {
        try (final FetchBuffer fetchBuffer = mock(FetchBuffer.class)) {
            wakeupTrigger.disableWakeups();
            wakeupTrigger.setFetchAction(fetchBuffer);
            wakeupTrigger.wakeup();
            verify(fetchBuffer, never()).wakeup();
        }
    }

    @Test
    public void testDisableWakeupPreservedByClearTask() {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        wakeupTrigger.disableWakeups();
        wakeupTrigger.setActiveTask(future);
        wakeupTrigger.clearTask();
        wakeupTrigger.wakeup();
        assertDoesNotThrow(() -> wakeupTrigger.maybeTriggerWakeup());
    }

    @Test
    public void testExceptionTriggeredWhenTaskAsynchronouslyCompleted() {
        final CompletableFuture<Void> task = new CompletableFuture<>();
        wakeupTrigger.setActiveTask(task);
        task.complete(null);
        wakeupTrigger.wakeup();
        assertNotNull(wakeupTrigger.getPendingTask());
        assertInstanceOf(WakeupTrigger.WakeupFuture.class, wakeupTrigger.getPendingTask());
        assertThrows(WakeupException.class, () -> wakeupTrigger.maybeTriggerWakeup());
    }

    @Test
    public void testExceptionTriggeredWhenTaskAsynchronouslyFailed() {
        final CompletableFuture<Void> task = new CompletableFuture<>();
        wakeupTrigger.setActiveTask(task);
        task.completeExceptionally(new RuntimeException("Simulated error"));
        wakeupTrigger.wakeup();
        assertNotNull(wakeupTrigger.getPendingTask());
        assertInstanceOf(WakeupTrigger.WakeupFuture.class, wakeupTrigger.getPendingTask());
        assertThrows(WakeupException.class, () -> wakeupTrigger.maybeTriggerWakeup());
    }

    @Test
    public void testExceptionTriggeredWhenTaskAsynchronouslyCancelled() {
        final CompletableFuture<Void> task = new CompletableFuture<>();
        wakeupTrigger.setActiveTask(task);
        task.cancel(true);
        wakeupTrigger.wakeup();
        assertNotNull(wakeupTrigger.getPendingTask());
        assertInstanceOf(WakeupTrigger.WakeupFuture.class, wakeupTrigger.getPendingTask());
        assertThrows(WakeupException.class, () -> wakeupTrigger.maybeTriggerWakeup());
    }

    private void assertWakeupExceptionIsThrown(final CompletableFuture<?> future) {
        assertTrue(future.isCompletedExceptionally());
        assertInstanceOf(WakeupException.class,
            assertThrows(ExecutionException.class, () -> future.get(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS)).getCause());
    }
}
