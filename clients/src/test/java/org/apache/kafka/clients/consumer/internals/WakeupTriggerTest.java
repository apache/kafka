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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class WakeupTriggerTest {
    private static long defaultTimeoutMs = 1000;
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
        wakeupTrigger.clearActiveTask();
        assertNull(wakeupTrigger.getPendingTask());
    }

    private void assertWakeupExceptionIsThrown(final CompletableFuture<?> future) {
        assertTrue(future.isCompletedExceptionally());
        try {
            future.get(defaultTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof WakeupException);
            return;
        } catch (Exception e) {
            fail("The task should throw an ExecutionException but got:" + e);
        }
        fail("The task should throw an ExecutionException");
    }
}
