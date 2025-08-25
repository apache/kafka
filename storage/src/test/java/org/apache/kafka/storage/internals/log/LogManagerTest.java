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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LogManagerTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testWaitForAllToComplete() throws ExecutionException, InterruptedException {
        AtomicInteger invokedCount = new AtomicInteger(0);
        Future<Boolean> success = mock(Future.class);
        when(success.get()).thenAnswer(a -> {
            invokedCount.incrementAndGet();
            return true;
        });
        Future<Boolean> failure = mock(Future.class);
        when(failure.get()).thenAnswer(a -> {
            invokedCount.incrementAndGet();
            throw new RuntimeException();
        });

        AtomicInteger failureCount = new AtomicInteger(0);
        // all futures should be evaluated
        assertFalse(LogManager.waitForAllToComplete(List.of(success, failure), t -> failureCount.incrementAndGet()));
        assertEquals(2, invokedCount.get());
        assertEquals(1, failureCount.get());
        assertFalse(LogManager.waitForAllToComplete(List.of(failure, success), t -> failureCount.incrementAndGet()));
        assertEquals(4, invokedCount.get());
        assertEquals(2, failureCount.get());
        assertTrue(LogManager.waitForAllToComplete(List.of(success, success), t -> failureCount.incrementAndGet()));
        assertEquals(6, invokedCount.get());
        assertEquals(2, failureCount.get());
        assertFalse(LogManager.waitForAllToComplete(List.of(failure, failure), t -> failureCount.incrementAndGet()));
        assertEquals(8, invokedCount.get());
        assertEquals(4, failureCount.get());
    }

    @Test
    public void testIsStrayReplica() {
        UnifiedLog log = mock(UnifiedLog.class);
        when(log.topicId()).thenReturn(Optional.of(Uuid.ONE_UUID));
        assertTrue(LogManager.isStrayReplica(List.of(), 0, log));
        assertTrue(LogManager.isStrayReplica(List.of(1, 2, 3), 0, log));
        assertFalse(LogManager.isStrayReplica(List.of(0, 1, 2), 0, log));
    }
}
